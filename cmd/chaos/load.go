package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// jobSpec is what the harness asked for under one idempotency key.
type jobSpec struct {
	Key           string
	DurationMS    int
	FailAttempts  int
	Cancel        bool // cancel after CancelDelay, whatever the state is then
	CancelDelay   time.Duration
	CancelRunning bool // cancel as soon as the harness sees it running
	Dup           int  // extra concurrent POSTs with the same key and body
	Mismatch      bool // one extra POST with the same key and a different body
}

// keyRecord is everything the API told us about one key.
type keyRecord struct {
	mu            sync.Mutex
	ids           map[string]int // job id -> number of 2xx responses carrying it
	created       int            // number of 201 responses
	replayed      int            // number of 200 responses
	mismatch422   int
	mismatchOther []int
	acked         bool
	cancelCode    int
	cancelOut     string
	cancelState   string
	cancelSent    atomic.Bool
}

type cancelResp struct {
	Outcome string `json:"outcome"`
	Job     struct {
		State string `json:"state"`
	} `json:"job"`
}

type loadStats struct {
	posts, dupPosts, mismatchPosts, retries, netErrors, http5xx atomic.Int64
	cancels                                                     atomic.Int64
	latMu                                                       sync.Mutex
	createLat                                                   []time.Duration
}

type loader struct {
	base   string
	client *http.Client
	recs   map[string]*keyRecord
	stats  loadStats
}

func newLoader(base string, specs []jobSpec) *loader {
	l := &loader{
		base: base,
		client: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
			MaxIdleConns: 256, MaxIdleConnsPerHost: 256, IdleConnTimeout: 30 * time.Second}},
		recs: make(map[string]*keyRecord, len(specs)),
	}
	for _, s := range specs {
		l.recs[s.Key] = &keyRecord{ids: map[string]int{}}
	}
	return l
}

func body(s jobSpec, mismatch bool) []byte {
	payload := map[string]any{"duration_ms": s.DurationMS, "fail_attempts": s.FailAttempts}
	if mismatch {
		payload["duration_ms"] = s.DurationMS + 1
	}
	b, _ := json.Marshal(map[string]any{"type": "train", "payload": payload, "max_attempts": maxAttempts})
	return b
}

// post sends one create request, retrying network errors and 5xx with the
// same key exactly like a well-behaved client would (this IS the "duplicate
// on request retry" scenario).
func (l *loader) post(ctx context.Context, s jobSpec, mismatch bool) {
	rec := l.recs[s.Key]
	b := body(s, mismatch)
	for attempt := 0; attempt < 8; attempt++ {
		if attempt > 0 {
			l.stats.retries.Add(1)
			time.Sleep(time.Duration(50*attempt) * time.Millisecond)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, l.base+"/v1/jobs", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", s.Key)
		start := time.Now()
		resp, err := l.client.Do(req)
		if err != nil {
			l.stats.netErrors.Add(1)
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		l.stats.latMu.Lock()
		l.stats.createLat = append(l.stats.createLat, time.Since(start))
		l.stats.latMu.Unlock()
		if resp.StatusCode >= 500 {
			l.stats.http5xx.Add(1)
			continue
		}
		rec.mu.Lock()
		switch {
		case mismatch && resp.StatusCode == http.StatusUnprocessableEntity:
			rec.mismatch422++
		case mismatch:
			rec.mismatchOther = append(rec.mismatchOther, resp.StatusCode)
		case resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK:
			var j struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(raw, &j)
			rec.ids[j.ID]++
			rec.acked = true
			if resp.StatusCode == http.StatusCreated {
				rec.created++
			} else {
				rec.replayed++
			}
		default:
			logf("unexpected create status %d for %s: %s", resp.StatusCode, s.Key, raw)
		}
		rec.mu.Unlock()
		return
	}
	logf("giving up on %s after retries", s.Key)
}

func (l *loader) cancel(ctx context.Context, key string) {
	rec := l.recs[key]
	if !rec.cancelSent.CompareAndSwap(false, true) {
		return
	}
	rec.mu.Lock()
	var id string
	for k := range rec.ids {
		id = k
	}
	rec.mu.Unlock()
	if id == "" {
		return
	}
	for attempt := 0; attempt < 8; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(50*attempt) * time.Millisecond)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, l.base+"/v1/jobs/"+id+"/cancel", nil)
		resp, err := l.client.Do(req)
		if err != nil {
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 500 {
			continue
		}
		var cr cancelResp
		_ = json.Unmarshal(raw, &cr)
		rec.mu.Lock()
		rec.cancelCode, rec.cancelOut, rec.cancelState = resp.StatusCode, cr.Outcome, cr.Job.State
		rec.mu.Unlock()
		l.stats.cancels.Add(1)
		return
	}
}

// run submits every spec with `clients` concurrent senders. Duplicates are
// fired concurrently with the original (racing on the unique index), and
// once more after a random delay; cancels fire after their delay.
func (l *loader) run(ctx context.Context, specs []jobSpec, clients int, rate float64) {
	work := make(chan jobSpec)
	var wg, bg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range work {
				var g sync.WaitGroup
				for d := 0; d <= s.Dup; d++ {
					g.Add(1)
					go func(first bool) {
						defer g.Done()
						l.stats.posts.Add(1)
						if !first {
							l.stats.dupPosts.Add(1)
						}
						l.post(ctx, s, false)
					}(d == 0)
				}
				g.Wait()
				if s.Dup > 0 { // late replay, while workers may be running it
					bg.Add(1)
					go func(s jobSpec) {
						defer bg.Done()
						time.Sleep(time.Duration(rand.N(3000)) * time.Millisecond)
						l.stats.posts.Add(1)
						l.stats.dupPosts.Add(1)
						l.post(ctx, s, false)
					}(s)
				}
				if s.Mismatch {
					l.stats.mismatchPosts.Add(1)
					l.post(ctx, s, true)
				}
				if s.Cancel {
					bg.Add(1)
					go func(s jobSpec) {
						defer bg.Done()
						time.Sleep(s.CancelDelay)
						l.cancel(ctx, s.Key)
					}(s)
				}
			}
		}()
	}
	// rate > 0: open-loop arrivals at a fixed rate (a closed loop would hide
	// queueing delay by slowing down when the server does — coordinated omission).
	var tick <-chan time.Time
	if rate > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / rate))
		defer t.Stop()
		tick = t.C
	}
	for _, s := range specs {
		if tick != nil {
			<-tick
		}
		work <- s
	}
	close(work)
	wg.Wait()
	bg.Wait()
}

func (l *loader) latencyQuantiles() string {
	l.stats.latMu.Lock()
	defer l.stats.latMu.Unlock()
	lat := append([]time.Duration(nil), l.stats.createLat...)
	if len(lat) == 0 {
		return "n/a"
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	q := func(p float64) time.Duration { return lat[min(int(p*float64(len(lat))), len(lat)-1)] }
	return fmt.Sprintf("p50=%s p95=%s p99=%s max=%s (n=%d)",
		q(.5).Round(time.Microsecond*100), q(.95).Round(time.Microsecond*100),
		q(.99).Round(time.Microsecond*100), lat[len(lat)-1].Round(time.Millisecond), len(lat))
}

// cancelRunning targets the cancel/complete race: it watches the database
// for target jobs that are currently running and cancels them right away.
// Targets that finish before being caught are cancelled at the end (409 path).
func (l *loader) cancelRunning(ctx context.Context, pool *pgxpool.Pool, specs []jobSpec, done <-chan struct{}) {
	idToKey := map[string]string{}
	pending := map[string]bool{}
	for _, s := range specs {
		if s.CancelRunning {
			pending[s.Key] = true
		}
	}
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for len(pending) > 0 {
		select {
		case <-ctx.Done():
			return
		case <-done:
			for k := range pending {
				l.cancel(ctx, k)
			}
			return
		case <-t.C:
		}
		ids := make([]string, 0, len(pending))
		for k := range pending {
			rec := l.recs[k]
			rec.mu.Lock()
			for id := range rec.ids {
				ids = append(ids, id)
				idToKey[id] = k
			}
			rec.mu.Unlock()
		}
		if len(ids) == 0 {
			continue
		}
		rows, err := pool.Query(ctx, `SELECT id::text FROM jobs WHERE state = 'running' AND id = ANY($1::uuid[])`, ids)
		if err != nil {
			continue
		}
		var hit []string
		for rows.Next() {
			var id string
			if rows.Scan(&id) == nil {
				hit = append(hit, id)
			}
		}
		rows.Close()
		for _, id := range hit {
			k := idToKey[id]
			delete(pending, k)
			go l.cancel(ctx, k)
		}
	}
}
