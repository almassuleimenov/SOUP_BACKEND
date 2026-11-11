package worker

// Integration test (needs Postgres):
//
//	TEST_DATABASE_URL=postgres://jobq:jobq@127.0.0.1:55432/jobq_test go test -race ./internal/worker/

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"jobqueue/internal/migrate"
	"jobqueue/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// Regression: the reaper used to run only between jobs. With every worker
// busy on a long job, a dead worker's job stayed "running" with an expired
// lease until some worker finished (an independent review measured ~21 s
// with 25 s jobs). The dead job must be reaped within lease + reap interval
// + margin while the only live worker is still busy.
func TestDeadWorkerReapedWhileAllWorkersBusy(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE job_effects, job_runs, job_transitions, jobs RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	st := store.New(pool, store.Options{})

	const (
		lease     = 1 * time.Second
		reapEvery = 200 * time.Millisecond
		longJob   = 8 * time.Second
	)
	mk := func(key string, ms int) string {
		p, _ := json.Marshal(Payload{DurationMS: ms})
		j, _, err := st.CreateJob(ctx, store.CreateParams{IdempotencyKey: key, Type: "train", Payload: p, MaxAttempts: 3})
		if err != nil {
			t.Fatal(err)
		}
		return j.ID
	}
	longID := mk("long", int(longJob/time.Millisecond))

	// The only live worker, which will be busy with the long job.
	w := New(Config{
		ID: "live", Lease: lease, Heartbeat: lease / 4, PollMin: 10 * time.Millisecond, PollMax: 50 * time.Millisecond,
		ReapEvery: reapEvery, RetryBase: 100 * time.Millisecond, RetryMax: time.Second,
		EffectMode: EffectInTx, FinishTimeout: 5 * time.Second,
	}, st, slog.New(slog.NewTextHandler(tWriter{t}, nil)), NewMetrics(prometheus.NewRegistry()))
	wctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(wctx) }()
	defer func() { stop(); <-done }()

	waitState(t, st, longID, store.StateRunning, 5*time.Second)

	// A "dead" worker claims the second job and never heartbeats (= kill -9).
	deadID := mk("dead", 10)
	c, err := st.Claim(ctx, "dead-worker", lease)
	if err != nil || c == nil || c.JobID != deadID {
		t.Fatalf("dead claim: %+v %v", c, err)
	}
	claimedAt := time.Now()

	// Deadline: lease + reap interval + generous margin, still well before the
	// live worker finishes its long job (8 s).
	deadline := lease + reapEvery + 1500*time.Millisecond
	for {
		var outcome *string
		if err := pool.QueryRow(ctx, `SELECT outcome FROM job_runs WHERE fence = $1`, c.Fence).Scan(&outcome); err != nil {
			t.Fatal(err)
		}
		if outcome != nil && *outcome == "lease_expired" {
			break
		}
		if time.Since(claimedAt) > deadline {
			t.Fatalf("dead worker's job not reaped within %s while the live worker was busy", deadline)
		}
		time.Sleep(50 * time.Millisecond)
	}
	reapedAfter := time.Since(claimedAt)
	if j, _ := st.GetJob(ctx, longID); j.State != store.StateRunning {
		t.Fatalf("test invalid: long job no longer running (%s), the worker was not busy", j.State)
	}
	t.Logf("dead job reaped %s after claim (lease %s) while the only live worker was busy", reapedAfter.Round(time.Millisecond), lease)
}

// tWriter routes worker logs into the test log: shown only if the test fails.
type tWriter struct{ t *testing.T }

func (w tWriter) Write(b []byte) (int, error) { w.t.Log(string(b)); return len(b), nil }

func waitState(t *testing.T, st *store.Store, id string, want store.State, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if j, err := st.GetJob(context.Background(), id); err == nil && j.State == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s within %s", id, want, d)
}
