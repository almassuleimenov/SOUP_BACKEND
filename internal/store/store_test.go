package store_test

// Integration tests against a real Postgres. Each one targets a silent
// failure: something that would not raise an error if the protocol were wrong.
//
//	TEST_DATABASE_URL=postgres://jobq:jobq@127.0.0.1:55432/jobq_test go test -race ./internal/store/
//
// Tests are skipped when TEST_DATABASE_URL is unset.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jobqueue/internal/migrate"
	"jobqueue/internal/store"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const lease = 5 * time.Second

func pool(t *testing.T, isolation string) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 64
	if isolation != "" {
		cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = isolation
	}
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	ctx := context.Background()
	if err := migrate.Up(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(ctx, `TRUNCATE job_effects, job_runs, job_transitions, jobs RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	return p
}

func createN(t *testing.T, st *store.Store, n int, payload string) []string {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		j, created, err := st.CreateJob(context.Background(), store.CreateParams{
			IdempotencyKey: fmt.Sprintf("k-%d", i), Type: "train", Payload: json.RawMessage(payload), MaxAttempts: 3})
		if err != nil || !created {
			t.Fatalf("create %d: created=%v err=%v", i, created, err)
		}
		ids[i] = j.ID
	}
	return ids
}

func countEffects(t *testing.T, p *pgxpool.Pool, id string) int {
	t.Helper()
	var n int
	if err := p.QueryRow(context.Background(), `SELECT count(*) FROM job_effects WHERE job_id = $1`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func expireLease(t *testing.T, p *pgxpool.Pool, id string) {
	t.Helper()
	if _, err := p.Exec(context.Background(),
		`UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
}

// claimAll lets `workers` goroutines claim until the queue is empty and
// returns how many times each job was claimed plus serialization errors.
func claimAll(t *testing.T, st *store.Store, workers int) (map[string]int, int64) {
	t.Helper()
	var mu sync.Mutex
	claims := map[string]int{}
	var serErr atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			empty := 0
			for empty < 3 {
				c, err := st.Claim(context.Background(), fmt.Sprintf("w%d", w), lease)
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
					serErr.Add(1)
					continue
				}
				if err != nil {
					// The racy negative control trips CHECK constraints; that is loud, not silent.
					continue
				}
				if c == nil {
					empty++
					continue
				}
				empty = 0
				mu.Lock()
				claims[c.JobID]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	return claims, serErr.Load()
}

// Silent failure: double claim. 32 concurrent claimers, 500 jobs: each job
// must be claimed exactly once. Run under each isolation level to show the
// protocol does not depend on SERIALIZABLE and what the stricter levels cost.
func TestClaimExactlyOnceUnderEveryIsolationLevel(t *testing.T) {
	for _, iso := range []string{"read committed", "repeatable read", "serializable"} {
		t.Run(iso, func(t *testing.T) {
			p := pool(t, iso)
			st := store.New(p, store.Options{})
			ids := createN(t, st, 500, `{}`)
			claims, serErr := claimAll(t, st, 32)
			for _, id := range ids {
				if claims[id] != 1 {
					t.Errorf("job %s claimed %d times", id, claims[id])
				}
			}
			t.Logf("isolation=%s: %d jobs claimed exactly once, %d serialization failures (retried)", iso, len(claims), serErr)
		})
	}
}

// Negative control: the racy claim must actually produce double claims,
// otherwise the test above would prove nothing.
func TestRacyClaimIsDetected(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{UnsafeRacyClaim: true})
	createN(t, st, 300, `{}`)
	claims, _ := claimAll(t, st, 32)
	doubles := 0
	for _, n := range claims {
		if n > 1 {
			doubles++
		}
	}
	if doubles == 0 {
		t.Fatal("racy claim produced no double claims: this test cannot distinguish broken from correct")
	}
	t.Logf("racy claim: %d of %d jobs double-claimed", doubles, len(claims))
}

// Silent failure: zombie worker. A worker's lease expires, the job is reaped
// and re-claimed; the old owner then reports success. It must be refused and
// leave no side effect, and the new owner's result must win.
func TestZombieWriteIsFenced(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{})
	ctx := context.Background()
	id := createN(t, st, 1, `{}`)[0]

	old, err := st.Claim(ctx, "zombie", lease)
	if err != nil || old == nil {
		t.Fatal(err)
	}
	expireLease(t, p, id)
	if n, err := st.ReapExpired(ctx, 10); err != nil || n != 1 {
		t.Fatalf("reap: n=%d err=%v", n, err)
	}
	if hb, err := st.Heartbeat(ctx, id, old.Fence, lease); err != nil || hb.Alive {
		t.Fatalf("zombie heartbeat must fail: %+v %v", hb, err)
	}
	cur, err := st.Claim(ctx, "successor", lease)
	if err != nil || cur == nil || cur.Fence <= old.Fence {
		t.Fatalf("successor claim: %+v %v", cur, err)
	}

	// Zombie wakes up while the successor is still running.
	res, err := st.Finish(ctx, store.FinishParams{JobID: id, Fence: old.Fence, WorkerID: "zombie",
		Outcome: store.OutcomeSuccess, Result: json.RawMessage(`{"by":"zombie"}`), EffectInTx: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied || countEffects(t, p, id) != 0 {
		t.Fatalf("zombie write accepted: %+v effects=%d", res, countEffects(t, p, id))
	}
	var fenceOnly int
	if err := p.QueryRow(ctx, `SELECT fence_only_rejections FROM job_runs WHERE fence = $1`, old.Fence).Scan(&fenceOnly); err != nil || fenceOnly != 1 {
		t.Fatalf("expected the rejection to be attributed to fencing: %d %v", fenceOnly, err)
	}

	res, err = st.Finish(ctx, store.FinishParams{JobID: id, Fence: cur.Fence, WorkerID: "successor",
		Outcome: store.OutcomeSuccess, Result: json.RawMessage(`{"by":"successor"}`), EffectInTx: true})
	if err != nil || !res.Applied || res.State != store.StateSucceeded {
		t.Fatalf("successor finish: %+v %v", res, err)
	}
	j, _ := st.GetJob(ctx, id)
	if string(j.Result) != `{"by": "successor"}` || countEffects(t, p, id) != 1 {
		t.Fatalf("wrong winner: result=%s effects=%d", j.Result, countEffects(t, p, id))
	}

	// Negative control: without fencing the same sequence corrupts the result.
	p2 := pool(t, "")
	bad := store.New(p2, store.Options{UnsafeNoFence: true})
	id = createN(t, bad, 1, `{}`)[0]
	old, _ = bad.Claim(ctx, "zombie", lease)
	expireLease(t, p2, id)
	_, _ = bad.ReapExpired(ctx, 10)
	_, _ = bad.Claim(ctx, "successor", lease)
	res, _ = bad.Finish(ctx, store.FinishParams{JobID: id, Fence: old.Fence, WorkerID: "zombie",
		Outcome: store.OutcomeSuccess, EffectInTx: true})
	if !res.Applied {
		t.Fatal("negative control: unfenced zombie write should have been (wrongly) accepted")
	}
}

// Silent failure: cancel/complete race. Run both operations concurrently many
// times; the API's answer must always agree with the final state, and an
// acknowledged cancel must never end with a side effect.
func TestCancelCompleteRace(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{})
	ctx := context.Background()
	const n = 300
	ids := createN(t, st, n, `{}`)
	claimed := map[string]*store.Claimed{}
	for range ids {
		c, err := st.Claim(ctx, "w", lease)
		if err != nil || c == nil {
			t.Fatal(err)
		}
		claimed[c.JobID] = c
	}
	var cancelWon, completeWon atomic.Int64
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			var outcome store.CancelOutcome
			var cerr error
			var inner sync.WaitGroup
			inner.Add(2)
			go func() { defer inner.Done(); _, outcome, cerr = st.CancelJob(ctx, id) }()
			go func() {
				defer inner.Done()
				_, _ = st.Finish(ctx, store.FinishParams{JobID: id, Fence: claimed[id].Fence, WorkerID: "w",
					Outcome: store.OutcomeSuccess, Result: json.RawMessage(`{}`), EffectInTx: true})
			}()
			inner.Wait()
			if cerr != nil {
				t.Errorf("cancel %s: %v", id, cerr)
				return
			}
			j, _ := st.GetJob(ctx, id)
			eff := countEffects(t, p, id)
			switch outcome {
			case store.CancelRequested:
				// Cancel landed while running. Worker's finish saw the flag.
				cancelWon.Add(1)
				if j.State != store.StateCancelled || eff != 0 {
					t.Errorf("job %s: cancel acknowledged but state=%s effects=%d", id, j.State, eff)
				}
			case store.AlreadyTerminal:
				completeWon.Add(1)
				if j.State != store.StateSucceeded || eff != 1 {
					t.Errorf("job %s: completion won but state=%s effects=%d", id, j.State, eff)
				}
			default:
				t.Errorf("job %s: unexpected cancel outcome %d", id, outcome)
			}
		}(id)
	}
	wg.Wait()
	t.Logf("cancel won %d, completion won %d", cancelWon.Load(), completeWon.Load())
	if cancelWon.Load() == 0 || completeWon.Load() == 0 {
		t.Log("WARNING: only one side of the race was observed; rerun or raise n")
	}
}

// Silent failure: duplicate on request retry. 50 concurrent creates with the
// same key: exactly one creates, all see the same id. A different body with
// the same key is rejected instead of silently returning the old job.
func TestIdempotentCreateConcurrent(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{})
	ctx := context.Background()
	var created atomic.Int64
	ids := sync.Map{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, c, err := st.CreateJob(ctx, store.CreateParams{IdempotencyKey: "same", Type: "train",
				Payload: json.RawMessage(`{"a":1}`), MaxAttempts: 3})
			if err != nil {
				t.Error(err)
				return
			}
			if c {
				created.Add(1)
			}
			ids.Store(j.ID, true)
		}()
	}
	wg.Wait()
	distinct := 0
	ids.Range(func(_, _ any) bool { distinct++; return true })
	if created.Load() != 1 || distinct != 1 {
		t.Fatalf("created=%d distinct ids=%d", created.Load(), distinct)
	}
	_, _, err := st.CreateJob(ctx, store.CreateParams{IdempotencyKey: "same", Type: "train",
		Payload: json.RawMessage(`{"a":2}`), MaxAttempts: 3})
	if !errors.Is(err, store.ErrIdempotencyMismatch) {
		t.Fatalf("expected mismatch, got %v", err)
	}
	var rows int
	_ = p.QueryRow(ctx, `SELECT count(*) FROM job_transitions`).Scan(&rows)
	if rows != 1 {
		t.Fatalf("duplicate inserts left %d audit rows, want 1 (BEFORE-trigger phantom?)", rows)
	}
}

// The state machine is enforced by the database, not just by Go code.
func TestTerminalStateIsImmutable(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{})
	ctx := context.Background()
	id := createN(t, st, 1, `{}`)[0]
	c, _ := st.Claim(ctx, "w", lease)
	if _, err := st.Finish(ctx, store.FinishParams{JobID: id, Fence: c.Fence, WorkerID: "w",
		Outcome: store.OutcomeSuccess, EffectInTx: true}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`UPDATE jobs SET state = 'queued', finished_at = NULL WHERE id = $1`,
		`UPDATE jobs SET result = '{"tampered":true}' WHERE id = $1`,
	} {
		if _, err := p.Exec(ctx, q, id); err == nil {
			t.Errorf("expected trigger to reject: %s", q)
		}
	}
	if _, err := p.Exec(ctx, `INSERT INTO jobs (idempotency_key, request_hash, job_type, payload, max_attempts, state)
		VALUES ('x', '\x00', 't', '{}', 1, 'running')`); err == nil {
		t.Error("expected insert in non-queued state to be rejected")
	}
}

// A job whose worker died after a cancel request must end cancelled (not
// requeued, not failed) when the reaper finds it.
func TestReaperHonoursCancel(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{})
	ctx := context.Background()
	id := createN(t, st, 1, `{}`)[0]
	if c, _ := st.Claim(ctx, "w", lease); c == nil {
		t.Fatal("no claim")
	}
	if _, out, _ := st.CancelJob(ctx, id); out != store.CancelRequested {
		t.Fatalf("outcome %d", out)
	}
	expireLease(t, p, id)
	if n, _ := st.ReapExpired(ctx, 10); n != 1 {
		t.Fatal("not reaped")
	}
	if j, _ := st.GetJob(ctx, id); j.State != store.StateCancelled {
		t.Fatalf("state %s", j.State)
	}
}

// crash simulates kill -9 of the current owner: expire its lease and reap.
func crash(t *testing.T, p *pgxpool.Pool, st *store.Store, id string) {
	t.Helper()
	expireLease(t, p, id)
	if n, err := st.ReapExpired(context.Background(), 10); err != nil || n != 1 {
		t.Fatalf("reap: n=%d err=%v", n, err)
	}
}

// Failures exhaust max_attempts; a crash in between is not a failure.
func TestFailuresExhaustToFailed(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{})
	ctx := context.Background()
	id := createN(t, st, 1, `{}`)[0] // max_attempts = 3
	steps := []string{"fail", "crash", "fail", "fail"}
	for i, step := range steps {
		c, err := st.Claim(ctx, "w", lease)
		if err != nil || c == nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if step == "crash" {
			crash(t, p, st, id)
			continue
		}
		if _, err := st.Finish(ctx, store.FinishParams{JobID: id, Fence: c.Fence, WorkerID: "w",
			Outcome: store.OutcomeFailure, Error: "boom"}); err != nil {
			t.Fatal(err)
		}
	}
	j, _ := st.GetJob(ctx, id)
	if j.State != store.StateFailed || j.Attempts != 4 || j.Failures != 3 || j.Crashes != 1 {
		t.Fatalf("state=%s attempts=%d failures=%d crashes=%d", j.State, j.Attempts, j.Failures, j.Crashes)
	}
}

// Review finding: worker deaths used to consume max_attempts, so a few
// deploys in a row killed a healthy job. Now 4 crashes (more than
// max_attempts=3) followed by a clean run must still succeed.
func TestCrashesDoNotSpendAttempts(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{})
	ctx := context.Background()
	id := createN(t, st, 1, `{}`)[0] // max_attempts = 3, max_crashes = 5
	for i := 0; i < 4; i++ {
		if c, err := st.Claim(ctx, "w", lease); err != nil || c == nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		crash(t, p, st, id)
	}
	c, _ := st.Claim(ctx, "w", lease)
	res, err := st.Finish(ctx, store.FinishParams{JobID: id, Fence: c.Fence, WorkerID: "w",
		Outcome: store.OutcomeSuccess, EffectInTx: true})
	if err != nil || res.State != store.StateSucceeded {
		t.Fatalf("finish after 4 crashes: %+v %v", res, err)
	}
	j, _ := st.GetJob(ctx, id)
	if j.Crashes != 4 || j.Failures != 0 || j.Attempts != 5 {
		t.Fatalf("crashes=%d failures=%d attempts=%d", j.Crashes, j.Failures, j.Attempts)
	}
}

// A poison job that kills every worker that takes it is still bounded.
func TestPoisonJobBoundedByCrashBudget(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{})
	ctx := context.Background()
	id := createN(t, st, 1, `{}`)[0] // max_crashes = 5
	for i := 0; i < 5; i++ {
		if c, err := st.Claim(ctx, "w", lease); err != nil || c == nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		crash(t, p, st, id)
	}
	j, _ := st.GetJob(ctx, id)
	if j.State != store.StateFailed || j.Crashes != 5 {
		t.Fatalf("state=%s crashes=%d", j.State, j.Crashes)
	}
	if c, _ := st.Claim(ctx, "w", lease); c != nil {
		t.Fatal("poison job claimed again after its crash budget was exhausted")
	}
}

// Silent failure: ambiguous commit. The worker's Finish committed but the
// reply was lost; the worker retries the same call. The retry must not write
// a second effect, must report success, and must not be counted as a zombie.
func TestFinishRetryAfterAmbiguousCommit(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{})
	ctx := context.Background()
	id := createN(t, st, 1, `{}`)[0]
	c, _ := st.Claim(ctx, "w", lease)
	fp := store.FinishParams{JobID: id, Fence: c.Fence, WorkerID: "w", Outcome: store.OutcomeSuccess,
		Result: json.RawMessage(`{}`), EffectInTx: true}
	for i := 0; i < 3; i++ {
		res, err := st.Finish(ctx, fp)
		if err != nil || !res.Applied || res.State != store.StateSucceeded {
			t.Fatalf("call %d: %+v %v", i, res, err)
		}
	}
	var late int
	_ = p.QueryRow(ctx, `SELECT late_write_rejected FROM job_runs WHERE fence = $1`, c.Fence).Scan(&late)
	if n := countEffects(t, p, id); n != 1 || late != 0 {
		t.Fatalf("effects=%d late_write_rejected=%d", n, late)
	}
}

// Found by the harness (FINAL2 main run, budgets_match_runs): a worker whose
// heartbeat timed out self-fences and closes its run as 'superseded' BEFORE
// the reaper sees the expired lease. The reaper then counted a crash but left
// the run labelled 'superseded', so jobs.crashes disagreed with job_runs.
func TestSelfFencedRunIsLabelledByReaper(t *testing.T) {
	p := pool(t, "")
	st := store.New(p, store.Options{})
	ctx := context.Background()
	id := createN(t, st, 1, `{}`)[0]
	c, err := st.Claim(ctx, "w", lease)
	if err != nil || c == nil {
		t.Fatalf("claim: %v", err)
	}
	if err := st.CloseRunIfOpen(ctx, c.Fence); err != nil { // worker gives up first
		t.Fatal(err)
	}
	crash(t, p, st, id) // then the lease expires and is reaped
	var outcome string
	if err := p.QueryRow(ctx, `SELECT outcome FROM job_runs WHERE fence = $1`, c.Fence).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	j, _ := st.GetJob(ctx, id)
	if outcome != "lease_expired" || j.Crashes != 1 || j.State != store.StateQueued {
		t.Fatalf("run outcome=%q crashes=%d state=%s, want lease_expired/1/queued", outcome, j.Crashes, j.State)
	}
}
