// Command chaos is the fault-injection harness.
//
// It starts the API and N worker processes against a dedicated database,
// submits jobs (with concurrent duplicate POSTs, mismatched replays and
// cancellations), kill -9's a random worker about every 2 seconds (and on
// Unix also SIGSTOPs one so it comes back as a zombie after its lease
// expired), waits for the queue to drain, and then checks invariants
// directly in the database. Exit code 0 = all invariants held.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"jobqueue/internal/migrate"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxAttempts = 5

var (
	logMu  sync.Mutex
	logOut io.Writer = os.Stdout
	t0               = time.Now()
)

func logf(format string, a ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	fmt.Fprintf(logOut, "[%7.2fs] %s\n", time.Since(t0).Seconds(), fmt.Sprintf(format, a...))
}

type opts struct {
	dbURL        string
	binDir       string
	outDir       string
	label        string
	jobs         int
	workers      int
	clients      int
	killEvery    time.Duration
	pauseProb    float64
	dupRate      float64
	mismatchRate float64
	cancelRate   float64
	failRate     float64
	minMS, maxMS int
	longRate     float64
	longMS       int
	freezeProb   float64
	lease        time.Duration
	heartbeat    time.Duration
	effectMode   string
	unsafeMode   string
	drainTimeout time.Duration
	port         int
	rate         float64
	allowAnyDB   bool
}

func main() {
	var o opts
	flag.StringVar(&o.dbURL, "db", os.Getenv("DATABASE_URL"), "database URL (database name must contain 'chaos')")
	flag.StringVar(&o.binDir, "bin", "", "directory with server and worker binaries (default: next to this binary)")
	flag.StringVar(&o.outDir, "out", "results", "directory for run artifacts")
	flag.StringVar(&o.label, "label", "", "label appended to the run directory name")
	flag.IntVar(&o.jobs, "jobs", 10000, "number of distinct jobs (idempotency keys)")
	flag.IntVar(&o.workers, "workers", 8, "worker processes")
	flag.IntVar(&o.clients, "clients", 32, "concurrent API clients")
	flag.DurationVar(&o.killEvery, "kill-every", 2*time.Second, "mean interval between kill -9 (uniform 0.5x..1.5x)")
	flag.Float64Var(&o.pauseProb, "pause-prob", 0.25, "per chaos tick, probability to also SIGSTOP a worker for 2x lease (Unix only)")
	flag.Float64Var(&o.dupRate, "dup-rate", 0.3, "fraction of keys POSTed again concurrently + replayed later")
	flag.Float64Var(&o.mismatchRate, "mismatch-rate", 0.02, "fraction of keys replayed with a different body (expect 422)")
	flag.Float64Var(&o.cancelRate, "cancel-rate", 0.1, "fraction of jobs cancelled at a random moment")
	flag.Float64Var(&o.failRate, "fail-rate", 0.05, "fraction of jobs that fail some attempts")
	flag.IntVar(&o.minMS, "min-ms", 10, "min simulated job duration")
	flag.IntVar(&o.maxMS, "max-ms", 150, "max simulated job duration")
	flag.Float64Var(&o.longRate, "long-rate", 0.05, "fraction of long jobs (training-like; widens the zombie/kill window)")
	flag.IntVar(&o.longMS, "long-ms", 4000, "max duration of a long job (uniform 1s..long-ms)")
	flag.Float64Var(&o.freezeProb, "freeze-prob", 0.02, "per attempt, probability the worker freezes for 2x lease right before finishing")
	flag.DurationVar(&o.lease, "lease", 2*time.Second, "lease length")
	flag.DurationVar(&o.heartbeat, "heartbeat", 500*time.Millisecond, "heartbeat period")
	flag.StringVar(&o.effectMode, "effect-mode", "tx", "tx | external")
	flag.StringVar(&o.unsafeMode, "unsafe", "", "negative control: no-fence | racy-claim")
	flag.DurationVar(&o.drainTimeout, "drain-timeout", 10*time.Minute, "max time to wait for the queue to drain")
	flag.IntVar(&o.port, "port", 18080, "API port")
	flag.Float64Var(&o.rate, "rate", 0, "if > 0, submit new jobs open-loop at this many per second (load test); 0 = as fast as possible")
	flag.BoolVar(&o.allowAnyDB, "allow-any-db", false, "allow a database whose name lacks 'chaos' (it will be TRUNCATED)")
	flag.Parse()

	code, err := run(o)
	if err != nil {
		logf("HARNESS ERROR: %v", err)
		os.Exit(2)
	}
	os.Exit(code)
}

func run(o opts) (int, error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// ---- artifacts ---------------------------------------------------------
	runID := time.Now().UTC().Format("20060102T150405Z")
	name := runID
	if o.label != "" {
		name += "-" + o.label
	}
	dir := filepath.Join(o.outDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	hl, err := os.Create(filepath.Join(dir, "harness.log"))
	if err != nil {
		return 0, err
	}
	defer hl.Close()
	logOut = io.MultiWriter(os.Stdout, hl)
	wlog, err := os.Create(filepath.Join(dir, "workers.log"))
	if err != nil {
		return 0, err
	}
	defer wlog.Close()
	alog, err := os.Create(filepath.Join(dir, "api.log"))
	if err != nil {
		return 0, err
	}
	defer alog.Close()

	logf("run %s  os=%s  args=%s", name, runtime.GOOS, strings.Join(os.Args[1:], " "))
	logf("config jobs=%d workers=%d kill-every=%s pause-prob=%.2f(pause supported=%v) dup=%.2f mismatch=%.2f cancel=%.2f fail=%.2f long=%.2f/%dms freeze=%.4f lease=%s hb=%s effect=%s unsafe=%q",
		o.jobs, o.workers, o.killEvery, o.pauseProb, canPause, o.dupRate, o.mismatchRate, o.cancelRate, o.failRate,
		o.longRate, o.longMS, o.freezeProb, o.lease, o.heartbeat, o.effectMode, o.unsafeMode)

	// ---- database ----------------------------------------------------------
	cfg, err := pgx.ParseConfig(o.dbURL)
	if err != nil {
		return 0, fmt.Errorf("parse -db: %w", err)
	}
	if !o.allowAnyDB && !strings.Contains(cfg.Database, "chaos") {
		return 0, fmt.Errorf("refusing to TRUNCATE database %q: name must contain 'chaos' (or pass -allow-any-db)", cfg.Database)
	}
	pool, err := pgxpool.New(ctx, o.dbURL)
	if err != nil {
		return 0, err
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		return 0, err
	}
	if _, err := pool.Exec(ctx, `TRUNCATE job_effects, job_runs, job_transitions, jobs RESTART IDENTITY CASCADE;
		ALTER SEQUENCE job_fence_seq RESTART`); err != nil {
		return 0, fmt.Errorf("reset: %w", err)
	}

	// ---- processes ---------------------------------------------------------
	binDir := o.binDir
	if binDir == "" {
		exe, _ := os.Executable()
		binDir = filepath.Dir(exe)
	}
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	baseEnv := []string{"DATABASE_URL=" + o.dbURL, "PATH=" + os.Getenv("PATH"), "SYSTEMROOT=" + os.Getenv("SYSTEMROOT")}
	addr := fmt.Sprintf("127.0.0.1:%d", o.port)
	apiProc, err := startProc(filepath.Join(binDir, "server"+ext), "api",
		append(append([]string{}, baseEnv...), "HTTP_ADDR="+addr, "DB_MAX_CONNS=40"), alog)
	if err != nil {
		return 0, err
	}
	defer func() { _ = apiProc.cmd.Process.Kill(); <-apiProc.done }()
	base := "http://" + addr
	if err := waitReady(ctx, base+"/readyz", 60*time.Second); err != nil {
		return 0, err
	}

	wenv := append(append([]string{}, baseEnv...),
		"LEASE="+o.lease.String(), "HEARTBEAT="+o.heartbeat.String(),
		"REAP_EVERY=500ms", "RETRY_BASE=100ms", "RETRY_MAX=1s",
		fmt.Sprintf("CHAOS_FREEZE_PROB=%g", o.freezeProb), "CHAOS_FREEZE_FOR="+(2*o.lease+time.Second).String(), /* => [lease+0.5s, 2*lease+1s) */
		"EFFECT_MODE="+o.effectMode, "UNSAFE_MODE="+o.unsafeMode)
	// worker-chaos = the worker built with -tags chaos (fault injection compiled in).
	fl := newFleet(filepath.Join(binDir, "worker-chaos"+ext), wenv, wlog, o.workers)
	if err := fl.fill(); err != nil {
		return 0, err
	}
	defer fl.stopAll()

	// ---- workload ----------------------------------------------------------
	specs := make([]jobSpec, o.jobs)
	for i := range specs {
		s := jobSpec{Key: fmt.Sprintf("%s-%06d", runID, i), DurationMS: o.minMS + rand.N(o.maxMS-o.minMS+1)}
		if rand.Float64() < o.longRate {
			s.DurationMS = 1000 + rand.N(max(o.longMS-1000, 1))
		}
		if rand.Float64() < o.failRate {
			s.FailAttempts = 1 + rand.N(maxAttempts) // 1..max: some recover, some exhaust
		}
		if rand.Float64() < o.dupRate {
			s.Dup = 1 + rand.N(3)
		}
		s.Mismatch = rand.Float64() < o.mismatchRate
		if r := rand.Float64(); r < o.cancelRate/2 {
			s.Cancel = true
			s.CancelDelay = time.Duration(rand.N(int64(3 * time.Second)))
		} else if r < o.cancelRate {
			s.CancelRunning = true
		}
		specs[i] = s
	}
	ld := newLoader(base, specs)

	// ---- chaos loop --------------------------------------------------------
	chaosCtx, stopChaos := context.WithCancel(ctx)
	var chaosWG sync.WaitGroup
	chaosWG.Add(1)
	go func() {
		defer chaosWG.Done()
		if o.killEvery <= 0 {
			logf("chaos disabled (-kill-every 0)")
			return
		}
		for {
			d := time.Duration(float64(o.killEvery) * (0.5 + rand.Float64()))
			select {
			case <-chaosCtx.Done():
				return
			case <-time.After(d):
			}
			if id := fl.killRandom(); id != "" {
				logf("chaos: kill -9 %s (kills=%d)", id, fl.kills.Load())
			}
			if canPause && rand.Float64() < o.pauseProb {
				if id := fl.pauseRandom(2 * o.lease); id != "" {
					logf("chaos: SIGSTOP %s for %s (stops=%d)", id, 2*o.lease, fl.stops.Load())
				}
			}
		}
	}()

	// ---- progress ----------------------------------------------------------
	var loadDone atomic.Bool
	progCtx, stopProg := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-progCtx.Done():
				return
			case <-t.C:
			}
			logf("progress posts=%d cancels=%d load_done=%v %s kills=%d", ld.stats.posts.Load(),
				ld.stats.cancels.Load(), loadDone.Load(), stateLine(ctx, pool), fl.kills.Load())
		}
	}()

	loadStart := time.Now()
	drainDone := make(chan struct{})
	var crWG sync.WaitGroup
	crWG.Add(1)
	go func() { defer crWG.Done(); ld.cancelRunning(ctx, pool, specs, drainDone) }()
	ld.run(ctx, specs, o.clients, o.rate)
	loadDone.Store(true)
	logf("load finished in %s: posts=%d dup_posts=%d mismatch_posts=%d cancels=%d client_retries=%d net_errors=%d 5xx=%d",
		time.Since(loadStart).Round(time.Millisecond), ld.stats.posts.Load(), ld.stats.dupPosts.Load(),
		ld.stats.mismatchPosts.Load(), ld.stats.cancels.Load(), ld.stats.retries.Load(),
		ld.stats.netErrors.Load(), ld.stats.http5xx.Load())

	// ---- drain (chaos keeps running) --------------------------------------
	drained := waitDrain(ctx, pool, o.drainTimeout)
	elapsed := time.Since(loadStart)
	close(drainDone) // leftover running-targets get cancelled now (=> 409 path)
	crWG.Wait()
	stopChaos()
	chaosWG.Wait()
	stopProg()
	if drained {
		logf("queue drained %s after load start", elapsed.Round(time.Millisecond))
	} else {
		logf("QUEUE DID NOT DRAIN within %s: %s", o.drainTimeout, stateLine(ctx, pool))
	}
	fl.stopAll()

	// ---- verify ------------------------------------------------------------
	checks, err := verify(ctx, pool, specs, ld)
	if err != nil {
		return 0, err
	}
	ev, err := evidence(ctx, pool)
	if err != nil {
		return 0, err
	}
	ev["harness"] = map[string]int64{
		"kills_sigkill": fl.kills.Load(), "pauses_sigstop": fl.stops.Load(), "respawns": fl.respawns.Load(),
		"unexpected_worker_exits": fl.crashes.Load(), "posts": ld.stats.posts.Load(), "dup_posts": ld.stats.dupPosts.Load(),
		"mismatch_posts": ld.stats.mismatchPosts.Load(), "cancel_requests": ld.stats.cancels.Load(),
		"client_retries": ld.stats.retries.Load(), "net_errors": ld.stats.netErrors.Load(), "http_5xx": ld.stats.http5xx.Load(),
	}
	cancelCodes := map[string]int64{}
	for _, r := range ld.recs {
		if r.cancelCode != 0 {
			cancelCodes[fmt.Sprintf("%d_%s", r.cancelCode, r.cancelOut)]++
		}
	}
	ev["cancel_responses"] = cancelCodes

	logf("==================== RESULT ====================")
	logf("jobs=%d workers=%d wall=%s throughput=%.1f jobs/s  create latency %s",
		o.jobs, o.workers, elapsed.Round(time.Millisecond), float64(o.jobs)/elapsed.Seconds(), ld.latencyQuantiles())
	logf("end-to-end (DB clock): %s", e2eLine(ctx, pool))
	logf("evidence (proves the failure paths were exercised):\n%s", formatEvidence(ev))
	failed := 0
	for _, c := range checks {
		status := "PASS"
		if c.Violations > 0 {
			status = "FAIL"
			failed++
		}
		logf("%s  %-95s violations=%d", status, c.Name, c.Violations)
		for _, e := range c.Examples {
			logf("        e.g. %s", e)
		}
	}
	if !drained {
		failed++
		logf("FAIL  %-95s", "queue_drained (all jobs reached a terminal state before the drain timeout)")
	}
	gaps := evidenceGaps(ev, o)
	for _, g := range gaps {
		logf("EVIDENCE GAP  %s", g)
	}
	verdict := "ALL INVARIANTS HELD"
	switch {
	case failed > 0:
		verdict = fmt.Sprintf("%d INVARIANT(S) VIOLATED", failed)
	case len(gaps) > 0:
		verdict = fmt.Sprintf("INVARIANTS HELD BUT %d FAILURE PATH(S) NOT EXERCISED — NOT PROOF", len(gaps))
	}
	logf("VERDICT: %s", verdict)

	summary := map[string]any{
		"run": name, "os": runtime.GOOS, "args": os.Args[1:], "drained": drained,
		"wall_seconds": elapsed.Seconds(), "checks": checks, "evidence": ev, "verdict": verdict,
	}
	if b, err := json.MarshalIndent(summary, "", "  "); err == nil {
		_ = os.WriteFile(filepath.Join(dir, "summary.json"), b, 0o644)
	}
	logf("artifacts in %s", dir)
	if failed > 0 {
		return 1, nil
	}
	if len(gaps) > 0 {
		return 3, nil
	}
	return 0, nil
}

// evidenceGaps lists failure paths the run was supposed to exercise but did
// not. A green run that never reaped a lease or never fenced a zombie proves
// nothing about those mechanisms.
func evidenceGaps(ev map[string]any, o opts) []string {
	get := func(group, key string) int64 {
		m, _ := ev[group].(map[string]int64)
		return m[key]
	}
	type req struct {
		on          bool
		group, key  string
		description string
	}
	reqs := []req{
		{o.killEvery > 0, "harness", "kills_sigkill", "no worker was killed"},
		{o.killEvery > 0, "run_outcomes", "lease_expired", "no dead worker was detected via lease expiry"},
		{o.freezeProb > 0 || canPause, "zombies", "late_writes_rejected_total", "no zombie write happened"},
		{o.freezeProb > 0 || canPause, "zombies", "fence_only_rejections", "no zombie wrote while a newer owner was running (fencing never decisive)"},
		{o.dupRate > 0, "harness", "dup_posts", "no duplicate POSTs"},
		{o.cancelRate > 0, "cancel_paths", "cancelled_while_queued", "no cancel of a queued job"},
		{o.cancelRate > 0, "cancel_paths", "cancelled_by_worker_while_running", "no cancel of a running job"},
		{o.failRate > 0, "run_outcomes", "retry", "no retried failure"},
		{o.failRate > 0, "final_states", "failed", "no permanently failed job"},
	}
	var gaps []string
	for _, r := range reqs {
		if r.on && get(r.group, r.key) == 0 {
			gaps = append(gaps, fmt.Sprintf("%s (%s.%s = 0)", r.description, r.group, r.key))
		}
	}
	return gaps
}

func waitReady(ctx context.Context, url string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("API not ready at %s after %s", url, d)
}

func stateLine(ctx context.Context, pool *pgxpool.Pool) string {
	rows, err := pool.Query(ctx, `SELECT state::text, count(*) FROM jobs GROUP BY 1 ORDER BY 1`)
	if err != nil {
		return "states=?"
	}
	defer rows.Close()
	var parts []string
	for rows.Next() {
		var s string
		var n int64
		if rows.Scan(&s, &n) == nil {
			parts = append(parts, fmt.Sprintf("%s=%d", s, n))
		}
	}
	return strings.Join(parts, " ")
}

func waitDrain(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state IN ('queued','running')`).Scan(&n); err == nil && n == 0 {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(250 * time.Millisecond):
		}
	}
	return false
}

// e2eLine reports queue wait (created -> first claim) and end-to-end time
// (created -> succeeded) for succeeded jobs, from the database clock.
func e2eLine(ctx context.Context, pool *pgxpool.Pool) string {
	var w50, w99, e50, e99, emax float64
	err := pool.QueryRow(ctx, `SELECT
		percentile_cont(0.5)  WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM started_at - created_at)),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM started_at - created_at)),
		percentile_cont(0.5)  WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM finished_at - created_at)),
		percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM finished_at - created_at)),
		max(EXTRACT(EPOCH FROM finished_at - created_at))
		FROM jobs WHERE state = 'succeeded'`).Scan(&w50, &w99, &e50, &e99, &emax)
	if err != nil {
		return "n/a: " + err.Error()
	}
	return fmt.Sprintf("queue_wait p50=%.3fs p99=%.3fs | created->succeeded p50=%.3fs p99=%.3fs max=%.3fs", w50, w99, e50, e99, emax)
}
