// Package worker runs the claim → execute → finish loop for one worker.
//
// One OS process = one worker = at most one job at a time. That makes
// "kill -9 a worker" mean exactly one thing in the chaos harness.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"jobqueue/internal/store"
)

// EffectMode selects where the emulated side effect is written.
type EffectMode string

// Effect modes.
const (
	// EffectInTx writes the effect in the same transaction as the fenced
	// completion: effect and "succeeded" commit together or not at all.
	EffectInTx EffectMode = "tx"
	// EffectExternal writes the effect in its own transaction before the
	// completion, like a call to an external system. At-least-once.
	EffectExternal EffectMode = "external"
)

// Config tunes a worker.
type Config struct {
	ID            string
	Lease         time.Duration // lease length granted per claim / heartbeat
	Heartbeat     time.Duration // heartbeat period, must be well below Lease
	PollMin       time.Duration // idle poll backoff bounds
	PollMax       time.Duration
	ReapEvery     time.Duration
	RetryBase     time.Duration // exponential backoff base for failed attempts
	RetryMax      time.Duration
	EffectMode    EffectMode
	FreezeProb    float64       // chaos: probability to freeze before finishing
	FreezeFor     time.Duration // chaos: freeze length (should exceed Lease)
	FinishTimeout time.Duration
}

// Payload is what the emulated "training job" understands.
type Payload struct {
	DurationMS   int `json:"duration_ms"`   // simulated work time
	FailAttempts int `json:"fail_attempts"` // the first N attempts fail
}

// Worker executes jobs.
type Worker struct {
	cfg   Config
	st    *store.Store
	log   *slog.Logger
	stats *Metrics
}

// New creates a worker.
func New(cfg Config, st *store.Store, log *slog.Logger, m *Metrics) *Worker {
	return &Worker{cfg: cfg, st: st, log: log.With("worker", cfg.ID), stats: m}
}

var (
	errLeaseLost       = errors.New("lease lost")
	errCancelRequested = errors.New("cancel requested")
)

// Run loops until ctx is cancelled. The job in flight at cancellation is
// finished (or abandoned to lease expiry) before Run returns.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker started", "lease", w.cfg.Lease, "heartbeat", w.cfg.Heartbeat, "effect_mode", w.cfg.EffectMode)

	// The reaper runs in its own goroutine, independent of job execution.
	// (It used to run only between jobs, so while every worker was busy with a
	// long job, a dead worker's job stayed "running" until someone finished:
	// found by an independent review, regression test in worker_test.go.)
	reapCtx, stopReap := context.WithCancel(ctx)
	reapDone := make(chan struct{})
	go func() { defer close(reapDone); w.reapLoop(reapCtx) }()
	defer func() { stopReap(); <-reapDone }()

	idle := w.cfg.PollMin
	for {
		if ctx.Err() != nil {
			w.log.Info("worker stopping")
			return nil
		}

		job, err := w.st.Claim(ctx, w.cfg.ID, w.cfg.Lease)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.log.Warn("claim failed", "err", err)
			sleep(ctx, w.cfg.PollMax)
			continue
		}
		if job == nil {
			sleep(ctx, jitter(idle))
			idle = min(idle*2, w.cfg.PollMax)
			continue
		}
		idle = w.cfg.PollMin
		w.stats.Claimed.Inc()
		w.process(job)
	}
}

// reapLoop returns expired leases to the queue every ReapEvery until ctx ends.
// Safe to run in every worker at once (ReapExpired uses SKIP LOCKED).
func (w *Worker) reapLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.ReapEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := w.st.ReapExpired(ctx, 100)
		switch {
		case err != nil && ctx.Err() == nil:
			w.log.Warn("reap failed", "err", err)
		case n > 0:
			w.stats.Reaped.Add(float64(n))
			w.log.Info("reaped expired leases", "count", n)
		}
	}
}

// process executes one claimed job. It deliberately does not take the Run
// context: on SIGTERM we finish the current job rather than abandon it.
func (w *Worker) process(job *store.Claimed) {
	log := w.log.With("job", job.JobID, "fence", job.Fence, "attempt", job.Attempt)
	log.Debug("claimed")

	execCtx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	hbDone := make(chan time.Time, 1)
	stopHB := make(chan struct{})
	go func() { hbDone <- w.heartbeat(execCtx, job, cancel, stopHB, log) }()

	start := time.Now()
	result, execErr := execute(execCtx, job)
	w.stats.ExecSeconds.Observe(time.Since(start).Seconds())

	// Chaos hook: a stop-the-world pause (GC, SIGSTOP, VM migration) landing
	// after the work is done and after any local "is my lease still valid?"
	// check, but before the write reaches the database. A real paused process
	// cannot notice it was paused, so we stop heartbeating too.
	frozen := false
	if w.cfg.FreezeProb > 0 && rand.Float64() < w.cfg.FreezeProb {
		frozen = true
		close(stopHB)
		w.stats.Freezes.Inc()
		// Random length in [FreezeFor/2, FreezeFor): sometimes the zombie wakes
		// while its successor is still running (only fencing can stop it),
		// sometimes after the successor finished (the state check stops it).
		d := jitter(w.cfg.FreezeFor)
		log.Warn("chaos: freezing before finish", "for", d)
		time.Sleep(d)
	} else {
		close(stopHB)
	}
	<-hbDone

	cause := context.Cause(execCtx)
	if !frozen && errors.Is(cause, errLeaseLost) {
		// We know we lost the lease: write nothing, someone else owns the job.
		w.stats.LeaseLost.Inc()
		log.Warn("lease lost during execution, abandoning")
		ctx, c := context.WithTimeout(context.Background(), w.cfg.FinishTimeout)
		defer c()
		if err := w.st.CloseRunIfOpen(ctx, job.Fence); err != nil {
			log.Warn("close run failed", "err", err)
		}
		return
	}

	p := store.FinishParams{JobID: job.JobID, Fence: job.Fence, WorkerID: w.cfg.ID, EffectInTx: w.cfg.EffectMode == EffectInTx}
	switch {
	case errors.Is(cause, errCancelRequested):
		p.Outcome = store.OutcomeCancelled
	case execErr != nil:
		p.Outcome = store.OutcomeFailure
		p.Error = execErr.Error()
		p.RetryDelay = backoff(w.cfg.RetryBase, w.cfg.RetryMax, job.Attempt)
	default:
		p.Outcome = store.OutcomeSuccess
		p.Result = result
	}

	ctx, c := context.WithTimeout(context.Background(), w.cfg.FinishTimeout)
	defer c()
	if p.Outcome == store.OutcomeSuccess && w.cfg.EffectMode == EffectExternal {
		if err := w.st.WriteExternalEffect(ctx, job.JobID, job.Fence, w.cfg.ID); err != nil {
			log.Error("external effect failed; leaving job to lease expiry", "err", err)
			return
		}
	}
	res, err := w.finishWithRetry(ctx, p)
	if err != nil {
		// Not fatal for correctness: the lease expires and the job is retried.
		log.Error("finish failed; job will be retried after lease expiry", "err", err)
		return
	}
	if !res.Applied {
		w.stats.StaleRejected.Inc()
		log.Warn("stale write rejected by fencing (zombie)", "job_state", res.State)
		return
	}
	w.stats.Finished.WithLabelValues(string(res.State)).Inc()
	log.Debug("finished", "state", res.State)
}

func (w *Worker) finishWithRetry(ctx context.Context, p store.FinishParams) (store.FinishResult, error) {
	var lastErr error
	for i := 0; i < 5; i++ {
		res, err := w.st.Finish(ctx, p)
		if err == nil {
			return res, nil
		}
		lastErr = err
		if !store.IsRetryable(err) {
			break
		}
		sleep(ctx, time.Duration(50*(i+1))*time.Millisecond)
	}
	return store.FinishResult{}, lastErr
}

// heartbeat extends the lease until stop is closed or the job context ends.
// It cancels the execution on lease loss or cancel request, and self-fences
// if the database has been unreachable for longer than the lease (the lease
// may already have been reaped; continuing would only produce a zombie).
func (w *Worker) heartbeat(ctx context.Context, job *store.Claimed, cancel context.CancelCauseFunc,
	stop <-chan struct{}, log *slog.Logger) time.Time {
	lastOK := time.Now() // monotonic; only durations are compared, never wall-clock instants
	t := time.NewTicker(w.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return lastOK
		case <-ctx.Done():
			return lastOK
		case <-t.C:
		}
		hctx, c := context.WithTimeout(ctx, w.cfg.Heartbeat)
		res, err := w.st.Heartbeat(hctx, job.JobID, job.Fence, w.cfg.Lease)
		c()
		switch {
		case err != nil:
			log.Warn("heartbeat failed", "err", err, "since_ok", time.Since(lastOK))
			if time.Since(lastOK) > w.cfg.Lease-w.cfg.Heartbeat {
				cancel(errLeaseLost)
				return lastOK
			}
		case !res.Alive:
			cancel(errLeaseLost)
			return lastOK
		case res.CancelRequested:
			lastOK = time.Now()
			cancel(errCancelRequested)
			return lastOK
		default:
			lastOK = time.Now()
		}
	}
}

// execute emulates a training step. The observable side effect is written
// by Finish (EffectInTx) or WriteExternalEffect, never here, so an attempt
// that is killed mid-way leaves no trace except its job_runs row.
func execute(ctx context.Context, job *store.Claimed) (json.RawMessage, error) {
	var p Payload
	if len(job.Payload) > 0 {
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			return nil, fmt.Errorf("bad payload: %w", err)
		}
	}
	d := time.Duration(max(p.DurationMS, 0)) * time.Millisecond
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-t.C:
	}
	if job.Attempt <= p.FailAttempts {
		return nil, fmt.Errorf("simulated failure on attempt %d", job.Attempt)
	}
	return json.RawMessage(fmt.Sprintf(`{"fence":%d,"attempt":%d}`, job.Fence, job.Attempt)), nil
}

func backoff(base, maxD time.Duration, attempt int) time.Duration {
	d := base << min(attempt-1, 16)
	if d > maxD || d <= 0 {
		d = maxD
	}
	return jitter(d)
}

// jitter returns a duration in [d/2, d).
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	half := d / 2
	return half + rand.N(half)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
