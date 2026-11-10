// Package store is the only code that touches the jobs tables. Every state
// transition is a single conditional UPDATE (or a SELECT ... FOR UPDATE + UPDATE
// in one transaction), so correctness relies on Postgres row locks, not on
// application-level checks that could race.
//
// Isolation level: READ COMMITTED (the default) on purpose. Every write
// re-checks its predicate against the latest committed row version after
// acquiring the row lock (EvalPlanQual), which is exactly the
// compare-and-swap semantics a queue needs. SERIALIZABLE would add 40001
// retries without closing any hole the conditional updates leave open.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// State is a job state. Values mirror the job_state enum.
type State string

// Job states.
const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

// Terminal reports whether no further transition is possible.
func (s State) Terminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateCancelled
}

// Valid reports whether s is a known state.
func (s State) Valid() bool {
	switch s {
	case StateQueued, StateRunning, StateSucceeded, StateFailed, StateCancelled:
		return true
	}
	return false
}

// Errors returned by the store.
var (
	ErrNotFound            = errors.New("job not found")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
)

// Job is the public view of a job row.
type Job struct {
	ID              string          `json:"id"`
	Seq             int64           `json:"-"`
	IdempotencyKey  string          `json:"idempotency_key"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	State           State           `json:"state"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"max_attempts"`
	CancelRequested bool            `json:"cancel_requested"`
	LastError       *string         `json:"last_error,omitempty"`
	Result          json.RawMessage `json:"result,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
}

const jobColumns = `id, seq, idempotency_key, job_type, payload, state::text, attempts, max_attempts,
	cancel_requested, last_error, result, created_at, updated_at, started_at, finished_at`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var state string
	err := row.Scan(&j.ID, &j.Seq, &j.IdempotencyKey, &j.Type, &j.Payload, &state, &j.Attempts,
		&j.MaxAttempts, &j.CancelRequested, &j.LastError, &j.Result, &j.CreatedAt, &j.UpdatedAt,
		&j.StartedAt, &j.FinishedAt)
	j.State = State(state)
	return j, err
}

// Options toggles deliberately broken behaviour. They exist only so the chaos
// harness can prove it detects the bugs it claims to detect (negative
// controls). Never enable them outside a test.
type Options struct {
	// UnsafeNoFence accepts a worker's completion if the job is merely
	// "running", without comparing fencing tokens (zombie writes succeed).
	UnsafeNoFence bool
	// UnsafeRacyClaim claims with SELECT-then-UPDATE and no row lock or state
	// predicate on the UPDATE (classic double-claim bug).
	UnsafeRacyClaim bool
}

// Store wraps the connection pool.
type Store struct {
	pool *pgxpool.Pool
	opts Options
}

// New returns a Store.
func New(pool *pgxpool.Pool, opts Options) *Store { return &Store{pool: pool, opts: opts} }

// Pool exposes the pool (health checks, metrics).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// ---------------------------------------------------------------------------
// API side
// ---------------------------------------------------------------------------

// CreateParams describes a new job.
type CreateParams struct {
	IdempotencyKey string
	Type           string
	Payload        json.RawMessage // must be canonical JSON (see api package)
	MaxAttempts    int
}

// RequestHash fingerprints the semantic content of a create request, so a key
// reused with a different body is rejected instead of silently returning the
// old job.
func (p CreateParams) RequestHash() []byte {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%d\x00", p.Type, p.MaxAttempts)
	h.Write(p.Payload)
	return h.Sum(nil)
}

// CreateJob inserts a job or returns the existing one for the same key.
// created is true only for the request that actually inserted the row.
//
// Concurrent requests with the same key are serialized by the unique index:
// the loser of INSERT ... ON CONFLICT DO NOTHING waits for the winner's
// commit and then inserts nothing; the following SELECT (a new statement, so
// a new READ COMMITTED snapshot) sees the winner's row.
func (s *Store) CreateJob(ctx context.Context, p CreateParams) (job Job, created bool, err error) {
	hash := p.RequestHash()
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (idempotency_key, request_hash, job_type, payload, max_attempts)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+jobColumns,
		p.IdempotencyKey, hash, p.Type, p.Payload, p.MaxAttempts)
	job, err = scanJob(row)
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	var existingHash []byte
	row = s.pool.QueryRow(ctx,
		`SELECT request_hash, `+jobColumns+` FROM jobs WHERE idempotency_key = $1`, p.IdempotencyKey)
	var state string
	err = row.Scan(&existingHash, &job.ID, &job.Seq, &job.IdempotencyKey, &job.Type, &job.Payload,
		&state, &job.Attempts, &job.MaxAttempts, &job.CancelRequested, &job.LastError, &job.Result,
		&job.CreatedAt, &job.UpdatedAt, &job.StartedAt, &job.FinishedAt)
	if err != nil {
		// The conflicting row cannot disappear (jobs are never deleted), so this
		// is a real error, not a race.
		return Job{}, false, fmt.Errorf("load existing job for key: %w", err)
	}
	job.State = State(state)
	if string(existingHash) != string(hash) {
		return job, false, ErrIdempotencyMismatch
	}
	return job, false, nil
}

// GetJob loads one job.
func (s *Store) GetJob(ctx context.Context, id string) (Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// ListParams selects a page of jobs, newest first.
type ListParams struct {
	State     State // empty = any
	Limit     int
	BeforeSeq int64 // 0 = first page
}

// ListJobs returns up to Limit jobs with seq < BeforeSeq (keyset pagination:
// stable under concurrent inserts, O(limit) regardless of page depth).
func (s *Store) ListJobs(ctx context.Context, p ListParams) ([]Job, error) {
	before := p.BeforeSeq
	if before <= 0 {
		before = 1<<63 - 1
	}
	var rows pgx.Rows
	var err error
	if p.State != "" {
		rows, err = s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs
			WHERE state = $1::job_state AND seq < $2 ORDER BY seq DESC LIMIT $3`,
			string(p.State), before, p.Limit)
	} else {
		rows, err = s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs
			WHERE seq < $1 ORDER BY seq DESC LIMIT $2`, before, p.Limit)
	}
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	jobs := make([]Job, 0, p.Limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// CancelOutcome says what a cancel request did.
type CancelOutcome int

// Cancel outcomes.
const (
	CancelledNow     CancelOutcome = iota // was queued, now cancelled
	CancelRequested                       // was running; worker will stop, final state will be cancelled
	AlreadyCancelled                      // idempotent repeat
	AlreadyTerminal                       // succeeded/failed before the cancel arrived
)

// CancelJob cancels a queued job immediately or flags a running one.
//
// Race with completion: both are UPDATEs of the same row, so they serialize
// on its lock. If cancel commits first, the worker's fenced completion sees
// cancel_requested and records cancelled (no side effect). If completion
// commits first, the cancel's predicate (state IN queued,running) no longer
// matches and the caller learns the job already finished.
func (s *Store) CancelJob(ctx context.Context, id string) (Job, CancelOutcome, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs SET
			cancel_requested = true,
			state            = CASE WHEN state = 'queued' THEN 'cancelled'::job_state ELSE state END,
			finished_at      = CASE WHEN state = 'queued' THEN now() ELSE finished_at END
		WHERE id = $1 AND state IN ('queued', 'running')
		RETURNING `+jobColumns, id)
	j, err := scanJob(row)
	if err == nil {
		if j.State == StateCancelled {
			return j, CancelledNow, nil
		}
		return j, CancelRequested, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, 0, fmt.Errorf("cancel job: %w", err)
	}
	j, err = s.GetJob(ctx, id)
	if err != nil {
		return Job{}, 0, err
	}
	if j.State == StateCancelled {
		return j, AlreadyCancelled, nil
	}
	return j, AlreadyTerminal, nil
}

// CountByState returns the number of jobs per state (metrics, harness).
func (s *Store) CountByState(ctx context.Context) (map[State]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT state::text, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("count by state: %w", err)
	}
	defer rows.Close()
	out := map[State]int64{}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[State(st)] = n
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Worker side
// ---------------------------------------------------------------------------

// Claimed is a job a worker now holds a lease on.
type Claimed struct {
	JobID       string
	Fence       int64
	Attempt     int
	MaxAttempts int
	Type        string
	Payload     json.RawMessage
}

func secs(d time.Duration) float64 { return d.Seconds() }

// Claim atomically takes the oldest runnable queued job, assigns a fresh
// fencing token and records the run. Returns (nil, nil) if nothing is runnable.
//
// FOR UPDATE SKIP LOCKED: concurrent claimers never block on, nor take, the
// same row; the outer UPDATE re-checks state = 'queued' after locking.
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (*Claimed, error) {
	if s.opts.UnsafeRacyClaim {
		return s.claimRacy(ctx, workerID, lease)
	}
	row := s.pool.QueryRow(ctx, `
		WITH next AS (
			SELECT id FROM jobs
			WHERE state = 'queued' AND run_after <= now()
			ORDER BY run_after, seq
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE jobs j SET
				state            = 'running',
				attempts         = j.attempts + 1,
				lease_token      = nextval('job_fence_seq'),
				lease_owner      = $1,
				lease_expires_at = now() + make_interval(secs => $2),
				started_at       = COALESCE(j.started_at, now())
			FROM next
			WHERE j.id = next.id AND j.state = 'queued'
			RETURNING j.id, j.lease_token, j.attempts, j.max_attempts, j.job_type, j.payload
		), run AS (
			INSERT INTO job_runs (fence, job_id, attempt, worker_id)
			SELECT lease_token, id, attempts, $1 FROM claimed
		)
		SELECT id, lease_token, attempts, max_attempts, job_type, payload FROM claimed`,
		workerID, secs(lease))
	var c Claimed
	err := row.Scan(&c.JobID, &c.Fence, &c.Attempt, &c.MaxAttempts, &c.Type, &c.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return &c, nil
}

// claimRacy is the BROKEN claim used as a negative control.
func (s *Store) claimRacy(ctx context.Context, workerID string, lease time.Duration) (*Claimed, error) {
	var id string
	err := s.pool.QueryRow(ctx, `SELECT id FROM jobs WHERE state = 'queued' AND run_after <= now()
		ORDER BY run_after, seq LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("racy select: %w", err)
	}
	var c Claimed
	err = s.pool.QueryRow(ctx, `
		WITH claimed AS (
			UPDATE jobs SET state = 'running', attempts = attempts + 1,
				lease_token = nextval('job_fence_seq'), lease_owner = $2,
				lease_expires_at = now() + make_interval(secs => $3),
				started_at = COALESCE(started_at, now())
			WHERE id = $1   -- BUG: no "AND state = 'queued'"
			RETURNING id, lease_token, attempts, max_attempts, job_type, payload
		), run AS (
			INSERT INTO job_runs (fence, job_id, attempt, worker_id)
			SELECT lease_token, id, attempts, $2 FROM claimed
		)
		SELECT id, lease_token, attempts, max_attempts, job_type, payload FROM claimed`,
		id, workerID, secs(lease)).Scan(&c.JobID, &c.Fence, &c.Attempt, &c.MaxAttempts, &c.Type, &c.Payload)
	if err != nil {
		return nil, fmt.Errorf("racy claim: %w", err)
	}
	return &c, nil
}

// HeartbeatResult reports whether the lease is still ours.
type HeartbeatResult struct {
	Alive           bool // false: lease lost (reaped / superseded) — stop working
	CancelRequested bool
}

// Heartbeat extends the lease if (and only if) the caller still holds it.
// Expiry is computed from the database clock, never the worker's clock.
func (s *Store) Heartbeat(ctx context.Context, jobID string, fence int64, lease time.Duration) (HeartbeatResult, error) {
	var cancel bool
	err := s.pool.QueryRow(ctx, `
		UPDATE jobs SET lease_expires_at = now() + make_interval(secs => $3)
		WHERE id = $1 AND lease_token = $2 AND state = 'running'
		RETURNING cancel_requested`, jobID, fence, secs(lease)).Scan(&cancel)
	if errors.Is(err, pgx.ErrNoRows) {
		return HeartbeatResult{Alive: false}, nil
	}
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("heartbeat: %w", err)
	}
	return HeartbeatResult{Alive: true, CancelRequested: cancel}, nil
}

// WriteExternalEffect performs the side effect OUTSIDE the completion
// transaction (EFFECT_MODE=external). This models a real external system
// (object storage, another service) and is expected to produce duplicates:
// the harness uses it to show where exactly-once stops.
func (s *Store) WriteExternalEffect(ctx context.Context, jobID string, fence int64, workerID string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO job_effects (job_id, fence, worker_id) VALUES ($1, $2, $3)`,
		jobID, fence, workerID)
	if err != nil {
		return fmt.Errorf("external effect: %w", err)
	}
	return nil
}

// Outcome of one execution attempt as seen by the worker.
type Outcome int

// Execution outcomes.
const (
	OutcomeSuccess Outcome = iota
	OutcomeFailure
	OutcomeCancelled // worker observed cancel_requested and stopped
)

// FinishParams reports the end of an execution attempt.
type FinishParams struct {
	JobID      string
	Fence      int64
	WorkerID   string
	Outcome    Outcome
	Result     json.RawMessage // success only
	Error      string          // failure only
	RetryDelay time.Duration   // backoff before the next attempt (failure only)
	EffectInTx bool            // write the side effect in this transaction
}

// FinishResult reports what Finish did.
type FinishResult struct {
	Applied bool  // false: the caller's lease was stale, nothing was written
	State   State // job state after the call
}

// Finish records the outcome of an attempt, fenced by the caller's token.
//
// It is a single call to the finish_job() function (migration 002), i.e. one
// round trip and one transaction. Inside it the row is locked first (SELECT
// ... FOR UPDATE) and the decision is made against the locked, current
// version. A zombie (stale fence) writes nothing to jobs or job_effects; its
// run is only annotated late_write_rejected (and fence_only_rejections when
// the job was running under a newer owner). cancel_requested always wins over
// success/failure, so a cancel that was acknowledged (202) can never end as
// succeeded. A retry after an ambiguous commit reports Applied=true.
func (s *Store) Finish(ctx context.Context, p FinishParams) (FinishResult, error) {
	var outcome string
	switch p.Outcome {
	case OutcomeSuccess:
		outcome = "success"
	case OutcomeFailure:
		outcome = "failure"
	case OutcomeCancelled:
		outcome = "cancelled"
	default:
		return FinishResult{}, fmt.Errorf("unknown outcome %d", p.Outcome)
	}
	var result any // NULL unless success
	if p.Outcome == OutcomeSuccess && len(p.Result) > 0 {
		result = p.Result
	}
	var applied bool
	var state string
	err := s.pool.QueryRow(ctx, `SELECT applied, new_state::text FROM finish_job($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		p.JobID, p.Fence, p.WorkerID, outcome, result, p.Error, secs(p.RetryDelay), p.EffectInTx,
		!s.opts.UnsafeNoFence).Scan(&applied, &state)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "P0002" {
		return FinishResult{}, ErrNotFound
	}
	if err != nil {
		return FinishResult{}, fmt.Errorf("finish_job: %w", err)
	}
	return FinishResult{Applied: applied, State: State(state)}, nil
}

// CloseRunIfOpen marks the caller's run as superseded when it discovers it
// lost its lease. A no-op when the reaper already closed the run.
func (s *Store) CloseRunIfOpen(ctx context.Context, fence int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE job_runs SET ended_at = clock_timestamp(), outcome = 'superseded'
		WHERE fence = $1 AND ended_at IS NULL`, fence)
	if err != nil {
		return fmt.Errorf("close run: %w", err)
	}
	return nil
}

// ReapExpired returns jobs whose lease expired (worker presumed dead) to the
// queue, or to a terminal state if they were cancelled or out of attempts.
// Safe to run concurrently from every worker: SKIP LOCKED + the re-checked
// predicate make each expired lease reaped exactly once, and a heartbeat that
// lands first simply makes the row no longer match.
func (s *Store) ReapExpired(ctx context.Context, limit int) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		WITH expired AS (
			SELECT id, lease_token FROM jobs
			WHERE state = 'running' AND lease_expires_at < now()
			ORDER BY lease_expires_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), upd AS (
			UPDATE jobs j SET
				state = CASE WHEN j.cancel_requested           THEN 'cancelled'::job_state
				             WHEN j.attempts >= j.max_attempts THEN 'failed'::job_state
				             ELSE 'queued'::job_state END,
				finished_at = CASE WHEN j.cancel_requested OR j.attempts >= j.max_attempts
				                   THEN now() ELSE NULL END,
				last_error = 'lease expired: worker ' || j.lease_owner || ' presumed dead',
				-- run_after is left unchanged on purpose: the job keeps its place
				-- at the head of the queue instead of waiting behind the backlog
				-- a second time because its worker died.
				lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
			FROM expired e
			WHERE j.id = e.id AND j.state = 'running' AND j.lease_expires_at < now()
			  AND j.lease_token = e.lease_token
			RETURNING e.lease_token AS old_fence
		), runs AS (
			UPDATE job_runs r SET ended_at = clock_timestamp(), outcome = 'lease_expired'
			FROM upd WHERE r.fence = upd.old_fence AND r.ended_at IS NULL
		)
		SELECT count(*) FROM upd`, limit).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("reap: %w", err)
	}
	return n, nil
}

// IsRetryable reports transient database errors worth retrying.
func IsRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01", "57P01", "53300": // serialization, deadlock, admin shutdown, too many conns
			return true
		}
		return false
	}
	return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}
