# Design note: training-job queue

**Stack:** Go 1.25, PostgreSQL 16, pgx. Three binaries: `server` (HTTP API), `worker` (1 process = 1 job at a time), `chaos` (harness).
All coordination goes through Postgres. There is no broker, no in-memory state and no leader.

## Why Postgres (not SQLite)

The core requirement is *several worker processes claiming concurrently and surviving kill -9*. Postgres gives:
row-level locks, `FOR UPDATE SKIP LOCKED` (claimers never block on, or double-take, a row), MVCC (readers never block
the queue), sequences for fencing tokens, and one authoritative clock (`now()`) for all lease arithmetic.
SQLite has a single writer lock per file: with 8 workers heartbeating and claiming, every write serializes on it,
`busy_timeout` replaces contention with latency spikes, and it can't be shared safely across hosts/containers.
SQLite would be correct but slow. Postgres is correct *and* behaves the way it will in production.

## States

```
queued ──claim──▶ running ──finish ok──▶ succeeded
  │                 │ ├──finish err, failures < max_attempts──▶ queued (run_after = now()+backoff)
  │                 │ ├──finish err, failures = max_attempts / reaper, crashes = max_crashes──▶ failed
  │                 │ └──lease expired (reaper), crashes < max_crashes──▶ queued  (keeps its place in line)
  └──cancel──▶ cancelled ◀──cancel_requested observed (worker finish or reaper)──┘
```
Two separate budgets: `failures` (the job reported an error, limit `max_attempts`) and `crashes` (its worker died or froze
past the lease, limit `max_crashes`=5). A deploy or OOM kill never spends the job's own retry budget, and a poison job that
kills every worker is still bounded. `attempts` counts claims (= rows in `job_runs`).
A trigger in the database enforces the arrows above (illegal transitions raise), makes terminal rows immutable, and logs every
transition to `job_transitions`. CHECK constraints tie the lease columns to `state='running'` and `finished_at` to terminal states.
A bug in any client fails loudly instead of corrupting data.

## How a worker claims a job

One statement, autocommit:
`SELECT id … WHERE state='queued' AND run_after<=now() ORDER BY run_after, seq LIMIT 1 FOR UPDATE SKIP LOCKED` →
`UPDATE … SET state='running', attempts+=1, lease_token=nextval('job_fence_seq'), lease_expires_at=now()+lease` →
`INSERT INTO job_runs(fence, …)`. The statement either commits all of that or none of it. A kill -9 at any point leaves
either a `queued` job or a `running` job with a lease that will expire. The `lease_token` is a **fencing token**: it is
strictly greater than every earlier claim of that job, and every later write by the worker carries it.

## How a dead worker is detected

Leases. The worker heartbeats every `lease/3`: `UPDATE … SET lease_expires_at = now()+lease WHERE id=$1 AND lease_token=$2
AND state='running'`. Zero rows means the lease was lost, so the worker abandons the job and writes nothing. Every worker also runs
the reaper **in its own goroutine**, every 0.5–1 s, independently of the job it is executing: `running AND lease_expires_at < now()` → `queued` (counting one crash, not a failure; `failed` if the crash budget `max_crashes` is exhausted; `cancelled`
if a cancel was requested). It uses `SKIP LOCKED` and re-checks the predicate, so concurrent reapers are safe, and a heartbeat that
commits first simply makes the row stop matching. **Only the database clock is used** (`now()`). Worker clocks never enter
any comparison, so worker clock skew cannot shorten or extend a lease. Workers only measure *durations* with a monotonic clock, to
self-fence (stop working) if they could not heartbeat for longer than a lease.
Detection latency is `lease + reap interval` (≈3–4 s with defaults) as long as at least one worker process is alive.
*(Until the independent review the reaper ran only between jobs, so with every worker busy on long jobs a dead worker's
job stayed `running` until someone finished. Fixed and covered by `TestDeadWorkerReapedWhileAllWorkersBusy`. Each worker
now holds 3 DB connections: loop, heartbeat, reaper.)* The trade-off: a shorter lease detects death faster but
turns long GC/network pauses into false deaths. Fencing makes those false deaths safe, but each one costs a duplicate *execution*.

## What happens when a running job is cancelled

`POST /v1/jobs/{id}/cancel` runs a single conditional UPDATE. A `queued` job becomes `cancelled` immediately (200). A `running`
job gets `cancel_requested=true` (202). The worker sees the flag in its next heartbeat (≤ lease/3) and stops execution. Its
fenced finish then records `cancelled` with no side effect. **`cancel_requested` beats success and failure**: the finish
transaction locks the row (`SELECT … FOR UPDATE`) and, if the flag is set, records `cancelled` even if the work completed.
The cancel and the completion are both writes to the same row, so they serialize on its lock. Whichever commits first wins, and the
API reports the truth: 202 always ends `cancelled`, and 409 `already_succeeded` always ends `succeeded`. If the worker dies after the
cancel, the reaper sees `cancel_requested` and moves the job to `cancelled`, not back to `queued`.

## Which guarantee, and why

* **Execution: at-least-once.** A worker that dies, or freezes past its lease, causes the job to run again. No lease-based
  system can avoid this: a paused process cannot tell it was paused.
* **Effect: exactly-once, but only for effects committed in the same Postgres transaction as the fenced completion.**
  The emulated side effect (`job_effects` row) is inserted in the same transaction that moves `running → succeeded`. That
  transaction first locks the row and verifies `state='running' AND lease_token = my_fence AND NOT cancel_requested`. So a
  zombie's late write, a cancelled job and a duplicate attempt produce no effect, and the effect and "succeeded" commit
  together or not at all.
* **Where it stops:** if the effect lives in *another system* (S3, a model registry, an email), Postgres can't make it atomic with
  the state change. That is at-least-once. The harness has `-effect-mode external` to show it (duplicates appear). Production
  fix: make the sink idempotent on `job_id` (deterministic object key, conditional put, or an outbox consumed idempotently),
  and pass the fencing token to sinks that can check it.

I chose this guarantee because at-most-once (mark done *before* running) silently loses work on every crash, which is
unacceptable for training jobs. "Exactly-once" in general is a claim I can't back for external effects.

## API

`POST /v1/jobs` (header `Idempotency-Key`, body `{type, payload, max_attempts}`) → 201 created / 200 replay
(`Idempotent-Replayed: true`) / 422 same key with a different body. · `GET /v1/jobs/{id}` · `GET /v1/jobs?state=&limit=&cursor=`
(keyset on an identity column: O(limit) at any depth, opaque cursor) · `POST /v1/jobs/{id}/cancel` → 200/202/409 ·
`/healthz`, `/readyz`, `/metrics`. Idempotency is enforced by the unique index (`INSERT … ON CONFLICT DO NOTHING`, then read
back), never by check-then-insert.
