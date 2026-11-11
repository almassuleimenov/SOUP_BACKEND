# Verdict at 100 requests per second

## DON'T SHIP yet. Close, but not as-is.

The **correctness core is ready**: no lost jobs, no double effects, no stuck jobs and no stale writes in any run of the
current build, under kill -9 every ~2 s plus zombies, concurrent duplicates and cancels. Every negative control is caught.
**Throughput at 100 rps is not the problem** (measured headroom 4–6x on one laptop). What blocks shipping is what happens
on day 30 and on the first bad day, not in the first 10 minutes. The list below is ordered by what I would fix first.

## What was measured (single Windows laptop: Postgres, API, workers and harness on the same machine)

| Scenario | Result |
|---|---|
| **100 new jobs/s open-loop for 60 s + full chaos** (kill -9 ~2 s, 30% concurrent duplicate POSTs ⇒ ~180 POST/s, cancels, zombies), 32 workers | **0 errors, all invariants held.** Create p50 3.2 ms, p99 21 ms, max 166 ms. Queue wait p50 0.5 s, p99 2.9 s. created→succeeded p99 4.9 s. Backlog oscillated around 0–400 and drained 10 s after load stopped. `results/*load-100rps-chaos-w32-v2` |
| Same, first attempt | **4 × 5xx** (`too many clients`, local `max_connections=100`). Clients retried with the same key, so no duplicates. This led to fixes #2 and #3 below. `results/*load-100rps-chaos-w32` |
| Machinery ceiling, zero-length jobs, no chaos | 300–640 jobs/s end-to-end. **Run-to-run noise ±35%** (same config: 304, 448, 592 jobs/s), so single-run comparisons below that noise level mean nothing. 8 workers ≥ 16 ≥ 32 workers: the box is CPU-bound, and more workers only add contention |
| 10x (1000 rps target) | **Does not hold.** Accepted only ~450–640 rps. Create p99 0.2–2 s, max up to 4.9 s, queue wait p99 1.5–15 s |

**What breaks first at 10x:** the single Postgres primary. Each job costs about 10 row writes (insert + audit row, claim
update + run row + audit, heartbeats, finish update + effect + run update + audit) and 3+ commits. Sampling
`pg_stat_activity` at 1000 rps showed the original 6-round-trip finish transaction leaving workers **idle in transaction
while holding the job's row lock** (272 of ~400 samples). I moved it into one server-side function (`finish_job`, migration
002). The idle-in-transaction samples dropped to 0, but throughput didn't measurably change: the machine is CPU-bound and
the work only moved into Postgres. I also tested the hypothesis "fsync is the bottleneck" by turning `synchronous_commit`
off. The change was within noise, so that hypothesis is rejected.

## Fix first (in this order)

1. **Retention / archival.** `jobs`, `job_runs`, `job_transitions` and `job_effects` grow forever. 100 rps is 8.6M jobs and
   ~40M audit rows per day. Indexes, autovacuum and backups grow without bound. Fix: partition by `created_at` (daily) and
   drop or archive old partitions. (The `/metrics` full scan that used to be the first thing to fall over is already fixed:
   it now counts only queued/running.)
2. **Connection budget and pooling.** Every worker process holds 3 connections (loop, heartbeat, reaper), the API holds
   up to 20 (pre-warmed, `MinConns`). `workers×3 + api + headroom` must stay below `max_connections`. That already failed once locally.
   Production: PgBouncer in transaction mode (everything here is transaction-scoped: `FOR UPDATE SKIP LOCKED`, the advisory
   *xact* lock and `finish_job` are all compatible) and an alert on connection saturation.
3. **Overload behaviour.** There is no rate limiting and no admission control. Under 10x load latency climbs to seconds
   instead of shedding load. Add a per-client rate limit and a bounded HTTP concurrency limit that returns 503 + Retry-After.
   Transient DB errors already map to 503 + Retry-After (fixed after the 5xx above). A retry with the same
   Idempotency-Key is safe.
4. **Idempotency keys are global, not per client.** In a multi-tenant deployment, client B using client A's key gets A's job
   back (or a 422 that reveals the key exists). Use `UNIQUE (tenant_id, idempotency_key)`, add auth (`API_TOKEN` is only a
   single shared bearer token), and expire keys after N days.
5. **Postgres HA semantics.** Not tested: with async replication, failover can lose acknowledged jobs and roll back a
   `succeeded`. Use synchronous replication (`synchronous_commit=remote_apply` or a quorum) before calling "no job is lost"
   a guarantee. Rehearse the failover with this harness (kill the primary instead of a worker).
6. **External side effects.** Real training jobs write to object storage and registries, which is at-least-once
   (`-effect-mode external` shows the duplicates). Sinks must be idempotent on `job_id` (deterministic output paths,
   conditional puts), and training code must be safe to re-run from a checkpoint.
7. **Observability you can page on.** Metrics exist (and, since the collector fix below, actually report) (`jobq_jobs{state}`, `jobq_oldest_queued_seconds`,
   `jobq_expired_leases`, worker counters for reaped/lease-lost/stale-rejected), but there are no alerts or SLOs. Proposed
   SLOs: create p99 < 100 ms; oldest runnable queued job < 30 s for 99% of minutes; `stale_rejected_total` rate as a canary
   for pauses and clock trouble; `expired_leases > 0` for 2 minutes means workers are dying faster than they are replaced.

**Would I SHIP after 1–3?** For an internal, single-tenant queue at 100 rps on a properly sized Postgres with synchronous
replication: yes. The correctness evidence holds up, and those fixes are about operating it. Items 4–6 are required before
any external or multi-tenant use.

## Found by an independent review (after the runs above)

An independent reviewer re-checked the code and re-ran the harness on Linux: 3,000 jobs, 67 × kill -9, 15 × SIGSTOP, all 18
invariants held. The reviewer confirmed claiming, fencing, the cancel/complete race, idempotency 422 and DB-clock-only SQL,
and also found problems that none of my runs caught. All of them are now fixed in code, not just documented:

* **Fixed: the reaper only ran between jobs.** It was called from the worker's main loop, before `Claim`. With every worker busy
  on long jobs, a dead worker's job stayed `running` with an expired lease until some worker finished. The reviewer reproduced
  it: 2 workers on 25 s jobs, kill -9 one, the dead job sat for ~21 s. For multi-hour training that is "stuck in running".
  My harness missed it because its jobs last 10 ms–4 s, so some worker was always free soon enough. That is exactly the
  "scale and time" blind spot listed in CHAOS.md. **Fix:** the reaper now runs in its own goroutine on a ticker,
  independent of job execution, and the worker holds a third connection for it. **Regression test:**
  `TestDeadWorkerReapedWhileAllWorkersBusy` (one live worker busy on an 8 s job, one dead claimant). It *fails* on the old code
  ("not reaped within 2.7 s") and passes on the new one (reaped 1.1 s after claim with a 1 s lease). The "≈3–4 s detection"
  figure in DESIGN.md was true only while some worker was idle. It holds now, as long as at least one worker process is alive.
* **Fixed: a worker death consumed an attempt.** The reaper counted a killed run against `max_attempts`, so three rolling
  deploys or OOM kills in a row turned a healthy job into `failed` (the reviewer saw 2 such jobs). Now there are two
  budgets (migration 003). `failures` counts runs where the job itself reported an error (limit `max_attempts`), and
  `crashes` counts runs whose lease expired (limit `max_crashes`, default 5). A poison job that kills every worker is still
  bounded. Tests: `TestCrashesDoNotSpendAttempts` (4 crashes with `max_attempts=3`, then success) and
  `TestPoisonJobBoundedByCrashBudget`. New harness invariants: `budgets_match_runs` (the counters equal the run outcomes in
  `job_runs`), and `failure_semantics` now requires that a succeeded job failed exactly `fail_attempts` times.
  **The new invariant then caught a bug in the fix itself.** In `FINAL2-main-10k` one job of 10,000 had `crashes=1` but no
  `lease_expired` run: its worker's heartbeat timed out under load, the worker self-fenced and closed its run as
  `superseded`, and the reaper (which only labelled open runs) counted the crash without relabelling. States and effects
  were correct; the bookkeeping that a "poison job" alert would rely on was not. Fixed in the reaper, with
  `TestSelfFencedRunIsLabelledByReaper` (fails on the old code). The busy-fleet run on the same build had 2 of 80 such jobs. Both failed runs are kept in `results/`; the reruns (`FINAL3-*`) are green.
* **Fixed: chaos and negative-control switches shipped in the production binary.** `UNSAFE_MODE`, `CHAOS_FREEZE_*` and
  `EFFECT_MODE=external` now exist only in `worker-chaos` (built with `-tags chaos`, spawned by the harness). The production
  `worker` refuses to start if any of them is set, instead of silently ignoring it.
* **Fixed: `/metrics` was unauthenticated and did a full `count(*)`.** It now sits behind the same bearer token as the API
  (when `API_TOKEN` is set) and counts only `queued` and `running`. EXPLAIN ANALYZE on 200k terminal + 500 live rows: Index
  Scan, 1.0 ms, versus a 105 ms Seq Scan for the old `GROUP BY state`.
* **Found while fixing that: the queue metrics had never worked.** The collector's SQL had a syntax error since the first
  version (`FILTER` attached to `EXTRACT` instead of `min()`), so every scrape silently returned `jobq_queue_scrape_ok 0` and
  no queue depth, age or expired-lease metrics. Nothing in the harness scraped `/metrics`, so no run could have noticed it.
  It was found only because I ran EXPLAIN on the new query. `TestQueueCollectorAgainstPostgres` now runs the real query
  (and fails on the old SQL). Lesson: the observability I proposed to page on was itself untested.
* **Fixed: payloads were not validated per job type.** `{"duration_ms":"abc"}` used to get 201 and then fail every
  attempt. Now an unknown `type` gives 400 `unknown_type`, and a `train` payload must have integer
  `duration_ms ∈ [0, 24h]` and `fail_attempts ∈ [0, 20]` (other fields pass through). Test: `TestValidateTrainPayload`.

## Where the design is weakest (my own pre-mortem)

* **Lease length is a single global trade-off.** 3 s is fine for the emulated work, but for multi-hour training a 3 s lease
  means a 3 s network blip or GC pause re-runs hours of work (fencing makes it *safe*, not *cheap*). Lease, heartbeat and
  checkpointing should be per job type.
* **A worker frozen for 5 minutes that comes back:** its heartbeat fails (fence mismatch), so it stops. If it was frozen
  right before writing, `finish_job` rejects the write. Proven by `TestZombieWriteIsFenced` and `fence_only_rejections > 0`
  in the main runs. The cost is one wasted execution, and the job itself was reaped ~3–4 s after the freeze began (true only after the reaper fix above; before it, only if some worker was idle).
* **Everything depends on one database clock and one primary.** Clock steps on that host and failover are untested
  (see [CHAOS.md](CHAOS.md#what-the-harness-does-not-catch)).
* **Retry fairness.** Retries back off by moving `run_after` forward, so under a backlog a failing job waits behind every
  newer job each time. Permanent failures finish last, as you can see at the tail of the 10k run.

## How do I know 10,000 jobs was enough?

I don't, in general. A count isn't a proof, and the evidence block matters more than the job count. What I can say:
* The harness reports how many times each failure path actually happened, and **refuses to call a run proof if any
  path never happened** (exit 3). That gate caught two blind runs of my own.
* In the 10k run the rarest path, "a zombie wrote while a newer owner was running" (the only case where fencing, not the
  state check, saves you), happened **9 times**. For rarer interleavings (a kill -9 exactly between the effect INSERT and the
  COMMIT) I rely on transactional atomicity by argument, not by observation.
* Every negative control (`racy-claim`, `no-fence`, `external`) fails at 1,000 jobs, with dozens to hundreds of violations,
  so the detectors fire well before 10k. To push further I would run many short runs with different seeds rather than one
  longer run: the variance in interleavings matters more than the volume.
