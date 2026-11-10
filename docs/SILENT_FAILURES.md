# Silent failures: what can break without an error, and how each was checked

For each failure mode: the mechanism that prevents it, the **test that would go red if the mechanism were removed**, and a
**negative control** showing that test actually detects the broken version. A check that can't fail proves nothing.

Test file: `internal/store/store_test.go`. Harness: `cmd/chaos`. Raw outputs: `results/`.

---

## 1. Double claim

**Failure:** two workers both think they own a job. Classic causes: `SELECT … WHERE state='queued'` followed by a separate
`UPDATE … WHERE id=$1` (read-then-write without a lock or predicate), or `FOR UPDATE` without `SKIP LOCKED` combined with a
predicate that isn't re-checked.

**Mechanism:** one statement, `FOR UPDATE SKIP LOCKED`, plus `AND state='queued'` on the UPDATE. Under READ COMMITTED,
Postgres re-evaluates the predicate on the newest row version after taking the lock (EvalPlanQual). A fresh fencing token
per claim, and `UNIQUE (job_id, attempt)` on `job_runs` as a tripwire.

**How checked:**
* `TestClaimExactlyOnceUnderEveryIsolationLevel`: 32 goroutines, 500 jobs, each job must be claimed exactly once.
* Harness invariant `no_double_claim`: for every job, runs ordered by fence must not overlap in time
  (`prev.ended_at <= next.claimed_at`, both from the DB clock).
* **Negative control** `UNSAFE_MODE=racy-claim` (the read-then-write bug): `TestRacyClaimIsDetected` and the harness both go red.
  See `results/dev-iterations/*neg-racy-claim*`: `no_double_claim violations=154`, plus `transition_log_consistent` and
  `all_runs_closed` failing. Note that even with double claims, **`at_most_one_effect` still passed**, because the fenced finish
  rejected the loser. Fencing hid the claim bug from the effect invariant. That is why the harness checks run overlap
  separately: "one effect" alone would have called a broken claim correct.

## 2. Zombie worker writes a stale result

**Failure:** a worker freezes (GC pause, SIGSTOP, VM migration, network partition) past its lease. The job is reaped and
re-claimed, then the zombie wakes up and writes its result, or a second side effect, over the new owner's.

**Mechanism:** fencing token. `Finish` locks the row and requires `state='running' AND lease_token = my_fence`. The heartbeat
also requires the fence, so a zombie learns it lost the lease the moment it heartbeats. Rejections are recorded on the run:
`late_write_rejected`, and **`fence_only_rejections`** for the cases where the job *was* running (under a newer owner), so a
state check alone would have accepted the write.

**How checked:**
* `TestZombieWriteIsFenced`: deterministic. Claim, expire, reap, re-claim, then the zombie finishes: refused, 0 effects, and
  `fence_only_rejections = 1`. Then the successor's result wins. The same test includes a negative control: with
  `UnsafeNoFence` the zombie's write is accepted.
* Harness: the worker freezes right before its finish with probability `-freeze-prob`, for a random time in
  [lease+0.5 s, 2·lease+1 s), and stops heartbeating while frozen. On Linux the harness also sends real `SIGSTOP`/`SIGCONT`.
  Invariant `no_stale_write`: the effect's fence must equal both the newest run's fence and the succeeded run's fence.
* **The negative control was initially blind (an important finding).** `UNSAFE_MODE=no-fence` passed green in three runs
  (`neg-no-fence`, `-v2`, `-v3`). Two reasons: (a) short jobs meant the successor had always finished before the zombie woke,
  so the state check caught it; (b) the reaper reset `run_after`, so the re-queued job waited behind the whole backlog and was
  not even re-claimed before the zombie woke. Fencing was **never decisive** in any of those runs, including the "passing"
  correct runs. Fixes: count `fence_only_rejections`, make the harness refuse to call a run proof when it is 0 (evidence gate),
  add long jobs, and keep `run_after` on reap. After that, `neg-no-fence-v4` fails (`no_stale_write violations=20`,
  `no_double_claim violations=21`) and the correct build passes with `fence_only_rejections=22`.

## 3. Cancel / complete race

**Failure:** the client is told "cancel accepted", but the job ends `succeeded` and its side effect happened. Or the reverse:
"already succeeded", but it ends cancelled.

**Mechanism:** both are writes to the same row and serialize on its lock. The cancel is a conditional UPDATE
(`state IN ('queued','running')`). The finish transaction does `SELECT … FOR UPDATE` and lets `cancel_requested` beat success.
So 202 means the final state *will* be `cancelled`, and 409 `already_succeeded` means it *was* `succeeded`. The reaper also
honours `cancel_requested`.

**How checked:**
* `TestCancelCompleteRace`: 300 jobs. For each, cancel and finish fire concurrently, and the test asserts the cancel's answer
  matches the final state and the effect count. Both orders are observed (the log line prints how many times each side won).
* `TestReaperHonoursCancel`: the worker dies after the cancel request, and the reaper ends the job `cancelled`, not `queued`.
* Harness: half the cancels are aimed at jobs the harness sees in `running`. Invariants `cancel_wins`
  (`cancel_requested ⇔ cancelled`) and `cancel_response_consistency` (200/202 ⇒ cancelled; 409 ⇒ the state reported in the
  409). The evidence block shows all three paths: `cancelled_while_queued`, `cancelled_by_worker_while_running`,
  `cancelled_by_reaper_after_worker_death`.

## 4. Duplicate on request retry

**Failure:** a client times out and retries `POST /jobs`, and two jobs are created. Or the same key is reused with a different
body, and the client silently gets the old job back, believing its new request was accepted.

**Mechanism:** `UNIQUE (idempotency_key)` with `INSERT … ON CONFLICT DO NOTHING`, then read back. Never check-then-insert. The
stored `request_hash` (sha256 of type + max_attempts + canonical payload with sorted keys and exact numbers) must match, or
the API answers 422.

**How checked:**
* `TestIdempotentCreateConcurrent`: 50 concurrent creates with one key give exactly one `created`, one distinct id, and one
  audit row. A different body gives `ErrIdempotencyMismatch`.
* Harness: 30% of keys are POSTed 2–4 times *concurrently* and once more later, while the job may be running. 2% are replayed
  with a different body. Invariants: one id per key, at most one 201 per key, 422 on mismatch, no phantom jobs.
* Unit `TestCanonicalObject`: key order doesn't change the hash, and 2^53+1 is not rounded (`UseNumber`). Without that,
  two different payloads could hash equal.

## 5. Transaction isolation level

**Failure:** code that is correct only under SERIALIZABLE but runs under the default READ COMMITTED. Or the reverse: switching
to SERIALIZABLE "for safety" turns contention into a storm of `40001` errors that nobody retries.

**Mechanism:** the protocol never reads a value in one statement and acts on it in another without a lock. Every transition is
a conditional UPDATE or runs under `SELECT … FOR UPDATE`. That is correct under READ COMMITTED because of EvalPlanQual
re-checks.

**How checked:** `TestClaimExactlyOnceUnderEveryIsolationLevel` runs the same 32-way claim under READ COMMITTED, REPEATABLE
READ and SERIALIZABLE. It asserts exactly-once claims in all three and logs the number of serialization failures (final run, `results/tests.txt`: READ COMMITTED 0, REPEATABLE
READ 455, SERIALIZABLE 5266 serialization failures for the same 500 claims). The stricter levels add retries and buy no extra safety. The read-then-write bug
(`racy-claim`) is what actually breaks under READ COMMITTED, and the harness catches it (§1).

## 6. Clock skew

**Failure:** a worker with a fast clock decides its lease is still valid (or a reaper with a slow clock decides others'
leases have expired) and two owners overlap.

**Mechanism:** **no worker clock reaches any lease decision.** `lease_expires_at = now() + lease` and
`lease_expires_at < now()` are both evaluated by the database. Workers only use Go's monotonic clock to measure *durations*
(heartbeat period, self-fencing after `lease − heartbeat` without a successful heartbeat), and that is immune to wall-clock
jumps. Even if a worker misjudges, fencing makes its writes harmless.

**How checked:**
* Code audit: no `time.Now()` value is passed as a SQL parameter anywhere in `internal/store` (the only time-like parameter is
  the lease *duration*). `grep -n "time.Now" internal/store` returns nothing.
* Residual risk, **not tested:** a wall-clock step on the *database* host. A forward step of more than one lease expires every
  lease at once: mass re-claims and duplicate executions (effects stay safe thanks to fencing). A backward step delays
  dead-worker detection. With a failover to a replica whose clock differs, the same applies. Mitigation: NTP slewing (not
  stepping) on the DB host, and alerting on `jobq_expired_leases` spikes.

## Also checked (bonus)

* **Ambiguous commit on finish** (`TestFinishRetryAfterAmbiguousCommit`). The worker's COMMIT succeeds but the reply is lost,
  so the worker retries. The retry must not write a second effect and must not count itself as a zombie. `finish_job`
  recognises its own already-closed run and reports success. (Found in code review, not by the harness: the harness never
  drops replies.)

* **Terminal states are immutable** (`TestTerminalStateIsImmutable`): the DB trigger rejects any update of a terminal row,
  including "harmless" ones like rewriting `result`.
* **Attempts are bounded and a dead worker counts as an attempt** (`TestAttemptsExhaustToFailed`): a poison job can't loop forever.
