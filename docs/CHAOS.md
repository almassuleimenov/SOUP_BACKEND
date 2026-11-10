# Chaos harness

`cmd/chaos` starts the API and N worker **processes** against a dedicated database (it refuses any database whose name lacks
`chaos`, because it TRUNCATEs). It then does the following, all in parallel:

* **Submits jobs** from 32 concurrent clients, as fast as possible or open-loop at `-rate`. 30% of keys are POSTed 2–4 times
  **at the same instant** (racing on the unique index) and once more 0–3 s later, possibly while the job is running. 2%
  are replayed with a **different body** (must get 422). Network errors and 5xx are retried with the same key, like a real client.
* **Kills a random worker with `kill -9` every 1–3 s** (`Process.Kill`: SIGKILL on Linux, TerminateProcess on Windows) and
  immediately starts a replacement, so the fleet stays at N. On Linux, with probability 0.25 per tick, it also **SIGSTOPs** a
  worker for 2×lease and then SIGCONTs it: a real zombie.
* **Workers freeze themselves** (`-freeze-prob`, default 2% of attempts) for a random [lease+0.5 s, 2·lease+1 s), *after*
  doing the work and *before* writing the result, and stop heartbeating while frozen. This is the worst moment for a
  stop-the-world pause, and it works on any OS.
* **Cancels** 10% of jobs: half at a random moment (usually still queued), half **aimed at jobs the harness sees running in
  the database** (the cancel/complete race).
* Job mix: 10–150 ms (95%), 1–4 s "training-like" (5%), and 5% that fail their first 1–5 attempts (`max_attempts=5`), so some
  recover and some exhaust.

When the load is done it keeps killing until the queue drains (10–15 min timeout, after which `none_stuck` fails), stops
everything, and checks the invariants **directly in the database** using the harness's own record of every HTTP answer.

## Invariants (each is a query whose every row is a violation)

| Invariant | Meaning |
|---|---|
| `no_lost_jobs` | every key the API acknowledged exists, with the id the API returned |
| `no_phantom_jobs` | the DB has no job the harness never asked for |
| `idempotency` | one id per key across all 2xx answers; ≤ 1 × 201 per key |
| `idempotency_mismatch` | same key + different body ⇒ 422, every time |
| `none_stuck` | no `queued`/`running` job after drain |
| `at_most_one_effect` | no job has > 1 side-effect row |
| `succeeded_has_exactly_one_effect` / `non_succeeded_has_no_effect` | effect ⇔ succeeded |
| `no_stale_write` | the effect's fence = newest run's fence = the succeeded run's fence (a zombie never won) |
| `no_double_claim` | a job's runs, ordered by fence, never overlap in time (`prev.ended_at ≤ next.claimed_at`, DB clock) |
| `all_runs_closed` | every attempt ended with an outcome (no orphaned execution) |
| `runs_match_attempts` | exactly one `job_runs` row per counted attempt |
| `terminal_run_matches_state` | succeeded ⇔ exactly one run with outcome `succeeded` |
| `cancel_wins` | `cancel_requested` ⇔ final state `cancelled` |
| `cancel_response_consistency` | 200/202 ⇒ cancelled; 409 ⇒ the state the 409 reported |
| `transition_log_consistent` / `_starts_queued` | the audit log (written by the DB trigger) replays to the current state; one `→running` per attempt |
| `failure_semantics` | a job failing all attempts never succeeds; `failed` ⇒ attempts exhausted |

## Evidence gate

A green run where nothing went wrong proves nothing. Every run prints counters showing *how often each failure path actually
happened* (kills, leases expired, zombie writes rejected, **fence-only rejections**, cancels by path, retries, duplicates).
If a path that was switched on never happened, the run is reported as **`INVARIANTS HELD BUT … NOT EXERCISED — NOT PROOF`**
(exit 3), not green. That gate exists because two of my runs were green while blind (see the history below).

## Negative controls: does the harness detect a broken queue?

Each switch breaks exactly one mechanism (`UNSAFE_MODE` in the worker, or `-effect-mode`):

| Control | What is broken | Caught by |
|---|---|---|
| `-unsafe racy-claim` | claim = `SELECT` then `UPDATE … WHERE id=$1` (no lock, no state predicate) | `no_double_claim`, `all_runs_closed`, `transition_log_consistent` |
| `-unsafe no-fence` | finish accepts any worker while the job is `running` | `no_stale_write`, `no_double_claim` |
| `-effect-mode external` | effect written outside the completion transaction (an external system) | `at_most_one_effect`, `succeeded_has_exactly_one_effect`, `non_succeeded_has_no_effect`, `no_stale_write` |

## Runs

Every run, including failed, blind and superseded ones, is listed in **[results/INDEX.md](../results/INDEX.md)** with its
raw `harness.log`, `workers.log`, `api.log` and `summary.json`. In total there are 30 runs.
Early development runs are in `results/dev-iterations/`.

### History (what failed, and what I learned)

| Stage | Runs | What happened |
|---|---|---|
| 1. smoke | `dev-iterations/*smoke` | Green, but **no cancel ever hit a running job**. The cancel/complete race was untested. Added targeted cancels (`smoke2`: 48 cancels on running jobs) |
| 2. first negative controls | `*neg-racy-claim`, `*neg-external-effect`, `*neg-no-fence` | racy-claim: caught (154 overlapping runs). external: caught (duplicate effects). **no-fence: GREEN. The harness was blind** |
| 3. making no-fence visible | `*neg-no-fence-v2`, `-v3`, `*pos-correct-zombies` | Still green with long jobs and randomized freezes. Added `fence_only_rejections` plus the evidence gate. The *correct* build was then reported "NOT PROOF": fencing had never been decisive in any run so far |
| 4. root cause | `*neg-no-fence-v4`, `*pos-correct-zombies-v2` | The reaper reset `run_after`, so re-queued jobs waited behind the whole backlog and were re-claimed only after the zombie woke. Kept `run_after`. no-fence now **fails** (20 stale writes) and the correct build passes with 22 fence-only rejections |
| 5. main 10k (pre-final build) | `*main-10k-run1` | All invariants held. 209 kills, 362 leases expired, 9 fence-only rejections |
| 6. load tests | `*load-*` | Connection exhaustion (5xx), fixed by pool sizing + 503. 6-round-trip finish held row locks while idle, so it moved into `finish_job()`. Run-to-run noise ±35% (see [VERDICT.md](VERDICT.md)) |
| 7. **final build** | `*FINAL-*` | Main 10k: **all invariants held**. All three negative controls caught (below) |

### Final build: negative controls

| Run | Exit | Violations |
|---|---|---|
| `FINAL-neg-racy-claim` | 1 | `no_double_claim`=100, `transition_log_consistent`=92, `all_runs_closed`=1 |
| `FINAL-neg-no-fence` | 1 | `no_double_claim`=17, `no_stale_write`=15, `all_runs_closed`=3 |
| `FINAL-neg-external-effect` | 1 | `no_stale_write`=86, `non_succeeded_has_no_effect`=52, `at_most_one_effect`=32, `succeeded_has_exactly_one_effect`=32 |

(In `no-fence` the evidence gate also reports `fence_only_rejections=0`. That is expected: with fencing switched off those
writes are *accepted* instead of rejected, and that is exactly the bug the violations show.)

### Final build: main run, raw output (`results/20261110T233654Z-FINAL-main-10k`)

10,000 jobs, 8 workers, kill -9 every 1–3 s. Windows, so no SIGSTOP; zombies come from the in-worker freeze. The
`queue_wait` of minutes is expected: 10k jobs are submitted in ~10 s and then drained by 8 workers at ~24 jobs/s.
`other:retry=1` under `cancel_paths` is a job that had failed an attempt, was back in `queued`, and was then cancelled. That is
correct behaviour, just a path my classifier didn't name.

```
[   0.00s] run 20261110T233654Z-FINAL-main-10k  os=windows  args=-db postgres://jobq:jobq@127.0.0.1:55432/jobq_chaos?sslmode=disable -jobs 10000 -workers 8 -out results -label FINAL-main-10k
[   0.00s] config jobs=10000 workers=8 kill-every=2s pause-prob=0.25(pause supported=false) dup=0.30 mismatch=0.02 cancel=0.10 fail=0.05 long=0.05/4000ms freeze=0.0200 lease=2s hb=500ms effect=tx unsafe=""
[... kill / progress lines omitted here, full log in harness.log ...]
[ 421.21s] progress posts=19208 cancels=1038 load_done=true cancelled=1029 failed=97 running=4 succeeded=8870 kills=211
[ 423.53s] chaos: kill -9 w216 (kills=212)
[ 425.28s] queue drained 7m4.063s after load start
[ 425.72s] ==================== RESULT ====================
[ 425.73s] jobs=10000 workers=8 wall=7m4.063s throughput=23.6 jobs/s  create latency p50=21ms p95=64.8ms p99=161.3ms max=1.147s (n=19425)
[ 425.76s] end-to-end (DB clock): queue_wait p50=189.776s p99=362.475s | created->succeeded p50=197.373s p99=409.491s max=413.979s
[ 425.76s] evidence (proves the failure paths were exercised):
  attempts_per_job  0=523 1=8695 2=369 3=115 4=81 5=217
  cancel_paths      cancelled_by_reaper_after_worker_death=13 cancelled_by_worker_while_running=492 cancelled_while_queued=523 other:retry=1
  cancel_responses  200_cancelled=524 202_cancel_requested=505 409_already_succeeded=10
  effects           jobs_with_effect=8871 total=8871
  final_states      cancelled=1029 failed=100 succeeded=8871
  harness           cancel_requests=1039 client_retries=0 dup_posts=9208 http_5xx=0 kills_sigkill=212 mismatch_posts=217 net_errors=0 pauses_sigstop=0 posts=19208 respawns=212 unexpected_worker_exits=0
  run_outcomes      cancelled=492 failed=93 lease_expired=381 retry=1350 succeeded=8871
  zombies           fence_only_rejections=11 late_writes_rejected_total=174 runs_with_late_write_rejected=174

[ 425.76s] PASS  no_lost_jobs (every acknowledged key exists with the acknowledged id)                           violations=0
[ 425.76s] PASS  no_phantom_jobs (db has no job the harness never asked for)                                     violations=0
[ 425.76s] PASS  idempotency (one job per key; all 2xx for a key carry the same id; <=1 x 201)                   violations=0
[ 425.76s] PASS  idempotency_mismatch (same key + different body => 422, job unchanged)                          violations=0
[ 425.76s] PASS  none_stuck (no queued/running job after drain)                                                  violations=0
[ 425.76s] PASS  at_most_one_effect (no job has >1 side effect)                                                  violations=0
[ 425.76s] PASS  succeeded_has_exactly_one_effect                                                                violations=0
[ 425.76s] PASS  non_succeeded_has_no_effect                                                                     violations=0
[ 425.76s] PASS  no_stale_write (effect comes from the newest run, which is the one that succeeded)              violations=0
[ 425.76s] PASS  no_double_claim (runs of a job never overlap in time)                                           violations=0
[ 425.76s] PASS  all_runs_closed (every attempt has an end and an outcome)                                       violations=0
[ 425.76s] PASS  runs_match_attempts (one job_runs row per counted attempt)                                      violations=0
[ 425.76s] PASS  terminal_run_matches_state (succeeded <=> exactly one succeeded run)                            violations=0
[ 425.76s] PASS  cancel_wins (cancel_requested <=> final state cancelled)                                        violations=0
[ 425.76s] PASS  transition_log_consistent (last logged state = current state; one running entry per attempt)    violations=0
[ 425.76s] PASS  transition_log_starts_queued                                                                    violations=0
[ 425.76s] PASS  failure_semantics (jobs that fail every attempt never succeed; failed => attempts exhausted)    violations=0
[ 425.76s] PASS  cancel_response_consistency (API answer matches final state)                                    violations=0
[ 425.76s] VERDICT: ALL INVARIANTS HELD
[ 425.76s] artifacts in results\20261110T233654Z-FINAL-main-10k
```

## What the harness does NOT catch

Be explicit about the blind spots. A green run says nothing about these:

1. **Postgres itself failing.** The database is never killed, restarted, failed over or partitioned. Every guarantee
   here assumes one durable primary. Async-replica failover can lose acknowledged commits: a job the API answered 201
   for could disappear, and a `succeeded` could revert to `running`, with fencing tokens reused after `nextval` rolls back
   on the new primary. **Untested.**
2. **The API process dying.** Only workers are killed. A crash between the INSERT commit and the HTTP response gives the
   client a network error. The client then retries with the same key and gets 200, which is correct but never exercised
   here (`net_errors=0` in every run).
3. **Real external side effects.** The effect is a row in the same database. That is exactly why it can be made exactly-once.
   With `-effect-mode external` the harness shows duplicates do appear, but it does not test an idempotent sink.
4. **Wall-clock jumps on the database host.** All lease maths uses `now()` on one server, and the overlap invariant uses
   `clock_timestamp()` on the same server. An NTP step on that host would shorten or extend every lease at once and could
   also fool the overlap check. Not injected.
5. **Long pauses inside a transaction.** Zombies are frozen *between* transactions. A worker frozen while holding the row
   lock in `Finish` blocks the cancel and the reaper on that row until `idle_in_transaction_session_timeout` (15 s) kills its
   session. That bound is configured but not exercised.
6. **Scale and time.** 10k jobs, 8 workers, minutes of runtime. It doesn't cover table bloat after millions of rows,
   autovacuum falling behind on the hot `jobs` rows, index growth from heartbeats, connection exhaustion with hundreds of
   workers, or a 10x arrival rate. The load test covers the rate question separately.
7. **Rare interleavings.** Kill timing is random, not exhaustive. A window of a few microseconds (for example, kill -9
   exactly between the effect INSERT and COMMIT) is covered by transactional atomicity *by argument*, not because the
   harness hit it. The harness proves the paths it reports in its evidence block, and only those.
8. **Semantic correctness of the job.** "Succeeded" means the emulated step returned. Nothing checks that a real training job
   would be deterministic or resumable on retry, which matters a lot for at-least-once execution of multi-hour jobs.
9. **Windows vs Linux.** On Windows there is no SIGSTOP, so zombies come only from the in-worker freeze hook. That hook
   freezes at one chosen point (after work, before the write), which is the worst point. A real SIGSTOP can land anywhere.
   The Docker/Linux run adds real SIGSTOP/SIGCONT.
10. **Starvation and fairness.** Order is checked for correctness, not fairness. A poison job that crashes every worker that
    takes it keeps its place at the head of the queue after each reap (deliberately), so it can kill up to `max_attempts`
    workers in a row.
