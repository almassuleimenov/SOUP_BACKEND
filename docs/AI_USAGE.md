# AI usage

**Tool:** Claude Code (Claude Opus), in an interactive session. It also used public skill prompts (golang-pro, postgres-pro,
chaos-engineer, test-master, code-reviewer, security-reviewer and others from `jeffallan/claude-skills`) as checklists.

**What the AI did:** it wrote most of the code and the first drafts of the documents. Design decisions (Postgres, leases +
fencing tokens, effect inside the completion transaction, cancel-wins) were discussed and accepted. Every claim in these
docs is backed by a harness run or a test in this repository. Re-run them, don't trust the prose.

## Where the AI's first version was wrong, and how it was caught

None of these produced an error message. Each was caught by looking at evidence, not by tests going red.

| # | First version | How it was caught | Fix |
|---|---|---|---|
| 1 | Audit-log trigger was `BEFORE INSERT`. It fires even for `INSERT … ON CONFLICT DO NOTHING`, so every duplicate POST would have written a phantom audit row | Reasoning about trigger semantics before the first run. Locked in by `TestIdempotentCreateConcurrent` (asserts 1 audit row after 50 concurrent creates) | `AFTER INSERT` trigger |
| 2 | Reaper closed the old run using `RETURNING (SELECT lease_token …)`. After the UPDATE that value is already NULL, so runs of dead workers would never have been closed | Self-review before running | Carry the fence out of the locking CTE |
| 3 | Harness v1 went green, but **no cancel ever hit a running job** (`cancel_paths: cancelled_while_queued=44`, nothing else). The cancel/complete race was not tested at all | Reading the evidence block, not the PASS lines | Targeted "cancel while running" in the harness |
| 4 | **The `no-fence` negative control passed green** three times. The harness could not tell a correct worker from one with fencing removed. Zombies always woke up after their successor had finished, so the plain `state='running'` check rejected them and fencing was never decisive | Negative controls (`results/dev-iterations/*neg-no-fence*`) | New `fence_only_rejections` counter + **evidence gate**: a run that never exercised a path is reported "NOT PROOF" (exit 3), not green |
| 5 | Even with long jobs, fencing stayed non-decisive. Root cause: **the reaper reset `run_after = now()`, so a reclaimed job went to the back of the queue** and waited out the whole backlog. That is also a real latency bug: a job whose worker died waits twice | The evidence gate from #4 | Reaper keeps the original `run_after` |
| 6 | Each worker opened a pool of 4 connections, and the API pool started cold | Load test: 93 × 5xx "connect timeout", then "too many clients" | Worker pool = 2 (loop + heartbeat), API pool pre-warmed, transient DB errors → 503 + Retry-After |
| 7 | Finish was a 6-round-trip transaction holding the job's row lock | Sampling `pg_stat_activity` at 1000 rps: 272/~400 samples "idle in transaction" | `finish_job()` PL/pgSQL, one round trip. Idle-in-tx went to 0. **Throughput gain was NOT demonstrated** (within noise) |
| 8 | A Finish retried after a lost COMMIT reply was counted as a zombie | Code review | Own-run check, `TestFinishRetryAfterAmbiguousCommit` |
| 9 | I drew conclusions from single load-test runs ("fsync +10%", "finish fix made it slower") | Repeating the same config: 304 / 448 / 592 jobs/s | Noise is ±35%. Those claims are withdrawn, and only repeatable effects are stated in VERDICT.md |

## What was verified by hand / by measurement

* Negative controls: `racy-claim`, `no-fence` and `external` effect mode each make the harness fail with the matching
  invariant (see [CHAOS.md](CHAOS.md)). Unit-level negative controls live in `internal/store/store_test.go`.
* Isolation-level claims (READ COMMITTED is enough; REPEATABLE READ / SERIALIZABLE only add 40001 retries) were measured by
  `TestClaimExactlyOnceUnderEveryIsolationLevel`, not taken on faith.
* Latency numbers in the verdict come from the open-loop load test, not from estimates.

## What I would still not trust without checking

* The harness was run on Windows (no SIGSTOP; zombies come from an in-process freeze hook). On Linux in Docker it also does
  real SIGSTOP/SIGCONT. The Linux numbers should be re-run before trusting them.
* AI-written prose tends to overstate. Wherever a doc says "guaranteed", the backing is a CHECK constraint, a trigger, or a
  harness invariant, and is named next to it. If it isn't named, treat it as a claim.
