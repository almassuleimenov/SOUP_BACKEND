# AI usage

**Tool:** Claude Code (Claude Opus), in an interactive session. It also used public skill prompts (golang-pro, postgres-pro,
chaos-engineer, test-master, code-reviewer, security-reviewer and others from `jeffallan/claude-skills`) as checklists.

**What the AI did:** it wrote most of the code and the first drafts of the documents. It also proposed the design
decisions (Postgres, leases + fencing tokens, effect inside the completion transaction, cancel-wins), and I accepted them.
Every claim in these docs is backed by a harness run or a test in this repository. Re-run them, don't trust the prose.

## What I did myself

* **Picked and installed the tooling.** I found the `jeffallan/claude-skills` repository and chose which skills fit this
  task: Go, PostgreSQL/SQL, architecture and API design, security review, code review, debugging, testing, DevOps,
  monitoring, SRE. I had them installed globally so they're available in every session.
* **Set up the environment** for working with Claude Code on my Windows machine: the Go toolchain, the MSYS2 gcc that
  `go test -race` needs, and the git repository with the GitHub remote. Docker isn't installed on this machine. Because of
  that, every run in `results/` was made on Windows against an embedded Postgres, and the `docker compose` path still has
  to be verified (see below).
* **Set the direction.** I gave Claude the task spec and asked for the strongest solution to it, not just a passing one.
* **Checked the answers.** I followed the progress, asked for a full explanation of the design and results in Russian so I
  could understand them, and reviewed the documents. I noticed that this file showed only the AI's part and asked for my
  own part to be written down honestly. I also compared the local working copy with the GitHub version.
* **Had the work independently reviewed and acted on it.** I got an independent review of the code and docs. The
  review found a real bug that neither the AI nor the harness had caught: the reaper ran only between jobs (row 10 below). It
  also found docs promising more than the code did, plus several known risks. I brought the findings back and asked for them
  to be **fixed in code, not only documented**: the reaper, the crash budget, the production build without chaos switches,
  `/metrics` auth and cost, and payload validation. Each fix has a test, and the reaper and metrics tests were also run
  against the old code to show that they fail there. See VERDICT.md.
* **Got a Linux/Docker run without Docker locally.** Because there is no Docker on my machine, I added a GitHub Actions
  workflow (`.github/workflows/compose-chaos.yml`, approach suggested in the review). On `ubuntu-latest` it runs
  `docker compose up --build`, a smoke test, `go test -race` on Linux and the 10k chaos harness via `docker compose`
  (real kill -9 and SIGSTOP), and uploads the raw output as an artifact. I pushed, the run passed
  ([run 43115919523](https://github.com/almassuleimenov/SOUP_BACKEND/actions/runs/43115919523), commit `8899edf`: 10,000 jobs, 200 × kill -9, 39 × SIGSTOP, all invariants
  held; 28 tests pass with `-race` on Linux), and I downloaded the artifact and added it to `results/`.
* **Owned the repository.** I make and push every commit myself.

I did not write the Go code or the SQL by hand, and I did not run the harness or the tests from my own terminal. Those
commands ran inside the Claude Code session, and their raw output is in `results/`.

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
| 10 | **The reaper only ran between jobs** (called from the main loop before `Claim`). With every worker busy on long jobs, a dead worker's job stayed `running` with an expired lease until someone finished (~21 s in the reviewer's repro with 25 s jobs). DESIGN.md claimed 3–4 s detection regardless | **Independent review**, not the harness: harness jobs last ≤ 4 s, so some worker was always free soon | Reaper in its own goroutine (3rd DB connection). `TestDeadWorkerReapedWhileAllWorkersBusy` fails on the old code and passes on the new one. Docs corrected |
| 11 | **The queue metrics never worked.** The collector's SQL had `FILTER` attached to `EXTRACT` instead of `min()`, a syntax error since the first version. Every scrape silently returned `jobq_queue_scrape_ok 0`, and VERDICT.md proposed paging on those very metrics | Found only by running `EXPLAIN ANALYZE` on the rewritten query while fixing a review item. No harness run scrapes `/metrics` | Fixed. `TestQueueCollectorAgainstPostgres` runs the real query; it was run against the old SQL and fails there |
| 12 | One unexplained failure of `TestDeadWorkerReapedWhileAllWorkersBusy` ("did not reach running within 5s") in the first full-suite run after the crash-budget change | Not reproduced in 4 later full runs with `-race` | Root cause **unknown**. Worker logs are now routed into the test log, so a recurrence (e.g. in CI) shows what happened. I am not claiming it is fixed |
| 13 | **Crash counter disagreed with the run history.** A worker whose heartbeat *timed out* under load self-fenced and closed its run as `superseded`; the reaper then counted the crash but only labelled runs that were still open. 1 job out of 10,000 | The new harness invariant `budgets_match_runs`, in `FINAL2-main-10k` (1 of 10,000 jobs) and `FINAL2-busy-fleet` (2 of 80), both exit 1, raw output kept. Not by any unit test or review | Fixed: the reaper relabels a self-fenced run as `lease_expired` in the same statement that counts the crash. `TestSelfFencedRunIsLabelledByReaper` fails on the old code and passes on the new. Rerun as `FINAL3-*`. Effects and states were never wrong; only the accounting was |

## What was verified by hand / by measurement

* Negative controls: `racy-claim`, `no-fence` and `external` effect mode each make the harness fail with the matching
  invariant (see [CHAOS.md](CHAOS.md)). Unit-level negative controls live in `internal/store/store_test.go`.
* Isolation-level claims (READ COMMITTED is enough; REPEATABLE READ / SERIALIZABLE only add 40001 retries) were measured by
  `TestClaimExactlyOnceUnderEveryIsolationLevel`, not taken on faith.
* Latency numbers in the verdict come from the open-loop load test, not from estimates.

## Still to do before submission

* Link the Actions run in the submission email. (The Linux/Docker run itself is done and committed.)

## What I would still not trust without checking

* Most harness runs were on Windows (no SIGSTOP; zombies come from an in-process freeze hook). The Linux/Docker run (with
  real SIGSTOP/SIGCONT) is a single 10k run on a shared CI runner: one green run, not a statistic. Its latency numbers are
  from a different machine and are not comparable to the laptop numbers.
* AI-written prose tends to overstate. Wherever a doc says "guaranteed", the backing is a CHECK constraint, a trigger, or a
  harness invariant, and is named next to it. If it isn't named, treat it as a claim.
