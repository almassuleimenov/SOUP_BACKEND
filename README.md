# jobq: a training-job queue that has to survive kill -9

Go + PostgreSQL. HTTP API, N worker processes, and a chaos harness that kills workers and then checks invariants.

| Document | What's in it |
|---|---|
| [docs/DESIGN.md](docs/DESIGN.md) | One-page design: claiming, dead-worker detection, cancel semantics, the guarantee and why |
| [docs/CHAOS.md](docs/CHAOS.md) | The harness, the invariants, **all runs including failed ones**, what it does NOT catch |
| [docs/SILENT_FAILURES.md](docs/SILENT_FAILURES.md) | Double claim, zombie, cancel/complete race, retry duplicates, isolation level, clock skew: how each was checked |
| [docs/VERDICT.md](docs/VERDICT.md) | SHIP / DON'T SHIP at 100 rps, load-test numbers, what to fix first |
| [docs/AI_USAGE.md](docs/AI_USAGE.md) | Where AI was used, what I accepted, what I verified or redid |
| [results/](results/) | Raw harness output (`harness.log`, `workers.log`, `api.log`, `summary.json`) for every run |

## Run

```sh
docker compose up --build            # Postgres + API on :8080 + 4 workers
docker compose --profile chaos run --rm chaos                       # 10k jobs, 8 workers, kill -9 every ~2 s
docker compose --profile chaos run --rm chaos -jobs 1000 -unsafe no-fence -freeze-prob 0.1 -long-rate 0.2   # negative control
```

```sh
curl -s -XPOST localhost:8080/v1/jobs -H 'Idempotency-Key: demo-1' \
     -d '{"type":"train","payload":{"duration_ms":2000}}'
curl -s localhost:8080/v1/jobs/<id>
curl -s 'localhost:8080/v1/jobs?state=running&limit=20'
curl -s -XPOST localhost:8080/v1/jobs/<id>/cancel
curl -s localhost:8080/metrics | grep ^jobq_
```

Tests (need a Postgres; integration tests skip without `TEST_DATABASE_URL`):

```sh
docker compose up -d db               # Postgres is published on localhost:5432 (dev only)
TEST_DATABASE_URL=postgres://jobq:jobq@localhost:5432/jobq_test?sslmode=disable go test -race -p 1 ./...   # -p 1: packages share the test DB
```

## What surprised me, and what still concerns me

What surprised me most was how easily the harness went green while testing nothing. The first version never cancelled a running
job. Worse, the `no-fence` negative control (fencing deliberately removed) **passed three runs in a row**, because the zombie
always woke after its successor had finished and the plain `state='running'` check caught it, so fencing was never the
deciding check. Even after I added long jobs it stayed blind. The reason was a real bug: the reaper reset `run_after`, which sent
every re-queued job to the back of the backlog. That bug was invisible to every invariant and only showed up once I made the
harness count *why* each zombie write was rejected (`fence_only_rejections`) and refuse to call a run proof when a path never
happened. The second surprise was the measurement noise: the same load test gave 304, 448 and 592 jobs/s, so two of my
single-run "findings" (fsync, finish latency) didn't hold up. What still concerns me: everything rests on one Postgres primary
and its clock, and the harness never kills the database, fails it over or steps its clock. Exactly-once effects are only real
for effects inside that database. The 3 s lease that is fine here would re-run hours of real training after a 3 s pause. And
the tables have no retention, so at 100 rps the service is correct today and degrading within weeks.

## Layout

```
cmd/server     HTTP API
cmd/worker     one worker = one process = one job at a time
cmd/chaos      fault-injection harness + invariant checker
internal/store all SQL; every state transition lives here
internal/worker claim → execute (heartbeat) → fenced finish
internal/api   handlers, validation, metrics
internal/migrate/sql/001_init.sql   schema, state-machine trigger, constraints
```

## Worker configuration (env)

`LEASE` (3s) · `HEARTBEAT` (750ms, ≤ LEASE/3) · `REAP_EVERY` (1s) · `RETRY_BASE`/`RETRY_MAX` (200ms/10s) ·
`EFFECT_MODE` (`tx` | `external`) · chaos only: `CHAOS_FREEZE_PROB`, `CHAOS_FREEZE_FOR`, `UNSAFE_MODE` (`no-fence` | `racy-claim`).
API: `HTTP_ADDR`, `DB_MAX_CONNS`, `API_TOKEN` (optional bearer auth).
