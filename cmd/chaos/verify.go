package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// check is one invariant: zero violations = pass.
type check struct {
	Name       string   `json:"name"`
	Violations int      `json:"violations"`
	Examples   []string `json:"examples,omitempty"`
}

func (c *check) fail(format string, a ...any) {
	c.Violations++
	if len(c.Examples) < 5 {
		c.Examples = append(c.Examples, fmt.Sprintf(format, a...))
	}
}

type dbJob struct {
	id, key, state string
	attempts, max  int
	cancelReq      bool
	failAttempts   int
}

// sqlCheck runs a query whose every returned row is a violation.
func sqlCheck(ctx context.Context, pool *pgxpool.Pool, name, q string) check {
	c := check{Name: name}
	rows, err := pool.Query(ctx, q)
	if err != nil {
		c.fail("query error: %v", err)
		return c
	}
	defer rows.Close()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			c.fail("scan error: %v", err)
			continue
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			if u, ok := v.([16]byte); ok {
				parts[i] = fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
			} else {
				parts[i] = fmt.Sprint(v)
			}
		}
		c.fail("%s", strings.Join(parts, " | "))
	}
	if err := rows.Err(); err != nil {
		c.fail("rows error: %v", err)
	}
	return c
}

func verify(ctx context.Context, pool *pgxpool.Pool, specs []jobSpec, l *loader) ([]check, error) {
	rows, err := pool.Query(ctx, `SELECT id::text, idempotency_key, state::text, attempts, max_attempts,
		cancel_requested, COALESCE((payload->>'fail_attempts')::int, 0) FROM jobs`)
	if err != nil {
		return nil, fmt.Errorf("load jobs: %w", err)
	}
	byKey := map[string]dbJob{}
	for rows.Next() {
		var j dbJob
		if err := rows.Scan(&j.id, &j.key, &j.state, &j.attempts, &j.max, &j.cancelReq, &j.failAttempts); err != nil {
			rows.Close()
			return nil, err
		}
		byKey[j.key] = j
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var checks []check

	// --- No job is lost; idempotency holds ------------------------------
	lost := check{Name: "no_lost_jobs (every acknowledged key exists with the acknowledged id)"}
	idem := check{Name: "idempotency (one job per key; all 2xx for a key carry the same id; <=1 x 201)"}
	mism := check{Name: "idempotency_mismatch (same key + different body => 422, job unchanged)"}
	cancelC := check{Name: "cancel_response_consistency (API answer matches final state)"}
	for _, s := range specs {
		rec := l.recs[s.Key]
		j, ok := byKey[s.Key]
		if rec.acked && !ok {
			lost.fail("key %s acknowledged but missing", s.Key)
			continue
		}
		if len(rec.ids) > 1 {
			idem.fail("key %s returned %d different ids: %v", s.Key, len(rec.ids), rec.ids)
		}
		for id := range rec.ids {
			if ok && id != j.id {
				lost.fail("key %s acknowledged id %s but db has %s", s.Key, id, j.id)
			}
		}
		if rec.created > 1 {
			idem.fail("key %s got %d x 201 Created", s.Key, rec.created)
		}
		if s.Mismatch && (rec.mismatch422 != 1 || len(rec.mismatchOther) > 0) {
			mism.fail("key %s mismatch replay: 422=%d other=%v", s.Key, rec.mismatch422, rec.mismatchOther)
		}
		if rec.cancelCode != 0 && ok {
			switch rec.cancelCode {
			case 200, 202:
				if j.state != "cancelled" {
					cancelC.fail("key %s cancel answered %d/%s but final state %s", s.Key, rec.cancelCode, rec.cancelOut, j.state)
				}
			case 409:
				if j.state != rec.cancelState || j.state == "cancelled" {
					cancelC.fail("key %s cancel answered 409 state=%s but final %s", s.Key, rec.cancelState, j.state)
				}
			default:
				cancelC.fail("key %s cancel answered unexpected %d", s.Key, rec.cancelCode)
			}
		}
	}
	phantom := check{Name: "no_phantom_jobs (db has no job the harness never asked for)"}
	known := make(map[string]bool, len(specs))
	for _, s := range specs {
		known[s.Key] = true
	}
	for k := range byKey {
		if !known[k] {
			phantom.fail("unexpected key %s", k)
		}
	}

	// --- Semantics that need payload knowledge ----------------------------
	semantics := check{Name: "failure_semantics (jobs that fail every attempt never succeed; failed => attempts exhausted)"}
	for _, j := range byKey {
		if j.failAttempts >= j.max && j.state == "succeeded" {
			semantics.fail("job %s fails all %d attempts but succeeded", j.id, j.max)
		}
		if j.state == "failed" && j.attempts != j.max {
			semantics.fail("job %s failed after %d/%d attempts", j.id, j.attempts, j.max)
		}
	}
	checks = append(checks, lost, phantom, idem, mism)

	// --- Pure database invariants ---------------------------------------
	checks = append(checks,
		sqlCheck(ctx, pool, "none_stuck (no queued/running job after drain)",
			`SELECT id, state, attempts, lease_owner, lease_expires_at FROM jobs WHERE state IN ('queued','running')`),
		sqlCheck(ctx, pool, "at_most_one_effect (no job has >1 side effect)",
			`SELECT job_id, count(*), array_agg(fence ORDER BY fence) FROM job_effects GROUP BY job_id HAVING count(*) > 1`),
		sqlCheck(ctx, pool, "succeeded_has_exactly_one_effect",
			`SELECT j.id, count(e.id) FROM jobs j LEFT JOIN job_effects e ON e.job_id = j.id
			 WHERE j.state = 'succeeded' GROUP BY j.id HAVING count(e.id) <> 1`),
		sqlCheck(ctx, pool, "non_succeeded_has_no_effect",
			`SELECT j.id, j.state, count(*) FROM jobs j JOIN job_effects e ON e.job_id = j.id
			 WHERE j.state <> 'succeeded' GROUP BY j.id, j.state`),
		sqlCheck(ctx, pool, "no_stale_write (effect comes from the newest run, which is the one that succeeded)",
			`SELECT e.job_id, e.fence AS effect_fence, r.max_fence, r.succ_fence
			 FROM job_effects e JOIN (
			   SELECT job_id, max(fence) AS max_fence,
			          max(fence) FILTER (WHERE outcome = 'succeeded') AS succ_fence
			   FROM job_runs GROUP BY job_id) r ON r.job_id = e.job_id
			 WHERE e.fence <> r.max_fence OR r.succ_fence IS NULL OR e.fence <> r.succ_fence`),
		sqlCheck(ctx, pool, "no_double_claim (runs of a job never overlap in time)",
			`SELECT job_id, prev_fence, fence, prev_end, claimed_at FROM (
			   SELECT job_id, fence, claimed_at,
			          lag(fence)    OVER w AS prev_fence,
			          lag(ended_at) OVER w AS prev_end
			   FROM job_runs WINDOW w AS (PARTITION BY job_id ORDER BY fence)) x
			 WHERE prev_fence IS NOT NULL AND (prev_end IS NULL OR prev_end > claimed_at)`),
		sqlCheck(ctx, pool, "all_runs_closed (every attempt has an end and an outcome)",
			`SELECT fence, job_id, worker_id, claimed_at FROM job_runs WHERE ended_at IS NULL OR outcome IS NULL`),
		sqlCheck(ctx, pool, "runs_match_attempts (one job_runs row per counted attempt)",
			`SELECT j.id, j.attempts, count(r.fence) FROM jobs j LEFT JOIN job_runs r ON r.job_id = j.id
			 GROUP BY j.id, j.attempts HAVING count(r.fence) <> j.attempts`),
		sqlCheck(ctx, pool, "terminal_run_matches_state (succeeded <=> exactly one succeeded run)",
			`SELECT j.id, j.state, count(r.fence) FILTER (WHERE r.outcome = 'succeeded') AS succ_runs
			 FROM jobs j LEFT JOIN job_runs r ON r.job_id = j.id GROUP BY j.id, j.state
			 HAVING (j.state = 'succeeded') <> (count(r.fence) FILTER (WHERE r.outcome = 'succeeded') = 1)
			     OR count(r.fence) FILTER (WHERE r.outcome = 'succeeded') > 1`),
		sqlCheck(ctx, pool, "cancel_wins (cancel_requested <=> final state cancelled)",
			`SELECT id, state, cancel_requested FROM jobs WHERE cancel_requested <> (state = 'cancelled')`),
		sqlCheck(ctx, pool, "transition_log_consistent (last logged state = current state; one running entry per attempt)",
			`SELECT j.id, j.state, t.last_state, t.runs, j.attempts FROM jobs j JOIN (
			   SELECT job_id,
			          (array_agg(to_state ORDER BY id DESC))[1] AS last_state,
			          count(*) FILTER (WHERE to_state = 'running') AS runs
			   FROM job_transitions GROUP BY job_id) t ON t.job_id = j.id
			 WHERE t.last_state <> j.state OR t.runs <> j.attempts`),
		sqlCheck(ctx, pool, "transition_log_starts_queued",
			`SELECT j.id FROM jobs j WHERE NOT EXISTS (
			   SELECT 1 FROM job_transitions t WHERE t.job_id = j.id AND t.from_state IS NULL AND t.to_state = 'queued')`),
	)
	checks = append(checks, semantics, cancelC)
	return checks, nil
}

// evidence collects counters proving the chaos actually exercised the
// failure paths. A green run with zero reaped leases would prove nothing.
func evidence(ctx context.Context, pool *pgxpool.Pool) (map[string]any, error) {
	ev := map[string]any{}
	q := func(name, sql string) error {
		rows, err := pool.Query(ctx, sql)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		defer rows.Close()
		m := map[string]int64{}
		for rows.Next() {
			var k string
			var v int64
			if err := rows.Scan(&k, &v); err != nil {
				return err
			}
			m[k] = v
		}
		ev[name] = m
		return rows.Err()
	}
	for _, x := range []struct{ name, sql string }{
		{"final_states", `SELECT state::text, count(*) FROM jobs GROUP BY 1`},
		{"attempts_per_job", `SELECT attempts::text, count(*) FROM jobs GROUP BY 1`},
		{"run_outcomes", `SELECT COALESCE(outcome, 'OPEN'), count(*) FROM job_runs GROUP BY 1`},
		{"effects", `SELECT 'total', count(*) FROM job_effects UNION ALL
		             SELECT 'jobs_with_effect', count(DISTINCT job_id) FROM job_effects`},
		{"zombies", `SELECT 'runs_with_late_write_rejected', count(*) FROM job_runs WHERE late_write_rejected > 0
		             UNION ALL SELECT 'late_writes_rejected_total', COALESCE(sum(late_write_rejected), 0) FROM job_runs
		             UNION ALL SELECT 'fence_only_rejections', COALESCE(sum(fence_only_rejections), 0) FROM job_runs`},
		{"cancel_paths", `SELECT CASE WHEN r.outcome IS NULL THEN 'cancelled_while_queued'
		                              WHEN r.outcome = 'cancelled' THEN 'cancelled_by_worker_while_running'
		                              WHEN r.outcome = 'lease_expired' THEN 'cancelled_by_reaper_after_worker_death'
		                              ELSE 'other:' || r.outcome END, count(*)
		                  FROM jobs j LEFT JOIN LATERAL (
		                      SELECT outcome FROM job_runs WHERE job_id = j.id ORDER BY fence DESC LIMIT 1) r ON true
		                  WHERE j.state = 'cancelled' GROUP BY 1`},
	} {
		if err := q(x.name, x.sql); err != nil {
			return nil, err
		}
	}
	return ev, nil
}

func formatEvidence(ev map[string]any) string {
	var b strings.Builder
	names := make([]string, 0, len(ev))
	for k := range ev {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, n := range names {
		m, ok := ev[n].(map[string]int64)
		if !ok {
			fmt.Fprintf(&b, "  %-14s %v\n", n, ev[n])
			continue
		}
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = fmt.Sprintf("%s=%d", k, m[k])
		}
		fmt.Fprintf(&b, "  %-17s %s\n", n, strings.Join(parts, " "))
	}
	return b.String()
}
