-- 003_crash_budget.sql — a dead worker no longer consumes an execution attempt.
--
-- Before: `attempts` counted every claim, and the reaper failed a job once
-- attempts >= max_attempts. Three rolling deploys / OOM kills in a row turned
-- a healthy job into `failed` (found by an independent review).
--
-- Now two budgets:
--   failures — the job itself reported an error   (limit: max_attempts, client-visible)
--   crashes  — the lease expired: worker died/froze (limit: max_crashes, default 5)
-- `attempts` stays "number of claims" (= rows in job_runs), so the harness's
-- runs_match_attempts invariant is unchanged. A poison job that kills every
-- worker that takes it is still bounded, by max_crashes.

ALTER TABLE jobs
    ADD COLUMN failures    int NOT NULL DEFAULT 0,
    ADD COLUMN crashes     int NOT NULL DEFAULT 0,
    ADD COLUMN max_crashes int NOT NULL DEFAULT 5 CHECK (max_crashes BETWEEN 1 AND 100);

ALTER TABLE jobs DROP CONSTRAINT attempts_bounded;
ALTER TABLE jobs ADD CONSTRAINT budgets_bounded CHECK (
        failures BETWEEN 0 AND max_attempts
    AND crashes  BETWEEN 0 AND max_crashes
    AND attempts >= failures + crashes
    AND attempts <= max_attempts + max_crashes);

CREATE OR REPLACE FUNCTION finish_job(
    p_job         uuid,
    p_fence       bigint,
    p_worker      text,
    p_outcome     text,              -- 'success' | 'failure' | 'cancelled'
    p_result      jsonb,
    p_error       text,
    p_retry_secs  double precision,
    p_effect      boolean,
    p_check_fence boolean            -- false ONLY for the no-fence negative control
) RETURNS TABLE (applied boolean, new_state job_state)
LANGUAGE plpgsql AS $$
DECLARE
    j        jobs%ROWTYPE;
    own      text;
    nxt      job_state;
    run_out  text;
BEGIN
    SELECT * INTO j FROM jobs WHERE id = p_job FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'job % not found', p_job USING ERRCODE = 'no_data_found';
    END IF;

    IF j.state <> 'running' OR (p_check_fence AND j.lease_token IS DISTINCT FROM p_fence) THEN
        SELECT r.outcome INTO own FROM job_runs r WHERE r.fence = p_fence;
        IF own IS NOT NULL AND own NOT IN ('lease_expired', 'superseded') THEN
            RETURN QUERY SELECT true, j.state;   -- ambiguous-commit retry
            RETURN;
        END IF;
        UPDATE job_runs r SET
            late_write_rejected   = r.late_write_rejected + 1,
            fence_only_rejections = r.fence_only_rejections + CASE WHEN j.state = 'running' THEN 1 ELSE 0 END,
            ended_at              = COALESCE(r.ended_at, clock_timestamp()),
            outcome               = COALESCE(r.outcome, 'superseded')
        WHERE r.fence = p_fence;
        RETURN QUERY SELECT false, j.state;
        RETURN;
    END IF;

    IF j.cancel_requested THEN
        nxt := 'cancelled'; run_out := 'cancelled';
    ELSIF p_outcome = 'cancelled' THEN
        RAISE EXCEPTION 'job %: cancelled outcome without cancel request', p_job;
    ELSIF p_outcome = 'success' THEN
        nxt := 'succeeded'; run_out := 'succeeded';
    ELSIF j.failures + 1 >= j.max_attempts THEN      -- this failure exhausts the budget
        nxt := 'failed'; run_out := 'failed';
    ELSE
        nxt := 'queued'; run_out := 'retry';
    END IF;

    UPDATE jobs SET
        state            = nxt,
        failures         = failures + CASE WHEN run_out IN ('failed', 'retry') THEN 1 ELSE 0 END,
        result           = CASE WHEN nxt = 'succeeded' THEN p_result END,
        last_error       = CASE WHEN run_out IN ('failed', 'retry') THEN p_error ELSE last_error END,
        run_after        = CASE WHEN nxt = 'queued' THEN now() + make_interval(secs => p_retry_secs) ELSE run_after END,
        finished_at      = CASE WHEN nxt = 'queued' THEN NULL ELSE now() END,
        lease_token      = NULL,
        lease_owner      = NULL,
        lease_expires_at = NULL
    WHERE id = p_job;

    IF nxt = 'succeeded' AND p_effect THEN
        INSERT INTO job_effects (job_id, fence, worker_id) VALUES (p_job, p_fence, p_worker);
    END IF;

    UPDATE job_runs r SET ended_at = clock_timestamp(), outcome = run_out WHERE r.fence = p_fence;

    RETURN QUERY SELECT true, nxt;
END;
$$;
