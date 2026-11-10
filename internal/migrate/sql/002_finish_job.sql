-- 002_finish_job.sql — the fenced completion as ONE server-side call.
--
-- Why: the Go version needed 6 round trips (BEGIN, SELECT .. FOR UPDATE,
-- UPDATE jobs, INSERT effect, UPDATE job_runs, COMMIT) while holding the job's
-- row lock. Sampling pg_stat_activity at 1000 rps showed workers spending most
-- of their time "idle in transaction / ClientRead": the lock was held while
-- Postgres waited for a CPU-starved client. One call = one round trip, and the
-- row lock is held only for the duration of the function body.
--
-- Semantics are identical to the previous Go implementation (see git history /
-- store.Finish doc): lock row; reject stale fence (recording fence-only
-- rejections); ambiguous-commit retries report success; cancel_requested beats
-- success and failure; effect is written in the same transaction.

CREATE FUNCTION finish_job(
    p_job         uuid,
    p_fence       bigint,
    p_worker      text,
    p_outcome     text,              -- 'success' | 'failure' | 'cancelled'
    p_result      jsonb,
    p_error       text,
    p_retry_secs  double precision,
    p_effect      boolean,           -- write the side effect in this transaction
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
        -- Ambiguous commit: our earlier call committed, only the reply was lost.
        SELECT r.outcome INTO own FROM job_runs r WHERE r.fence = p_fence;
        IF own IS NOT NULL AND own NOT IN ('lease_expired', 'superseded') THEN
            RETURN QUERY SELECT true, j.state;
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
    ELSIF j.attempts >= j.max_attempts THEN
        nxt := 'failed'; run_out := 'failed';
    ELSE
        nxt := 'queued'; run_out := 'retry';
    END IF;

    UPDATE jobs SET
        state            = nxt,
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
