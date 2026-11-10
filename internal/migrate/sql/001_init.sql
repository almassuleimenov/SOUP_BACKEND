-- 001_init.sql — job queue schema.
--
-- Design rules enforced here (not only in Go code), so a bug in any client
-- fails loudly instead of silently corrupting state:
--   * the state machine is validated by a trigger (illegal transitions raise);
--   * terminal rows are immutable;
--   * lease columns are set iff state = 'running' (CHECK constraint);
--   * every state change is appended to job_transitions (audit trail).

CREATE TYPE job_state AS ENUM ('queued', 'running', 'succeeded', 'failed', 'cancelled');

-- Fencing tokens. Monotonic across the whole cluster: a newer claim always
-- has a larger token than any older claim of the same job.
CREATE SEQUENCE job_fence_seq AS bigint;

CREATE TABLE jobs (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    seq              bigint      GENERATED ALWAYS AS IDENTITY UNIQUE, -- keyset pagination
    idempotency_key  text        NOT NULL UNIQUE
                                 CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    request_hash     bytea       NOT NULL,           -- sha256 of the canonical request
    job_type         text        NOT NULL,
    payload          jsonb       NOT NULL,
    state            job_state   NOT NULL DEFAULT 'queued',
    attempts         int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    cancel_requested boolean     NOT NULL DEFAULT false,
    lease_token      bigint,                         -- fencing token of the current run
    lease_owner      text,
    lease_expires_at timestamptz,
    run_after        timestamptz NOT NULL DEFAULT now(), -- retry backoff
    last_error       text,
    result           jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    finished_at      timestamptz,

    CONSTRAINT attempts_bounded CHECK (attempts BETWEEN 0 AND max_attempts),
    CONSTRAINT lease_iff_running CHECK (
        (state = 'running') = (lease_token IS NOT NULL
                               AND lease_owner IS NOT NULL
                               AND lease_expires_at IS NOT NULL)),
    CONSTRAINT finished_iff_terminal CHECK (
        (state IN ('succeeded', 'failed', 'cancelled')) = (finished_at IS NOT NULL))
);

-- Claim path: oldest runnable queued job. Partial => tiny, only hot rows.
CREATE INDEX jobs_claim_idx  ON jobs (run_after, seq) WHERE state = 'queued';
-- Reaper path: running jobs ordered by lease expiry.
CREATE INDEX jobs_lease_idx  ON jobs (lease_expires_at) WHERE state = 'running';
-- List with state filter.
CREATE INDEX jobs_state_seq_idx ON jobs (state, seq DESC);

-- One row per claim (= per execution attempt that started).
CREATE TABLE job_runs (
    fence               bigint      PRIMARY KEY,
    job_id              uuid        NOT NULL REFERENCES jobs (id),
    attempt             int         NOT NULL,
    worker_id           text        NOT NULL,
    claimed_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    ended_at            timestamptz,
    outcome             text CHECK (outcome IN
                            ('succeeded', 'failed', 'retry', 'cancelled',
                             'lease_expired', 'superseded')),
    late_write_rejected int         NOT NULL DEFAULT 0, -- zombie writes refused (any reason)
    -- ...of which refused ONLY by the fencing token: the job was running under
    -- a newer owner, so a state check alone would have accepted the write.
    fence_only_rejections int       NOT NULL DEFAULT 0,
    UNIQUE (job_id, attempt)
);

-- The emulated side effect of "executing" a job.
-- Deliberately NO unique constraint on job_id: the chaos harness must measure
-- the claim/fencing protocol, not a constraint that would mask its bugs.
-- (In production you would add UNIQUE (job_id) as defence in depth.)
CREATE TABLE job_effects (
    id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id     uuid        NOT NULL REFERENCES jobs (id),
    fence      bigint      NOT NULL,
    worker_id  text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX job_effects_job_idx ON job_effects (job_id);

CREATE TABLE job_transitions (
    id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id     uuid        NOT NULL,
    from_state job_state,
    to_state   job_state   NOT NULL,
    fence      bigint,
    at         timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX job_transitions_job_idx ON job_transitions (job_id, id);

-- AFTER INSERT: fires only for rows actually inserted (not for
-- INSERT ... ON CONFLICT DO NOTHING duplicates, unlike a BEFORE trigger).
CREATE FUNCTION jobs_log_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state <> 'queued' THEN
        RAISE EXCEPTION 'job % must be created as queued, got %', NEW.id, NEW.state
            USING ERRCODE = 'check_violation';
    END IF;
    INSERT INTO job_transitions (job_id, from_state, to_state) VALUES (NEW.id, NULL, 'queued');
    RETURN NULL;
END;
$$;

CREATE FUNCTION jobs_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state IN ('succeeded', 'failed', 'cancelled') THEN
        RAISE EXCEPTION 'job % is terminal (%) and immutable', OLD.id, OLD.state
            USING ERRCODE = 'check_violation';
    END IF;

    IF NEW.state IS DISTINCT FROM OLD.state THEN
        IF NOT (
               (OLD.state = 'queued'  AND NEW.state IN ('running', 'cancelled'))
            OR (OLD.state = 'running' AND NEW.state IN ('queued', 'succeeded', 'failed', 'cancelled'))
        ) THEN
            RAISE EXCEPTION 'illegal transition % -> % for job %', OLD.state, NEW.state, OLD.id
                USING ERRCODE = 'check_violation';
        END IF;
        INSERT INTO job_transitions (job_id, from_state, to_state, fence)
        VALUES (NEW.id, OLD.state, NEW.state, COALESCE(NEW.lease_token, OLD.lease_token));
    END IF;

    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER jobs_log_ins   AFTER INSERT  ON jobs FOR EACH ROW EXECUTE FUNCTION jobs_log_insert();
CREATE TRIGGER jobs_guard_upd BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION jobs_guard();
