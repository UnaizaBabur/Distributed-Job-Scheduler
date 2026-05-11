-- ============================================================================
-- Distributed Job Scheduler - PostgreSQL Schema
-- ============================================================================
-- Target: Amazon RDS PostgreSQL 15+
-- Purpose: Persist job lifecycle state for the serverless scheduling pipeline.
--
-- Design notes:
--  * jobs table = source of truth for job lifecycle (one row per job_id).
--  * job_events table = append-only audit log (one row per state transition).
--    This split lets us answer "current status?" cheaply (jobs) AND "what
--    happened to this job over time?" (job_events) without one denormalised
--    bloated table.
--  * Status is an ENUM, not a free-text column, so the database itself rejects
--    invalid transitions caused by buggy Lambda code.
-- ============================================================================

CREATE TYPE job_status AS ENUM (
    'submitted',   -- job arrived in queue, not yet picked up
    'processing',  -- a Lambda invocation has claimed the job
    'succeeded',   -- job completed successfully
    'failed',      -- job failed (may still be retried)
    'dlq'          -- job exhausted retries and was routed to DLQ
);

-- ----------------------------------------------------------------------------
-- jobs: current state of every job that has entered the system
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jobs (
    job_id          UUID PRIMARY KEY,
    job_type        VARCHAR(64)  NOT NULL,            -- e.g. 'image_resize', 'send_email'
    payload         JSONB        NOT NULL,            -- arbitrary task payload
    status          job_status   NOT NULL DEFAULT 'submitted',
    retry_count     INTEGER      NOT NULL DEFAULT 0,
    max_retries     INTEGER      NOT NULL DEFAULT 3,
    last_error      TEXT,                             -- stack trace / error msg
    submitted_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    sqs_message_id  VARCHAR(128)                      -- for tracing back to SQS
);

-- Indexes for the dashboard's most common queries
CREATE INDEX IF NOT EXISTS idx_jobs_status        ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_submitted_at  ON jobs(submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_status_time   ON jobs(status, submitted_at DESC);

-- ----------------------------------------------------------------------------
-- job_events: append-only audit trail
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_events (
    event_id     BIGSERIAL    PRIMARY KEY,
    job_id       UUID         NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    event_type   VARCHAR(32)  NOT NULL,   -- 'submitted','started','retry','succeeded','failed','dlq'
    detail       TEXT,                    -- optional human-readable detail
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_job_id     ON job_events(job_id);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON job_events(created_at DESC);

-- ----------------------------------------------------------------------------
-- Helper view: dashboard summary (counts by status)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_status_counts AS
SELECT status, COUNT(*) AS n
FROM jobs
GROUP BY status;

-- ----------------------------------------------------------------------------
-- Helper view: recent failures (last 24h) for the DLQ panel
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_recent_dlq AS
SELECT job_id, job_type, retry_count, last_error, finished_at
FROM jobs
WHERE status = 'dlq'
  AND finished_at > NOW() - INTERVAL '24 hours'
ORDER BY finished_at DESC;
