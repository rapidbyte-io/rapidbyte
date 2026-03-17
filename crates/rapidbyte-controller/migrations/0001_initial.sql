CREATE SEQUENCE IF NOT EXISTS lease_epoch_seq;

CREATE TABLE IF NOT EXISTS runs (
    id               TEXT PRIMARY KEY,
    idempotency_key  TEXT UNIQUE,
    pipeline_name    TEXT NOT NULL,
    pipeline_yaml    TEXT NOT NULL,
    state            TEXT NOT NULL DEFAULT 'pending',
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    attempt          INTEGER NOT NULL DEFAULT 1,
    max_retries      INTEGER NOT NULL DEFAULT 0,
    timeout_seconds  BIGINT,
    error_code       TEXT,
    error_message    TEXT,
    rows_read        BIGINT,
    rows_written     BIGINT,
    bytes_read       BIGINT,
    bytes_written    BIGINT,
    duration_ms      BIGINT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_runs_state ON runs(state);
CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC);

CREATE TABLE IF NOT EXISTS tasks (
    id               TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    attempt          INTEGER NOT NULL,
    state            TEXT NOT NULL DEFAULT 'pending',
    agent_id         TEXT,
    lease_epoch      BIGINT,
    lease_expires_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(run_id, attempt)
);

CREATE INDEX IF NOT EXISTS idx_tasks_poll ON tasks(created_at ASC) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS idx_tasks_lease ON tasks(lease_expires_at) WHERE state = 'running';

CREATE TABLE IF NOT EXISTS agents (
    id                   TEXT PRIMARY KEY,
    plugins              TEXT[] NOT NULL DEFAULT '{}',
    max_concurrent_tasks INTEGER NOT NULL DEFAULT 1,
    last_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    registered_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agents_last_seen ON agents(last_seen_at);
