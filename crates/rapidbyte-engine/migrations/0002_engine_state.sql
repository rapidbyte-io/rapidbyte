CREATE TABLE IF NOT EXISTS sync_cursors (
    pipeline     TEXT NOT NULL,
    stream       TEXT NOT NULL,
    cursor_field TEXT,
    cursor_value TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, stream)
);

CREATE TABLE IF NOT EXISTS sync_runs (
    id              BIGSERIAL PRIMARY KEY,
    pipeline        TEXT NOT NULL,
    stream          TEXT NOT NULL,
    status          TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    records_read    BIGINT DEFAULT 0,
    records_written BIGINT DEFAULT 0,
    bytes_read      BIGINT DEFAULT 0,
    bytes_written   BIGINT DEFAULT 0,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS dlq_records (
    id             BIGSERIAL PRIMARY KEY,
    pipeline       TEXT NOT NULL,
    run_id         BIGINT NOT NULL REFERENCES sync_runs(id),
    stream_name    TEXT NOT NULL,
    record_json    TEXT NOT NULL,
    error_message  TEXT NOT NULL,
    error_category TEXT NOT NULL,
    failed_at      TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_pipeline ON sync_runs(pipeline, stream);
CREATE INDEX IF NOT EXISTS idx_dlq_pipeline_run ON dlq_records(pipeline, run_id);
