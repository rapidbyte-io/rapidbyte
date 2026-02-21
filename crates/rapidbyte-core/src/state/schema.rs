pub const CREATE_TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS sync_cursors (
    pipeline TEXT NOT NULL,
    stream TEXT NOT NULL,
    cursor_field TEXT,
    cursor_value TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (pipeline, stream)
);

CREATE TABLE IF NOT EXISTS sync_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    stream TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    records_read INTEGER DEFAULT 0,
    records_written INTEGER DEFAULT 0,
    bytes_read INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS dlq_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    stream_name TEXT NOT NULL,
    record_json TEXT NOT NULL,
    error_message TEXT NOT NULL,
    error_category TEXT NOT NULL,
    failed_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dlq_pipeline_run ON dlq_records (pipeline, run_id);
"#;
