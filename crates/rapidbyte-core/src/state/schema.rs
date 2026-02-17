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
"#;
