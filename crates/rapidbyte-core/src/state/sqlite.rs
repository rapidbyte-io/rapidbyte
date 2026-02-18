use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use rusqlite::Connection;

use super::backend::{CursorState, RunStats, RunStatus, StateBackend};
use super::schema;

pub struct SqliteStateBackend {
    conn: Mutex<Connection>,
}

impl SqliteStateBackend {
    /// Open or create a SQLite state database at the given path.
    pub fn new(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create state directory: {}", parent.display())
            })?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open state database: {}", path.display()))?;
        conn.execute_batch(schema::CREATE_TABLES)
            .context("Failed to initialize state schema")?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Create an in-memory SQLite state backend (for testing).
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(schema::CREATE_TABLES)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl StateBackend for SqliteStateBackend {
    fn get_cursor(&self, pipeline: &str, stream: &str) -> Result<Option<CursorState>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT cursor_field, cursor_value, updated_at FROM sync_cursors WHERE pipeline = ?1 AND stream = ?2",
        )?;

        let result = stmt.query_row(rusqlite::params![pipeline, stream], |row| {
            let cursor_field: Option<String> = row.get(0)?;
            let cursor_value: Option<String> = row.get(1)?;
            let updated_at_str: String = row.get(2)?;
            Ok((cursor_field, cursor_value, updated_at_str))
        });

        match result {
            Ok((cursor_field, cursor_value, updated_at_str)) => {
                let updated_at =
                    NaiveDateTime::parse_from_str(&updated_at_str, "%Y-%m-%d %H:%M:%S")
                        .map(|ndt| DateTime::from_naive_utc_and_offset(ndt, Utc))
                        .unwrap_or_else(|_| Utc::now());
                Ok(Some(CursorState {
                    cursor_field,
                    cursor_value,
                    updated_at,
                }))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn set_cursor(&self, pipeline: &str, stream: &str, cursor: &CursorState) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let updated_at = cursor.updated_at.format("%Y-%m-%d %H:%M:%S").to_string();
        conn.execute(
            "INSERT INTO sync_cursors (pipeline, stream, cursor_field, cursor_value, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(pipeline, stream)
             DO UPDATE SET cursor_field = ?3, cursor_value = ?4, updated_at = ?5",
            rusqlite::params![
                pipeline,
                stream,
                cursor.cursor_field,
                cursor.cursor_value,
                updated_at,
            ],
        )?;
        Ok(())
    }

    fn compare_and_set(
        &self,
        pipeline: &str,
        stream: &str,
        expected: Option<&str>,
        new_value: &str,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let rows_affected = match expected {
            Some(expected_val) => {
                // Update only if current value matches expected
                conn.execute(
                    "UPDATE sync_cursors SET cursor_value = ?1, updated_at = ?2
                     WHERE pipeline = ?3 AND stream = ?4 AND cursor_value = ?5",
                    rusqlite::params![new_value, now, pipeline, stream, expected_val],
                )?
            }
            None => {
                // Insert only if key doesn't exist (INSERT OR IGNORE)
                conn.execute(
                    "INSERT OR IGNORE INTO sync_cursors (pipeline, stream, cursor_value, updated_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![pipeline, stream, new_value, now],
                )?
            }
        };

        Ok(rows_affected > 0)
    }

    fn start_run(&self, pipeline: &str, stream: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO sync_runs (pipeline, stream, status) VALUES (?1, ?2, ?3)",
            rusqlite::params![pipeline, stream, RunStatus::Running.as_str()],
        )?;
        Ok(conn.last_insert_rowid())
    }

    fn complete_run(&self, run_id: i64, status: RunStatus, stats: &RunStats) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE sync_runs SET status = ?1, finished_at = datetime('now'),
             records_read = ?2, records_written = ?3, bytes_read = ?4, error_message = ?5
             WHERE id = ?6",
            rusqlite::params![
                status.as_str(),
                stats.records_read as i64,
                stats.records_written as i64,
                stats.bytes_read as i64,
                stats.error_message,
                run_id,
            ],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor_roundtrip() {
        let backend = SqliteStateBackend::in_memory().unwrap();

        // Initially no cursor
        let cursor = backend.get_cursor("pipe1", "users").unwrap();
        assert!(cursor.is_none());

        // Set a cursor
        let now = Utc::now();
        backend
            .set_cursor(
                "pipe1",
                "users",
                &CursorState {
                    cursor_field: Some("updated_at".to_string()),
                    cursor_value: Some("2024-01-15T10:00:00Z".to_string()),
                    updated_at: now,
                },
            )
            .unwrap();

        // Read it back
        let cursor = backend.get_cursor("pipe1", "users").unwrap().unwrap();
        assert_eq!(cursor.cursor_field, Some("updated_at".to_string()));
        assert_eq!(
            cursor.cursor_value,
            Some("2024-01-15T10:00:00Z".to_string())
        );
    }

    #[test]
    fn test_cursor_upsert() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let now = Utc::now();

        backend
            .set_cursor(
                "pipe1",
                "users",
                &CursorState {
                    cursor_field: Some("id".to_string()),
                    cursor_value: Some("100".to_string()),
                    updated_at: now,
                },
            )
            .unwrap();

        // Update the same cursor
        backend
            .set_cursor(
                "pipe1",
                "users",
                &CursorState {
                    cursor_field: Some("id".to_string()),
                    cursor_value: Some("200".to_string()),
                    updated_at: now,
                },
            )
            .unwrap();

        let cursor = backend.get_cursor("pipe1", "users").unwrap().unwrap();
        assert_eq!(cursor.cursor_value, Some("200".to_string()));
    }

    #[test]
    fn test_different_pipelines_independent() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let now = Utc::now();

        backend
            .set_cursor(
                "pipe_a",
                "users",
                &CursorState {
                    cursor_field: None,
                    cursor_value: Some("aaa".to_string()),
                    updated_at: now,
                },
            )
            .unwrap();

        backend
            .set_cursor(
                "pipe_b",
                "users",
                &CursorState {
                    cursor_field: None,
                    cursor_value: Some("bbb".to_string()),
                    updated_at: now,
                },
            )
            .unwrap();

        let a = backend.get_cursor("pipe_a", "users").unwrap().unwrap();
        let b = backend.get_cursor("pipe_b", "users").unwrap().unwrap();
        assert_eq!(a.cursor_value, Some("aaa".to_string()));
        assert_eq!(b.cursor_value, Some("bbb".to_string()));
    }

    #[test]
    fn test_run_lifecycle() {
        let backend = SqliteStateBackend::in_memory().unwrap();

        // Start a run
        let run_id = backend.start_run("pipe1", "users").unwrap();
        assert!(run_id > 0);

        // Complete it successfully
        backend
            .complete_run(
                run_id,
                RunStatus::Completed,
                &RunStats {
                    records_read: 1000,
                    records_written: 1000,
                    bytes_read: 50000,
                    error_message: None,
                },
            )
            .unwrap();

        // Verify via direct query
        let conn = backend.conn.lock().unwrap();
        let (status, records_read, finished): (String, i64, Option<String>) = conn
            .query_row(
                "SELECT status, records_read, finished_at FROM sync_runs WHERE id = ?1",
                [run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "completed");
        assert_eq!(records_read, 1000);
        assert!(finished.is_some());
    }

    #[test]
    fn test_run_failure() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let run_id = backend.start_run("pipe1", "orders").unwrap();

        backend
            .complete_run(
                run_id,
                RunStatus::Failed,
                &RunStats {
                    records_read: 50,
                    records_written: 0,
                    bytes_read: 2000,
                    error_message: Some("Connection reset".to_string()),
                },
            )
            .unwrap();

        let conn = backend.conn.lock().unwrap();
        let (status, error_msg): (String, Option<String>) = conn
            .query_row(
                "SELECT status, error_message FROM sync_runs WHERE id = ?1",
                [run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "failed");
        assert_eq!(error_msg, Some("Connection reset".to_string()));
    }

    #[test]
    fn test_multiple_runs() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let run1 = backend.start_run("pipe1", "users").unwrap();
        let run2 = backend.start_run("pipe1", "users").unwrap();
        assert_ne!(run1, run2);
        assert!(run2 > run1);
    }

    #[test]
    fn test_compare_and_set_success() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let cursor = CursorState {
            cursor_field: Some("id".to_string()),
            cursor_value: Some("100".to_string()),
            updated_at: Utc::now(),
        };
        backend.set_cursor("pipe", "stream1", &cursor).unwrap();

        // CAS: expect "100", set to "200" — should succeed
        let result = backend
            .compare_and_set("pipe", "stream1", Some("100"), "200")
            .unwrap();
        assert!(result, "CAS should succeed when expected matches");

        let got = backend.get_cursor("pipe", "stream1").unwrap().unwrap();
        assert_eq!(got.cursor_value, Some("200".to_string()));
    }

    #[test]
    fn test_compare_and_set_failure_mismatch() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let cursor = CursorState {
            cursor_field: Some("id".to_string()),
            cursor_value: Some("100".to_string()),
            updated_at: Utc::now(),
        };
        backend.set_cursor("pipe", "stream1", &cursor).unwrap();

        // CAS: expect "999", set to "200" — should fail (current is "100")
        let result = backend
            .compare_and_set("pipe", "stream1", Some("999"), "200")
            .unwrap();
        assert!(!result, "CAS should fail when expected doesn't match");

        // Value should remain unchanged
        let got = backend.get_cursor("pipe", "stream1").unwrap().unwrap();
        assert_eq!(got.cursor_value, Some("100".to_string()));
    }

    #[test]
    fn test_compare_and_set_from_none() {
        let backend = SqliteStateBackend::in_memory().unwrap();

        // CAS on nonexistent key: expect None, set to "50" — should succeed (insert)
        let result = backend
            .compare_and_set("pipe", "stream1", None, "50")
            .unwrap();
        assert!(result, "CAS from None should succeed when key doesn't exist");

        let got = backend.get_cursor("pipe", "stream1").unwrap().unwrap();
        assert_eq!(got.cursor_value, Some("50".to_string()));
    }

    #[test]
    fn test_compare_and_set_from_none_but_exists() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let cursor = CursorState {
            cursor_field: Some("id".to_string()),
            cursor_value: Some("100".to_string()),
            updated_at: Utc::now(),
        };
        backend.set_cursor("pipe", "stream1", &cursor).unwrap();

        // CAS: expect None but key exists — should fail
        let result = backend
            .compare_and_set("pipe", "stream1", None, "200")
            .unwrap();
        assert!(!result, "CAS from None should fail when key already exists");

        let got = backend.get_cursor("pipe", "stream1").unwrap().unwrap();
        assert_eq!(got.cursor_value, Some("100".to_string()));
    }
}
