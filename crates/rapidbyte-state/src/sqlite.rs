//! `SQLite`-backed implementation of [`StateBackend`].
//!
//! Uses a single `Mutex<Connection>` for thread safety. Suitable for
//! single-pipeline workloads; connection pooling is not needed at
//! current scale.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use chrono::{DateTime, NaiveDateTime, Utc};
use rapidbyte_types::envelope::DlqRecord;
use rusqlite::Connection;

use crate::backend::{CursorState, PipelineId, RunStats, RunStatus, StateBackend, StreamName};
use crate::error::{self, StateError};
use crate::schema;

/// `SQLite` datetime format (UTC, no timezone suffix).
const SQLITE_DATETIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

/// `SQLite`-backed state storage.
///
/// Create with [`SqliteStateBackend::open`] for file-backed persistence
/// or [`SqliteStateBackend::in_memory`] for tests.
pub struct SqliteStateBackend {
    conn: Mutex<Connection>,
}

impl SqliteStateBackend {
    /// Open or create a `SQLite` state database at `path`.
    ///
    /// Creates parent directories if they don't exist. Runs DDL on
    /// every open (idempotent `CREATE TABLE IF NOT EXISTS`).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Io`] if the directory can't be created,
    /// or [`StateError::Sqlite`] if the database can't be opened.
    pub fn open(path: &Path) -> error::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(schema::CREATE_TABLES)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Create an in-memory `SQLite` backend (for testing).
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Sqlite`] if the in-memory database can't
    /// be initialized.
    pub fn in_memory() -> error::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(schema::CREATE_TABLES)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Acquire the connection lock.
    fn lock_conn(&self) -> error::Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(|_| StateError::LockPoisoned)
    }

    #[cfg(test)]
    fn get_run_row(
        &self,
        run_id: i64,
    ) -> error::Result<(String, i64, Option<String>, Option<String>)> {
        let conn = self.lock_conn()?;
        let row = conn.query_row(
            "SELECT status, records_read, finished_at, error_message FROM sync_runs WHERE id = ?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        Ok(row)
    }

    #[cfg(test)]
    fn count_dlq_records_for_run(
        &self,
        pipeline: &PipelineId,
        run_id: i64,
    ) -> error::Result<i64> {
        let conn = self.lock_conn()?;
        let count = conn.query_row(
            "SELECT COUNT(*) FROM dlq_records WHERE pipeline = ?1 AND run_id = ?2",
            rusqlite::params![pipeline.as_str(), run_id],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    #[cfg(test)]
    fn first_dlq_stream_error(
        &self,
        pipeline: &PipelineId,
    ) -> error::Result<(String, String)> {
        let conn = self.lock_conn()?;
        let row = conn.query_row(
            "SELECT stream_name, error_message FROM dlq_records \
             WHERE pipeline = ?1 ORDER BY id LIMIT 1",
            rusqlite::params![pipeline.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(row)
    }
}

impl StateBackend for SqliteStateBackend {
    fn get_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> error::Result<Option<CursorState>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT cursor_field, cursor_value, updated_at \
             FROM sync_cursors WHERE pipeline = ?1 AND stream = ?2",
        )?;

        let result = stmt.query_row(
            rusqlite::params![pipeline.as_str(), stream.as_str()],
            |row| {
                let cursor_field: Option<String> = row.get(0)?;
                let cursor_value: Option<String> = row.get(1)?;
                let updated_at_str: String = row.get(2)?;
                Ok((cursor_field, cursor_value, updated_at_str))
            },
        );

        match result {
            Ok((cursor_field, cursor_value, updated_at_str)) => {
                let updated_at =
                    NaiveDateTime::parse_from_str(&updated_at_str, SQLITE_DATETIME_FMT)
                        .map_or_else(
                            |e| {
                                tracing::warn!(
                                    pipeline = pipeline.as_str(),
                                    stream = stream.as_str(),
                                    raw = updated_at_str,
                                    error = %e,
                                    "failed to parse cursor updated_at; defaulting to now"
                                );
                                Utc::now()
                            },
                            |ndt| DateTime::from_naive_utc_and_offset(ndt, Utc),
                        );
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

    fn set_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        cursor: &CursorState,
    ) -> error::Result<()> {
        let conn = self.lock_conn()?;
        let updated_at = cursor.updated_at.format(SQLITE_DATETIME_FMT).to_string();
        conn.execute(
            "INSERT INTO sync_cursors (pipeline, stream, cursor_field, cursor_value, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(pipeline, stream) \
             DO UPDATE SET cursor_field = ?3, cursor_value = ?4, updated_at = ?5",
            rusqlite::params![
                pipeline.as_str(),
                stream.as_str(),
                cursor.cursor_field,
                cursor.cursor_value,
                updated_at,
            ],
        )?;
        Ok(())
    }

    fn start_run(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> error::Result<i64> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT INTO sync_runs (pipeline, stream, status) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                pipeline.as_str(),
                stream.as_str(),
                RunStatus::Running.as_str()
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    #[allow(clippy::cast_possible_wrap, clippy::similar_names)]
    fn complete_run(
        &self,
        run_id: i64,
        status: RunStatus,
        stats: &RunStats,
    ) -> error::Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "UPDATE sync_runs SET status = ?1, finished_at = datetime('now'), \
             records_read = ?2, records_written = ?3, bytes_read = ?4, error_message = ?5 \
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

    #[allow(clippy::similar_names)]
    fn compare_and_set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        expected: Option<&str>,
        new_value: &str,
    ) -> error::Result<bool> {
        let conn = self.lock_conn()?;
        let now = chrono::Utc::now().format(SQLITE_DATETIME_FMT).to_string();

        let rows_affected = match expected {
            Some(expected_val) => {
                // Update only if current value matches expected
                conn.execute(
                    "UPDATE sync_cursors SET cursor_value = ?1, updated_at = ?2 \
                     WHERE pipeline = ?3 AND stream = ?4 AND cursor_value = ?5",
                    rusqlite::params![
                        new_value,
                        now,
                        pipeline.as_str(),
                        stream.as_str(),
                        expected_val
                    ],
                )?
            }
            None => {
                // Insert only if key doesn't exist (INSERT OR IGNORE)
                conn.execute(
                    "INSERT OR IGNORE INTO sync_cursors (pipeline, stream, cursor_value, updated_at) \
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![pipeline.as_str(), stream.as_str(), new_value, now],
                )?
            }
        };

        Ok(rows_affected > 0)
    }

    fn insert_dlq_records(
        &self,
        pipeline: &PipelineId,
        run_id: i64,
        records: &[DlqRecord],
    ) -> error::Result<u64> {
        if records.is_empty() {
            return Ok(0);
        }

        let conn = self.lock_conn()?;
        let tx = conn.unchecked_transaction()?;
        let mut stmt = tx.prepare(
            "INSERT INTO dlq_records \
             (pipeline, run_id, stream_name, record_json, error_message, error_category, failed_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )?;

        let mut count = 0u64;
        for record in records {
            stmt.execute(rusqlite::params![
                pipeline.as_str(),
                run_id,
                record.stream_name,
                record.record_json,
                record.error_message,
                record.error_category.to_string(),
                record.failed_at.as_str(),
            ])?;
            count += 1;
        }
        drop(stmt);
        tx.commit()?;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_types::envelope::Timestamp;
    use rapidbyte_types::error::ErrorCategory;

    fn pid(name: &str) -> PipelineId {
        PipelineId::new(name)
    }

    fn stream(name: &str) -> StreamName {
        StreamName::new(name)
    }

    #[test]
    fn cursor_roundtrip() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let cursor = backend.get_cursor(&pid("pipe1"), &stream("users")).unwrap();
        assert!(cursor.is_none());

        let now = Utc::now();
        backend
            .set_cursor(
                &pid("pipe1"),
                &stream("users"),
                &CursorState {
                    cursor_field: Some("updated_at".into()),
                    cursor_value: Some("2024-01-15T10:00:00Z".into()),
                    updated_at: now,
                },
            )
            .unwrap();

        let cursor = backend
            .get_cursor(&pid("pipe1"), &stream("users"))
            .unwrap()
            .unwrap();
        assert_eq!(cursor.cursor_field, Some("updated_at".into()));
        assert_eq!(cursor.cursor_value, Some("2024-01-15T10:00:00Z".into()));
    }

    #[test]
    fn cursor_upsert() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let now = Utc::now();

        backend
            .set_cursor(
                &pid("pipe1"),
                &stream("users"),
                &CursorState {
                    cursor_field: Some("id".into()),
                    cursor_value: Some("100".into()),
                    updated_at: now,
                },
            )
            .unwrap();

        backend
            .set_cursor(
                &pid("pipe1"),
                &stream("users"),
                &CursorState {
                    cursor_field: Some("id".into()),
                    cursor_value: Some("200".into()),
                    updated_at: now,
                },
            )
            .unwrap();

        let cursor = backend
            .get_cursor(&pid("pipe1"), &stream("users"))
            .unwrap()
            .unwrap();
        assert_eq!(cursor.cursor_value, Some("200".into()));
    }

    #[test]
    fn different_pipelines_independent() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let now = Utc::now();

        backend
            .set_cursor(
                &pid("pipe_a"),
                &stream("users"),
                &CursorState {
                    cursor_field: None,
                    cursor_value: Some("aaa".into()),
                    updated_at: now,
                },
            )
            .unwrap();

        backend
            .set_cursor(
                &pid("pipe_b"),
                &stream("users"),
                &CursorState {
                    cursor_field: None,
                    cursor_value: Some("bbb".into()),
                    updated_at: now,
                },
            )
            .unwrap();

        let a = backend
            .get_cursor(&pid("pipe_a"), &stream("users"))
            .unwrap()
            .unwrap();
        let b = backend
            .get_cursor(&pid("pipe_b"), &stream("users"))
            .unwrap()
            .unwrap();
        assert_eq!(a.cursor_value, Some("aaa".into()));
        assert_eq!(b.cursor_value, Some("bbb".into()));
    }

    #[test]
    fn run_lifecycle() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let run_id = backend.start_run(&pid("pipe1"), &stream("users")).unwrap();
        assert!(run_id > 0);

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

        let (status, records_read, finished, _error) = backend.get_run_row(run_id).unwrap();
        assert_eq!(status, "completed");
        assert_eq!(records_read, 1000);
        assert!(finished.is_some());
    }

    #[test]
    fn run_failure() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let run_id = backend.start_run(&pid("pipe1"), &stream("orders")).unwrap();

        backend
            .complete_run(
                run_id,
                RunStatus::Failed,
                &RunStats {
                    records_read: 50,
                    records_written: 0,
                    bytes_read: 2000,
                    error_message: Some("Connection reset".into()),
                },
            )
            .unwrap();

        let (status, _records, _finished, error_msg) = backend.get_run_row(run_id).unwrap();
        assert_eq!(status, "failed");
        assert_eq!(error_msg, Some("Connection reset".into()));
    }

    #[test]
    fn multiple_runs() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let run1 = backend.start_run(&pid("pipe1"), &stream("users")).unwrap();
        let run2 = backend.start_run(&pid("pipe1"), &stream("users")).unwrap();
        assert_ne!(run1, run2);
        assert!(run2 > run1);
    }

    #[test]
    fn dlq_records_insert_and_count() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let run_id = backend.start_run(&pid("pipe1"), &stream("users")).unwrap();

        let records = vec![
            DlqRecord {
                stream_name: "users".into(),
                record_json: r#"{"id":1}"#.into(),
                error_message: "not-null violation".into(),
                error_category: ErrorCategory::Data,
                failed_at: Timestamp::new("2026-02-21T12:00:00+00:00"),
            },
            DlqRecord {
                stream_name: "users".into(),
                record_json: r#"{"id":2}"#.into(),
                error_message: "type mismatch".into(),
                error_category: ErrorCategory::Data,
                failed_at: Timestamp::new("2026-02-21T12:00:01+00:00"),
            },
        ];

        let count = backend
            .insert_dlq_records(&pid("pipe1"), run_id, &records)
            .unwrap();
        assert_eq!(count, 2);

        let stored = backend
            .count_dlq_records_for_run(&pid("pipe1"), run_id)
            .unwrap();
        assert_eq!(stored, 2);

        let (stream_name, error_msg) = backend.first_dlq_stream_error(&pid("pipe1")).unwrap();
        assert_eq!(stream_name, "users");
        assert_eq!(error_msg, "not-null violation");
    }

    #[test]
    fn dlq_records_empty_insert() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let count = backend
            .insert_dlq_records(&pid("pipe1"), 1, &[])
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn compare_and_set_success() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let cursor = CursorState {
            cursor_field: Some("id".into()),
            cursor_value: Some("100".into()),
            updated_at: Utc::now(),
        };
        backend
            .set_cursor(&pid("pipe"), &stream("stream1"), &cursor)
            .unwrap();

        // CAS: expect "100", set to "200" -- should succeed
        let result = backend
            .compare_and_set(&pid("pipe"), &stream("stream1"), Some("100"), "200")
            .unwrap();
        assert!(result, "CAS should succeed when expected matches");

        let got = backend
            .get_cursor(&pid("pipe"), &stream("stream1"))
            .unwrap()
            .unwrap();
        assert_eq!(got.cursor_value, Some("200".into()));
    }

    #[test]
    fn compare_and_set_failure_mismatch() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let cursor = CursorState {
            cursor_field: Some("id".into()),
            cursor_value: Some("100".into()),
            updated_at: Utc::now(),
        };
        backend
            .set_cursor(&pid("pipe"), &stream("stream1"), &cursor)
            .unwrap();

        // CAS: expect "999", set to "200" -- should fail (current is "100")
        let result = backend
            .compare_and_set(&pid("pipe"), &stream("stream1"), Some("999"), "200")
            .unwrap();
        assert!(!result, "CAS should fail when expected doesn't match");

        let got = backend
            .get_cursor(&pid("pipe"), &stream("stream1"))
            .unwrap()
            .unwrap();
        assert_eq!(got.cursor_value, Some("100".into()));
    }

    #[test]
    fn compare_and_set_from_none() {
        let backend = SqliteStateBackend::in_memory().unwrap();

        // CAS on nonexistent key: expect None, set to "50" -- should succeed (insert)
        let result = backend
            .compare_and_set(&pid("pipe"), &stream("stream1"), None, "50")
            .unwrap();
        assert!(
            result,
            "CAS from None should succeed when key doesn't exist"
        );

        let got = backend
            .get_cursor(&pid("pipe"), &stream("stream1"))
            .unwrap()
            .unwrap();
        assert_eq!(got.cursor_value, Some("50".into()));
    }

    #[test]
    fn compare_and_set_from_none_but_exists() {
        let backend = SqliteStateBackend::in_memory().unwrap();
        let cursor = CursorState {
            cursor_field: Some("id".into()),
            cursor_value: Some("100".into()),
            updated_at: Utc::now(),
        };
        backend
            .set_cursor(&pid("pipe"), &stream("stream1"), &cursor)
            .unwrap();

        // CAS: expect None but key exists -- should fail
        let result = backend
            .compare_and_set(&pid("pipe"), &stream("stream1"), None, "200")
            .unwrap();
        assert!(!result, "CAS from None should fail when key already exists");

        let got = backend
            .get_cursor(&pid("pipe"), &stream("stream1"))
            .unwrap()
            .unwrap();
        assert_eq!(got.cursor_value, Some("100".into()));
    }
}
