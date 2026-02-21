use anyhow::Result;
use chrono::{DateTime, Utc};
use rapidbyte_sdk::protocol::DlqRecord;

#[derive(Debug, Clone)]
pub struct CursorState {
    pub cursor_field: Option<String>,
    pub cursor_value: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunStats {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub error_message: Option<String>,
}

pub trait StateBackend: Send + Sync {
    fn get_cursor(&self, pipeline: &str, stream: &str) -> Result<Option<CursorState>>;
    fn set_cursor(&self, pipeline: &str, stream: &str, cursor: &CursorState) -> Result<()>;

    /// Compare-and-set: atomically update cursor_value only if it matches `expected`.
    /// Returns `true` if the update was applied, `false` if the current value didn't match.
    /// When `expected` is `None`, succeeds only if the key does not exist (insert-if-absent).
    fn compare_and_set(
        &self,
        pipeline: &str,
        stream: &str,
        expected: Option<&str>,
        new_value: &str,
    ) -> Result<bool>;

    fn start_run(&self, pipeline: &str, stream: &str) -> Result<i64>;
    fn complete_run(&self, run_id: i64, status: RunStatus, stats: &RunStats) -> Result<()>;

    /// Insert DLQ records into the state backend. Returns the number of records inserted.
    fn insert_dlq_records(
        &self,
        pipeline: &str,
        run_id: i64,
        records: &[DlqRecord],
    ) -> Result<u64>;
}
