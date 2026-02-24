//! State backend trait and shared model types.
//!
//! [`StateBackend`] defines the storage contract for pipeline cursors,
//! run history, and dead-letter queue records. Model types (`PipelineId`,
//! `StreamName`, etc.) provide type-safe identifiers.

use chrono::{DateTime, Utc};
use rapidbyte_types::envelope::DlqRecord;

use crate::error;

// ---------------------------------------------------------------------------
// Newtypes
// ---------------------------------------------------------------------------

/// Opaque pipeline identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PipelineId(String);

impl PipelineId {
    /// Create a new pipeline identifier.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PipelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl<S: Into<String>> From<S> for PipelineId {
    fn from(value: S) -> Self {
        Self(value.into())
    }
}

/// Opaque stream name (e.g. `"public.users"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamName(String);

impl StreamName {
    /// Create a new stream name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Borrow the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StreamName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl<S: Into<String>> From<S> for StreamName {
    fn from(value: S) -> Self {
        Self(value.into())
    }
}

// ---------------------------------------------------------------------------
// Model types
// ---------------------------------------------------------------------------

/// Snapshot of a persisted cursor for a (pipeline, stream) pair.
#[derive(Debug, Clone)]
pub struct CursorState {
    /// Column used for incremental sync (e.g. `"updated_at"`).
    pub cursor_field: Option<String>,
    /// Last-seen value of the cursor column.
    pub cursor_value: Option<String>,
    /// When this cursor was last written.
    pub updated_at: DateTime<Utc>,
}

/// Terminal status of a sync run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
}

impl RunStatus {
    /// Wire-format string for storage.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Aggregate statistics for a completed sync run.
#[derive(Debug, Clone, Default)]
pub struct RunStats {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub error_message: Option<String>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Storage contract for pipeline state.
///
/// Implementations must be `Send + Sync` for use behind `Arc<dyn StateBackend>`.
pub trait StateBackend: Send + Sync {
    /// Read the current cursor for a (pipeline, stream) pair.
    ///
    /// Returns `Ok(None)` when no cursor has been persisted yet.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::error::StateError) on storage failure.
    fn get_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> error::Result<Option<CursorState>>;

    /// Upsert the cursor for a (pipeline, stream) pair.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::error::StateError) on storage failure.
    fn set_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        cursor: &CursorState,
    ) -> error::Result<()>;

    /// Begin a new sync run, returning its unique ID.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::error::StateError) on storage failure.
    fn start_run(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> error::Result<i64>;

    /// Finalize a sync run with status and aggregate stats.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::error::StateError) on storage failure.
    fn complete_run(
        &self,
        run_id: i64,
        status: RunStatus,
        stats: &RunStats,
    ) -> error::Result<()>;

    /// Persist dead-letter queue records. Returns the count inserted.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::error::StateError) on storage failure.
    fn insert_dlq_records(
        &self,
        pipeline: &PipelineId,
        run_id: i64,
        records: &[DlqRecord],
    ) -> error::Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_id_display_and_as_str() {
        let pid = PipelineId::new("my-pipeline");
        assert_eq!(pid.as_str(), "my-pipeline");
        assert_eq!(pid.to_string(), "my-pipeline");
    }

    #[test]
    fn stream_name_from_and_display() {
        let sn = StreamName::from("public.users");
        assert_eq!(sn.as_str(), "public.users");
        assert_eq!(sn.to_string(), "public.users");
    }

    #[test]
    fn pipeline_id_eq_and_hash() {
        use std::collections::HashSet;
        let a = PipelineId::new("p1");
        let b = PipelineId::new("p1");
        assert_eq!(a, b);
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    #[test]
    fn run_status_as_str() {
        assert_eq!(RunStatus::Running.as_str(), "running");
        assert_eq!(RunStatus::Completed.as_str(), "completed");
        assert_eq!(RunStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn run_stats_default_is_zeroed() {
        let stats = RunStats::default();
        assert_eq!(stats.records_read, 0);
        assert_eq!(stats.records_written, 0);
        assert_eq!(stats.bytes_read, 0);
        assert!(stats.error_message.is_none());
    }
}
