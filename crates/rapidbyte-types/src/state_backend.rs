//! State backend trait definition.
//!
//! [`StateBackend`] defines the storage contract for pipeline cursors,
//! run history, and dead-letter queue records. Model types live in
//! [`crate::state`].

use crate::envelope::DlqRecord;
use crate::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
use crate::state_error;

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
    /// Returns [`StateError`](crate::state_error::StateError) on storage failure.
    fn get_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> state_error::Result<Option<CursorState>>;

    /// Upsert the cursor for a (pipeline, stream) pair.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::state_error::StateError) on storage failure.
    fn set_cursor(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        cursor: &CursorState,
    ) -> state_error::Result<()>;

    /// Begin a new sync run, returning its unique ID.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::state_error::StateError) on storage failure.
    fn start_run(&self, pipeline: &PipelineId, stream: &StreamName) -> state_error::Result<i64>;

    /// Finalize a sync run with status and aggregate stats.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::state_error::StateError) on storage failure.
    fn complete_run(
        &self,
        run_id: i64,
        status: RunStatus,
        stats: &RunStats,
    ) -> state_error::Result<()>;

    /// Compare-and-set: atomically update `cursor_value` only if it matches `expected`.
    ///
    /// Returns `true` if the update was applied, `false` if the current value didn't match.
    /// When `expected` is `None`, succeeds only if the key does not exist (insert-if-absent).
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::state_error::StateError) on storage failure.
    fn compare_and_set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        expected: Option<&str>,
        new_value: &str,
    ) -> state_error::Result<bool>;

    /// Persist dead-letter queue records. Returns the count inserted.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::state_error::StateError) on storage failure.
    fn insert_dlq_records(
        &self,
        pipeline: &PipelineId,
        run_id: i64,
        records: &[DlqRecord],
    ) -> state_error::Result<u64>;
}

// ---------------------------------------------------------------------------
// No-op implementation
// ---------------------------------------------------------------------------

/// No-op [`StateBackend`] that discards all writes and returns empty
/// defaults. Useful for contexts that don't need real persistence
/// (discover, validate, dev REPL).
pub struct NoopStateBackend;

impl StateBackend for NoopStateBackend {
    fn get_cursor(
        &self,
        _pipeline: &PipelineId,
        _stream: &StreamName,
    ) -> state_error::Result<Option<CursorState>> {
        Ok(None)
    }
    fn set_cursor(
        &self,
        _pipeline: &PipelineId,
        _stream: &StreamName,
        _cursor: &CursorState,
    ) -> state_error::Result<()> {
        Ok(())
    }
    fn start_run(&self, _pipeline: &PipelineId, _stream: &StreamName) -> state_error::Result<i64> {
        Ok(1)
    }
    fn complete_run(
        &self,
        _run_id: i64,
        _status: RunStatus,
        _stats: &RunStats,
    ) -> state_error::Result<()> {
        Ok(())
    }
    fn compare_and_set(
        &self,
        _pipeline: &PipelineId,
        _stream: &StreamName,
        _expected: Option<&str>,
        _new_value: &str,
    ) -> state_error::Result<bool> {
        Ok(true)
    }
    fn insert_dlq_records(
        &self,
        _pipeline: &PipelineId,
        _run_id: i64,
        _records: &[DlqRecord],
    ) -> state_error::Result<u64> {
        Ok(0)
    }
}

/// Returns an `Arc<dyn StateBackend>` backed by a [`NoopStateBackend`].
#[must_use]
pub fn noop_state_backend() -> std::sync::Arc<dyn StateBackend> {
    std::sync::Arc::new(NoopStateBackend)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the trait is object-safe (can be used as `dyn StateBackend`).
    #[test]
    fn trait_is_object_safe() {
        fn _assert_object_safe(_: &dyn StateBackend) {}
    }

    #[test]
    fn noop_backend_returns_defaults() {
        let backend = NoopStateBackend;
        let pid = PipelineId::new("test");
        let stream = StreamName::new("s");
        assert!(backend.get_cursor(&pid, &stream).unwrap().is_none());
        assert!(backend
            .set_cursor(
                &pid,
                &stream,
                &CursorState {
                    cursor_field: None,
                    cursor_value: None,
                    updated_at: String::new(),
                }
            )
            .is_ok());
        assert_eq!(backend.start_run(&pid, &stream).unwrap(), 1);
    }
}
