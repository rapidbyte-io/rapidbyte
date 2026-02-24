//! State backend trait definition.
//!
//! [`StateBackend`] defines the storage contract for pipeline cursors,
//! run history, and dead-letter queue records. Model types live in
//! [`rapidbyte_types::state`].

use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};

use crate::error;

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

    /// Compare-and-set: atomically update `cursor_value` only if it matches `expected`.
    ///
    /// Returns `true` if the update was applied, `false` if the current value didn't match.
    /// When `expected` is `None`, succeeds only if the key does not exist (insert-if-absent).
    ///
    /// # Errors
    ///
    /// Returns [`StateError`](crate::error::StateError) on storage failure.
    fn compare_and_set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        expected: Option<&str>,
        new_value: &str,
    ) -> error::Result<bool>;

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

    /// Verify the trait is object-safe (can be used as `dyn StateBackend`).
    #[test]
    fn trait_is_object_safe() {
        fn _assert_object_safe(_: &dyn StateBackend) {}
    }
}
