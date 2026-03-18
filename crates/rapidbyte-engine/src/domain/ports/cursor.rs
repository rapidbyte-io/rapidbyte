//! Cursor repository port trait.
//!
//! Abstracts cursor persistence so the orchestrator can read and write
//! incremental sync cursors without coupling to a specific state backend.

use async_trait::async_trait;
use rapidbyte_types::state::{CursorState, PipelineId, StreamName};

use super::RepositoryError;

/// Port for reading and writing incremental sync cursors.
///
/// Cursors track the last-seen value for a (pipeline, stream) pair
/// so that subsequent runs can resume from where the previous run left off.
#[async_trait]
pub trait CursorRepository: Send + Sync {
    /// Get the current cursor state for a (pipeline, stream) pair.
    ///
    /// Returns `None` if no cursor has been persisted yet.
    async fn get(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> Result<Option<CursorState>, RepositoryError>;

    /// Unconditionally set the cursor for a (pipeline, stream) pair.
    async fn set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        cursor: &CursorState,
    ) -> Result<(), RepositoryError>;

    /// Atomically compare-and-set the cursor value.
    ///
    /// Returns `true` if the swap succeeded (i.e. the current value
    /// matched `expected`). Returns `false` if the current value has
    /// diverged and the write was skipped.
    async fn compare_and_set(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
        expected: Option<&str>,
        new_value: &str,
    ) -> Result<bool, RepositoryError>;
}
