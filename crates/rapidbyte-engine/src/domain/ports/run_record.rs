//! Run record repository port trait.
//!
//! Abstracts run lifecycle persistence (start, complete) so the orchestrator
//! can track pipeline execution history without coupling to a specific backend.

use async_trait::async_trait;
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus, StreamName};

use super::RepositoryError;

/// Port for recording pipeline run lifecycle events.
///
/// Each run is identified by a backend-assigned `i64` run ID returned
/// from [`start`](RunRecordRepository::start). The orchestrator calls
/// [`complete`](RunRecordRepository::complete) when the run finishes.
#[async_trait]
pub trait RunRecordRepository: Send + Sync {
    /// Record the start of a new run for a (pipeline, stream) pair.
    ///
    /// Returns a backend-assigned run ID.
    async fn start(
        &self,
        pipeline: &PipelineId,
        stream: &StreamName,
    ) -> Result<i64, RepositoryError>;

    /// Record the completion of a run.
    async fn complete(
        &self,
        run_id: i64,
        status: RunStatus,
        stats: &RunStats,
    ) -> Result<(), RepositoryError>;
}
