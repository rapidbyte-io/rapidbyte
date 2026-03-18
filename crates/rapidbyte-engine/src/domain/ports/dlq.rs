//! Dead-letter queue repository port trait.
//!
//! Abstracts DLQ persistence so the orchestrator can store failed records
//! without coupling to a specific state backend.

use async_trait::async_trait;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::state::PipelineId;

use super::RepositoryError;

/// Port for persisting dead-letter queue records.
///
/// Records that fail processing during a pipeline run are collected
/// and bulk-inserted via this trait after the run completes.
#[async_trait]
pub trait DlqRepository: Send + Sync {
    /// Insert a batch of DLQ records for a given pipeline run.
    ///
    /// Returns the number of records successfully inserted.
    async fn insert(
        &self,
        pipeline: &PipelineId,
        run_id: i64,
        records: &[DlqRecord],
    ) -> Result<u64, RepositoryError>;
}
