//! DLQ persistence helper used by the orchestrator.

use rapidbyte_types::envelope::DlqRecord;

use rapidbyte_state::backend::PipelineId;
use rapidbyte_state::StateBackend;

/// Maximum number of DLQ records to keep in memory per run.
/// Prevents unbounded memory growth if a connector emits millions of failures.
pub(crate) const MAX_DLQ_RECORDS: usize = 10_000;

/// Persist collected DLQ records to the state backend.
pub(crate) fn persist_dlq_records(
    state_backend: &dyn StateBackend,
    pipeline: &PipelineId,
    run_id: i64,
    records: &[DlqRecord],
) {
    if records.is_empty() {
        return;
    }

    let dlq_count = records.len();

    match state_backend.insert_dlq_records(pipeline, run_id, records) {
        Ok(inserted) => {
            tracing::info!(
                pipeline = pipeline.as_str(),
                dlq_records = inserted,
                "Persisted DLQ records to state backend"
            );
        }
        Err(e) => {
            tracing::error!(
                pipeline = pipeline.as_str(),
                dlq_count,
                error = %e,
                "Failed to persist DLQ records"
            );
        }
    }
}
