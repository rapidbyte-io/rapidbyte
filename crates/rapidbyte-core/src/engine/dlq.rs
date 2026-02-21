use rapidbyte_sdk::protocol::DlqRecord;

use crate::state::backend::StateBackend;

/// Maximum number of DLQ records to keep in memory per run.
/// Prevents unbounded memory growth if a connector emits millions of failures.
pub(crate) const MAX_DLQ_RECORDS: usize = 10_000;

/// Persist collected DLQ records to the state backend.
pub(crate) fn persist_dlq_records(
    state_backend: &dyn StateBackend,
    pipeline: &str,
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
                pipeline,
                dlq_records = inserted,
                "Persisted DLQ records to state backend"
            );
        }
        Err(e) => {
            tracing::error!(
                pipeline,
                dlq_count,
                error = %e,
                "Failed to persist DLQ records"
            );
        }
    }
}
