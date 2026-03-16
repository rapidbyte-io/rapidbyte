//! Concurrency management: semaphore permits, JoinSet collection, task results.

use std::sync::Arc;

use tokio::sync::mpsc as tokio_mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::metric::{ReadSummary, WriteSummary};

use crate::error::PipelineError;
use crate::execution::DryRunStreamResult;
use crate::progress::ProgressEvent;

/// Type alias for the progress channel sender used throughout the orchestrator.
pub(crate) type ProgressTx = Option<tokio_mpsc::UnboundedSender<ProgressEvent>>;

/// Per-stream execution result with summaries, checkpoints, and timings.
pub(crate) struct StreamShardOutcome {
    pub(crate) stream_name: String,
    pub(crate) partition_index: Option<u32>,
    pub(crate) partition_count: Option<u32>,
    pub(crate) read_summary: ReadSummary,
    pub(crate) write_summary: WriteSummary,
    pub(crate) source_checkpoints: Vec<Checkpoint>,
    pub(crate) dest_checkpoints: Vec<Checkpoint>,
    pub(crate) source_duration_secs: f64,
    pub(crate) dest_duration_secs: f64,
    pub(crate) wasm_instantiation_secs: f64,
    pub(crate) frame_receive_secs: f64,
    pub(crate) transform_durations: Vec<f64>,
    pub(crate) dry_run_result: Option<DryRunStreamResult>,
}

/// Collection of completed stream tasks, distinguishing successes from the first error.
pub(crate) struct StreamTaskOutcomes {
    pub(crate) successes: Vec<StreamShardOutcome>,
    pub(crate) first_error: Option<PipelineError>,
}

/// Collection of completed transform tasks with durations and first error.
pub(crate) struct TransformStageOutcomes {
    pub(crate) durations: Vec<f64>,
    pub(crate) first_error: Option<PipelineError>,
}

/// Send a progress event through the channel, ignoring send errors.
pub(crate) fn send_progress(tx: &ProgressTx, event: ProgressEvent) {
    if let Some(tx) = tx {
        let _ = tx.send(event);
    }
}

/// Check if the cancellation token has fired; return a `PipelineError::cancelled` if so.
pub(crate) fn ensure_not_cancelled(
    cancel_token: &CancellationToken,
    message: &str,
) -> Result<(), PipelineError> {
    if cancel_token.is_cancelled() {
        return Err(PipelineError::cancelled(message));
    }
    Ok(())
}

/// Acquire a semaphore permit, returning a cancellation error if the token fires first.
pub(crate) async fn acquire_permit_cancellable(
    semaphore: &Arc<tokio::sync::Semaphore>,
    cancel: &CancellationToken,
    context: &str,
) -> Result<tokio::sync::OwnedSemaphorePermit, PipelineError> {
    tokio::select! {
        () = cancel.cancelled() => {
            Err(PipelineError::cancelled(context))
        }
        permit = semaphore.clone().acquire_owned() => {
            permit.map_err(|e| PipelineError::infra(format!("Semaphore closed: {e}")))
        }
    }
}

/// Join all stream tasks in the set, collecting successes and aborting siblings on first error.
pub(crate) async fn collect_stream_task_results(
    mut stream_join_set: JoinSet<Result<StreamShardOutcome, PipelineError>>,
    progress_tx: &ProgressTx,
) -> Result<StreamTaskOutcomes, PipelineError> {
    let mut successes = Vec::new();
    let mut first_error: Option<PipelineError> = None;

    while let Some(joined) = stream_join_set.join_next().await {
        match joined {
            Ok(Ok(sr)) if first_error.is_none() => {
                send_progress(
                    progress_tx,
                    ProgressEvent::StreamCompleted {
                        stream: sr.stream_name.clone(),
                    },
                );
                successes.push(sr);
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::error!("Stream failed: {}", error);
                if first_error.is_none() {
                    first_error = Some(error);
                    stream_join_set.abort_all();
                }
            }
            Err(join_err) if join_err.is_cancelled() && first_error.is_some() => {
                // Expected: sibling tasks cancelled after first stream failure.
            }
            Err(join_err) => {
                return Err(PipelineError::infra(format!(
                    "Stream task panicked: {join_err}"
                )));
            }
        }
    }

    Ok(StreamTaskOutcomes {
        successes,
        first_error,
    })
}

/// Join all transform task handles, collecting durations and the first error encountered.
pub(crate) async fn collect_transform_results(
    transform_handles: Vec<(
        usize,
        tokio::task::JoinHandle<Result<crate::runner::TransformOutcome, PipelineError>>,
    )>,
    stream_name: &str,
) -> Result<TransformStageOutcomes, PipelineError> {
    let mut durations = Vec::new();
    let mut first_error: Option<PipelineError> = None;
    for (i, t_handle) in transform_handles {
        let result = t_handle.await.map_err(|e| {
            PipelineError::infra(format!(
                "Transform {i} task panicked for stream '{stream_name}': {e}"
            ))
        })?;
        match result {
            Ok(tr) => {
                tracing::info!(
                    transform_index = i,
                    stream = stream_name,
                    duration_secs = tr.duration_secs,
                    records_in = tr.summary.records_in,
                    records_out = tr.summary.records_out,
                    "Transform stage completed for stream"
                );
                durations.push(tr.duration_secs);
            }
            Err(e) => {
                tracing::error!(
                    transform_index = i,
                    stream = stream_name,
                    "Transform failed: {e}",
                );
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }
    Ok(TransformStageOutcomes {
        durations,
        first_error,
    })
}

#[cfg(test)]
mod semaphore_tests {
    use super::*;

    #[tokio::test]
    async fn acquire_permit_cancellable_succeeds() {
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let cancel = CancellationToken::new();
        let permit = acquire_permit_cancellable(&sem, &cancel, "test").await;
        assert!(permit.is_ok());
    }

    #[tokio::test]
    async fn acquire_permit_cancellable_returns_cancelled() {
        let sem = Arc::new(tokio::sync::Semaphore::new(0));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = acquire_permit_cancellable(&sem, &cancel, "test").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_retryable());
    }
}

#[cfg(test)]
mod stream_task_collection_tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn success_result(stream_name: &str) -> StreamShardOutcome {
        StreamShardOutcome {
            stream_name: stream_name.to_string(),
            partition_index: None,
            partition_count: None,
            read_summary: ReadSummary {
                records_read: 0,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
            },
            write_summary: WriteSummary {
                records_written: 0,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
            },
            source_checkpoints: Vec::new(),
            dest_checkpoints: Vec::new(),
            source_duration_secs: 0.0,
            dest_duration_secs: 0.0,
            wasm_instantiation_secs: 0.0,
            frame_receive_secs: 0.0,
            transform_durations: Vec::new(),
            dry_run_result: None,
        }
    }

    #[tokio::test]
    async fn collect_stream_tasks_fails_fast_and_cancels_siblings() {
        let mut join_set: JoinSet<Result<StreamShardOutcome, PipelineError>> = JoinSet::new();
        join_set.spawn(async {
            tokio::time::sleep(Duration::from_millis(250)).await;
            Ok(success_result("slow_stream"))
        });
        join_set.spawn(async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            Err(PipelineError::infra("expected failure"))
        });

        let start = Instant::now();
        let collected = collect_stream_task_results(join_set, &None)
            .await
            .expect("collector should return first error, not infra panic");

        assert!(collected.first_error.is_some());
        assert!(collected.successes.is_empty());
        assert!(start.elapsed() < Duration::from_millis(200));
    }
}
