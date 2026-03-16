//! Run finalization: error handling, checkpoint correlation, DLQ persistence,
//! run-status marking, and timing/result assembly.

use std::sync::Arc;
use std::time::Instant;

use rapidbyte_state::StateBackend;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::metric::{ReadSummary, WriteSummary};
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus};

use crate::config::types::PipelineConfig;
use crate::error::PipelineError;
use crate::execution::{DryRunResult, DryRunStreamResult};
use crate::finalizers::checkpoint::correlate_and_persist_cursors;
use crate::pipeline::planner::compute_pipeline_parallelism;
use crate::plugin::loader::PluginModules;
use crate::result::{DestTiming, PipelineCounts, PipelineResult, SourceTiming, StreamShardMetric};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

pub(crate) struct StreamAggregation {
    pub(crate) execution_parallelism: u32,
    pub(crate) total_read_summary: ReadSummary,
    pub(crate) total_write_summary: WriteSummary,
    pub(crate) source_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    pub(crate) dest_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    pub(crate) max_source_duration: f64,
    pub(crate) max_dest_duration: f64,
    pub(crate) max_vm_setup_secs: f64,
    pub(crate) max_recv_secs: f64,
    pub(crate) transform_durations: Vec<f64>,
    pub(crate) dlq_records: Vec<DlqRecord>,
    pub(crate) final_stats: RunStats,
    pub(crate) first_error: Option<PipelineError>,
    pub(crate) dry_run_streams: Vec<DryRunStreamResult>,
    pub(crate) stream_metrics: Vec<StreamShardMetric>,
}

pub(crate) struct MetricsRuntime<'a> {
    pub(crate) snapshot_reader: &'a rapidbyte_metrics::snapshot::SnapshotReader,
    pub(crate) meter_provider: &'a opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl MetricsRuntime<'_> {
    pub(crate) fn snapshot_for_run(
        &self,
        pipeline: &str,
        run: Option<&str>,
    ) -> rapidbyte_metrics::snapshot::PipelineMetricsSnapshot {
        self.snapshot_reader
            .flush_and_snapshot_for_run(self.meter_provider, pipeline, run)
    }
}

pub(crate) fn prepare_metrics_runtime<'a>(
    snapshot_reader: &'a rapidbyte_metrics::snapshot::SnapshotReader,
    meter_provider: &'a opentelemetry_sdk::metrics::SdkMeterProvider,
) -> MetricsRuntime<'a> {
    MetricsRuntime {
        snapshot_reader,
        meter_provider,
    }
}

// ---------------------------------------------------------------------------
// Main finalization entry point
// ---------------------------------------------------------------------------

/// Finalize a pipeline run: handle stream errors, correlate checkpoints,
/// persist DLQ records, mark run status, and assemble timing results.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn finalize_pipeline_execution(
    config: &PipelineConfig,
    pipeline_id: &PipelineId,
    state: Arc<dyn StateBackend>,
    run_id: i64,
    attempt: u32,
    start: Instant,
    metric_run_label: &str,
    modules: &PluginModules,
    mut aggregated: StreamAggregation,
    metrics_runtime: &MetricsRuntime<'_>,
) -> Result<PipelineResult, PipelineError> {
    // 1. If any stream failed → mark Failed, persist DLQ, return error
    if let Some(err) = aggregated.first_error {
        // Drain the run's snapshot entry so it doesn't accumulate in the
        // SnapshotReader's finished_run_snapshots map (memory leak in
        // long-lived agent processes).
        let _ = metrics_runtime.snapshot_for_run(&config.pipeline, Some(metric_run_label));

        let run_stats = RunStats {
            records_read: aggregated.final_stats.records_read,
            records_written: aggregated.final_stats.records_written,
            bytes_read: aggregated.final_stats.bytes_read,
            bytes_written: aggregated.final_stats.bytes_written,
            error_message: Some(format!("Stream error: {err}")),
        };
        mark_run_complete(state.clone(), run_id, RunStatus::Failed, run_stats).await;

        persist_dlq(
            state.clone(),
            pipeline_id,
            run_id,
            &mut aggregated.dlq_records,
        )
        .await?;

        return Err(err);
    }

    // 2. Take metrics snapshot
    let snap = metrics_runtime.snapshot_for_run(&config.pipeline, Some(metric_run_label));

    let wasm_overhead_secs = compute_wasm_overhead_secs(&snap, &aggregated);

    tracing::debug!(
        pipeline = config.pipeline,
        source_checkpoint_count = aggregated.source_checkpoints.len(),
        dest_checkpoint_count = aggregated.dest_checkpoints.len(),
        "About to correlate checkpoints"
    );

    // 3-5. Correlate checkpoints, persist DLQ, mark run status
    let cursors_advanced =
        persist_run_state(state.clone(), pipeline_id, run_id, &mut aggregated).await?;
    if cursors_advanced > 0 {
        tracing::info!(
            pipeline = config.pipeline,
            cursors_advanced,
            "Checkpoint coordination complete"
        );
    }

    // 6. Assemble and return PipelineResult
    let duration = start.elapsed();
    let transform_load_times_ms = modules
        .transform_modules
        .iter()
        .map(|m| m.load_ms)
        .collect::<Vec<_>>();

    tracing::info!(
        pipeline = config.pipeline,
        records_read = aggregated.total_read_summary.records_read,
        records_written = aggregated.total_write_summary.records_written,
        duration_secs = duration.as_secs_f64(),
        "Pipeline run completed"
    );

    Ok(PipelineResult {
        counts: PipelineCounts {
            records_read: aggregated.total_read_summary.records_read,
            records_written: aggregated.total_write_summary.records_written,
            bytes_read: aggregated.total_read_summary.bytes_read,
            bytes_written: aggregated.total_write_summary.bytes_written,
        },
        source: build_source_timing(
            &snap,
            aggregated.max_source_duration,
            modules.source_module_load_ms,
        ),
        dest: build_dest_timing(&snap, &aggregated, modules.dest_module_load_ms),
        num_transforms: aggregated.transform_durations.len(),
        total_transform_secs: aggregated.transform_durations.iter().sum(),
        transform_load_times_ms,
        duration_secs: duration.as_secs_f64(),
        wasm_overhead_secs,
        retry_count: attempt.saturating_sub(1),
        parallelism: reported_parallelism(config, &aggregated),
        stream_metrics: aggregated.stream_metrics,
    })
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Fire-and-forget run status marker. The old code used `let _ =` on errors
/// from `complete_run_status` in failure paths, so we unify on fire-and-forget
/// to simplify the control flow.
async fn mark_run_complete(
    backend: Arc<dyn StateBackend>,
    run_id: i64,
    status: RunStatus,
    run_stats: RunStats,
) {
    let result =
        tokio::task::spawn_blocking(move || backend.complete_run(run_id, status, &run_stats)).await;
    match result {
        Err(e) => tracing::warn!("mark_run_complete task panicked: {e}"),
        Ok(Err(e)) => tracing::warn!("mark_run_complete backend error: {e}"),
        Ok(Ok(())) => {}
    }
}

/// Persist DLQ records, spawning blocking work off the async runtime.
async fn persist_dlq(
    state: Arc<dyn StateBackend>,
    pipeline_id: &PipelineId,
    run_id: i64,
    dlq_records: &mut Vec<DlqRecord>,
) -> Result<(), PipelineError> {
    let records = std::mem::take(dlq_records);
    let pipeline_id = pipeline_id.clone();
    tokio::task::spawn_blocking(move || {
        crate::finalizers::dlq::persist_dlq_records(state.as_ref(), &pipeline_id, run_id, &records)
    })
    .await
    .map_err(|e| PipelineError::task_panicked("persist_dlq_records", e))?
    .map_err(|e| PipelineError::Infrastructure(e.into()))?;
    Ok(())
}

/// Correlate checkpoints, persist DLQ records, and mark the run status.
///
/// On success returns the number of cursors advanced and marks the run as
/// `Completed`. On failure marks the run as `Failed` and returns an error.
pub(crate) async fn persist_run_state(
    state: Arc<dyn StateBackend>,
    pipeline_id: &PipelineId,
    run_id: i64,
    aggregated: &mut StreamAggregation,
) -> Result<u64, PipelineError> {
    let state_for_cursor = state.clone();
    let pipeline_id_for_cursor = pipeline_id.clone();
    let source_checkpoints = std::mem::take(&mut aggregated.source_checkpoints);
    let dest_checkpoints = std::mem::take(&mut aggregated.dest_checkpoints);
    let cursor_result = tokio::task::spawn_blocking(move || {
        correlate_and_persist_cursors(
            state_for_cursor.as_ref(),
            &pipeline_id_for_cursor,
            &source_checkpoints,
            &dest_checkpoints,
        )
    })
    .await
    .map_err(|e| PipelineError::task_panicked("correlate_and_persist_cursors", e))?;

    let cursors_advanced = match cursor_result {
        Ok(n) => n,
        Err(error) => {
            let failed_stats = RunStats {
                records_read: aggregated.total_read_summary.records_read,
                records_written: aggregated.total_write_summary.records_written,
                bytes_read: aggregated.total_read_summary.bytes_read,
                bytes_written: aggregated.total_write_summary.bytes_written,
                error_message: Some(format!("Post-run finalization failed: {error}")),
            };
            mark_run_complete(state, run_id, RunStatus::Failed, failed_stats).await;
            return Err(PipelineError::Infrastructure(error));
        }
    };

    let state_for_dlq = state.clone();
    let pipeline_id_for_dlq = pipeline_id.clone();
    let dlq_records = std::mem::take(&mut aggregated.dlq_records);
    let dlq_result = tokio::task::spawn_blocking(move || {
        crate::finalizers::dlq::persist_dlq_records(
            state_for_dlq.as_ref(),
            &pipeline_id_for_dlq,
            run_id,
            &dlq_records,
        )
    })
    .await
    .map_err(|e| PipelineError::task_panicked("persist_dlq_records", e))?;
    if let Err(error) = dlq_result {
        let failed_stats = RunStats {
            records_read: aggregated.total_read_summary.records_read,
            records_written: aggregated.total_write_summary.records_written,
            bytes_read: aggregated.total_read_summary.bytes_read,
            bytes_written: aggregated.total_write_summary.bytes_written,
            error_message: Some(format!("Post-run finalization failed: {error}")),
        };
        mark_run_complete(state, run_id, RunStatus::Failed, failed_stats).await;
        return Err(PipelineError::Infrastructure(error.into()));
    }

    let complete_stats = RunStats {
        records_read: aggregated.total_read_summary.records_read,
        records_written: aggregated.total_write_summary.records_written,
        bytes_read: aggregated.total_read_summary.bytes_read,
        bytes_written: aggregated.total_write_summary.bytes_written,
        error_message: None,
    };
    mark_run_complete(state, run_id, RunStatus::Completed, complete_stats).await;

    Ok(cursors_advanced)
}

// ---------------------------------------------------------------------------
// Timing / metric helpers (moved from orchestrator.rs unchanged)
// ---------------------------------------------------------------------------

pub(crate) fn reported_parallelism(config: &PipelineConfig, aggregated: &StreamAggregation) -> u32 {
    if aggregated.execution_parallelism > 0 {
        aggregated.execution_parallelism
    } else {
        compute_pipeline_parallelism(config, false)
    }
}

pub(crate) fn build_source_timing(
    snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
    max_source_duration: f64,
    source_module_load_ms: u64,
) -> SourceTiming {
    SourceTiming {
        duration_secs: max_source_duration,
        module_load_ms: source_module_load_ms,
        connect_secs: snap.source_connect_secs,
        query_secs: snap.source_query_secs,
        fetch_secs: snap.source_fetch_secs,
        arrow_encode_secs: snap.source_encode_secs,
        emit_nanos: snap.emit_batch_nanos,
        compress_nanos: snap.compress_nanos,
        emit_count: snap.emit_count,
    }
}

pub(crate) fn build_dry_run_result(
    snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
    aggregated: StreamAggregation,
    source_module_load_ms: u64,
    duration_secs: f64,
) -> DryRunResult {
    DryRunResult {
        streams: aggregated.dry_run_streams,
        source: build_source_timing(snap, aggregated.max_source_duration, source_module_load_ms),
        num_transforms: aggregated.transform_durations.len(),
        total_transform_secs: aggregated.transform_durations.iter().sum(),
        duration_secs,
    }
}

pub(crate) fn compute_wasm_overhead_secs(
    snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
    aggregated: &StreamAggregation,
) -> f64 {
    let plugin_internal_secs =
        snap.dest_connect_secs + snap.dest_flush_secs + snap.dest_commit_secs;

    (aggregated.max_dest_duration
        - aggregated.max_vm_setup_secs
        - aggregated.max_recv_secs
        - plugin_internal_secs)
        .max(0.0)
}

pub(crate) fn build_dest_timing(
    snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
    aggregated: &StreamAggregation,
    dest_module_load_ms: u64,
) -> DestTiming {
    DestTiming {
        duration_secs: aggregated.max_dest_duration,
        module_load_ms: dest_module_load_ms,
        connect_secs: snap.dest_connect_secs,
        flush_secs: snap.dest_flush_secs,
        commit_secs: snap.dest_commit_secs,
        arrow_decode_secs: snap.dest_decode_secs,
        wasm_instantiation_secs: aggregated.max_vm_setup_secs,
        frame_receive_secs: aggregated.max_recv_secs,
        frame_receive_nanos: snap.next_batch_nanos,
        frame_wait_nanos: snap.next_batch_wait_nanos,
        frame_process_nanos: snap.next_batch_process_nanos,
        decompress_nanos: snap.decompress_nanos,
        frame_count: snap.next_batch_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_state::error::{Result as StateResult, StateError};
    use rapidbyte_types::checkpoint::{Checkpoint, CheckpointKind};
    use rapidbyte_types::cursor::CursorValue;
    use rapidbyte_types::state::CursorState;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use rapidbyte_types::state::StreamName;

    struct TestStateBackend {
        complete_statuses: Mutex<Vec<(RunStatus, Option<String>)>>,
        cursor_written: AtomicBool,
        fail_set_cursor: bool,
        fail_insert_dlq: bool,
    }

    impl TestStateBackend {
        fn new(fail_set_cursor: bool, fail_insert_dlq: bool) -> Self {
            Self {
                complete_statuses: Mutex::new(Vec::new()),
                cursor_written: AtomicBool::new(false),
                fail_set_cursor,
                fail_insert_dlq,
            }
        }
    }

    impl StateBackend for TestStateBackend {
        fn get_cursor(
            &self,
            _pipeline: &PipelineId,
            _stream: &StreamName,
        ) -> StateResult<Option<CursorState>> {
            Ok(None)
        }

        fn set_cursor(
            &self,
            _pipeline: &PipelineId,
            _stream: &StreamName,
            _cursor: &CursorState,
        ) -> StateResult<()> {
            if self.fail_set_cursor {
                return Err(StateError::backend(std::io::Error::other(
                    "cursor write failed",
                )));
            }
            self.cursor_written.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn start_run(&self, _pipeline: &PipelineId, _stream: &StreamName) -> StateResult<i64> {
            Ok(1)
        }

        fn complete_run(
            &self,
            _run_id: i64,
            run_status: RunStatus,
            completion_stats: &RunStats,
        ) -> StateResult<()> {
            self.complete_statuses
                .lock()
                .expect("complete statuses lock poisoned")
                .push((run_status, completion_stats.error_message.clone()));
            Ok(())
        }

        fn compare_and_set(
            &self,
            _pipeline: &PipelineId,
            _stream: &StreamName,
            _expected: Option<&str>,
            _new_value: &str,
        ) -> StateResult<bool> {
            Ok(true)
        }

        fn insert_dlq_records(
            &self,
            _pipeline: &PipelineId,
            _run_id: i64,
            _records: &[DlqRecord],
        ) -> StateResult<u64> {
            if self.fail_insert_dlq {
                return Err(StateError::backend(std::io::Error::other(
                    "dlq insert failed",
                )));
            }
            Ok(0)
        }
    }

    fn make_aggregated_results() -> StreamAggregation {
        StreamAggregation {
            execution_parallelism: 1,
            total_read_summary: ReadSummary {
                records_read: 10,
                bytes_read: 100,
                batches_emitted: 1,
                checkpoint_count: 1,
                records_skipped: 0,
            },
            total_write_summary: WriteSummary {
                records_written: 10,
                bytes_written: 100,
                batches_written: 1,
                checkpoint_count: 1,
                records_failed: 0,
            },
            source_checkpoints: vec![Checkpoint {
                id: 7,
                kind: CheckpointKind::Source,
                stream: "users".to_string(),
                cursor_field: Some("id".to_string()),
                cursor_value: Some(CursorValue::Int64 { value: 42 }),
                records_processed: 10,
                bytes_processed: 100,
            }],
            dest_checkpoints: vec![Checkpoint {
                id: 7,
                kind: CheckpointKind::Dest,
                stream: "users".to_string(),
                cursor_field: None,
                cursor_value: None,
                records_processed: 10,
                bytes_processed: 100,
            }],
            max_source_duration: 0.0,
            max_dest_duration: 0.0,
            max_vm_setup_secs: 0.0,
            max_recv_secs: 0.0,
            transform_durations: Vec::new(),
            dlq_records: Vec::new(),
            final_stats: RunStats::default(),
            first_error: None,
            dry_run_streams: Vec::new(),
            stream_metrics: Vec::new(),
        }
    }

    #[test]
    fn reported_parallelism_prefers_aggregated_execution_parallelism() {
        let yaml = r#"
version: "1.0"
pipeline: test_parallelism
source:
  use: postgres
  config: {}
  streams:
    - name: bench_events
      sync_mode: full_refresh
destination:
  use: postgres
  config: {}
  write_mode: append
resources:
  parallelism: auto
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).expect("valid pipeline yaml");
        let mut aggregated = make_aggregated_results();
        aggregated.execution_parallelism = 12;

        assert_eq!(reported_parallelism(&config, &aggregated), 12);
    }

    #[tokio::test]
    async fn successful_finalization_marks_run_completed_after_cursor_persist() {
        let backend = Arc::new(TestStateBackend::new(false, false));
        let mut aggregated = make_aggregated_results();

        let advanced =
            persist_run_state(backend.clone(), &PipelineId::new("p"), 1, &mut aggregated)
                .await
                .expect("finalization should succeed");

        assert_eq!(advanced, 1);
        assert!(backend.cursor_written.load(Ordering::SeqCst));
        assert_eq!(
            backend
                .complete_statuses
                .lock()
                .expect("complete statuses lock poisoned")
                .as_slice(),
            &[(RunStatus::Completed, None)]
        );
    }

    #[tokio::test]
    async fn cursor_finalization_failure_marks_run_failed_not_completed() {
        let backend = Arc::new(TestStateBackend::new(true, false));
        let mut aggregated = make_aggregated_results();

        let result =
            persist_run_state(backend.clone(), &PipelineId::new("p"), 1, &mut aggregated).await;

        assert!(result.is_err());
        assert!(!backend.cursor_written.load(Ordering::SeqCst));

        let statuses = backend
            .complete_statuses
            .lock()
            .expect("complete statuses lock poisoned");
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].0, RunStatus::Failed);
        assert!(statuses[0]
            .1
            .as_deref()
            .unwrap_or_default()
            .contains("Post-run finalization failed"));
    }

    #[tokio::test]
    async fn dlq_finalization_failure_marks_run_failed_not_completed() {
        let backend = Arc::new(TestStateBackend::new(false, true));
        let mut aggregated = make_aggregated_results();
        aggregated.dlq_records.push(DlqRecord {
            stream_name: "users".to_string(),
            record_json: "{\"id\":42}".to_string(),
            error_message: "bad row".to_string(),
            error_category: rapidbyte_types::error::ErrorCategory::Data,
            failed_at: rapidbyte_types::envelope::Timestamp::new("2026-03-08T00:00:00Z"),
        });

        let result =
            persist_run_state(backend.clone(), &PipelineId::new("p"), 1, &mut aggregated).await;

        assert!(result.is_err());
        assert!(backend.cursor_written.load(Ordering::SeqCst));

        let statuses = backend
            .complete_statuses
            .lock()
            .expect("complete statuses lock poisoned");
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].0, RunStatus::Failed);
        assert!(statuses[0]
            .1
            .as_deref()
            .unwrap_or_default()
            .contains("Post-run finalization failed"));
    }
}
