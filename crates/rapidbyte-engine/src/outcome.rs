//! Pipeline operation output types: results, timings, dry-run data, and check statuses.
//!
//! This module contains all public data types returned by pipeline operations
//! (`run_pipeline`, `check_pipeline`) and the input options (`ExecutionOptions`).

use arrow::record_batch::RecordBatch;
use rapidbyte_types::error::ValidationResult;

// ---------------------------------------------------------------------------
// Execution input
// ---------------------------------------------------------------------------

/// Runtime execution options (not part of pipeline YAML config).
#[derive(Debug, Clone, Default)]
pub struct ExecutionOptions {
    /// Skip destination, print output to stdout.
    pub dry_run: bool,
    /// Maximum rows to read per stream (only used with `dry_run`).
    pub limit: Option<u64>,
}

// ---------------------------------------------------------------------------
// Pipeline run output
// ---------------------------------------------------------------------------

/// Either a normal pipeline result or a dry-run result.
#[derive(Debug)]
pub enum PipelineOutcome {
    Run(PipelineResult),
    DryRun(DryRunResult),
}

/// Result of a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    pub counts: PipelineCounts,
    pub source: SourceTiming,
    pub dest: DestTiming,
    pub num_transforms: usize,
    pub total_transform_secs: f64,
    pub transform_load_times_ms: Vec<u64>,
    pub duration_secs: f64,
    pub wasm_overhead_secs: f64,
    pub retry_count: u32,
    pub parallelism: u32,
    pub stream_metrics: Vec<StreamShardMetric>,
}

/// Aggregate record/byte counts for a pipeline run.
#[derive(Debug, Clone, Default)]
pub struct PipelineCounts {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Source plugin timing breakdown.
#[derive(Debug, Clone, Default)]
pub struct SourceTiming {
    pub duration_secs: f64,
    pub module_load_ms: u64,
    pub connect_secs: f64,
    pub query_secs: f64,
    pub fetch_secs: f64,
    pub arrow_encode_secs: f64,
    pub emit_nanos: u64,
    pub compress_nanos: u64,
    pub emit_count: u64,
}

/// Destination plugin timing breakdown.
#[derive(Debug, Clone, Default)]
pub struct DestTiming {
    pub duration_secs: f64,
    pub module_load_ms: u64,
    pub connect_secs: f64,
    pub flush_secs: f64,
    pub commit_secs: f64,
    pub arrow_decode_secs: f64,
    pub wasm_instantiation_secs: f64,
    pub frame_receive_secs: f64,
    pub frame_receive_nanos: u64,
    pub frame_wait_nanos: u64,
    pub frame_process_nanos: u64,
    pub decompress_nanos: u64,
    pub frame_count: u64,
}

/// Per-stream/per-shard metrics for skew analysis.
#[derive(Debug, Clone)]
pub struct StreamShardMetric {
    pub stream_name: String,
    pub partition_index: Option<u32>,
    pub partition_count: Option<u32>,
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub source_duration_secs: f64,
    pub dest_duration_secs: f64,
    pub dest_wasm_instantiation_secs: f64,
    pub dest_frame_receive_secs: f64,
}

// ---------------------------------------------------------------------------
// Dry-run output
// ---------------------------------------------------------------------------

/// Result of a dry-run pipeline execution.
#[derive(Debug)]
pub struct DryRunResult {
    pub streams: Vec<DryRunStreamResult>,
    pub source: SourceTiming,
    pub num_transforms: usize,
    pub total_transform_secs: f64,
    pub duration_secs: f64,
}

/// Result of a single stream in dry-run mode.
#[derive(Debug)]
pub struct DryRunStreamResult {
    pub stream_name: String,
    pub batches: Vec<RecordBatch>,
    pub total_rows: u64,
    pub total_bytes: u64,
}

// ---------------------------------------------------------------------------
// Check output
// ---------------------------------------------------------------------------

/// Result of a single check item (manifest, config, state, etc.).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckStatus {
    pub ok: bool,
    pub message: String,
}

/// Result of a full pipeline check.
#[derive(Debug, Clone)]
pub struct CheckResult {
    pub source_manifest: Option<CheckStatus>,
    pub destination_manifest: Option<CheckStatus>,
    pub source_config: Option<CheckStatus>,
    pub destination_config: Option<CheckStatus>,
    pub transform_configs: Vec<CheckStatus>,
    pub source_validation: ValidationResult,
    pub destination_validation: ValidationResult,
    pub transform_validations: Vec<ValidationResult>,
    pub state: CheckStatus,
}

// ---------------------------------------------------------------------------
// Internal timing constructors (pub(crate) — used by finalizers/run.rs)
// ---------------------------------------------------------------------------

impl SourceTiming {
    /// Construct a `SourceTiming` from a metrics snapshot and wall-clock totals.
    pub(crate) fn from_snapshot(
        snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
        max_source_duration: f64,
        source_module_load_ms: u64,
    ) -> Self {
        Self {
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
}

impl DestTiming {
    /// Construct a `DestTiming` from a metrics snapshot and wall-clock totals.
    pub(crate) fn from_snapshot(
        snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
        max_dest_duration: f64,
        max_wasm_instantiation_secs: f64,
        max_frame_receive_secs: f64,
        dest_module_load_ms: u64,
    ) -> Self {
        Self {
            duration_secs: max_dest_duration,
            module_load_ms: dest_module_load_ms,
            connect_secs: snap.dest_connect_secs,
            flush_secs: snap.dest_flush_secs,
            commit_secs: snap.dest_commit_secs,
            arrow_decode_secs: snap.dest_decode_secs,
            wasm_instantiation_secs: max_wasm_instantiation_secs,
            frame_receive_secs: max_frame_receive_secs,
            frame_receive_nanos: snap.next_batch_nanos,
            frame_wait_nanos: snap.next_batch_wait_nanos,
            frame_process_nanos: snap.next_batch_process_nanos,
            decompress_nanos: snap.decompress_nanos,
            frame_count: snap.next_batch_count,
        }
    }
}

/// Compute the WASM overhead seconds from a metrics snapshot and wall-clock totals.
pub(crate) fn compute_wasm_overhead_secs(
    snap: &rapidbyte_metrics::snapshot::PipelineMetricsSnapshot,
    max_dest_duration: f64,
    max_wasm_instantiation_secs: f64,
    max_frame_receive_secs: f64,
) -> f64 {
    let plugin_internal_secs =
        snap.dest_connect_secs + snap.dest_flush_secs + snap.dest_commit_secs;

    (max_dest_duration
        - max_wasm_instantiation_secs
        - max_frame_receive_secs
        - plugin_internal_secs)
        .max(0.0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_options_default_is_normal_mode() {
        let opts = ExecutionOptions::default();
        assert!(!opts.dry_run);
        assert!(opts.limit.is_none());
    }

    #[test]
    fn dry_run_stream_result_holds_batches() {
        let result = DryRunStreamResult {
            stream_name: "public.users".into(),
            batches: vec![],
            total_rows: 0,
            total_bytes: 0,
        };
        assert_eq!(result.stream_name, "public.users");
    }
}
