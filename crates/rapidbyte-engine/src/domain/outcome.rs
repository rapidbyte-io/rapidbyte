//! Pipeline operation output types: results, timings, dry-run data, and check statuses.
//!
//! These are pure data containers ported from the legacy `outcome` module to
//! live alongside the domain model.

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
