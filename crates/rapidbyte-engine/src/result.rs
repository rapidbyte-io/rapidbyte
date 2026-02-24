//! Pipeline execution result types and timing breakdowns.

use rapidbyte_types::error::ValidationResult;

/// Aggregate record/byte counts for a pipeline run.
#[derive(Debug, Clone, Default)]
pub struct PipelineCounts {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Source connector timing breakdown.
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

/// Destination connector timing breakdown.
#[derive(Debug, Clone, Default)]
pub struct DestTiming {
    pub duration_secs: f64,
    pub module_load_ms: u64,
    pub connect_secs: f64,
    pub flush_secs: f64,
    pub commit_secs: f64,
    pub arrow_decode_secs: f64,
    pub vm_setup_secs: f64,
    pub recv_secs: f64,
    pub recv_nanos: u64,
    pub decompress_nanos: u64,
    pub recv_count: u64,
}

/// Result of a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    pub counts: PipelineCounts,
    pub source: SourceTiming,
    pub dest: DestTiming,
    pub transform_count: usize,
    pub transform_duration_secs: f64,
    pub transform_module_load_ms: Vec<u64>,
    pub duration_secs: f64,
    pub wasm_overhead_secs: f64,
    pub retry_count: u32,
}

/// Result of a pipeline check.
#[derive(Debug)]
pub struct CheckResult {
    pub source_validation: ValidationResult,
    pub destination_validation: ValidationResult,
    pub transform_validations: Vec<ValidationResult>,
    pub state_ok: bool,
}
