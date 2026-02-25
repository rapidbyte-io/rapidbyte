//! Execution mode types for pipeline runs.

use arrow::record_batch::RecordBatch;

use crate::result::{PipelineResult, SourceTiming};

/// Runtime execution options (not part of pipeline YAML config).
#[derive(Debug, Clone, Default)]
pub struct ExecutionOptions {
    /// Skip destination, print output to stdout.
    pub dry_run: bool,
    /// Maximum rows to read per stream (only used with `dry_run`).
    pub limit: Option<u64>,
}

/// Result of a single stream in dry-run mode.
#[derive(Debug)]
pub struct DryRunStreamResult {
    pub stream_name: String,
    pub batches: Vec<RecordBatch>,
    pub total_rows: u64,
    pub total_bytes: u64,
}

/// Result of a dry-run pipeline execution.
#[derive(Debug)]
pub struct DryRunResult {
    pub streams: Vec<DryRunStreamResult>,
    pub source: SourceTiming,
    pub transform_count: usize,
    pub transform_duration_secs: f64,
    pub duration_secs: f64,
}

/// Either a normal pipeline result or a dry-run result.
#[derive(Debug)]
pub enum PipelineOutcome {
    Run(PipelineResult),
    DryRun(DryRunResult),
}

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
