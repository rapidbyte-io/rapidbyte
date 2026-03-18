//! Metrics snapshot port trait.
//!
//! Abstracts metrics collection so the orchestrator can obtain a
//! point-in-time snapshot of pipeline counters and timings without
//! depending on a specific telemetry backend.

use rapidbyte_metrics::snapshot::PipelineMetricsSnapshot;

/// Port for obtaining a point-in-time metrics snapshot.
///
/// Implementations flush pending telemetry data and return an aggregated
/// snapshot of all counters and histograms recorded during a pipeline run.
pub trait MetricsSnapshot: Send + Sync {
    /// Collect and return a metrics snapshot for the current run.
    fn snapshot_for_run(&self) -> PipelineMetricsSnapshot;
}
