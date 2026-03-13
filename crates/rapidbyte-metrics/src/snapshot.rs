//! [`InMemoryMetricExporter`] wrapper and `PipelineResult` bridge.

use opentelemetry_sdk::metrics::data::Aggregation;
use opentelemetry_sdk::metrics::InMemoryMetricExporter;
use opentelemetry_sdk::metrics::PeriodicReader;

/// Snapshot of pipeline metrics from `OTel` instruments.
/// The engine converts this to `PipelineResult` by adding non-instrument fields
/// (`wasm_overhead`, `retry_count`, `parallelism`, `stream_metrics`).
#[derive(Debug, Clone, Default)]
pub struct PipelineMetricsSnapshot {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub pipeline_duration_secs: f64,
    // Source timings (from plugin histograms)
    pub source_connect_secs: f64,
    pub source_query_secs: f64,
    pub source_fetch_secs: f64,
    pub source_encode_secs: f64,
    // Dest timings (from plugin histograms)
    pub dest_connect_secs: f64,
    pub dest_flush_secs: f64,
    pub dest_commit_secs: f64,
    pub dest_decode_secs: f64,
    // Host timings (from host histograms — sum of recorded values)
    pub emit_batch_nanos: u64,
    pub compress_nanos: u64,
    pub emit_count: u64,
    pub next_batch_nanos: u64,
    pub next_batch_wait_nanos: u64,
    pub next_batch_process_nanos: u64,
    pub decompress_nanos: u64,
    pub next_batch_count: u64,
}

/// Wraps an [`InMemoryMetricExporter`] for point-in-time snapshots.
pub struct SnapshotReader {
    exporter: InMemoryMetricExporter,
}

impl SnapshotReader {
    /// Create a new snapshot reader.
    #[must_use]
    pub fn new() -> Self {
        Self {
            exporter: InMemoryMetricExporter::default(),
        }
    }

    /// Build a [`PeriodicReader`] to register on the meter provider.
    #[must_use]
    pub fn build_reader(&self) -> PeriodicReader {
        PeriodicReader::builder(self.exporter.clone()).build()
    }

    /// Force a collect and return a pipeline metrics snapshot,
    /// filtered by the given pipeline label value.
    ///
    /// Note: [`InMemoryMetricExporter`] requires the [`PeriodicReader`] to have
    /// flushed. Call `meter_provider.force_flush()` before this method
    /// to ensure all pending metrics are available.
    #[must_use]
    pub fn snapshot_pipeline_result(&self, pipeline: &str) -> PipelineMetricsSnapshot {
        let metrics = self.exporter.get_finished_metrics().unwrap_or_default();

        let mut snap = PipelineMetricsSnapshot::default();

        for resource_metrics in &metrics {
            for scope_metrics in &resource_metrics.scope_metrics {
                for metric in &scope_metrics.metrics {
                    extract_metric_value(&metric.name, metric.data.as_ref(), pipeline, &mut snap);
                }
            }
        }

        snap
    }
}

impl Default for SnapshotReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract a metric value into the snapshot struct by matching instrument name.
fn extract_metric_value(
    name: &str,
    data: &dyn Aggregation,
    _pipeline: &str,
    snap: &mut PipelineMetricsSnapshot,
) {
    // Stub: matches instrument names to snapshot fields.
    // Full implementation will downcast `data` to `Sum<u64>`, `Histogram<f64>`, etc.
    // and read data points filtered by pipeline label attribute.
    let _ = (name, data, snap);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_returns_default_when_no_metrics_recorded() {
        let reader = SnapshotReader::new();
        let result = reader.snapshot_pipeline_result("test-pipeline");
        assert_eq!(result.records_read, 0);
        assert_eq!(result.records_written, 0);
    }
}
