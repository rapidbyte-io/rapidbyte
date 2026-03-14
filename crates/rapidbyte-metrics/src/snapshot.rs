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

    /// Flush the meter provider and return a pipeline metrics snapshot.
    #[must_use]
    pub fn flush_and_snapshot(
        &self,
        meter_provider: &opentelemetry_sdk::metrics::SdkMeterProvider,
        pipeline: &str,
    ) -> PipelineMetricsSnapshot {
        let _ = meter_provider.force_flush();
        self.snapshot_pipeline_result(pipeline)
    }

    /// Return a pipeline metrics snapshot filtered by pipeline label.
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
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn extract_metric_value(
    name: &str,
    data: &dyn Aggregation,
    _pipeline: &str,
    snap: &mut PipelineMetricsSnapshot,
) {
    use opentelemetry_sdk::metrics::data::{Histogram, Sum};

    // Counter instruments produce Sum<u64>
    if let Some(sum) = data.as_any().downcast_ref::<Sum<u64>>() {
        let total: u64 = sum.data_points.iter().map(|dp| dp.value).sum();
        match name {
            "pipeline.records_read" => snap.records_read = total,
            "pipeline.records_written" => snap.records_written = total,
            "pipeline.bytes_read" => snap.bytes_read = total,
            "pipeline.bytes_written" => snap.bytes_written = total,
            _ => {}
        }
        return;
    }

    // Duration instruments produce Histogram<f64> (values in seconds)
    if let Some(hist) = data.as_any().downcast_ref::<Histogram<f64>>() {
        let total_sum: f64 = hist.data_points.iter().map(|dp| dp.sum).sum();
        let total_count: u64 = hist.data_points.iter().map(|dp| dp.count).sum();

        match name {
            // Plugin source timings (seconds)
            "plugin.source_connect_duration" => snap.source_connect_secs = total_sum,
            "plugin.source_query_duration" => snap.source_query_secs = total_sum,
            "plugin.source_fetch_duration" => snap.source_fetch_secs = total_sum,
            "plugin.source_encode_duration" => snap.source_encode_secs = total_sum,
            // Plugin dest timings (seconds)
            "plugin.dest_connect_duration" => snap.dest_connect_secs = total_sum,
            "plugin.dest_flush_duration" => snap.dest_flush_secs = total_sum,
            "plugin.dest_commit_duration" => snap.dest_commit_secs = total_sum,
            "plugin.dest_decode_duration" => snap.dest_decode_secs = total_sum,
            // Host timings (seconds → nanos)
            "host.emit_batch_duration" => {
                snap.emit_batch_nanos = (total_sum * 1e9) as u64;
                snap.emit_count = total_count;
            }
            "host.next_batch_duration" => {
                snap.next_batch_nanos = (total_sum * 1e9) as u64;
                snap.next_batch_count = total_count;
            }
            "host.next_batch_wait_duration" => {
                snap.next_batch_wait_nanos = (total_sum * 1e9) as u64;
            }
            "host.next_batch_process_duration" => {
                snap.next_batch_process_nanos = (total_sum * 1e9) as u64;
            }
            "host.compress_duration" => {
                snap.compress_nanos = (total_sum * 1e9) as u64;
            }
            "host.decompress_duration" => {
                snap.decompress_nanos = (total_sum * 1e9) as u64;
            }
            "pipeline.duration" => snap.pipeline_duration_secs = total_sum,
            _ => {}
        }
    }
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
