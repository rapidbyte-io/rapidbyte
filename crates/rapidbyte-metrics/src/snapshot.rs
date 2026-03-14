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

/// Check whether a data point's attributes contain a matching pipeline label.
fn has_pipeline_attr(attrs: &[opentelemetry::KeyValue], pipeline: &str) -> bool {
    // If no pipeline label is present, include the data point (host/global instruments).
    // If present, it must match the requested pipeline.
    attrs
        .iter()
        .all(|kv| kv.key.as_str() != crate::labels::PIPELINE || kv.value.as_str() == pipeline)
}

/// Extract a metric value into the snapshot struct by matching instrument name.
/// Only data points matching the requested pipeline are included.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn extract_metric_value(
    name: &str,
    data: &dyn Aggregation,
    pipeline: &str,
    snap: &mut PipelineMetricsSnapshot,
) {
    use opentelemetry_sdk::metrics::data::{Histogram, Sum};

    // Counter instruments produce Sum<u64>
    if let Some(sum) = data.as_any().downcast_ref::<Sum<u64>>() {
        let total: u64 = sum
            .data_points
            .iter()
            .filter(|dp| has_pipeline_attr(&dp.attributes, pipeline))
            .map(|dp| dp.value)
            .sum();
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
        let filtered = hist
            .data_points
            .iter()
            .filter(|dp| has_pipeline_attr(&dp.attributes, pipeline));
        let mut total_sum: f64 = 0.0;
        let mut total_count: u64 = 0;
        for dp in filtered {
            total_sum += dp.sum;
            total_count += dp.count;
        }

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
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::Resource;

    /// Create a test meter provider wired to a snapshot reader.
    fn test_provider() -> (SdkMeterProvider, SnapshotReader) {
        let reader = SnapshotReader::new();
        let provider = SdkMeterProvider::builder()
            .with_resource(Resource::builder().with_service_name("test").build())
            .with_reader(reader.build_reader())
            .build();
        (provider, reader)
    }

    #[test]
    fn snapshot_returns_default_when_no_metrics_recorded() {
        let reader = SnapshotReader::new();
        let result = reader.snapshot_pipeline_result("test-pipeline");
        assert_eq!(result.records_read, 0);
        assert_eq!(result.records_written, 0);
    }

    #[test]
    fn snapshot_extracts_counter_values() {
        let (provider, reader) = test_provider();
        let meter = provider.meter("test");

        let records_read = meter.u64_counter("pipeline.records_read").build();
        let bytes_read = meter.u64_counter("pipeline.bytes_read").build();
        let labels = [KeyValue::new(crate::labels::PIPELINE, "my-pipe")];

        records_read.add(100, &labels);
        records_read.add(50, &labels);
        bytes_read.add(4096, &labels);

        let snap = reader.flush_and_snapshot(&provider, "my-pipe");
        assert_eq!(snap.records_read, 150);
        assert_eq!(snap.bytes_read, 4096);
    }

    #[test]
    fn snapshot_extracts_histogram_values() {
        let (provider, reader) = test_provider();
        let meter = provider.meter("test");

        let hist = meter
            .f64_histogram("plugin.source_connect_duration")
            .build();
        let labels = [KeyValue::new(crate::labels::PIPELINE, "my-pipe")];

        hist.record(0.5, &labels);
        hist.record(1.5, &labels);

        let snap = reader.flush_and_snapshot(&provider, "my-pipe");
        assert!((snap.source_connect_secs - 2.0).abs() < 0.001);
    }

    #[test]
    fn snapshot_filters_by_pipeline_label() {
        let (provider, reader) = test_provider();
        let meter = provider.meter("test");

        let counter = meter.u64_counter("pipeline.records_read").build();
        counter.add(100, &[KeyValue::new(crate::labels::PIPELINE, "pipe-a")]);
        counter.add(200, &[KeyValue::new(crate::labels::PIPELINE, "pipe-b")]);

        let snap_a = reader.flush_and_snapshot(&provider, "pipe-a");
        assert_eq!(snap_a.records_read, 100);

        let snap_b = reader.flush_and_snapshot(&provider, "pipe-b");
        assert_eq!(snap_b.records_read, 200);
    }

    #[test]
    fn snapshot_host_timing_converts_seconds_to_nanos() {
        let (provider, reader) = test_provider();
        let meter = provider.meter("test");

        let hist = meter.f64_histogram("host.emit_batch_duration").build();
        let labels = [KeyValue::new(crate::labels::PIPELINE, "my-pipe")];
        hist.record(0.001, &labels); // 1ms
        hist.record(0.002, &labels); // 2ms

        let snap = reader.flush_and_snapshot(&provider, "my-pipe");
        assert_eq!(snap.emit_batch_nanos, 3_000_000); // 3ms in nanos
        assert_eq!(snap.emit_count, 2);
    }
}
