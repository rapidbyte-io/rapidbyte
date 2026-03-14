//! [`InMemoryMetricExporter`] wrapper and `PipelineResult` bridge.

use std::collections::HashMap;
use std::time::Duration;

use opentelemetry_sdk::metrics::data::Aggregation;
use opentelemetry_sdk::metrics::InMemoryMetricExporter;
use opentelemetry_sdk::metrics::InMemoryMetricExporterBuilder;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::Temporality;

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
    plugin_timing_series_totals: HashMap<(String, String, usize), f64>,
}

impl PipelineMetricsSnapshot {
    fn duration_nanos(duration: Duration) -> u64 {
        u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
    }

    pub fn record_emit_batch(&mut self, duration: Duration) {
        self.emit_batch_nanos = self
            .emit_batch_nanos
            .saturating_add(Self::duration_nanos(duration));
        self.emit_count = self.emit_count.saturating_add(1);
    }

    pub fn record_next_batch(&mut self, total: Duration, wait: Duration, process: Duration) {
        self.next_batch_nanos = self
            .next_batch_nanos
            .saturating_add(Self::duration_nanos(total));
        self.next_batch_wait_nanos = self
            .next_batch_wait_nanos
            .saturating_add(Self::duration_nanos(wait));
        self.next_batch_process_nanos = self
            .next_batch_process_nanos
            .saturating_add(Self::duration_nanos(process));
        self.next_batch_count = self.next_batch_count.saturating_add(1);
    }

    pub fn record_compress(&mut self, duration: Duration) {
        self.compress_nanos = self
            .compress_nanos
            .saturating_add(Self::duration_nanos(duration));
    }

    pub fn record_decompress(&mut self, duration: Duration) {
        self.decompress_nanos = self
            .decompress_nanos
            .saturating_add(Self::duration_nanos(duration));
    }

    pub fn record_plugin_duration(&mut self, name: &str, value_secs: f64) {
        self.record_plugin_duration_for_series(name, "", 0, value_secs);
    }

    pub fn record_plugin_duration_for_series(
        &mut self,
        name: &str,
        stream: &str,
        shard: usize,
        value_secs: f64,
    ) {
        if !is_builtin_plugin_duration_name(name) {
            return;
        }

        let total = {
            let entry = self
                .plugin_timing_series_totals
                .entry((name.to_owned(), stream.to_owned(), shard))
                .or_default();
            *entry += value_secs;
            *entry
        };
        match name {
            "source_connect_secs" => self.source_connect_secs = self.source_connect_secs.max(total),
            "source_query_secs" => self.source_query_secs = self.source_query_secs.max(total),
            "source_fetch_secs" => self.source_fetch_secs = self.source_fetch_secs.max(total),
            "source_arrow_encode_secs" => {
                self.source_encode_secs = self.source_encode_secs.max(total);
            }
            "dest_connect_secs" => self.dest_connect_secs = self.dest_connect_secs.max(total),
            "dest_flush_secs" => self.dest_flush_secs = self.dest_flush_secs.max(total),
            "dest_commit_secs" => self.dest_commit_secs = self.dest_commit_secs.max(total),
            "dest_decode_secs" | "dest_arrow_decode_secs" => {
                self.dest_decode_secs = self.dest_decode_secs.max(total);
            }
            _ => {}
        }
    }

    #[must_use]
    pub fn tracked_plugin_timing_series_count(&self) -> usize {
        self.plugin_timing_series_totals.len()
    }
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
            exporter: InMemoryMetricExporterBuilder::new()
                .with_temporality(Temporality::Delta)
                .build(),
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
        self.flush_and_snapshot_for_run(meter_provider, pipeline, None)
    }

    /// Flush the meter provider and return a pipeline metrics snapshot for one run label.
    #[must_use]
    pub fn flush_and_snapshot_for_run(
        &self,
        meter_provider: &opentelemetry_sdk::metrics::SdkMeterProvider,
        pipeline: &str,
        run: Option<&str>,
    ) -> PipelineMetricsSnapshot {
        let _ = meter_provider.force_flush();
        self.snapshot_pipeline_result_for_run(pipeline, run)
    }

    /// Return a pipeline metrics snapshot filtered by pipeline label.
    ///
    /// Note: [`InMemoryMetricExporter`] requires the [`PeriodicReader`] to have
    /// flushed. Call `meter_provider.force_flush()` before this method
    /// to ensure all pending metrics are available.
    #[must_use]
    pub fn snapshot_pipeline_result(&self, pipeline: &str) -> PipelineMetricsSnapshot {
        self.snapshot_pipeline_result_for_run(pipeline, None)
    }

    /// Return a pipeline metrics snapshot filtered by pipeline and optional run label.
    #[must_use]
    pub fn snapshot_pipeline_result_for_run(
        &self,
        pipeline: &str,
        run: Option<&str>,
    ) -> PipelineMetricsSnapshot {
        let metrics = self.exporter.get_finished_metrics().unwrap_or_default();

        let mut snap = PipelineMetricsSnapshot::default();

        for resource_metrics in &metrics {
            for scope_metrics in &resource_metrics.scope_metrics {
                for metric in &scope_metrics.metrics {
                    extract_metric_value(
                        &metric.name,
                        metric.data.as_ref(),
                        pipeline,
                        run,
                        &mut snap,
                    );
                }
            }
        }

        snap
    }

    /// Clears collected finished metrics from the in-memory snapshot exporter.
    pub fn reset(&self) {
        self.exporter.reset();
    }
}

impl Default for SnapshotReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Check whether a data point's attributes contain a matching pipeline label.
fn matches_metric_scope(
    attrs: &[opentelemetry::KeyValue],
    pipeline: &str,
    run: Option<&str>,
) -> bool {
    let pipeline_matches = attrs
        .iter()
        .any(|kv| kv.key.as_str() == crate::labels::PIPELINE && kv.value.as_str() == pipeline);

    if !pipeline_matches {
        return false;
    }

    match run {
        Some(run) => attrs
            .iter()
            .any(|kv| kv.key.as_str() == crate::labels::RUN && kv.value.as_str() == run),
        None => true,
    }
}

/// Extract a metric value into the snapshot struct by matching instrument name.
/// Only data points matching the requested pipeline are included.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn extract_metric_value(
    name: &str,
    data: &dyn Aggregation,
    pipeline: &str,
    run: Option<&str>,
    snap: &mut PipelineMetricsSnapshot,
) {
    use opentelemetry_sdk::metrics::data::{Histogram, Sum};

    // Counter instruments produce Sum<u64>
    if let Some(sum) = data.as_any().downcast_ref::<Sum<u64>>() {
        let total: u64 = sum
            .data_points
            .iter()
            .filter(|dp| matches_metric_scope(&dp.attributes, pipeline, run))
            .map(|dp| dp.value)
            .sum();
        match name {
            "pipeline.records_read" => snap.records_read = snap.records_read.saturating_add(total),
            "pipeline.records_written" => {
                snap.records_written = snap.records_written.saturating_add(total);
            }
            "pipeline.bytes_read" => snap.bytes_read = snap.bytes_read.saturating_add(total),
            "pipeline.bytes_written" => {
                snap.bytes_written = snap.bytes_written.saturating_add(total);
            }
            _ => {}
        }
        return;
    }

    // Duration instruments produce Histogram<f64> (values in seconds)
    if let Some(hist) = data.as_any().downcast_ref::<Histogram<f64>>() {
        if let Some(raw_name) = builtin_plugin_duration_snapshot_name(name) {
            for dp in hist
                .data_points
                .iter()
                .filter(|dp| matches_metric_scope(&dp.attributes, pipeline, run))
            {
                let (stream, shard) = plugin_timing_series_from_attrs(&dp.attributes);
                snap.record_plugin_duration_for_series(raw_name, &stream, shard, dp.sum);
            }
            return;
        }

        let filtered = hist
            .data_points
            .iter()
            .filter(|dp| matches_metric_scope(&dp.attributes, pipeline, run));
        let mut total_sum: f64 = 0.0;
        let mut total_count: u64 = 0;
        let mut max_value: f64 = 0.0;
        let mut saw_datapoint = false;
        for dp in filtered {
            total_sum += dp.sum;
            total_count += dp.count;
            max_value = max_value.max(dp.max.unwrap_or(dp.sum));
            saw_datapoint = true;
        }

        if !saw_datapoint {
            return;
        }

        match name {
            // Host timings (seconds → nanos)
            "host.emit_batch_duration" => {
                snap.emit_batch_nanos = snap
                    .emit_batch_nanos
                    .saturating_add((total_sum * 1e9) as u64);
                snap.emit_count = snap.emit_count.saturating_add(total_count);
            }
            "host.next_batch_duration" => {
                snap.next_batch_nanos = snap
                    .next_batch_nanos
                    .saturating_add((total_sum * 1e9) as u64);
                snap.next_batch_count = snap.next_batch_count.saturating_add(total_count);
            }
            "host.next_batch_wait_duration" => {
                snap.next_batch_wait_nanos = snap
                    .next_batch_wait_nanos
                    .saturating_add((total_sum * 1e9) as u64);
            }
            "host.next_batch_process_duration" => {
                snap.next_batch_process_nanos = snap
                    .next_batch_process_nanos
                    .saturating_add((total_sum * 1e9) as u64);
            }
            "host.compress_duration" => {
                snap.compress_nanos = snap.compress_nanos.saturating_add((total_sum * 1e9) as u64);
            }
            "host.decompress_duration" => {
                snap.decompress_nanos = snap
                    .decompress_nanos
                    .saturating_add((total_sum * 1e9) as u64);
            }
            "pipeline.duration" => {
                snap.pipeline_duration_secs = snap.pipeline_duration_secs.max(max_value);
            }
            _ => {}
        }
    }
}

fn is_builtin_plugin_duration_name(name: &str) -> bool {
    matches!(
        name,
        "source_connect_secs"
            | "source_query_secs"
            | "source_fetch_secs"
            | "source_arrow_encode_secs"
            | "dest_connect_secs"
            | "dest_flush_secs"
            | "dest_commit_secs"
            | "dest_decode_secs"
            | "dest_arrow_decode_secs"
    )
}

fn builtin_plugin_duration_snapshot_name(name: &str) -> Option<&'static str> {
    match name {
        "plugin.source_connect_duration" => Some("source_connect_secs"),
        "plugin.source_query_duration" => Some("source_query_secs"),
        "plugin.source_fetch_duration" => Some("source_fetch_secs"),
        "plugin.source_encode_duration" => Some("source_arrow_encode_secs"),
        "plugin.dest_connect_duration" => Some("dest_connect_secs"),
        "plugin.dest_flush_duration" => Some("dest_flush_secs"),
        "plugin.dest_commit_duration" => Some("dest_commit_secs"),
        "plugin.dest_decode_duration" => Some("dest_arrow_decode_secs"),
        _ => None,
    }
}

fn plugin_timing_series_from_attrs(attrs: &[opentelemetry::KeyValue]) -> (String, usize) {
    let stream = attrs
        .iter()
        .find(|kv| kv.key.as_str() == crate::labels::STREAM)
        .map(|kv| kv.value.as_str().into_owned())
        .unwrap_or_default();
    let shard = attrs
        .iter()
        .find(|kv| kv.key.as_str() == crate::labels::SHARD)
        .and_then(|kv| kv.value.as_str().parse::<usize>().ok())
        .unwrap_or_default();
    (stream, shard)
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
    fn snapshot_uses_cumulative_plugin_histogram_totals_per_series() {
        let (provider, reader) = test_provider();
        let meter = provider.meter("test");

        let hist = meter.f64_histogram("plugin.dest_decode_duration").build();
        let users_labels = [
            KeyValue::new(crate::labels::PIPELINE, "my-pipe"),
            KeyValue::new(crate::labels::RUN, "run-1"),
            KeyValue::new(crate::labels::STREAM, "users"),
        ];
        let orders_labels = [
            KeyValue::new(crate::labels::PIPELINE, "my-pipe"),
            KeyValue::new(crate::labels::RUN, "run-1"),
            KeyValue::new(crate::labels::STREAM, "orders"),
        ];

        hist.record(0.2, &users_labels);
        hist.record(0.3, &users_labels);
        hist.record(0.8, &orders_labels);

        let snap = reader.flush_and_snapshot_for_run(&provider, "my-pipe", Some("run-1"));
        assert!((snap.dest_decode_secs - 0.8).abs() < 0.001);
    }

    #[test]
    fn snapshot_accumulates_histogram_totals_per_series_across_multiple_flushes() {
        let (provider, reader) = test_provider();
        let meter = provider.meter("test");
        let users_labels = [
            KeyValue::new(crate::labels::PIPELINE, "my-pipe"),
            KeyValue::new(crate::labels::RUN, "run-1"),
            KeyValue::new(crate::labels::STREAM, "users"),
        ];
        let orders_labels = [
            KeyValue::new(crate::labels::PIPELINE, "my-pipe"),
            KeyValue::new(crate::labels::RUN, "run-1"),
            KeyValue::new(crate::labels::STREAM, "orders"),
        ];

        let records_read = meter.u64_counter("pipeline.records_read").build();
        let dest_decode = meter.f64_histogram("plugin.dest_decode_duration").build();

        records_read.add(100, &users_labels);
        dest_decode.record(0.2, &users_labels);
        dest_decode.record(0.3, &users_labels);
        dest_decode.record(0.8, &orders_labels);
        let _ = provider.force_flush();

        records_read.add(50, &users_labels);
        dest_decode.record(0.4, &users_labels);
        dest_decode.record(0.1, &orders_labels);
        let _ = provider.force_flush();

        let snap = reader.snapshot_pipeline_result_for_run("my-pipe", Some("run-1"));
        assert_eq!(snap.records_read, 150);
        assert!((snap.dest_decode_secs - 0.9).abs() < 0.001);
    }

    #[test]
    fn snapshot_uses_critical_path_max_across_shard_labeled_plugin_series() {
        let (provider, reader) = test_provider();
        let meter = provider.meter("test");
        let hist = meter
            .f64_histogram("plugin.source_connect_duration")
            .build();

        hist.record(
            1.0,
            &[
                KeyValue::new(crate::labels::PIPELINE, "my-pipe"),
                KeyValue::new(crate::labels::RUN, "run-1"),
                KeyValue::new(crate::labels::STREAM, "users"),
                KeyValue::new(crate::labels::SHARD, "0"),
            ],
        );
        hist.record(
            1.0,
            &[
                KeyValue::new(crate::labels::PIPELINE, "my-pipe"),
                KeyValue::new(crate::labels::RUN, "run-1"),
                KeyValue::new(crate::labels::STREAM, "users"),
                KeyValue::new(crate::labels::SHARD, "1"),
            ],
        );

        let snap = reader.flush_and_snapshot_for_run(&provider, "my-pipe", Some("run-1"));
        assert!((snap.source_connect_secs - 1.0).abs() < 0.001);
    }

    #[test]
    fn raw_snapshot_uses_critical_path_max_across_parallel_shards() {
        let mut snapshot = PipelineMetricsSnapshot::default();

        snapshot.record_plugin_duration_for_series("source_connect_secs", "users", 0, 1.0);
        snapshot.record_plugin_duration_for_series("source_connect_secs", "users", 1, 1.0);
        snapshot.record_plugin_duration_for_series("source_connect_secs", "users", 0, 0.25);

        assert!((snapshot.source_connect_secs - 1.25).abs() < 0.001);
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
    fn snapshot_excludes_unlabeled_datapoints() {
        let (provider, reader) = test_provider();
        let meter = provider.meter("test");

        let counter = meter.u64_counter("pipeline.records_read").build();
        counter.add(50, &[]);
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

    #[test]
    fn snapshot_can_filter_metrics_by_run_label() {
        let (provider, reader) = test_provider();
        let meter = provider.meter("test");
        let hist = meter
            .f64_histogram("plugin.source_connect_duration")
            .build();

        hist.record(
            0.5,
            &[
                KeyValue::new(crate::labels::PIPELINE, "my-pipe"),
                KeyValue::new(crate::labels::RUN, "run-a"),
            ],
        );
        let run_a = reader.flush_and_snapshot_for_run(&provider, "my-pipe", Some("run-a"));
        assert!((run_a.source_connect_secs - 0.5).abs() < 0.001);

        hist.record(
            1.5,
            &[
                KeyValue::new(crate::labels::PIPELINE, "my-pipe"),
                KeyValue::new(crate::labels::RUN, "run-b"),
            ],
        );
        let run_b = reader.flush_and_snapshot_for_run(&provider, "my-pipe", Some("run-b"));
        assert!((run_b.source_connect_secs - 1.5).abs() < 0.001);
    }

    #[test]
    fn raw_snapshot_accepts_destination_arrow_decode_key() {
        let mut snapshot = PipelineMetricsSnapshot::default();

        snapshot.record_plugin_duration("dest_arrow_decode_secs", 0.75);

        assert!((snapshot.dest_decode_secs - 0.75).abs() < 0.001);
    }

    #[test]
    fn raw_snapshot_ignores_unknown_plugin_duration_names() {
        let mut snapshot = PipelineMetricsSnapshot::default();

        snapshot.record_plugin_duration_for_series("custom_metric_name", "users", 0, 1.0);

        assert_eq!(snapshot.tracked_plugin_timing_series_count(), 0);
        assert!(snapshot.dest_decode_secs.abs() < f64::EPSILON);
    }
}
