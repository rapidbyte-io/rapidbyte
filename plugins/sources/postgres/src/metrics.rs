//! Shared source-postgres metrics helpers.

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::context::Context;

/// Maximum number of rows per Arrow `RecordBatch`.
pub(crate) const BATCH_SIZE: usize = 10_000;

/// Per-batch emit counters shared by both full-refresh and CDC read paths.
pub(crate) struct EmitState {
    pub(crate) total_records: u64,
    pub(crate) total_bytes: u64,
    pub(crate) batches_emitted: u64,
    pub(crate) arrow_encode_nanos: u64,
    /// Tracks the last emitted cumulative values so we emit deltas to the host counter.
    pub(crate) last_emitted_records: u64,
    pub(crate) last_emitted_bytes: u64,
}

pub(crate) trait MetricSink {
    fn counter(&self, name: &str, value: u64) -> Result<(), PluginError>;
    fn histogram(&self, name: &str, value: f64) -> Result<(), PluginError>;
}

impl MetricSink for Metrics {
    fn counter(&self, name: &str, value: u64) -> Result<(), PluginError> {
        Metrics::counter(self, name, value)
    }

    fn histogram(&self, name: &str, value: f64) -> Result<(), PluginError> {
        Metrics::histogram(self, name, value)
    }
}

impl MetricSink for Context {
    fn counter(&self, name: &str, value: u64) -> Result<(), PluginError> {
        Context::counter(self, name, value)
    }

    fn histogram(&self, name: &str, value: f64) -> Result<(), PluginError> {
        Context::histogram(self, name, value)
    }
}

/// Emit per-batch source read counter deltas for a stream.
///
/// Best-effort: metric failures are silently ignored to avoid aborting
/// data movement on telemetry issues.
pub(crate) fn emit_batch_counters(metrics: &impl MetricSink, state: &mut EmitState) {
    let delta_records = state.total_records - state.last_emitted_records;
    let delta_bytes = state.total_bytes - state.last_emitted_bytes;
    state.last_emitted_records = state.total_records;
    state.last_emitted_bytes = state.total_bytes;
    if delta_records > 0 {
        let _ = metrics.counter("records_read", delta_records);
    }
    if delta_bytes > 0 {
        let _ = metrics.counter("bytes_read", delta_bytes);
    }
}

/// Emit end-of-stream source timing histograms (connect, query, fetch, encode).
///
/// Best-effort: metric failures are silently ignored.
pub(crate) fn emit_source_timings(metrics: &impl MetricSink, perf: &ReadPerf) {
    let _ = metrics.histogram("source_connect_secs", perf.connect_secs);
    let _ = metrics.histogram("source_query_secs", perf.query_secs);
    let _ = metrics.histogram("source_fetch_secs", perf.fetch_secs);
    let _ = metrics.histogram("source_arrow_encode_secs", perf.arrow_encode_secs);
}
