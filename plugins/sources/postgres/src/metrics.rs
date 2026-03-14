//! Shared source-postgres metrics helpers.

use rapidbyte_sdk::prelude::*;

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

/// Emit per-batch source read counter deltas for a stream.
pub(crate) fn emit_read_metrics(ctx: &Context, state: &mut EmitState) {
    let delta_records = state.total_records - state.last_emitted_records;
    let delta_bytes = state.total_bytes - state.last_emitted_bytes;
    state.last_emitted_records = state.total_records;
    state.last_emitted_bytes = state.total_bytes;
    if delta_records > 0 {
        ctx.counter("records_read", delta_records);
    }
    if delta_bytes > 0 {
        ctx.counter("bytes_read", delta_bytes);
    }
}

/// Emit source read timing metrics so the host can aggregate per-phase timings.
pub(crate) fn emit_read_perf_metrics(ctx: &Context, perf: &ReadPerf) {
    ctx.histogram("source_connect_secs", perf.connect_secs);
    ctx.histogram("source_query_secs", perf.query_secs);
    ctx.histogram("source_fetch_secs", perf.fetch_secs);
    ctx.histogram("source_arrow_encode_secs", perf.arrow_encode_secs);
}
