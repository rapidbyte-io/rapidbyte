//! Shared source-postgres metrics helpers.

use rapidbyte_sdk::prelude::*;

/// Maximum number of rows per Arrow `RecordBatch`.
pub(crate) const BATCH_SIZE: usize = 10_000;

/// Cumulative emit counters shared by both full-refresh and CDC read paths.
pub(crate) struct EmitState {
    pub(crate) total_records: u64,
    pub(crate) total_bytes: u64,
    pub(crate) batches_emitted: u64,
    pub(crate) arrow_encode_nanos: u64,
}

/// Emit cumulative source read counters for a stream.
pub(crate) fn emit_read_metrics(ctx: &Context, total_records: u64, total_bytes: u64) {
    ctx.counter("records_read", total_records);
    ctx.counter("bytes_read", total_bytes);
}

/// Emit source read timing metrics so the host can aggregate per-phase timings.
pub(crate) fn emit_read_perf_metrics(ctx: &Context, perf: &ReadPerf) {
    ctx.histogram("source_connect_secs", perf.connect_secs);
    ctx.histogram("source_query_secs", perf.query_secs);
    ctx.histogram("source_fetch_secs", perf.fetch_secs);
    ctx.histogram("source_arrow_encode_secs", perf.arrow_encode_secs);
}
