//! Shared source-postgres metrics helpers.

use rapidbyte_sdk::prelude::*;

/// Emit cumulative source read counters for a stream.
pub(crate) fn emit_read_metrics(ctx: &Context, total_records: u64, total_bytes: u64) {
    let _ = ctx.metric(&Metric {
        name: "records_read".to_string(),
        value: MetricValue::Counter(total_records),
        labels: vec![],
    });
    let _ = ctx.metric(&Metric {
        name: "bytes_read".to_string(),
        value: MetricValue::Counter(total_bytes),
        labels: vec![],
    });
}

/// Emit source read timing metrics so the host can aggregate per-phase timings.
pub(crate) fn emit_read_perf_metrics(ctx: &Context, perf: &ReadPerf) {
    let gauges = [
        ("source_connect_secs", perf.connect_secs),
        ("source_query_secs", perf.query_secs),
        ("source_fetch_secs", perf.fetch_secs),
        ("source_arrow_encode_secs", perf.arrow_encode_secs),
    ];

    for (name, value) in gauges {
        let _ = ctx.metric(&Metric {
            name: name.to_string(),
            value: MetricValue::Gauge(value),
            labels: vec![],
        });
    }
}
