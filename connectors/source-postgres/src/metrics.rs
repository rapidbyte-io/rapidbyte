//! Shared source-postgres metrics helpers.

use rapidbyte_sdk::context::Context;
use rapidbyte_sdk::protocol::{Metric, MetricValue};

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
