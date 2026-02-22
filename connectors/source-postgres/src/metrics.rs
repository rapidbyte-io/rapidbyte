//! Shared source-postgres metrics helpers.

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{Metric, MetricValue};

/// Emit cumulative source read counters for a stream.
pub(crate) fn emit_read_metrics(stream_name: &str, total_records: u64, total_bytes: u64) {
    let _ = host_ffi::metric(
        "source-postgres",
        stream_name,
        &Metric {
            name: "records_read".to_string(),
            value: MetricValue::Counter(total_records),
            labels: vec![],
        },
    );
    let _ = host_ffi::metric(
        "source-postgres",
        stream_name,
        &Metric {
            name: "bytes_read".to_string(),
            value: MetricValue::Counter(total_bytes),
            labels: vec![],
        },
    );
}
