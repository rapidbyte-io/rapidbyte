//! Execution summaries for source, destination, and transform plugins.
//!
//! Plugins return role-specific summaries ([`ReadSummary`], [`WriteSummary`],
//! [`TransformSummary`]) upon completion, along with optional timing
//! breakdowns ([`ReadPerf`], [`WritePerf`]).

use serde::{Deserialize, Serialize};

// ── Performance Breakdowns ──────────────────────────────────────────

/// Timing breakdown for a source read operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadPerf {
    /// Time spent establishing the connection.
    pub connect_secs: f64,
    /// Time spent executing the query.
    pub query_secs: f64,
    /// Time spent fetching result rows.
    pub fetch_secs: f64,
    /// Time spent encoding rows into Arrow batches.
    #[serde(default)]
    pub arrow_encode_secs: f64,
}

/// Timing breakdown for a destination write operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WritePerf {
    /// Time spent establishing the connection.
    pub connect_secs: f64,
    /// Time spent flushing data to the destination.
    pub flush_secs: f64,
    /// Time spent committing transactions.
    pub commit_secs: f64,
    /// Time spent decoding Arrow batches into destination format.
    #[serde(default)]
    pub arrow_decode_secs: f64,
}

// ── Summaries ───────────────────────────────────────────────────────

/// Aggregate metrics for a completed source read operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadSummary {
    pub records_read: u64,
    pub bytes_read: u64,
    pub batches_emitted: u64,
    pub checkpoint_count: u64,
    #[serde(default)]
    pub records_skipped: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub perf: Option<ReadPerf>,
}

/// Aggregate metrics for a completed destination write operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteSummary {
    pub records_written: u64,
    pub bytes_written: u64,
    pub batches_written: u64,
    pub checkpoint_count: u64,
    #[serde(default)]
    pub records_failed: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub perf: Option<WritePerf>,
}

/// Aggregate metrics for a completed transform operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransformSummary {
    pub records_in: u64,
    pub records_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub batches_processed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_summary_optional_perf() {
        let s = ReadSummary {
            records_read: 1000,
            bytes_read: 65536,
            batches_emitted: 2,
            checkpoint_count: 1,
            records_skipped: 0,
            perf: None,
        };
        let json = serde_json::to_value(&s).unwrap();
        assert!(json.get("perf").is_none());
    }

    #[test]
    fn write_summary_roundtrip() {
        let s = WriteSummary {
            records_written: 500,
            bytes_written: 32768,
            batches_written: 1,
            checkpoint_count: 1,
            records_failed: 0,
            perf: Some(WritePerf {
                connect_secs: 0.1,
                flush_secs: 0.5,
                commit_secs: 0.05,
                arrow_decode_secs: 0.02,
            }),
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: WriteSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }
}
