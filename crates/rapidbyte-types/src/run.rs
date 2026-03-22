//! Run request and result types for pipeline execution.
//!
//! [`RunRequest`] initiates a pipeline run, [`StreamResult`] captures per-stream
//! outcomes, and [`RunSummary`] aggregates results across all streams.

use crate::stream::StreamContext;
use serde::{Deserialize, Serialize};

/// A request to execute a pipeline run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRequest {
    /// Stream contexts describing each stream to process.
    pub streams: Vec<StreamContext>,
}

/// Outcome of a single stream within a pipeline run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamResult {
    /// Index of the stream in the run request.
    pub stream_index: u32,
    /// Human-readable stream name.
    pub stream_name: String,
    /// JSON-encoded outcome details (plugin-specific).
    pub outcome_json: String,
    /// Whether this stream completed successfully.
    pub succeeded: bool,
}

/// Aggregated results for a completed pipeline run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSummary {
    /// Per-stream results.
    pub results: Vec<StreamResult>,
}

impl RunSummary {
    /// Number of streams that completed successfully.
    #[must_use]
    pub fn succeeded_count(&self) -> usize {
        self.results.iter().filter(|r| r.succeeded).count()
    }

    /// Number of streams that failed.
    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.results.iter().filter(|r| !r.succeeded).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_result(index: u32, name: &str, succeeded: bool) -> StreamResult {
        StreamResult {
            stream_index: index,
            stream_name: name.into(),
            outcome_json: if succeeded {
                r#"{"records":100}"#.into()
            } else {
                r#"{"error":"timeout"}"#.into()
            },
            succeeded,
        }
    }

    #[test]
    fn stream_result_success() {
        let r = make_result(0, "users", true);
        let json = serde_json::to_string(&r).unwrap();
        let back: StreamResult = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
        assert!(back.succeeded);
    }

    #[test]
    fn stream_result_failure() {
        let r = make_result(1, "orders", false);
        let json = serde_json::to_string(&r).unwrap();
        let back: StreamResult = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
        assert!(!back.succeeded);
    }

    #[test]
    fn run_summary_mixed_results() {
        let summary = RunSummary {
            results: vec![
                make_result(0, "users", true),
                make_result(1, "orders", false),
                make_result(2, "products", true),
            ],
        };
        assert_eq!(summary.succeeded_count(), 2);
        assert_eq!(summary.failed_count(), 1);
    }

    #[test]
    fn run_request_single_stream() {
        let req = RunRequest {
            streams: vec![StreamContext::test_default("users")],
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: RunRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn run_request_deserializes_without_flags() {
        let json = serde_json::json!({
            "streams": [StreamContext::test_default("users")]
        });
        let back: RunRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back.streams.len(), 1);
        assert_eq!(back.streams[0].stream_name, "users");
    }
}
