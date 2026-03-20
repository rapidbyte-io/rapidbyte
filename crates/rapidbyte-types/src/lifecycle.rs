//! Apply and teardown lifecycle types for the v7 wire protocol.
//!
//! [`ApplyRequest`] and [`ApplyReport`] model the schema-apply phase where
//! a destination plugin creates or alters tables before data flows.
//!
//! [`TeardownRequest`] and [`TeardownReport`] model the teardown phase where
//! a destination plugin cleans up resources after a pipeline completes or
//! is explicitly torn down.

use serde::{Deserialize, Serialize};

use crate::stream::StreamContext;

// ── Apply ────────────────────────────────────────────────────────────

/// Request to apply schema changes for one or more streams.
///
/// When `dry_run` is `true`, the plugin should report what it *would* do
/// without executing any DDL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApplyRequest {
    /// Streams whose schemas should be applied.
    pub streams: Vec<StreamContext>,
    /// If `true`, report planned actions without executing them.
    pub dry_run: bool,
}

/// A single action taken (or planned) during schema apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyAction {
    /// Name of the stream the action applies to.
    pub stream_name: String,
    /// Human-readable description of the action.
    pub description: String,
    /// DDL statement executed (or planned), if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ddl_executed: Option<String>,
}

/// Aggregated result of a schema-apply phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyReport {
    /// Individual actions taken (or planned).
    pub actions: Vec<ApplyAction>,
}

impl ApplyReport {
    /// Create an empty report indicating no actions were needed.
    #[must_use]
    pub fn noop() -> Self {
        Self {
            actions: Vec::new(),
        }
    }
}

// ── Teardown ─────────────────────────────────────────────────────────

/// Request to tear down (clean up) resources for a set of streams.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeardownRequest {
    /// Names of the streams to tear down.
    pub streams: Vec<String>,
    /// Human-readable reason for the teardown.
    pub reason: String,
}

/// Aggregated result of a teardown phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeardownReport {
    /// Descriptions of actions taken during teardown.
    pub actions: Vec<String>,
}

impl TeardownReport {
    /// Create an empty report indicating no teardown actions were needed.
    #[must_use]
    pub fn noop() -> Self {
        Self {
            actions: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::StreamSchema;
    use crate::wire::SyncMode;

    fn test_stream_context(name: &str) -> StreamContext {
        StreamContext {
            stream_name: name.into(),
            source_stream_name: None,
            stream_index: 0,
            schema: StreamSchema {
                fields: vec![],
                primary_key: vec![],
                partition_keys: vec![],
                source_defined_cursor: None,
                schema_id: None,
            },
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: Default::default(),
            policies: Default::default(),
            write_mode: None,
            selected_columns: None,
            partition_key: None,
            partition_count: None,
            partition_index: None,
            effective_parallelism: None,
            partition_strategy: None,
            copy_flush_bytes_override: None,
        }
    }

    #[test]
    fn apply_report_noop() {
        let report = ApplyReport::noop();
        assert!(report.actions.is_empty());
    }

    #[test]
    fn apply_action_with_ddl() {
        let action = ApplyAction {
            stream_name: "public.users".into(),
            description: "Created table".into(),
            ddl_executed: Some("CREATE TABLE public.users (id INT)".into()),
        };
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("ddl_executed"));
        let back: ApplyAction = serde_json::from_str(&json).unwrap();
        assert_eq!(
            back.ddl_executed.as_deref(),
            Some("CREATE TABLE public.users (id INT)")
        );
    }

    #[test]
    fn apply_request_dry_run() {
        let req = ApplyRequest {
            streams: vec![test_stream_context("public.users")],
            dry_run: true,
        };
        assert!(req.dry_run);
        assert_eq!(req.streams.len(), 1);
        assert_eq!(req.streams[0].stream_name, "public.users");
    }

    #[test]
    fn teardown_request_roundtrip() {
        let req = TeardownRequest {
            streams: vec!["public.users".into(), "public.orders".into()],
            reason: "pipeline deleted".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: TeardownRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn teardown_report_noop() {
        let report = TeardownReport::noop();
        assert!(report.actions.is_empty());
    }

    #[test]
    fn teardown_report_with_actions() {
        let report = TeardownReport {
            actions: vec![
                "Dropped table public.users".into(),
                "Dropped table public.orders".into(),
            ],
        };
        assert_eq!(report.actions.len(), 2);
        assert_eq!(report.actions[0], "Dropped table public.users");
        assert_eq!(report.actions[1], "Dropped table public.orders");
    }
}
