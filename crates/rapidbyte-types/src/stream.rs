//! Stream execution context, limits, and policies.
//!
//! [`StreamContext`] is the central type passed to connectors for each
//! stream operation. It bundles schema, sync configuration, resource
//! limits, and error handling policies.

use crate::catalog::SchemaHint;
use crate::cursor::CursorInfo;
use crate::wire::{SyncMode, WriteMode};
use serde::{Deserialize, Serialize};

// ── Policies ────────────────────────────────────────────────────────

/// How to handle records that fail validation or conversion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataErrorPolicy {
    /// Skip the invalid record and continue.
    Skip,
    /// Fail the entire batch.
    #[default]
    Fail,
    /// Route the record to the dead-letter queue.
    Dlq,
}

/// How to handle new or removed columns between runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnPolicy {
    /// Automatically add new columns / silently ignore removed ones.
    #[default]
    Add,
    /// Ignore the schema change entirely.
    Ignore,
    /// Fail on any column change.
    Fail,
}

/// How to handle type changes between runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeChangePolicy {
    /// Attempt to coerce the value to the new type.
    Coerce,
    /// Fail on any type change.
    #[default]
    Fail,
    /// Convert the value to null.
    Null,
}

/// How to handle nullability changes between runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullabilityPolicy {
    /// Allow nullability changes.
    #[default]
    Allow,
    /// Fail on nullability changes.
    Fail,
}

/// Schema evolution behavior when source schema changes between runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaEvolutionPolicy {
    #[serde(default)]
    pub new_column: ColumnPolicy,
    #[serde(default = "default_removed_column")]
    pub removed_column: ColumnPolicy,
    #[serde(default)]
    pub type_change: TypeChangePolicy,
    #[serde(default)]
    pub nullability_change: NullabilityPolicy,
}

fn default_removed_column() -> ColumnPolicy {
    ColumnPolicy::Ignore
}

impl Default for SchemaEvolutionPolicy {
    fn default() -> Self {
        Self {
            new_column: ColumnPolicy::Add,
            removed_column: ColumnPolicy::Ignore,
            type_change: TypeChangePolicy::Fail,
            nullability_change: NullabilityPolicy::Allow,
        }
    }
}

// ── Stream Policies ─────────────────────────────────────────────────

/// Combined error handling and schema evolution policies for a stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPolicies {
    #[serde(default)]
    pub on_data_error: DataErrorPolicy,
    #[serde(default)]
    pub schema_evolution: SchemaEvolutionPolicy,
}

// ── Stream Limits ───────────────────────────────────────────────────

/// Resource and batching limits for a stream operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamLimits {
    /// Maximum batch size in bytes.
    pub max_batch_bytes: u64,
    /// Maximum single record size in bytes.
    pub max_record_bytes: u64,
    /// Maximum batches in flight between source and destination.
    pub max_inflight_batches: u32,
    /// Maximum parallel requests to the external system.
    pub max_parallel_requests: u32,
    /// Checkpoint after this many bytes processed.
    pub checkpoint_interval_bytes: u64,
    /// Checkpoint after this many rows (0 = disabled).
    #[serde(default)]
    pub checkpoint_interval_rows: u64,
    /// Checkpoint after this many seconds (0 = disabled).
    #[serde(default)]
    pub checkpoint_interval_seconds: u64,
    /// Maximum number of records to read (None = unlimited).
    /// Used by dry-run mode to cap source reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records: Option<u64>,
}

impl StreamLimits {
    /// 64 MiB default batch size.
    pub const DEFAULT_MAX_BATCH_BYTES: u64 = 64 * 1024 * 1024;
    /// 16 MiB default record size.
    pub const DEFAULT_MAX_RECORD_BYTES: u64 = 16 * 1024 * 1024;
    /// 64 MiB default checkpoint interval.
    pub const DEFAULT_CHECKPOINT_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;
    /// 16 default inflight batches.
    pub const DEFAULT_MAX_INFLIGHT_BATCHES: u32 = 16;
    /// 1 default parallel request.
    pub const DEFAULT_MAX_PARALLEL_REQUESTS: u32 = 1;
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            max_batch_bytes: Self::DEFAULT_MAX_BATCH_BYTES,
            max_record_bytes: Self::DEFAULT_MAX_RECORD_BYTES,
            max_inflight_batches: Self::DEFAULT_MAX_INFLIGHT_BATCHES,
            max_parallel_requests: Self::DEFAULT_MAX_PARALLEL_REQUESTS,
            checkpoint_interval_bytes: Self::DEFAULT_CHECKPOINT_INTERVAL_BYTES,
            checkpoint_interval_rows: 0,
            checkpoint_interval_seconds: 0,
            max_records: None,
        }
    }
}

// ── Stream Context ──────────────────────────────────────────────────

/// Execution context passed to a connector for a single stream operation.
///
/// Bundles everything a connector needs: stream identity, schema, sync
/// configuration, resource limits, and error handling policies.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamContext {
    /// Name of the stream being processed.
    pub stream_name: String,
    /// Physical source table/view name when execution stream naming differs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_stream_name: Option<String>,
    /// Schema of the stream.
    pub schema: SchemaHint,
    /// How data is read from the source.
    pub sync_mode: SyncMode,
    /// Cursor tracking state for incremental sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_info: Option<CursorInfo>,
    /// Resource and batching limits.
    #[serde(default)]
    pub limits: StreamLimits,
    /// Error handling and schema evolution policies.
    #[serde(default)]
    pub policies: StreamPolicies,
    /// How data is written to the destination (`None` for source/transform).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_mode: Option<WriteMode>,
    /// Column projection (`None` = all columns).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_columns: Option<Vec<String>>,
    /// Total number of source partitions for this stream execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_count: Option<u32>,
    /// Zero-based partition index for this stream execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_index: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::ArrowDataType;
    use crate::catalog::ColumnSchema;

    #[test]
    fn stream_limits_defaults() {
        let limits = StreamLimits::default();
        assert_eq!(limits.max_batch_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.max_record_bytes, 16 * 1024 * 1024);
        assert_eq!(limits.max_inflight_batches, 16);
        assert_eq!(limits.max_parallel_requests, 1);
        assert_eq!(limits.checkpoint_interval_rows, 0);
    }

    #[test]
    fn schema_evolution_defaults() {
        let policy = SchemaEvolutionPolicy::default();
        assert_eq!(policy.new_column, ColumnPolicy::Add);
        assert_eq!(policy.removed_column, ColumnPolicy::Ignore);
        assert_eq!(policy.type_change, TypeChangePolicy::Fail);
        assert_eq!(policy.nullability_change, NullabilityPolicy::Allow);
    }

    #[test]
    fn stream_context_roundtrip() {
        let ctx = StreamContext {
            stream_name: "public.users".into(),
            source_stream_name: None,
            schema: SchemaHint::Columns(vec![ColumnSchema {
                name: "id".into(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            }]),
            sync_mode: SyncMode::Incremental,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: Some(WriteMode::Append),
            selected_columns: None,
            partition_count: Some(4),
            partition_index: Some(2),
        };
        let json = serde_json::to_string(&ctx).unwrap();
        let back: StreamContext = serde_json::from_str(&json).unwrap();
        assert_eq!(ctx, back);
    }

    #[test]
    fn data_error_policy_default_is_fail() {
        assert_eq!(DataErrorPolicy::default(), DataErrorPolicy::Fail);
    }

    #[test]
    fn stream_limits_max_records_serde_roundtrip() {
        let limits = StreamLimits {
            max_records: Some(500),
            ..StreamLimits::default()
        };
        let json = serde_json::to_string(&limits).unwrap();
        let back: StreamLimits = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_records, Some(500));
    }

    #[test]
    fn stream_limits_max_records_default_is_none() {
        let limits = StreamLimits::default();
        assert_eq!(limits.max_records, None);
    }
}
