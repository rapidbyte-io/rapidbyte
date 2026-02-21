use serde::{Deserialize, Serialize};

use super::{CursorInfo, SchemaHint, SyncMode, WriteMode};

/// Default maximum batch size: 64 MiB.
pub const DEFAULT_MAX_BATCH_BYTES: u64 = 64 * 1024 * 1024;
/// Default maximum record size: 16 MiB.
pub const DEFAULT_MAX_RECORD_BYTES: u64 = 16 * 1024 * 1024;
/// Default checkpoint interval: 64 MiB.
pub const DEFAULT_CHECKPOINT_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;
/// Default maximum in-flight batches.
pub const DEFAULT_MAX_INFLIGHT_BATCHES: u32 = 16;
/// Default max parallel requests.
pub const DEFAULT_MAX_PARALLEL_REQUESTS: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct StreamLimits {
    pub max_batch_bytes: u64,
    pub max_record_bytes: u64,
    pub max_inflight_batches: u32,
    pub max_parallel_requests: u32,
    pub checkpoint_interval_bytes: u64,
    /// Checkpoint after this many rows. 0 = disabled (default).
    pub checkpoint_interval_rows: u64,
    /// Checkpoint after this many seconds. 0 = disabled (default).
    pub checkpoint_interval_seconds: u64,
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_inflight_batches: DEFAULT_MAX_INFLIGHT_BATCHES,
            max_parallel_requests: DEFAULT_MAX_PARALLEL_REQUESTS,
            checkpoint_interval_bytes: DEFAULT_CHECKPOINT_INTERVAL_BYTES,
            checkpoint_interval_rows: 0,
            checkpoint_interval_seconds: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DataErrorPolicy {
    Skip,
    Fail,
    Dlq,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ColumnPolicy {
    Add,
    Ignore,
    Fail,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TypeChangePolicy {
    Coerce,
    Fail,
    Null,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum NullabilityPolicy {
    Allow,
    Fail,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SchemaEvolutionPolicy {
    pub new_column: ColumnPolicy,
    /// Policy for columns removed at source. Note: `ColumnPolicy::Add` is not meaningful
    /// for removed columns; use `Ignore` or `Fail`.
    pub removed_column: ColumnPolicy,
    pub type_change: TypeChangePolicy,
    pub nullability_change: NullabilityPolicy,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct StreamPolicies {
    pub on_data_error: DataErrorPolicy,
    pub schema_evolution: SchemaEvolutionPolicy,
}

impl Default for StreamPolicies {
    fn default() -> Self {
        Self {
            on_data_error: DataErrorPolicy::Fail,
            schema_evolution: SchemaEvolutionPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamContext {
    pub stream_name: String,
    pub schema: SchemaHint,
    pub sync_mode: SyncMode,
    pub cursor_info: Option<CursorInfo>,
    pub limits: StreamLimits,
    pub policies: StreamPolicies,
    pub write_mode: Option<WriteMode>,
    #[serde(default)]
    pub selected_columns: Option<Vec<String>>,
}
