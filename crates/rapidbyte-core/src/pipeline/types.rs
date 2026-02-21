//! Pipeline configuration types and config parsing helpers.

use rapidbyte_types::protocol::{DataErrorPolicy, SchemaEvolutionPolicy, SyncMode, WriteMode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::engine::compression::CompressionCodec;

const DEFAULT_STATE_BACKEND: StateBackendKind = StateBackendKind::Sqlite;
const DEFAULT_MAX_MEMORY: &str = "256mb";
const DEFAULT_MAX_BATCH_BYTES: &str = "64mb";
const DEFAULT_PARALLELISM: u32 = 1;
const DEFAULT_CHECKPOINT_INTERVAL_BYTES: &str = "64mb";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_MAX_INFLIGHT_BATCHES: u32 = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub version: String,
    pub pipeline: String,
    pub source: SourceConfig,
    #[serde(default)]
    pub transforms: Vec<TransformConfig>,
    pub destination: DestinationConfig,
    #[serde(default)]
    pub state: StateConfig,
    #[serde(default)]
    pub resources: ResourceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    #[serde(rename = "use")]
    pub use_ref: String,
    pub config: serde_json::Value,
    pub streams: Vec<StreamConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: String,
    pub sync_mode: SyncMode,
    pub cursor_field: Option<String>,
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    #[serde(rename = "use")]
    pub use_ref: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PipelineWriteMode {
    Append,
    Replace,
    Upsert,
}

impl PipelineWriteMode {
    pub fn to_protocol(self, primary_key: Vec<String>) -> WriteMode {
        match self {
            Self::Append => WriteMode::Append,
            Self::Replace => WriteMode::Replace,
            Self::Upsert => WriteMode::Upsert { primary_key },
        }
    }
}

fn default_on_data_error() -> DataErrorPolicy {
    DataErrorPolicy::Fail
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationConfig {
    #[serde(rename = "use")]
    pub use_ref: String,
    pub config: serde_json::Value,
    pub write_mode: PipelineWriteMode,
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default = "default_on_data_error")]
    pub on_data_error: DataErrorPolicy,
    #[serde(default)]
    pub schema_evolution: Option<SchemaEvolutionPolicy>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StateBackendKind {
    Sqlite,
}

impl Default for StateBackendKind {
    fn default() -> Self {
        DEFAULT_STATE_BACKEND
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct StateConfig {
    pub backend: StateBackendKind,
    pub connection: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ResourceConfig {
    pub max_memory: String,
    pub max_batch_bytes: String,
    pub parallelism: u32,
    /// Destination commits after writing this many bytes. "0" disables chunking.
    pub checkpoint_interval_bytes: String,
    /// Destination commits after writing this many rows. 0 = disabled.
    pub checkpoint_interval_rows: u64,
    /// Destination commits after this many seconds elapse. 0 = disabled.
    pub checkpoint_interval_seconds: u64,
    /// Maximum number of retry attempts for retryable errors. 0 disables retries.
    pub max_retries: u32,
    /// IPC compression codec for data transferred between source and destination.
    pub compression: Option<CompressionCodec>,
    /// Channel capacity between pipeline stages. Controls backpressure.
    /// Must be >= 1. Default: 16.
    pub max_inflight_batches: u32,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            max_memory: DEFAULT_MAX_MEMORY.to_string(),
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES.to_string(),
            parallelism: DEFAULT_PARALLELISM,
            checkpoint_interval_bytes: DEFAULT_CHECKPOINT_INTERVAL_BYTES.to_string(),
            checkpoint_interval_rows: 0,
            checkpoint_interval_seconds: 0,
            max_retries: DEFAULT_MAX_RETRIES,
            compression: None,
            max_inflight_batches: DEFAULT_MAX_INFLIGHT_BATCHES,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseByteSizeError {
    #[error("byte size cannot be empty")]
    Empty,
    #[error("invalid number in byte size '{0}'")]
    InvalidNumber(String),
}

/// Parse a human-readable byte size like "64mb" into bytes.
pub fn parse_byte_size(raw: &str) -> Result<u64, ParseByteSizeError> {
    let s = raw.trim().to_ascii_lowercase();
    if s.is_empty() {
        return Err(ParseByteSizeError::Empty);
    }

    let (num, multiplier) = if let Some(n) = s.strip_suffix("gb") {
        (n.trim(), 1024_u64 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n.trim(), 1024_u64 * 1024)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n.trim(), 1024_u64)
    } else {
        (s.as_str(), 1_u64)
    };

    let value = num
        .parse::<u64>()
        .map_err(|_| ParseByteSizeError::InvalidNumber(raw.to_string()))?;
    Ok(value.saturating_mul(multiplier))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_minimal_pipeline() {
        let yaml = r#"
version: "1.0"
pipeline: test_pg_to_pg
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config: {}
  write_mode: append
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.pipeline, "test_pg_to_pg");
        assert_eq!(config.source.streams[0].sync_mode, SyncMode::FullRefresh);
        assert_eq!(config.destination.write_mode, PipelineWriteMode::Append);
        assert_eq!(config.destination.on_data_error, DataErrorPolicy::Fail);
        assert_eq!(config.state.backend, StateBackendKind::Sqlite);
        assert_eq!(config.resources.max_batch_bytes, "64mb");
    }

    #[test]
    fn test_deserialize_full_pipeline() {
        let yaml = r#"
version: "1.0"
pipeline: full_test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
destination:
  use: dest-postgres
  config: {}
  write_mode: upsert
  primary_key: [id]
  on_data_error: skip
state:
  backend: sqlite
  connection: /tmp/state.db
resources:
  max_memory: 512mb
  max_batch_bytes: 128mb
  parallelism: 4
  checkpoint_interval_bytes: 32mb
  compression: zstd
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.source.streams[0].sync_mode, SyncMode::Incremental);
        assert_eq!(config.destination.write_mode, PipelineWriteMode::Upsert);
        assert_eq!(config.destination.primary_key, vec!["id"]);
        assert_eq!(config.destination.on_data_error, DataErrorPolicy::Skip);
        assert_eq!(config.state.connection, Some("/tmp/state.db".to_string()));
        assert_eq!(config.resources.compression, Some(CompressionCodec::Zstd));
    }

    #[test]
    fn test_write_mode_to_protocol() {
        assert_eq!(
            PipelineWriteMode::Append.to_protocol(vec![]),
            WriteMode::Append
        );
        assert_eq!(
            PipelineWriteMode::Replace.to_protocol(vec![]),
            WriteMode::Replace
        );
        assert_eq!(
            PipelineWriteMode::Upsert.to_protocol(vec!["id".to_string()]),
            WriteMode::Upsert {
                primary_key: vec!["id".to_string()]
            }
        );
    }

    #[test]
    fn test_parse_byte_size() {
        assert_eq!(parse_byte_size("64mb").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_byte_size("128mb").unwrap(), 128 * 1024 * 1024);
        assert_eq!(parse_byte_size("1gb").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("512kb").unwrap(), 512 * 1024);
        assert_eq!(parse_byte_size("1024").unwrap(), 1024);
        assert_eq!(parse_byte_size("  64MB  ").unwrap(), 64 * 1024 * 1024);
    }

    #[test]
    fn test_parse_byte_size_invalid() {
        assert!(matches!(
            parse_byte_size(""),
            Err(ParseByteSizeError::Empty)
        ));
        assert!(matches!(
            parse_byte_size("sixty-four mb"),
            Err(ParseByteSizeError::InvalidNumber(_))
        ));
    }

    #[test]
    fn test_schema_evolution_typed() {
        let yaml = r#"
version: "1"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config: {}
  write_mode: append
  schema_evolution:
    new_column: ignore
    type_change: coerce
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        let se = config.destination.schema_evolution.unwrap();
        assert_eq!(
            se,
            SchemaEvolutionPolicy {
                new_column: rapidbyte_types::protocol::ColumnPolicy::Ignore,
                removed_column: rapidbyte_types::protocol::ColumnPolicy::Ignore,
                type_change: rapidbyte_types::protocol::TypeChangePolicy::Coerce,
                nullability_change: rapidbyte_types::protocol::NullabilityPolicy::Allow,
            }
        );
    }
}
