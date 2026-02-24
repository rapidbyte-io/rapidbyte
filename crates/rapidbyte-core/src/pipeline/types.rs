//! Pipeline configuration types and config parsing helpers.

use rapidbyte_types::stream::{DataErrorPolicy, SchemaEvolutionPolicy};
use rapidbyte_types::wire::{SyncMode, WriteMode};
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

/// Pipeline-level network permission overrides.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PipelineNetworkPermissions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_hosts: Option<Vec<String>>,
}

/// Pipeline-level environment variable permission overrides.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PipelineEnvPermissions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_vars: Option<Vec<String>>,
}

/// Pipeline-level filesystem permission overrides.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PipelineFsPermissions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_preopens: Option<Vec<String>>,
}

/// Combined pipeline-level permission overrides for a connector.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PipelinePermissions {
    pub network: PipelineNetworkPermissions,
    pub env: PipelineEnvPermissions,
    pub fs: PipelineFsPermissions,
}

/// Pipeline-level resource limit overrides for a connector.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PipelineLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_memory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    #[serde(rename = "use")]
    pub use_ref: String,
    pub config: serde_json::Value,
    pub streams: Vec<StreamConfig>,
    #[serde(default)]
    pub permissions: Option<PipelinePermissions>,
    #[serde(default)]
    pub limits: Option<PipelineLimits>,
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
    #[serde(default)]
    pub permissions: Option<PipelinePermissions>,
    #[serde(default)]
    pub limits: Option<PipelineLimits>,
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
    #[serde(default)]
    pub permissions: Option<PipelinePermissions>,
    #[serde(default)]
    pub limits: Option<PipelineLimits>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StateBackendKind {
    Sqlite,
    Postgres,
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
                new_column: rapidbyte_types::stream::ColumnPolicy::Ignore,
                removed_column: rapidbyte_types::stream::ColumnPolicy::Ignore,
                type_change: rapidbyte_types::stream::TypeChangePolicy::Coerce,
                nullability_change: rapidbyte_types::stream::NullabilityPolicy::Allow,
            }
        );
    }

    #[test]
    fn test_on_data_error_dlq_accepted() {
        let yaml = r#"
version: "1.0"
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
  on_data_error: dlq
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.destination.on_data_error, DataErrorPolicy::Dlq);
    }

    #[test]
    fn test_compression_lz4_accepted() {
        let yaml = r#"
version: "1.0"
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
resources:
  compression: lz4
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.resources.compression, Some(CompressionCodec::Lz4));
    }

    #[test]
    fn test_compression_zstd_accepted() {
        let yaml = r#"
version: "1.0"
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
resources:
  compression: zstd
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.resources.compression, Some(CompressionCodec::Zstd));
    }

    #[test]
    fn test_compression_absent_is_none() {
        let yaml = r#"
version: "1.0"
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
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.resources.compression, None);
    }

    #[test]
    fn test_columns_projection_parsed() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
      columns: [id, name]
destination:
  use: dest-postgres
  config: {}
  write_mode: append
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.source.streams[0].columns,
            Some(vec!["id".to_string(), "name".to_string()])
        );
    }

    #[test]
    fn test_invalid_compression_rejected() {
        let yaml = r#"
version: "1.0"
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
resources:
  compression: gzip
"#;
        assert!(serde_yaml::from_str::<PipelineConfig>(yaml).is_err());
    }

    #[test]
    fn test_invalid_write_mode_rejected() {
        let yaml = r#"
version: "1.0"
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
  write_mode: truncate
"#;
        assert!(serde_yaml::from_str::<PipelineConfig>(yaml).is_err());
    }

    #[test]
    fn test_invalid_sync_mode_rejected() {
        let yaml = r#"
version: "1.0"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: snapshot
destination:
  use: dest-postgres
  config: {}
  write_mode: append
"#;
        assert!(serde_yaml::from_str::<PipelineConfig>(yaml).is_err());
    }

    #[test]
    fn test_pipeline_permissions_and_limits_full() {
        let yaml = r#"
version: "1.0"
pipeline: test_perms
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
  permissions:
    network:
      allowed_hosts:
        - db.example.com
        - "*.internal.corp"
    env:
      allowed_vars: [DATABASE_URL, PG_PASSWORD]
    fs:
      allowed_preopens: [/data/exports]
  limits:
    max_memory: 128mb
    timeout_seconds: 60
destination:
  use: dest-postgres
  config: {}
  write_mode: append
  permissions:
    network:
      allowed_hosts:
        - warehouse.example.com
  limits:
    max_memory: 256mb
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        let src_perms = config.source.permissions.as_ref().unwrap();
        assert_eq!(
            src_perms.network.allowed_hosts,
            Some(vec![
                "db.example.com".to_string(),
                "*.internal.corp".to_string()
            ])
        );
        assert_eq!(
            src_perms.env.allowed_vars,
            Some(vec!["DATABASE_URL".to_string(), "PG_PASSWORD".to_string()])
        );
        assert_eq!(
            src_perms.fs.allowed_preopens,
            Some(vec!["/data/exports".to_string()])
        );
        let src_limits = config.source.limits.as_ref().unwrap();
        assert_eq!(src_limits.max_memory, Some("128mb".to_string()));
        assert_eq!(src_limits.timeout_seconds, Some(60));
        let dst_perms = config.destination.permissions.as_ref().unwrap();
        assert_eq!(
            dst_perms.network.allowed_hosts,
            Some(vec!["warehouse.example.com".to_string()])
        );
        assert!(dst_perms.env.allowed_vars.is_none());
        let dst_limits = config.destination.limits.as_ref().unwrap();
        assert_eq!(dst_limits.max_memory, Some("256mb".to_string()));
        assert!(dst_limits.timeout_seconds.is_none());
    }

    #[test]
    fn test_pipeline_permissions_and_limits_absent() {
        let config: PipelineConfig = serde_yaml::from_str(
            r#"
version: "1.0"
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
"#,
        )
        .unwrap();
        assert!(config.source.permissions.is_none());
        assert!(config.source.limits.is_none());
        assert!(config.destination.permissions.is_none());
        assert!(config.destination.limits.is_none());
    }

    #[test]
    fn test_fixture_pipeline_with_permissions_parses() {
        let yaml = include_str!(
            "../../../../tests/connectors/postgres/fixtures/pipelines/e2e_permissions.yaml"
        );
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.pipeline, "e2e_permissions");
        let src_perms = config.source.permissions.as_ref().unwrap();
        assert_eq!(
            src_perms.network.allowed_hosts,
            Some(vec!["localhost".to_string(), "127.0.0.1".to_string()])
        );
        assert_eq!(
            src_perms.env.allowed_vars,
            Some(vec!["PGPASSWORD".to_string()])
        );
        let src_limits = config.source.limits.as_ref().unwrap();
        assert_eq!(src_limits.max_memory, Some("256mb".to_string()));
        assert_eq!(src_limits.timeout_seconds, Some(120));
    }

    #[test]
    fn test_state_backend_postgres_variant() {
        let yaml = r#"
version: "1.0"
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
state:
  backend: postgres
  connection: "host=localhost dbname=test"
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.state.backend, StateBackendKind::Postgres);
        assert_eq!(
            config.state.connection,
            Some("host=localhost dbname=test".to_string())
        );
    }

    #[test]
    fn test_pipeline_limits_only_no_permissions() {
        let config: PipelineConfig = serde_yaml::from_str(
            r#"
version: "1.0"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
  limits:
    timeout_seconds: 30
destination:
  use: dest-postgres
  config: {}
  write_mode: append
"#,
        )
        .unwrap();
        assert!(config.source.permissions.is_none());
        let limits = config.source.limits.as_ref().unwrap();
        assert_eq!(limits.timeout_seconds, Some(30));
        assert!(limits.max_memory.is_none());
    }
}
