use serde::{Deserialize, Serialize};

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
    pub sync_mode: String,
    pub cursor_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    #[serde(rename = "use")]
    pub use_ref: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationConfig {
    #[serde(rename = "use")]
    pub use_ref: String,
    pub config: serde_json::Value,
    pub write_mode: String,
    #[serde(default)]
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    #[serde(default = "default_backend")]
    pub backend: String,
    pub connection: Option<String>,
}

fn default_backend() -> String {
    "sqlite".to_string()
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            connection: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceConfig {
    #[serde(default = "default_max_memory")]
    pub max_memory: String,
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: String,
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,
    /// Destination commits after writing this many bytes. "0" disables chunking.
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_bytes: String,
    /// Maximum number of retry attempts for retryable errors. 0 disables retries.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

fn default_max_memory() -> String {
    "256mb".to_string()
}
fn default_max_batch_bytes() -> String {
    "64mb".to_string()
}
fn default_parallelism() -> u32 {
    1
}
fn default_checkpoint_interval() -> String {
    "64mb".to_string()
}
fn default_max_retries() -> u32 {
    3
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            max_memory: default_max_memory(),
            max_batch_bytes: default_max_batch_bytes(),
            parallelism: default_parallelism(),
            checkpoint_interval_bytes: default_checkpoint_interval(),
            max_retries: default_max_retries(),
        }
    }
}

/// Parse a human-readable byte size like "64mb" into bytes.
pub fn parse_byte_size(s: &str) -> u64 {
    let s = s.trim().to_lowercase();
    if let Some(n) = s.strip_suffix("gb") {
        n.trim().parse::<u64>().unwrap_or(0) * 1024 * 1024 * 1024
    } else if let Some(n) = s.strip_suffix("mb") {
        n.trim().parse::<u64>().unwrap_or(0) * 1024 * 1024
    } else if let Some(n) = s.strip_suffix("kb") {
        n.trim().parse::<u64>().unwrap_or(0) * 1024
    } else {
        s.parse::<u64>().unwrap_or(0)
    }
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
  use: rapidbyte/source-postgres@v0.1.0
  config:
    host: localhost
    port: 5432
    user: postgres
    password: secret
    database: source_db
  streams:
    - name: users
      sync_mode: full_refresh

destination:
  use: rapidbyte/dest-postgres@v0.1.0
  config:
    host: localhost
    port: 5433
    user: postgres
    password: secret
    database: dest_db
    schema: raw
  write_mode: append
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.pipeline, "test_pg_to_pg");
        assert_eq!(config.version, "1.0");
        assert_eq!(config.source.use_ref, "rapidbyte/source-postgres@v0.1.0");
        assert_eq!(config.source.streams.len(), 1);
        assert_eq!(config.source.streams[0].name, "users");
        assert_eq!(config.source.streams[0].sync_mode, "full_refresh");
        assert_eq!(config.destination.write_mode, "append");
        // Defaults applied
        assert_eq!(config.state.backend, "sqlite");
        assert_eq!(config.resources.parallelism, 1);
        assert_eq!(config.resources.max_memory, "256mb");
        assert_eq!(config.resources.checkpoint_interval_bytes, "64mb");
        assert_eq!(config.resources.max_retries, 3);
    }

    #[test]
    fn test_deserialize_full_pipeline() {
        let yaml = r#"
version: "1.0"
pipeline: full_test

source:
  use: source-postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
    - name: orders
      sync_mode: full_refresh

destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: upsert
  primary_key:
    - id

state:
  backend: sqlite
  connection: /tmp/state.db

resources:
  max_memory: 512mb
  max_batch_bytes: 128mb
  parallelism: 4
  checkpoint_interval_bytes: 32mb
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.source.streams.len(), 2);
        assert_eq!(
            config.source.streams[0].cursor_field,
            Some("updated_at".to_string())
        );
        assert!(config.source.streams[1].cursor_field.is_none());
        assert_eq!(config.destination.primary_key, vec!["id"]);
        assert_eq!(
            config.state.connection,
            Some("/tmp/state.db".to_string())
        );
        assert_eq!(config.resources.parallelism, 4);
        assert_eq!(config.resources.max_memory, "512mb");
        assert_eq!(config.resources.checkpoint_interval_bytes, "32mb");
    }

    #[test]
    fn test_parse_byte_size() {
        assert_eq!(parse_byte_size("64mb"), 64 * 1024 * 1024);
        assert_eq!(parse_byte_size("128mb"), 128 * 1024 * 1024);
        assert_eq!(parse_byte_size("1gb"), 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("512kb"), 512 * 1024);
        assert_eq!(parse_byte_size("1024"), 1024);
        assert_eq!(parse_byte_size("0"), 0);
        assert_eq!(parse_byte_size("  64MB  "), 64 * 1024 * 1024);
    }

    #[test]
    fn test_deserialize_pipeline_with_transforms() {
        let yaml = r#"
version: "1.0"
pipeline: transform_test

source:
  use: source-postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh

transforms:
  - use: rapidbyte/transform-mask@v0.1.0
    config:
      columns:
        - email

destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.transforms.len(), 1);
        assert_eq!(config.transforms[0].use_ref, "rapidbyte/transform-mask@v0.1.0");
    }

    #[test]
    fn test_deserialize_pipeline_without_transforms_defaults_empty() {
        let yaml = r#"
version: "1.0"
pipeline: no_transforms

source:
  use: source-postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh

destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.transforms.is_empty());
    }

    #[test]
    fn test_deserialize_pipeline_multiple_transforms() {
        let yaml = r#"
version: "1.0"
pipeline: multi_transform

source:
  use: source-postgres
  config:
    host: localhost
  streams:
    - name: users
      sync_mode: full_refresh

transforms:
  - use: transform-mask@v0.1.0
    config:
      columns: [email]
  - use: transform-filter@v0.1.0
    config:
      condition: "active = true"

destination:
  use: dest-postgres
  config:
    host: localhost
  write_mode: append
"#;
        let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.transforms.len(), 2);
        assert_eq!(config.transforms[0].use_ref, "transform-mask@v0.1.0");
        assert_eq!(config.transforms[1].use_ref, "transform-filter@v0.1.0");
    }
}
