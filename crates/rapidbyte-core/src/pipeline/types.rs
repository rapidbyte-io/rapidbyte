use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub version: String,
    pub pipeline: String,
    pub source: SourceConfig,
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

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            max_memory: default_max_memory(),
            max_batch_bytes: default_max_batch_bytes(),
            parallelism: default_parallelism(),
        }
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
    }
}
