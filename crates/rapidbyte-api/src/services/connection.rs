//! Local-mode implementation of [`ConnectionService`].
//!
//! Extracts connection information from pipeline configs. In the current
//! pipeline model, connections are embedded in source/destination configs
//! (the `use` field), not standalone entities.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use rapidbyte_pipeline_config::PipelineConfig;

use crate::error::ApiError;
use crate::traits::ConnectionService;
use crate::types::{
    ConnectionDetail, ConnectionSummary, ConnectionTestResult, DiscoverRequest, DiscoverResult,
};

type Result<T> = std::result::Result<T, ApiError>;

/// Sensitive config keys whose values should be redacted.
const SENSITIVE_KEYS: &[&str] = &["password", "secret", "token", "api_key", "private_key"];

/// Local-mode connection service.
///
/// Scans the pipeline catalog to extract connection information from
/// source and destination configurations.
pub struct LocalConnectionService {
    catalog: Arc<HashMap<String, PipelineConfig>>,
}

impl LocalConnectionService {
    /// Create a new `LocalConnectionService`.
    #[must_use]
    pub fn new(catalog: Arc<HashMap<String, PipelineConfig>>) -> Self {
        Self { catalog }
    }
}

/// Returns `true` if the key looks like a sensitive field.
fn is_sensitive_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    SENSITIVE_KEYS.iter().any(|s| lower.contains(s))
}

/// Recursively redact sensitive values in a JSON object.
fn redact_sensitive(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let redacted = map
                .iter()
                .map(|(k, v)| {
                    if is_sensitive_key(k) {
                        (
                            k.clone(),
                            serde_json::Value::String("***REDACTED***".into()),
                        )
                    } else {
                        (k.clone(), redact_sensitive(v))
                    }
                })
                .collect();
            serde_json::Value::Object(redacted)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(redact_sensitive).collect())
        }
        other => other.clone(),
    }
}

/// Collected info about a connection extracted from the catalog.
struct ConnectionInfo {
    connector: String,
    config: serde_json::Value,
    used_by: Vec<String>,
}

/// Scan catalog and collect connection info keyed by connection name (`use_ref`).
fn collect_connections(
    catalog: &HashMap<String, PipelineConfig>,
) -> HashMap<String, ConnectionInfo> {
    let mut connections: HashMap<String, ConnectionInfo> = HashMap::new();

    for (pipeline_name, config) in catalog {
        // Source connection
        let src_name = &config.source.use_ref;
        connections
            .entry(src_name.clone())
            .and_modify(|info| {
                if !info.used_by.contains(pipeline_name) {
                    info.used_by.push(pipeline_name.clone());
                }
            })
            .or_insert_with(|| ConnectionInfo {
                connector: src_name.clone(),
                config: config.source.config.clone(),
                used_by: vec![pipeline_name.clone()],
            });

        // Destination connection
        let dst_name = &config.destination.use_ref;
        connections
            .entry(dst_name.clone())
            .and_modify(|info| {
                if !info.used_by.contains(pipeline_name) {
                    info.used_by.push(pipeline_name.clone());
                }
            })
            .or_insert_with(|| ConnectionInfo {
                connector: dst_name.clone(),
                config: config.destination.config.clone(),
                used_by: vec![pipeline_name.clone()],
            });
    }

    connections
}

#[async_trait]
impl ConnectionService for LocalConnectionService {
    async fn list(&self) -> Result<Vec<ConnectionSummary>> {
        let connections = collect_connections(&self.catalog);

        let mut items: Vec<ConnectionSummary> = connections
            .into_iter()
            .map(|(name, info)| ConnectionSummary {
                name,
                connector: info.connector,
                used_by: info.used_by,
            })
            .collect();

        // Stable sort by name for deterministic output.
        items.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(items)
    }

    async fn get(&self, name: &str) -> Result<ConnectionDetail> {
        let connections = collect_connections(&self.catalog);

        let info = connections.get(name).ok_or_else(|| {
            ApiError::not_found(
                "connection_not_found",
                format!("Connection '{name}' does not exist"),
            )
        })?;

        Ok(ConnectionDetail {
            name: name.to_string(),
            connector: info.connector.clone(),
            config: redact_sensitive(&info.config),
            used_by: info.used_by.clone(),
        })
    }

    async fn test(&self, _name: &str) -> Result<ConnectionTestResult> {
        Err(ApiError::not_implemented("connection testing"))
    }

    async fn discover(&self, _request: DiscoverRequest) -> Result<DiscoverResult> {
        Err(ApiError::not_implemented("stream discovery"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal `PipelineConfig` from YAML for testing.
    fn minimal_pipeline_config(name: &str, source: &str, dest: &str) -> PipelineConfig {
        let yaml = format!(
            r#"
version: "1.0"
pipeline: {name}
source:
  use: {source}
  config: {{}}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: {dest}
  config: {{}}
  write_mode: append
"#
        );
        serde_yaml::from_str(&yaml).expect("minimal pipeline YAML must parse")
    }

    /// Build a pipeline config with sensitive fields in source config.
    fn pipeline_with_secrets(name: &str) -> PipelineConfig {
        let yaml = format!(
            r#"
version: "1.0"
pipeline: {name}
source:
  use: postgres
  config:
    host: localhost
    port: 5432
    user: admin
    password: super_secret
    api_key: my-key-123
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config:
    host: localhost
    password: dest_secret
  write_mode: append
"#
        );
        serde_yaml::from_str(&yaml).expect("pipeline with secrets must parse")
    }

    fn empty_catalog() -> Arc<HashMap<String, PipelineConfig>> {
        Arc::new(HashMap::new())
    }

    fn test_catalog() -> Arc<HashMap<String, PipelineConfig>> {
        let mut map = HashMap::new();
        map.insert(
            "pg-pipeline".into(),
            minimal_pipeline_config("pg-pipeline", "postgres", "postgres"),
        );
        map.insert(
            "mixed-pipeline".into(),
            minimal_pipeline_config("mixed-pipeline", "mysql", "postgres"),
        );
        Arc::new(map)
    }

    fn make_service(catalog: Arc<HashMap<String, PipelineConfig>>) -> LocalConnectionService {
        LocalConnectionService::new(catalog)
    }

    // ----- list -----

    #[tokio::test]
    async fn list_returns_connections() {
        let svc = make_service(test_catalog());
        let result = svc.list().await.unwrap();
        // "postgres" is used by both pipelines (source+dest), "mysql" by one
        assert_eq!(result.len(), 2);
        let names: Vec<&str> = result.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"postgres"));
        assert!(names.contains(&"mysql"));
    }

    #[tokio::test]
    async fn list_empty_catalog() {
        let svc = make_service(empty_catalog());
        let result = svc.list().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn list_deduplicates_connections() {
        // Both source and dest use "postgres" in pg-pipeline → single entry
        let mut map = HashMap::new();
        map.insert(
            "pg-pipeline".into(),
            minimal_pipeline_config("pg-pipeline", "postgres", "postgres"),
        );
        let svc = make_service(Arc::new(map));
        let result = svc.list().await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "postgres");
        assert!(result[0].used_by.contains(&"pg-pipeline".to_string()));
    }

    #[tokio::test]
    async fn list_used_by_tracks_pipelines() {
        let svc = make_service(test_catalog());
        let result = svc.list().await.unwrap();
        let pg = result.iter().find(|c| c.name == "postgres").unwrap();
        // postgres is used by both pipelines
        assert_eq!(pg.used_by.len(), 2);
    }

    #[tokio::test]
    async fn list_sorted_by_name() {
        let svc = make_service(test_catalog());
        let result = svc.list().await.unwrap();
        let names: Vec<&str> = result.iter().map(|c| c.name.as_str()).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted);
    }

    // ----- get -----

    #[tokio::test]
    async fn get_returns_detail() {
        let svc = make_service(test_catalog());
        let detail = svc.get("postgres").await.unwrap();
        assert_eq!(detail.name, "postgres");
        assert_eq!(detail.connector, "postgres");
        assert!(!detail.used_by.is_empty());
    }

    #[tokio::test]
    async fn get_not_found() {
        let svc = make_service(test_catalog());
        let err = svc.get("nonexistent").await.unwrap_err();
        assert!(matches!(err, ApiError::NotFound { .. }));
    }

    #[tokio::test]
    async fn get_redacts_sensitive_fields() {
        let mut map = HashMap::new();
        map.insert("secret-pipe".into(), pipeline_with_secrets("secret-pipe"));
        let svc = make_service(Arc::new(map));
        let detail = svc.get("postgres").await.unwrap();
        let config = detail.config.as_object().unwrap();
        // "host" should not be redacted
        assert_eq!(config.get("host").unwrap().as_str().unwrap(), "localhost");
        // "password" should be redacted
        assert_eq!(
            config.get("password").unwrap().as_str().unwrap(),
            "***REDACTED***"
        );
        // "api_key" should be redacted
        assert_eq!(
            config.get("api_key").unwrap().as_str().unwrap(),
            "***REDACTED***"
        );
    }

    // ----- test -----

    #[tokio::test]
    async fn test_returns_not_implemented() {
        let svc = make_service(empty_catalog());
        let err = svc.test("postgres").await.unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    // ----- discover -----

    #[tokio::test]
    async fn discover_returns_not_implemented() {
        let svc = make_service(empty_catalog());
        let err = svc
            .discover(DiscoverRequest {
                connection: "postgres".into(),
                table: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    // ----- redact_sensitive unit tests -----

    #[test]
    fn redact_sensitive_redacts_password() {
        let input = serde_json::json!({"host": "localhost", "password": "secret"});
        let result = redact_sensitive(&input);
        assert_eq!(result["host"], "localhost");
        assert_eq!(result["password"], "***REDACTED***");
    }

    #[test]
    fn redact_sensitive_case_insensitive() {
        let input =
            serde_json::json!({"HOST": "localhost", "PASSWORD": "secret", "API_KEY": "key123"});
        let result = redact_sensitive(&input);
        assert_eq!(result["HOST"], "localhost");
        assert_eq!(result["PASSWORD"], "***REDACTED***");
        assert_eq!(result["API_KEY"], "***REDACTED***");
    }

    #[test]
    fn redact_sensitive_nested() {
        let input = serde_json::json!({
            "connection": {
                "host": "localhost",
                "password": "nested_secret"
            }
        });
        let result = redact_sensitive(&input);
        assert_eq!(result["connection"]["host"], "localhost");
        assert_eq!(result["connection"]["password"], "***REDACTED***");
    }

    #[test]
    fn redact_sensitive_preserves_non_objects() {
        let input = serde_json::json!(42);
        assert_eq!(redact_sensitive(&input), serde_json::json!(42));

        let input = serde_json::json!("hello");
        assert_eq!(redact_sensitive(&input), serde_json::json!("hello"));
    }
}
