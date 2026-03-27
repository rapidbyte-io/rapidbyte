use async_trait::async_trait;

use crate::traits::connection::{
    ConnectionDetail, ConnectionDiscoverResponse, ConnectionService, ConnectionSummary,
    ConnectionTestResponse,
};
use crate::traits::ServiceError;

use super::AppServices;

#[async_trait]
impl ConnectionService for AppServices {
    async fn list(&self) -> Result<Vec<ConnectionSummary>, ServiceError> {
        let yaml = self
            .ctx
            .pipeline_source
            .connections_yaml()
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        let Some(yaml) = yaml else {
            return Ok(vec![]);
        };

        let value: serde_yaml::Value =
            serde_yaml::from_str(&yaml).map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        let mut connections = Vec::new();
        if let Some(map) = value.as_mapping() {
            for (key, val) in map {
                if let Some(name) = key.as_str() {
                    let connector = val
                        .get("connector")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();
                    connections.push(ConnectionSummary {
                        name: name.to_string(),
                        connector,
                        used_by: vec![],
                    });
                }
            }
        }
        Ok(connections)
    }

    async fn get(&self, name: &str) -> Result<ConnectionDetail, ServiceError> {
        let yaml = self
            .ctx
            .pipeline_source
            .connections_yaml()
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        let yaml = yaml.ok_or_else(|| ServiceError::NotFound {
            resource: "connection".into(),
            id: name.to_string(),
        })?;

        let value: serde_yaml::Value =
            serde_yaml::from_str(&yaml).map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        let conn = value.get(name).ok_or_else(|| ServiceError::NotFound {
            resource: "connection".into(),
            id: name.to_string(),
        })?;

        let connector = conn
            .get("connector")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let mut config = serde_json::to_value(conn).map_err(|e| ServiceError::Internal {
            message: e.to_string(),
        })?;
        redact_sensitive_fields(&mut config);

        Ok(ConnectionDetail {
            name: name.to_string(),
            connector,
            config,
            used_by: vec![],
        })
    }

    async fn test(&self, name: &str) -> Result<ConnectionTestResponse, ServiceError> {
        let (_connector, config) = raw_connection_config(&self.ctx, name).await?;

        let result = self
            .ctx
            .connection_tester
            .test(&config)
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        Ok(ConnectionTestResponse {
            name: name.to_string(),
            status: result.status,
            latency_ms: result.latency_ms,
            details: result.details,
            error: result.error.map(|e| {
                serde_json::json!({
                    "code": e.code,
                    "message": e.message,
                })
            }),
        })
    }

    async fn discover(
        &self,
        name: &str,
        table: Option<&str>,
    ) -> Result<ConnectionDiscoverResponse, ServiceError> {
        let (_connector, config) = raw_connection_config(&self.ctx, name).await?;

        let result = self
            .ctx
            .connection_tester
            .discover(&config, table)
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        Ok(ConnectionDiscoverResponse {
            connection: name.to_string(),
            streams: result
                .streams
                .into_iter()
                .map(|s| {
                    serde_json::json!({
                        "schema": s.schema,
                        "table": s.table,
                        "estimated_rows": s.estimated_rows,
                        "columns": s.columns,
                    })
                })
                .collect(),
        })
    }
}

/// Parse raw (unredacted) connection config for a named connection.
/// Used internally by test/discover which need real credentials.
async fn raw_connection_config(
    ctx: &crate::application::context::AppContext,
    name: &str,
) -> Result<(String, serde_json::Value), ServiceError> {
    let yaml =
        ctx.pipeline_source
            .connections_yaml()
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;
    let yaml = yaml.ok_or_else(|| ServiceError::NotFound {
        resource: "connection".into(),
        id: name.to_string(),
    })?;
    let value: serde_yaml::Value =
        serde_yaml::from_str(&yaml).map_err(|e| ServiceError::Internal {
            message: e.to_string(),
        })?;
    let conn = value.get(name).ok_or_else(|| ServiceError::NotFound {
        resource: "connection".into(),
        id: name.to_string(),
    })?;
    let connector = conn
        .get("connector")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let config = serde_json::to_value(conn).map_err(|e| ServiceError::Internal {
        message: e.to_string(),
    })?;
    Ok((connector, config))
}

fn redact_sensitive_fields(value: &mut serde_json::Value) {
    if let Some(obj) = value.as_object_mut() {
        for (key, val) in obj.iter_mut() {
            let lower = key.to_lowercase();
            if lower.contains("password")
                || lower.contains("secret")
                || lower.contains("token")
                || lower.contains("api_key")
            {
                *val = serde_json::Value::String("***REDACTED***".into());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::application::testing::{fake_app_services, fake_context, FakePipelineSource};
    use crate::traits::connection::ConnectionService;
    use crate::traits::ServiceError;

    fn services_with_connections(yaml: &str) -> Arc<crate::application::services::AppServices> {
        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(FakePipelineSource::new().with_connections_yaml(yaml));
        Arc::new(crate::application::services::AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ))
    }

    // --- no connections.yml (default fake) ---

    #[tokio::test]
    async fn list_returns_empty_when_no_connections_yaml() {
        let services = fake_app_services();
        let result = services.list().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn get_returns_not_found_when_no_connections_yaml() {
        let services = fake_app_services();
        let result = services.get("nonexistent").await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_returns_not_found_when_no_connections_yaml() {
        let services = fake_app_services();
        let result = services.test("nonexistent").await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn discover_returns_not_found_when_no_connections_yaml() {
        let services = fake_app_services();
        let result = services.discover("nonexistent", None).await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    // --- with connections.yml content ---

    const CONNECTIONS_YAML: &str = "\
my_pg:
  connector: postgres
  host: localhost
  port: 5432
  password: secret123
my_other:
  connector: mysql
  host: other-host
  api_key: abc123
";

    #[tokio::test]
    async fn list_returns_connections_from_yaml() {
        let services = services_with_connections(CONNECTIONS_YAML);
        let mut result = services.list().await.unwrap();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "my_other");
        assert_eq!(result[0].connector, "mysql");
        assert_eq!(result[1].name, "my_pg");
        assert_eq!(result[1].connector, "postgres");
    }

    #[tokio::test]
    async fn get_returns_detail_with_redacted_fields() {
        let services = services_with_connections(CONNECTIONS_YAML);
        let detail = services.get("my_pg").await.unwrap();
        assert_eq!(detail.name, "my_pg");
        assert_eq!(detail.connector, "postgres");
        assert_eq!(detail.config["password"], "***REDACTED***");
        // non-sensitive fields are preserved
        assert_eq!(detail.config["host"], "localhost");
    }

    #[tokio::test]
    async fn get_redacts_api_key() {
        let services = services_with_connections(CONNECTIONS_YAML);
        let detail = services.get("my_other").await.unwrap();
        assert_eq!(detail.config["api_key"], "***REDACTED***");
        assert_eq!(detail.config["host"], "other-host");
    }

    #[tokio::test]
    async fn get_returns_not_found_for_missing_name() {
        let services = services_with_connections(CONNECTIONS_YAML);
        let result = services.get("nonexistent").await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_delegates_to_connection_tester() {
        let services = services_with_connections(CONNECTIONS_YAML);
        let result = services.test("my_pg").await.unwrap();
        assert_eq!(result.name, "my_pg");
        assert_eq!(result.status, "ok");
        assert_eq!(result.latency_ms, Some(1));
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn discover_delegates_to_connection_tester() {
        let services = services_with_connections(CONNECTIONS_YAML);
        let result = services.discover("my_pg", None).await.unwrap();
        assert_eq!(result.connection, "my_pg");
        assert!(result.streams.is_empty());
    }

    #[tokio::test]
    async fn redact_sensitive_fields_password() {
        let mut val = serde_json::json!({
            "password": "hunter2",
            "host": "localhost"
        });
        super::redact_sensitive_fields(&mut val);
        assert_eq!(val["password"], "***REDACTED***");
        assert_eq!(val["host"], "localhost");
    }

    #[tokio::test]
    async fn redact_sensitive_fields_mixed_case() {
        let mut val = serde_json::json!({
            "DB_PASSWORD": "pw",
            "SECRET_KEY": "sk",
            "access_token": "at",
            "normal": "value"
        });
        super::redact_sensitive_fields(&mut val);
        assert_eq!(val["DB_PASSWORD"], "***REDACTED***");
        assert_eq!(val["SECRET_KEY"], "***REDACTED***");
        assert_eq!(val["access_token"], "***REDACTED***");
        assert_eq!(val["normal"], "value");
    }

    #[tokio::test]
    async fn test_connection_passes_unredacted_config() {
        use std::sync::{Arc, Mutex};

        use async_trait::async_trait;

        use crate::domain::ports::connection_tester::{
            ConnectionTestError, ConnectionTester, DiscoveryResult, TestResult,
        };

        // A recording ConnectionTester that captures the config it received.
        struct RecordingTester {
            captured: Mutex<Option<serde_json::Value>>,
        }

        #[async_trait]
        impl ConnectionTester for RecordingTester {
            async fn test(
                &self,
                connection_config: &serde_json::Value,
            ) -> Result<TestResult, ConnectionTestError> {
                *self.captured.lock().unwrap() = Some(connection_config.clone());
                Ok(TestResult {
                    status: "ok".into(),
                    latency_ms: Some(1),
                    details: None,
                    error: None,
                })
            }

            async fn discover(
                &self,
                _connection_config: &serde_json::Value,
                _table: Option<&str>,
            ) -> Result<DiscoveryResult, ConnectionTestError> {
                Ok(DiscoveryResult {
                    connection: "fake".into(),
                    streams: vec![],
                })
            }
        }

        let recorder = Arc::new(RecordingTester {
            captured: Mutex::new(None),
        });

        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(FakePipelineSource::new().with_connections_yaml(
            "db:\n  connector: postgres\n  password: real_secret\n  host: localhost\n",
        ));
        tc.ctx.connection_tester = Arc::clone(&recorder) as Arc<dyn ConnectionTester>;
        let services = Arc::new(crate::application::services::AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        services.test("db").await.unwrap();

        let captured = recorder.captured.lock().unwrap().clone().unwrap();
        // The tester must receive the real password, not the redacted placeholder.
        assert_eq!(
            captured["password"],
            serde_json::Value::String("real_secret".into()),
            "connection tester must receive unredacted credentials"
        );
    }
}
