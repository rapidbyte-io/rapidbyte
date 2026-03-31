use async_trait::async_trait;

use crate::domain::ports::connection_tester::ConnectionTestError;
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

        // connections.yml has a top-level "connections:" key
        let connections_map = value
            .get("connections")
            .and_then(|v| v.as_mapping())
            .ok_or_else(|| ServiceError::Internal {
                message: "connections.yml missing 'connections' key or not a mapping".into(),
            })?;

        let mut connections = Vec::new();
        for (key, val) in connections_map {
            if let Some(name) = key.as_str() {
                let connector = val
                    .get("use")
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

        let connections_map = value
            .get("connections")
            .and_then(|v| v.as_mapping())
            .ok_or_else(|| ServiceError::Internal {
                message: "connections.yml missing 'connections' key".into(),
            })?;

        let conn_key = serde_yaml::Value::String(name.to_string());
        let conn = connections_map
            .get(&conn_key)
            .ok_or_else(|| ServiceError::NotFound {
                resource: "connection".into(),
                id: name.to_string(),
            })?;

        let connector = conn
            .get("use")
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

        match self.ctx.connection_tester.test(&config).await {
            Ok(result) => Ok(ConnectionTestResponse {
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
            }),
            // Connection-level failures (bad creds, unreachable host) are expected
            // outcomes, not server faults — return 200 with status: "failed"
            Err(ConnectionTestError::Connection(msg)) => Ok(ConnectionTestResponse {
                name: name.to_string(),
                status: "failed".into(),
                latency_ms: None,
                details: None,
                error: Some(serde_json::json!({
                    "code": "connection_failed",
                    "message": msg,
                })),
            }),
            // Plugin-level errors are infrastructure faults
            Err(ConnectionTestError::Plugin(msg)) => Err(ServiceError::Internal { message: msg }),
        }
    }

    async fn discover(
        &self,
        name: &str,
        table: Option<&str>,
    ) -> Result<ConnectionDiscoverResponse, ServiceError> {
        let (_connector, config) = raw_connection_config(&self.ctx, name).await?;

        match self.ctx.connection_tester.discover(&config, table).await {
            Ok(result) => Ok(ConnectionDiscoverResponse {
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
            }),
            // Connection-level failures during discover are semantically invalid/unreachable
            // config — return 422 so clients know the connection config needs fixing.
            Err(ConnectionTestError::Connection(msg)) => Err(ServiceError::ValidationFailed {
                details: vec![crate::traits::FieldError {
                    field: "connection".into(),
                    reason: msg,
                }],
            }),
            // Plugin-level errors are infrastructure faults
            Err(ConnectionTestError::Plugin(msg)) => Err(ServiceError::Internal { message: msg }),
        }
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
    let connections_map = value
        .get("connections")
        .and_then(|v| v.as_mapping())
        .ok_or_else(|| ServiceError::Internal {
            message: "connections.yml missing 'connections' key".into(),
        })?;

    let conn_key = serde_yaml::Value::String(name.to_string());
    let conn = connections_map
        .get(&conn_key)
        .ok_or_else(|| ServiceError::NotFound {
            resource: "connection".into(),
            id: name.to_string(),
        })?;
    let connector = conn
        .get("use")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let config = serde_json::to_value(conn).map_err(|e| ServiceError::Internal {
        message: e.to_string(),
    })?;
    Ok((connector, config))
}

const SENSITIVE_PATTERNS: &[&str] = &[
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "access_key",
    "auth",
    "credential",
    "dsn",
    "connection_string",
    "conn_string",
    "jdbc_url",
    "database_url",
    "connection_url",
];

fn redact_sensitive_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(obj) => {
            for (key, val) in obj.iter_mut() {
                let lower = key.to_lowercase();
                if SENSITIVE_PATTERNS.iter().any(|p| lower.contains(p)) {
                    *val = serde_json::Value::String("***REDACTED***".into());
                } else {
                    redact_sensitive_fields(val);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for val in arr.iter_mut() {
                redact_sensitive_fields(val);
            }
        }
        _ => {}
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
connections:
  my_pg:
    use: postgres
    config:
      host: localhost
      port: 5432
      password: secret123
  my_other:
    use: mysql
    config:
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
        assert_eq!(detail.config["config"]["password"], "***REDACTED***");
        // non-sensitive fields are preserved
        assert_eq!(detail.config["config"]["host"], "localhost");
    }

    #[tokio::test]
    async fn get_redacts_api_key() {
        let services = services_with_connections(CONNECTIONS_YAML);
        let detail = services.get("my_other").await.unwrap();
        assert_eq!(detail.config["config"]["api_key"], "***REDACTED***");
        assert_eq!(detail.config["config"]["host"], "other-host");
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

    #[test]
    fn redact_nested_secrets() {
        let mut val = serde_json::json!({
            "host": "db.example.com",
            "tls_config": {
                "username": "admin",
                "password": "secret123"
            },
            "headers": [{"api_key": "key123"}]
        });
        super::redact_sensitive_fields(&mut val);
        // password inside a non-sensitive parent is still redacted (recursive)
        assert_eq!(val["tls_config"]["password"], "***REDACTED***");
        assert_eq!(val["tls_config"]["username"], "admin"); // not redacted
        assert_eq!(val["headers"][0]["api_key"], "***REDACTED***");
        assert_eq!(val["host"], "db.example.com"); // not redacted
    }

    #[test]
    fn redact_expanded_sensitive_patterns() {
        let mut val = serde_json::json!({
            "apikey": "ak_live_123",
            "private_key": "-----BEGIN RSA PRIVATE KEY-----",
            "access_key": "AKIAIOSFODNN7EXAMPLE",
            "auth_token": "bearer_xyz",
            "credentials": "base64encodedcreds",
            "host": "localhost",
            "username": "admin",
        });
        super::redact_sensitive_fields(&mut val);
        assert_eq!(val["apikey"], "***REDACTED***");
        assert_eq!(val["private_key"], "***REDACTED***");
        assert_eq!(val["access_key"], "***REDACTED***");
        assert_eq!(val["auth_token"], "***REDACTED***");
        assert_eq!(val["credentials"], "***REDACTED***");
        // non-sensitive fields are preserved
        assert_eq!(val["host"], "localhost");
        assert_eq!(val["username"], "admin");
    }

    #[test]
    fn redact_dsn_url_patterns() {
        let mut val = serde_json::json!({
            "dsn": "postgres://user:pass@localhost/db",
            "connection_string": "host=localhost;user=sa;password=secret",
            "conn_string": "Server=myserver;Database=mydb",
            "jdbc_url": "jdbc:postgresql://localhost/mydb",
            "database_url": "postgresql://user:pass@db.example.com/prod",
            "connection_url": "postgresql://admin:pw@host:5432/db",
            "host": "localhost",
            "port": 5432,
        });
        super::redact_sensitive_fields(&mut val);
        assert_eq!(val["dsn"], "***REDACTED***");
        assert_eq!(val["connection_string"], "***REDACTED***");
        assert_eq!(val["conn_string"], "***REDACTED***");
        assert_eq!(val["jdbc_url"], "***REDACTED***");
        assert_eq!(val["database_url"], "***REDACTED***");
        assert_eq!(val["connection_url"], "***REDACTED***");
        // non-sensitive fields are preserved
        assert_eq!(val["host"], "localhost");
        assert_eq!(val["port"], 5432);
    }

    #[test]
    fn redact_dsn_url_patterns_do_not_match_base_url_or_callback_url() {
        // "url" is deliberately NOT in SENSITIVE_PATTERNS to avoid false positives
        // on fields like "base_url", "callback_url", "webhook_url".
        let mut val = serde_json::json!({
            "base_url": "https://api.example.com",
            "callback_url": "https://app.example.com/callback",
            "webhook_url": "https://hooks.example.com/notify",
        });
        super::redact_sensitive_fields(&mut val);
        // None of these generic *_url fields should be redacted
        assert_ne!(val["base_url"], "***REDACTED***");
        assert_ne!(val["callback_url"], "***REDACTED***");
        assert_ne!(val["webhook_url"], "***REDACTED***");
    }

    #[tokio::test]
    async fn test_connection_failure_returns_200_with_failed_status() {
        use std::sync::Arc;

        use async_trait::async_trait;

        use crate::domain::ports::connection_tester::{
            ConnectionTestError, ConnectionTester, DiscoveryResult, TestResult,
        };

        struct FailingConnectionTester;

        #[async_trait]
        impl ConnectionTester for FailingConnectionTester {
            async fn test(
                &self,
                _connection_config: &serde_json::Value,
            ) -> Result<TestResult, ConnectionTestError> {
                Err(ConnectionTestError::Connection("connection refused".into()))
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

        let mut tc = fake_context();
        tc.ctx.pipeline_source =
            Arc::new(FakePipelineSource::new().with_connections_yaml(CONNECTIONS_YAML));
        tc.ctx.connection_tester = Arc::new(FailingConnectionTester) as Arc<dyn ConnectionTester>;
        let services = Arc::new(crate::application::services::AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services.test("my_pg").await;
        // Connection failures must return Ok, not Err
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let resp = result.unwrap();
        assert_eq!(resp.name, "my_pg");
        assert_eq!(resp.status, "failed");
        assert!(resp.latency_ms.is_none());
        assert!(resp.error.is_some());
        let err = resp.error.unwrap();
        assert_eq!(err["code"], "connection_failed");
        assert_eq!(err["message"], "connection refused");
    }

    #[tokio::test]
    async fn test_plugin_error_returns_500() {
        use std::sync::Arc;

        use async_trait::async_trait;

        use crate::domain::ports::connection_tester::{
            ConnectionTestError, ConnectionTester, DiscoveryResult, TestResult,
        };

        struct PluginErrorTester;

        #[async_trait]
        impl ConnectionTester for PluginErrorTester {
            async fn test(
                &self,
                _connection_config: &serde_json::Value,
            ) -> Result<TestResult, ConnectionTestError> {
                Err(ConnectionTestError::Plugin("wasm trap".into()))
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

        let mut tc = fake_context();
        tc.ctx.pipeline_source =
            Arc::new(FakePipelineSource::new().with_connections_yaml(CONNECTIONS_YAML));
        tc.ctx.connection_tester = Arc::new(PluginErrorTester) as Arc<dyn ConnectionTester>;
        let services = Arc::new(crate::application::services::AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services.test("my_pg").await;
        assert!(matches!(result, Err(ServiceError::Internal { .. })));
    }

    #[tokio::test]
    async fn discover_connection_failure_returns_validation_error() {
        use std::sync::Arc;

        use async_trait::async_trait;

        use crate::domain::ports::connection_tester::{
            ConnectionTestError, ConnectionTester, DiscoveryResult, TestResult,
        };

        struct FailingDiscoverTester;

        #[async_trait]
        impl ConnectionTester for FailingDiscoverTester {
            async fn test(
                &self,
                _connection_config: &serde_json::Value,
            ) -> Result<TestResult, ConnectionTestError> {
                Ok(TestResult {
                    status: "ok".into(),
                    latency_ms: None,
                    details: None,
                    error: None,
                })
            }

            async fn discover(
                &self,
                _connection_config: &serde_json::Value,
                _table: Option<&str>,
            ) -> Result<DiscoveryResult, ConnectionTestError> {
                Err(ConnectionTestError::Connection("host unreachable".into()))
            }
        }

        let mut tc = fake_context();
        tc.ctx.pipeline_source =
            Arc::new(FakePipelineSource::new().with_connections_yaml(CONNECTIONS_YAML));
        tc.ctx.connection_tester = Arc::new(FailingDiscoverTester) as Arc<dyn ConnectionTester>;
        let services = Arc::new(crate::application::services::AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services.discover("my_pg", None).await;
        assert!(
            matches!(result, Err(ServiceError::ValidationFailed { .. })),
            "expected ValidationFailed, got {result:?}"
        );
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
            "connections:\n  db:\n    use: postgres\n    config:\n      password: real_secret\n      host: localhost\n",
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
            captured["config"]["password"],
            serde_json::Value::String("real_secret".into()),
            "connection tester must receive unredacted credentials"
        );
    }
}
