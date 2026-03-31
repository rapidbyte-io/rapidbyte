//! Engine-backed [`ConnectionTester`] that delegates to `rapidbyte_engine`.

use async_trait::async_trait;
use rapidbyte_controller::domain::ports::connection_tester::{
    ConnectionTestError, ConnectionTester, DiscoveredStream, DiscoveryResult, TestResult,
};

/// Engine-backed [`ConnectionTester`] that delegates to the engine runtime.
pub struct EngineConnectionTester {
    registry_config: rapidbyte_registry::RegistryConfig,
}

impl EngineConnectionTester {
    /// Create a new connection tester with the given registry configuration.
    #[must_use]
    pub fn new(registry_config: rapidbyte_registry::RegistryConfig) -> Self {
        Self { registry_config }
    }
}

#[async_trait]
impl ConnectionTester for EngineConnectionTester {
    async fn test(&self, config: &serde_json::Value) -> Result<TestResult, ConnectionTestError> {
        // Extract plugin ref from the connection config "use" field.
        // We validate connectivity by running a discover with no table filter
        // and treating success as "connected".
        let source_ref = config.get("use").and_then(|v| v.as_str()).ok_or_else(|| {
            ConnectionTestError::Connection("missing 'use' field in connection config".into())
        })?;

        let start = std::time::Instant::now();
        let engine_ctx = rapidbyte_engine::build_discover_context(&self.registry_config)
            .await
            .map_err(|e| ConnectionTestError::Plugin(e.to_string()))?;

        rapidbyte_engine::discover_plugin(&engine_ctx, source_ref, Some(config))
            .await
            .map_err(|e| ConnectionTestError::Connection(e.to_string()))?;

        Ok(TestResult {
            status: "connected".into(),
            latency_ms: u64::try_from(start.elapsed().as_millis()).ok(),
            details: None,
            error: None,
        })
    }

    async fn discover(
        &self,
        config: &serde_json::Value,
        table: Option<&str>,
    ) -> Result<DiscoveryResult, ConnectionTestError> {
        let source_ref = config.get("use").and_then(|v| v.as_str()).ok_or_else(|| {
            ConnectionTestError::Connection("missing 'use' field in connection config".into())
        })?;

        let engine_ctx = rapidbyte_engine::build_discover_context(&self.registry_config)
            .await
            .map_err(|e| ConnectionTestError::Plugin(e.to_string()))?;

        let streams = rapidbyte_engine::discover_plugin(&engine_ctx, source_ref, Some(config))
            .await
            .map_err(|e| ConnectionTestError::Connection(e.to_string()))?;

        let discovered = streams
            .into_iter()
            .filter(|s| table.is_none_or(|t| s.name == t))
            .map(|s| {
                let columns = u32::try_from(s.schema.fields.len()).ok();
                DiscoveredStream {
                    schema: None,
                    table: s.name,
                    estimated_rows: s.estimated_row_count,
                    columns,
                }
            })
            .collect();

        Ok(DiscoveryResult {
            connection: String::new(),
            streams: discovered,
        })
    }
}
