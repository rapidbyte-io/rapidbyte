//! Engine-backed [`ConnectionTester`] that delegates to `rapidbyte_engine`.

use async_trait::async_trait;
use rapidbyte_controller::domain::ports::connection_tester::{
    ConnectionTestError, ConnectionTester, DiscoveredStream, DiscoveryResult, TestResult,
};

/// Classify an engine `PipelineError` into the correct `ConnectionTestError` variant.
///
/// `Plugin` errors are connector-level (bad credentials, host unreachable) and map
/// to `Connection`. `Infrastructure` errors are runtime/WASM faults and map to `Plugin`.
fn classify_engine_error(e: &rapidbyte_engine::PipelineError) -> ConnectionTestError {
    match e {
        rapidbyte_engine::PipelineError::Plugin(_) => {
            ConnectionTestError::Connection(e.to_string())
        }
        rapidbyte_engine::PipelineError::Infrastructure(_)
        | rapidbyte_engine::PipelineError::Cancelled => ConnectionTestError::Plugin(e.to_string()),
    }
}

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

    /// Run plugin discovery for the given connection config and return the raw
    /// discovered streams.  Used by both [`ConnectionTester::test`] and
    /// [`ConnectionTester::discover`] to avoid duplicating the setup logic.
    async fn run_discover(
        &self,
        config: &serde_json::Value,
    ) -> Result<Vec<rapidbyte_engine::domain::ports::DiscoveredStream>, ConnectionTestError> {
        let source_ref = config
            .get("use")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectionTestError::Connection("missing 'use' field".into()))?;
        // Extract the nested "config" object — engine expects plugin-specific config,
        // not the whole connection entry.
        let plugin_config = config.get("config");
        let engine_ctx = rapidbyte_engine::build_discover_context(&self.registry_config)
            .await
            .map_err(|e| ConnectionTestError::Plugin(e.to_string()))?;
        rapidbyte_engine::discover_plugin(&engine_ctx, source_ref, plugin_config)
            .await
            .map_err(|e| classify_engine_error(&e))
    }
}

#[async_trait]
impl ConnectionTester for EngineConnectionTester {
    async fn test(&self, config: &serde_json::Value) -> Result<TestResult, ConnectionTestError> {
        // Validate connectivity by running discover and treating success as "connected".
        let start = std::time::Instant::now();
        self.run_discover(config).await?;
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
        let streams = self.run_discover(config).await?;

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
