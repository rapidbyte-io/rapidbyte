//! Engine-backed [`TaskExecutor`] for the embedded agent.
//!
//! Wraps the engine's execution functions to fulfil the
//! [`rapidbyte_controller::application::embedded_agent::TaskExecutor`] contract.

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use rapidbyte_controller::application::embedded_agent::TaskExecutor;
use rapidbyte_controller::domain::run::RunMetrics;

/// Engine-backed [`TaskExecutor`] that delegates to `rapidbyte_engine`.
///
/// Stores a `RegistryConfig` and builds a fresh engine context per task so
/// that each execution gets an isolated WASM runtime and state connection.
pub struct EngineTaskExecutor {
    registry_config: rapidbyte_registry::RegistryConfig,
}

impl EngineTaskExecutor {
    /// Create a new executor with the given registry configuration.
    #[must_use]
    pub fn new(registry_config: rapidbyte_registry::RegistryConfig) -> Self {
        Self { registry_config }
    }
}

#[async_trait]
impl TaskExecutor for EngineTaskExecutor {
    async fn execute_sync(
        &self,
        pipeline_yaml: &str,
        cancel: &CancellationToken,
    ) -> Result<RunMetrics> {
        let config: rapidbyte_pipeline_config::PipelineConfig =
            serde_yaml::from_str(pipeline_yaml)?;

        let engine_ctx =
            rapidbyte_engine::build_run_context(&config, None, &self.registry_config).await?;

        let result = rapidbyte_engine::run_pipeline(&engine_ctx, &config, cancel.clone()).await?;

        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let duration_ms = (result.duration_secs * 1000.0) as u64;

        Ok(RunMetrics {
            rows_read: result.counts.records_read,
            rows_written: result.counts.records_written,
            bytes_read: result.counts.bytes_read,
            bytes_written: result.counts.bytes_written,
            duration_ms,
        })
    }

    async fn execute_check_apply(&self, pipeline_yaml: &str) -> Result<()> {
        let config: rapidbyte_pipeline_config::PipelineConfig =
            serde_yaml::from_str(pipeline_yaml)?;
        let engine_ctx =
            rapidbyte_engine::build_lightweight_context(&self.registry_config, &config).await?;
        let _result = rapidbyte_engine::check_pipeline(&engine_ctx, &config).await?;
        // Apply phase (resource provisioning) is not yet implemented in the engine.
        // Fail explicitly rather than reporting false success.
        anyhow::bail!("check passed but apply phase (resource provisioning) is not yet implemented")
    }

    async fn execute_teardown(&self, pipeline_yaml: &str, reason: &str) -> Result<()> {
        let config: rapidbyte_pipeline_config::PipelineConfig =
            serde_yaml::from_str(pipeline_yaml)?;
        let engine_ctx =
            rapidbyte_engine::build_lightweight_context(&self.registry_config, &config).await?;
        rapidbyte_engine::teardown_pipeline(&engine_ctx, &config, reason).await?;
        Ok(())
    }

    async fn execute_assert(&self, _pipeline_yaml: &str) -> Result<()> {
        anyhow::bail!("assertion execution is not yet implemented in the engine")
    }
}
