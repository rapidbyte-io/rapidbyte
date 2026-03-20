//! Outbound port for pipeline execution.

use async_trait::async_trait;
use rapidbyte_engine::ProgressEvent;
use rapidbyte_pipeline_config::PipelineConfig;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::domain::error::AgentError;
use crate::domain::task::TaskExecutionResult;

/// Outbound port for executing a parsed pipeline.
#[async_trait]
pub trait PipelineExecutor: Send + Sync {
    /// Execute a validated pipeline configuration.
    ///
    /// The `progress_tx` channel receives engine progress events for
    /// forwarding to the heartbeat loop.
    async fn execute(
        &self,
        config: &PipelineConfig,
        cancel: CancellationToken,
        progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    ) -> Result<TaskExecutionResult, AgentError>;
}
