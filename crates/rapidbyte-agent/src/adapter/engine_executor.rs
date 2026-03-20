//! Engine executor adapter — bridges PipelineExecutor port to rapidbyte-engine.

use std::sync::Arc;

use async_trait::async_trait;
use rapidbyte_engine::domain::error::PipelineError;
use rapidbyte_engine::ProgressEvent;
use rapidbyte_pipeline_config::PipelineConfig;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::domain::error::AgentError;
use crate::domain::ports::executor::PipelineExecutor;
use crate::domain::ports::metrics::MetricsProvider;
use crate::domain::task::{
    CommitState, TaskErrorInfo, TaskExecutionResult, TaskMetrics, TaskOutcomeKind,
};

/// Implements [`PipelineExecutor`] by calling `rapidbyte_engine::run_pipeline`.
pub struct EngineExecutor {
    registry_config: RwLock<rapidbyte_registry::RegistryConfig>,
    #[allow(dead_code)]
    metrics: Arc<dyn MetricsProvider>,
}

impl EngineExecutor {
    #[must_use]
    pub fn new(
        registry_config: rapidbyte_registry::RegistryConfig,
        metrics: Arc<dyn MetricsProvider>,
    ) -> Self {
        Self {
            registry_config: RwLock::new(registry_config),
            metrics,
        }
    }

    /// Update registry config after receiving it from the controller.
    pub async fn update_registry_config(&self, config: rapidbyte_registry::RegistryConfig) {
        *self.registry_config.write().await = config;
    }
}

#[async_trait]
impl PipelineExecutor for EngineExecutor {
    async fn execute(
        &self,
        config: &PipelineConfig,
        cancel: CancellationToken,
        progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    ) -> Result<TaskExecutionResult, AgentError> {
        let registry_config = self.registry_config.read().await.clone();

        let engine_ctx =
            rapidbyte_engine::build_run_context(config, Some(progress_tx), &registry_config)
                .await
                .map_err(|e| AgentError::ExecutionFailed(e.into()))?;

        let start = std::time::Instant::now();

        let result = rapidbyte_engine::run_pipeline(&engine_ctx, config, cancel).await;

        let elapsed = start.elapsed().as_secs_f64();

        match result {
            Ok(pipeline_result) => Ok(TaskExecutionResult {
                outcome: TaskOutcomeKind::Completed,
                metrics: TaskMetrics {
                    records_read: pipeline_result.counts.records_read,
                    records_written: pipeline_result.counts.records_written,
                    bytes_read: pipeline_result.counts.bytes_read,
                    bytes_written: pipeline_result.counts.bytes_written,
                    elapsed_seconds: elapsed,
                    cursors_advanced: 0,
                },
            }),
            Err(PipelineError::Cancelled) => Err(AgentError::Cancelled),
            Err(ref e) if is_pre_commit_cancellation(e) => Err(AgentError::Cancelled),
            Err(e) => Ok(TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(convert_pipeline_error(&e)),
                metrics: TaskMetrics {
                    elapsed_seconds: elapsed,
                    ..TaskMetrics::default()
                },
            }),
        }
    }
}

fn is_pre_commit_cancellation(error: &PipelineError) -> bool {
    match error {
        PipelineError::Plugin(pe) => {
            pe.code == "CANCELLED" && matches!(pe.commit_state, Some(CommitState::BeforeCommit))
        }
        PipelineError::Infrastructure(_) => false,
        PipelineError::Cancelled => true,
    }
}

fn convert_pipeline_error(error: &PipelineError) -> TaskErrorInfo {
    match error {
        PipelineError::Plugin(pe) => TaskErrorInfo {
            code: pe.code.clone(),
            message: pe.message.clone(),
            retryable: pe.retryable,
            safe_to_retry: pe.safe_to_retry,
            commit_state: pe.commit_state.unwrap_or(CommitState::BeforeCommit),
        },
        PipelineError::Infrastructure(e) => TaskErrorInfo {
            code: "INFRASTRUCTURE".into(),
            message: format!("{e:#}"),
            retryable: false,
            safe_to_retry: false,
            commit_state: CommitState::BeforeCommit,
        },
        PipelineError::Cancelled => TaskErrorInfo {
            code: "CANCELLED".into(),
            message: "pipeline cancelled".into(),
            retryable: true,
            safe_to_retry: true,
            commit_state: CommitState::BeforeCommit,
        },
    }
}
