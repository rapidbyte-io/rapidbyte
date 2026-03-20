//! Engine executor adapter — bridges `PipelineExecutor` port to rapidbyte-engine.

use std::sync::Arc;

use async_trait::async_trait;
use rapidbyte_engine::domain::error::PipelineError;
use rapidbyte_engine::ProgressEvent;
use rapidbyte_pipeline_config::PipelineConfig;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::domain::error::AgentError;
use crate::domain::ports::executor::PipelineExecutor;
use crate::domain::task::{
    CommitState, TaskErrorInfo, TaskExecutionResult, TaskMetrics, TaskOutcomeKind,
};

/// Implements [`PipelineExecutor`] by calling `rapidbyte_engine::run_pipeline`.
pub struct EngineExecutor {
    registry_config: RwLock<Arc<rapidbyte_registry::RegistryConfig>>,
}

impl EngineExecutor {
    #[must_use]
    pub fn new(registry_config: rapidbyte_registry::RegistryConfig) -> Self {
        Self {
            registry_config: RwLock::new(Arc::new(registry_config)),
        }
    }

    /// Return a snapshot of the current registry config.
    pub async fn registry_config_snapshot(&self) -> rapidbyte_registry::RegistryConfig {
        let guard = self.registry_config.read().await;
        (**guard).clone()
    }

    /// Merge controller-provided registry URL into the existing config,
    /// preserving `trust_policy` and `trusted_key_pems` set at startup.
    pub async fn merge_registry_url(&self, url: Option<&str>, insecure: bool) {
        let mut guard = self.registry_config.write().await;
        let mut config = (**guard).clone();
        config.default_registry = rapidbyte_registry::normalize_registry_url_option(url);
        config.insecure = insecure;
        *guard = Arc::new(config);
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
        let registry_config = {
            let guard = self.registry_config.read().await;
            Arc::clone(&guard)
        };

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
        PipelineError::Cancelled => unreachable!("matched before calling this helper"),
    }
}

fn convert_pipeline_error(error: &PipelineError) -> TaskErrorInfo {
    match error {
        PipelineError::Plugin(pe) => TaskErrorInfo {
            code: pe.code.clone(),
            message: pe.message.clone(),
            retryable: pe.retryable,

            commit_state: pe.commit_state.unwrap_or(CommitState::BeforeCommit),
        },
        PipelineError::Infrastructure(e) => TaskErrorInfo {
            code: "INFRASTRUCTURE".into(),
            message: format!("{e:#}"),
            retryable: false,

            commit_state: CommitState::BeforeCommit,
        },
        PipelineError::Cancelled => unreachable!("matched before calling this helper"),
    }
}
