//! Task execution use-case: parse YAML, run pipeline, report completion.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::domain::error::AgentError;
use crate::domain::ports::controller::{CompletionPayload, TaskAssignment};
use crate::domain::ports::progress::ProgressCollector;
use crate::domain::task::{
    CommitState, TaskErrorInfo, TaskExecutionResult, TaskMetrics, TaskOutcomeKind,
};

use super::context::AgentContext;

/// Execute a single task: parse YAML, run via executor port, report to controller.
pub async fn execute_task(
    ctx: &AgentContext,
    agent_id: &str,
    assignment: &TaskAssignment,
    progress_collector: &Arc<crate::adapter::AtomicProgressCollector>,
    cancel: CancellationToken,
    shutdown: &CancellationToken,
) {
    info!(
        task_id = assignment.task_id,
        run_id = assignment.run_id,
        attempt = assignment.attempt,
        lease_epoch = assignment.lease_epoch,
        "Received task"
    );

    // 1. Parse YAML -> PipelineConfig
    let config = match parse_pipeline(&assignment.pipeline_yaml).await {
        Ok(c) => c,
        Err(e) => {
            let result = TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                    code: "INVALID_PIPELINE".into(),
                    message: e,
                    retryable: false,
                    commit_state: CommitState::BeforeCommit,
                }),
                metrics: TaskMetrics::default(),
            };
            report_completion(ctx, agent_id, assignment, result, shutdown).await;
            return;
        }
    };

    // 2. Check for early cancellation
    if cancel.is_cancelled() {
        let result = TaskExecutionResult {
            outcome: TaskOutcomeKind::Cancelled,
            metrics: TaskMetrics::default(),
        };
        report_completion(ctx, agent_id, assignment, result, shutdown).await;
        return;
    }

    // 3. Set up progress channel and execute
    let (progress_tx, progress_rx) = mpsc::unbounded_channel();
    let bridge = tokio::spawn(bridge_progress(progress_collector.clone(), progress_rx));

    let result = ctx.executor.execute(&config, cancel, progress_tx).await;

    bridge.abort();
    ctx.progress.reset();

    // 4. Convert to TaskExecutionResult
    let result = match result {
        Ok(r) => r,
        Err(AgentError::Cancelled) => TaskExecutionResult {
            outcome: TaskOutcomeKind::Cancelled,
            metrics: TaskMetrics::default(),
        },
        Err(AgentError::ExecutionFailed(ref inner)) => {
            // ExecutionFailed comes from build_run_context (setup/infra),
            // not from the pipeline itself — no writes have occurred.
            TaskExecutionResult {
                outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                    code: "INFRASTRUCTURE".into(),
                    message: inner.to_string(),
                    retryable: false,
                    commit_state: CommitState::BeforeCommit,
                }),
                metrics: TaskMetrics::default(),
            }
        }
        Err(e) => TaskExecutionResult {
            outcome: TaskOutcomeKind::Failed(TaskErrorInfo {
                code: "EXECUTION_ERROR".into(),
                message: e.to_string(),
                retryable: false,
                commit_state: CommitState::BeforeCommit,
            }),
            metrics: TaskMetrics::default(),
        },
    };

    // 5. Report to controller
    report_completion(ctx, agent_id, assignment, result, shutdown).await;
}

async fn parse_pipeline(yaml: &str) -> Result<rapidbyte_pipeline_config::PipelineConfig, String> {
    let config = rapidbyte_pipeline_config::parser::parse_pipeline(
        yaml,
        &rapidbyte_secrets::SecretProviders::new(),
    )
    .await
    .map_err(|e| format!("{e:#}"))?;

    rapidbyte_pipeline_config::validator::validate_pipeline(&config)
        .map_err(|e| format!("{e:#}"))?;

    Ok(config)
}

/// Bridge engine progress events to the concrete `AtomicProgressCollector`.
///
/// Note: this function takes the concrete adapter type, not the port trait,
/// because it needs the write-side `update()` method which is not part of
/// the `ProgressCollector` port (read-only interface for heartbeat).
pub(crate) async fn bridge_progress(
    collector: Arc<crate::adapter::AtomicProgressCollector>,
    mut rx: mpsc::UnboundedReceiver<rapidbyte_engine::ProgressEvent>,
) {
    use rapidbyte_engine::ProgressEvent;

    while let Some(event) = rx.recv().await {
        let msg = match &event {
            ProgressEvent::BatchEmitted { bytes, .. } => format!("processing ({bytes} bytes)"),
            ProgressEvent::StreamCompleted { stream } => format!("stream {stream} completed"),
            ProgressEvent::PhaseChanged { phase } => format!("{phase:?}").to_lowercase(),
            ProgressEvent::StreamStarted { stream } => format!("stream {stream} starting"),
            ProgressEvent::RetryScheduled { .. } => continue,
        };
        collector.update(crate::domain::progress::ProgressSnapshot {
            message: Some(msg),
            progress_pct: None,
        });
    }
}

/// Report task completion to the controller with retry logic.
///
/// The retry loop is shutdown-aware: if the shutdown token is cancelled,
/// retries stop immediately so the agent can exit promptly.
pub(crate) async fn report_completion(
    ctx: &AgentContext,
    agent_id: &str,
    assignment: &TaskAssignment,
    result: TaskExecutionResult,
    shutdown: &CancellationToken,
) {
    let payload = CompletionPayload {
        agent_id: agent_id.to_owned(),
        task_id: assignment.task_id.clone(),
        lease_epoch: assignment.lease_epoch,
        result,
    };

    for attempt in 0..ctx.config.max_completion_retries {
        match ctx.gateway.complete(payload.clone()).await {
            Ok(()) => {
                info!(task_id = assignment.task_id, "Task completion reported");
                return;
            }
            Err(ref e) if is_retryable_controller_error(e) => {
                warn!(
                    task_id = assignment.task_id,
                    attempt,
                    error = %e,
                    "Completion failed, retrying"
                );
                // Sleep is shutdown-aware so SIGINT/SIGTERM stops retries promptly.
                tokio::select! {
                    () = tokio::time::sleep(ctx.config.completion_retry_delay) => {}
                    () = shutdown.cancelled() => {
                        warn!(
                            task_id = assignment.task_id,
                            "Stopping completion retries — agent shutting down"
                        );
                        return;
                    }
                }
            }
            Err(e) => {
                warn!(
                    task_id = assignment.task_id,
                    error = %e,
                    "Completion failed with non-retryable error, giving up"
                );
                return;
            }
        }
    }

    warn!(task_id = assignment.task_id, "Completion retries exhausted");
}

fn is_retryable_controller_error(e: &AgentError) -> bool {
    matches!(e, AgentError::Controller(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::{fake_context, VALID_YAML};
    use crate::domain::task::{TaskExecutionResult, TaskMetrics, TaskOutcomeKind};

    fn test_assignment() -> TaskAssignment {
        TaskAssignment {
            task_id: "task-1".into(),
            run_id: "run-1".into(),
            pipeline_yaml: String::new(), // invalid — will fail parse
            lease_epoch: 1,
            attempt: 1,
        }
    }

    #[tokio::test]
    async fn invalid_yaml_reports_failed_outcome() {
        let t = fake_context();
        t.gateway.enqueue_complete(Ok(()));

        let assignment = test_assignment();
        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(
            &t.ctx,
            "agent-1",
            &assignment,
            &pc,
            CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        let payloads = t.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
        match &payloads[0].result.outcome {
            TaskOutcomeKind::Failed(info) => {
                assert_eq!(info.code, "INVALID_PIPELINE");
                assert!(!info.retryable);
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pre_cancelled_token_reports_cancelled() {
        let t = fake_context();
        t.gateway.enqueue_complete(Ok(()));

        let token = CancellationToken::new();
        token.cancel();

        // Use valid YAML so parse succeeds but cancellation is caught
        let mut assignment = test_assignment();
        assignment.pipeline_yaml = VALID_YAML.into();

        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(
            &t.ctx,
            "agent-1",
            &assignment,
            &pc,
            token,
            &CancellationToken::new(),
        )
        .await;

        let payloads = t.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
        assert!(matches!(
            payloads[0].result.outcome,
            TaskOutcomeKind::Cancelled
        ));
    }

    #[tokio::test]
    async fn successful_execution_reports_completed() {
        let t = fake_context();
        t.executor.enqueue(Ok(TaskExecutionResult {
            outcome: TaskOutcomeKind::Completed,
            metrics: TaskMetrics {
                records_written: 42,
                ..TaskMetrics::default()
            },
        }));
        t.gateway.enqueue_complete(Ok(()));

        let mut assignment = test_assignment();
        assignment.pipeline_yaml = VALID_YAML.into();

        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(
            &t.ctx,
            "agent-1",
            &assignment,
            &pc,
            CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        let payloads = t.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
        assert!(matches!(
            payloads[0].result.outcome,
            TaskOutcomeKind::Completed
        ));
        assert_eq!(payloads[0].result.metrics.records_written, 42);
    }

    #[tokio::test]
    async fn completion_retry_on_transient_error() {
        let mut ctx = fake_context();
        ctx.ctx.config.completion_retry_delay = std::time::Duration::from_millis(1);
        ctx.executor.enqueue(Ok(TaskExecutionResult {
            outcome: TaskOutcomeKind::Completed,
            metrics: TaskMetrics::default(),
        }));
        // First attempt: transient failure. Second: success.
        ctx.gateway
            .enqueue_complete(Err(AgentError::Controller("unavailable: down".into())));
        ctx.gateway.enqueue_complete(Ok(()));

        let mut assignment = test_assignment();
        assignment.pipeline_yaml = VALID_YAML.into();

        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(
            &ctx.ctx,
            "agent-1",
            &assignment,
            &pc,
            CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        let payloads = ctx.gateway.completed_payloads();
        assert_eq!(payloads.len(), 2); // First attempt failed, second succeeded
    }

    #[tokio::test]
    async fn completion_stops_on_non_retryable_error() {
        let mut ctx = fake_context();
        ctx.ctx.config.completion_retry_delay = std::time::Duration::from_millis(1);
        ctx.executor.enqueue(Ok(TaskExecutionResult {
            outcome: TaskOutcomeKind::Completed,
            metrics: TaskMetrics::default(),
        }));
        ctx.gateway
            .enqueue_complete(Err(AgentError::ControllerNonRetryable("bad token".into())));

        let mut assignment = test_assignment();
        assignment.pipeline_yaml = VALID_YAML.into();

        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(
            &ctx.ctx,
            "agent-1",
            &assignment,
            &pc,
            CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        // Only 1 attempt — no retry on unauthenticated
        let payloads = ctx.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
    }

    #[tokio::test]
    async fn executor_error_produces_failed_outcome() {
        let t = fake_context();
        t.executor
            .enqueue(Err(AgentError::ExecutionFailed(anyhow::anyhow!(
                "engine blew up"
            ))));
        t.gateway.enqueue_complete(Ok(()));

        let mut assignment = test_assignment();
        assignment.pipeline_yaml = VALID_YAML.into();

        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(
            &t.ctx,
            "agent-1",
            &assignment,
            &pc,
            CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        let payloads = t.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
        match &payloads[0].result.outcome {
            TaskOutcomeKind::Failed(info) => {
                assert_eq!(info.code, "INFRASTRUCTURE");
                assert!(!info.retryable);
                assert_eq!(info.commit_state, CommitState::BeforeCommit);
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancelled_during_execution_reports_cancelled() {
        let t = fake_context();
        t.executor.enqueue(Err(AgentError::Cancelled));
        t.gateway.enqueue_complete(Ok(()));

        let mut assignment = test_assignment();
        assignment.pipeline_yaml = VALID_YAML.into();

        let pc = Arc::new(crate::adapter::AtomicProgressCollector::new());
        execute_task(
            &t.ctx,
            "agent-1",
            &assignment,
            &pc,
            CancellationToken::new(),
            &CancellationToken::new(),
        )
        .await;

        let payloads = t.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
        assert!(matches!(
            payloads[0].result.outcome,
            TaskOutcomeKind::Cancelled
        ));
    }
}
