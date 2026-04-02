//! Embedded agent — runs in-process alongside the controller.
//!
//! The agent polls for pending tasks, dispatches execution via the
//! [`TaskExecutor`] trait, and reports outcomes back to the controller.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::application::complete::{TaskError, TaskOutcome};
use crate::application::context::AppContext;
use crate::domain::agent::AgentCapabilities;
use crate::domain::run::{CommitState, RunMetrics};
use crate::domain::task::TaskOperation;

// ---------------------------------------------------------------------------
// TaskExecutor trait
// ---------------------------------------------------------------------------

/// Decouples the agent from the engine.  The CLI supplies a real
/// implementation backed by `EngineContext`; tests supply a fake.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Execute a sync pipeline run.
    ///
    /// # Errors
    ///
    /// Returns an error if the run fails.
    async fn execute_sync(
        &self,
        pipeline_yaml: &str,
        cancel: &CancellationToken,
    ) -> Result<RunMetrics>;

    /// Execute check + apply (provision resources).
    ///
    /// # Errors
    ///
    /// Returns an error if the check/apply fails.
    async fn execute_check_apply(&self, pipeline_yaml: &str) -> Result<()>;

    /// Execute teardown (clean up resources).
    ///
    /// # Errors
    ///
    /// Returns an error if the teardown fails.
    async fn execute_teardown(&self, pipeline_yaml: &str, reason: &str) -> Result<()>;

    /// Execute data quality assertions.
    ///
    /// # Errors
    ///
    /// Returns an error if any assertion fails.
    async fn execute_assert(&self, pipeline_yaml: &str) -> Result<()>;
}

// ---------------------------------------------------------------------------
// EmbeddedAgentConfig
// ---------------------------------------------------------------------------

/// Configuration for the embedded agent loop.
pub struct EmbeddedAgentConfig {
    /// How often to poll when no task is available.
    pub poll_interval: Duration,
    /// Maximum number of concurrent task executions.
    pub max_concurrent_tasks: u32,
}

impl Default for EmbeddedAgentConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            max_concurrent_tasks: 4,
        }
    }
}

// ---------------------------------------------------------------------------
// run_embedded_agent
// ---------------------------------------------------------------------------

/// Run the embedded agent loop until `cancel_token` is cancelled.
///
/// # Errors
///
/// Returns an error if registration or deregistration fails fatally.
pub async fn run_embedded_agent(
    app_ctx: Arc<AppContext>,
    executor: Arc<dyn TaskExecutor>,
    config: EmbeddedAgentConfig,
    cancel_token: CancellationToken,
) -> Result<()> {
    anyhow::ensure!(
        config.max_concurrent_tasks > 0,
        "max_concurrent_tasks must be > 0"
    );

    let agent_id = format!("embedded-{}", uuid::Uuid::new_v4());

    // 2. Register with the controller.
    // Only advertise operations the executor can actually complete successfully.
    // CheckApply and Assert are not yet implemented in the engine — advertising
    // them would cause the scheduler to assign work guaranteed to fail/retry.
    let caps = AgentCapabilities {
        plugins: vec![],
        max_concurrent_tasks: config.max_concurrent_tasks,
        supported_operations: vec![TaskOperation::Sync, TaskOperation::Teardown],
    };
    crate::application::register::register(&app_ctx, &agent_id, caps).await?;
    info!(agent_id = %agent_id, "embedded agent registered");

    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_tasks as usize));

    loop {
        if cancel_token.is_cancelled() {
            break;
        }

        // Try to acquire a slot; if at capacity skip polling until a slot frees.
        let Ok(permit) = semaphore.clone().try_acquire_owned() else {
            // At capacity — wait briefly or until cancelled.
            tokio::select! {
                () = cancel_token.cancelled() => break,
                () = tokio::time::sleep(config.poll_interval) => continue,
            }
        };

        // Poll for a task.
        let assignment = match crate::application::poll::poll_task(&app_ctx, &agent_id).await {
            Ok(Some(a)) => a,
            Ok(None) => {
                // No task — drop the permit and sleep.
                drop(permit);
                tokio::select! {
                    () = cancel_token.cancelled() => break,
                    () = tokio::time::sleep(config.poll_interval) => continue,
                }
            }
            Err(e) => {
                warn!(error = %e, "poll_task error, retrying");
                drop(permit);
                tokio::select! {
                    () = cancel_token.cancelled() => break,
                    () = tokio::time::sleep(config.poll_interval) => continue,
                }
            }
        };

        // Compute heartbeat interval here so config.poll_interval is not moved
        // into the spawned closure.
        let heartbeat_interval = config.poll_interval * 2;

        // Spawn execution + heartbeat for this assignment.
        let ctx = Arc::clone(&app_ctx);
        let exec = Arc::clone(&executor);
        // Per-task child token: cancelling it stops only this task, not the agent.
        // The agent shutdown token is the parent — when it fires, all children
        // (including this one) are also cancelled.
        let task_cancel = cancel_token.child_token();
        let agent_id_owned = agent_id.clone();

        tokio::spawn(async move {
            let _permit = permit;
            run_task_with_heartbeat(
                ctx,
                exec,
                agent_id_owned,
                assignment,
                task_cancel,
                heartbeat_interval,
            )
            .await;
        });
    }

    // 5. Drain in-flight tasks by waiting for the semaphore to reach full
    //    capacity (all permits returned), with a timeout to avoid hanging.
    let drain_timeout = Duration::from_secs(30);
    let drained = tokio::time::timeout(
        drain_timeout,
        semaphore.acquire_many(config.max_concurrent_tasks),
    )
    .await
    .is_ok();

    if drained {
        info!("embedded agent drained all tasks");
    } else {
        warn!("embedded agent drain timed out after {drain_timeout:?}, tasks still in-flight");
    }

    // 6. Deregister only if all tasks drained successfully.
    //    If tasks are still running, deregistration would force-timeout them
    //    and potentially trigger retries while original execution continues,
    //    causing duplicate side effects. Let the agent_reaper handle cleanup
    //    after heartbeats stop.
    if drained {
        if let Err(e) = crate::application::register::deregister(&app_ctx, &agent_id).await {
            warn!(agent_id = %agent_id, error = %e, "deregister failed");
        } else {
            info!(agent_id = %agent_id, "embedded agent deregistered");
        }
    } else {
        warn!(
            agent_id = %agent_id,
            "skipping deregister — tasks still in-flight; agent_reaper will clean up after heartbeats stop"
        );
    }

    Ok(())
}

/// Execute a single task alongside a heartbeat loop, then report the outcome.
async fn run_task_with_heartbeat(
    ctx: Arc<AppContext>,
    exec: Arc<dyn TaskExecutor>,
    agent_id: String,
    assignment: crate::application::poll::TaskAssignment,
    task_cancel: CancellationToken,
    heartbeat_interval: Duration,
) {
    let task_id = assignment.task_id.clone();
    let lease_epoch = assignment.lease_epoch;

    // Child token cancelled when this task finishes (success or failure).
    let task_done = CancellationToken::new();

    // Spawn heartbeat loop.
    let hb_ctx = Arc::clone(&ctx);
    let hb_agent_id = agent_id.clone();
    let hb_task_id = task_id.clone();
    let hb_done = task_done.clone();
    let hb_cancel = task_cancel.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = hb_done.cancelled() => break,
                () = hb_cancel.cancelled() => {
                    // Shutdown/cancel requested — stop renewing the lease so
                    // the controller's lease_sweep can time out and handle the task.
                    tracing::debug!(task_id = %hb_task_id, "heartbeat loop stopping on cancel");
                    break;
                },
                () = tokio::time::sleep(heartbeat_interval) => {}
            }
            let input = crate::application::heartbeat::TaskHeartbeatInput {
                task_id: hb_task_id.clone(),
                lease_epoch,
                progress_message: None,
                progress_pct: None,
            };
            let directives =
                crate::application::heartbeat::heartbeat(&hb_ctx, &hb_agent_id, vec![input]).await;
            if let Ok(directives) = directives {
                if directives.iter().any(|d| d.cancel_requested) {
                    tracing::info!(task_id = %hb_task_id, "cancel requested via heartbeat");
                    hb_cancel.cancel();
                    break;
                }
            }
        }
    });

    let outcome = dispatch(&exec, &assignment, &task_cancel).await;

    // Signal the heartbeat loop to stop.
    task_done.cancel();

    let report_outcome = match outcome {
        Ok(task_outcome) => task_outcome,
        Err(e) => {
            // If the task was cancelled (via heartbeat or shutdown), report
            // Cancelled — not Failed — so the run transitions correctly.
            if task_cancel.is_cancelled() {
                info!(task_id = %task_id, "task cancelled");
                TaskOutcome::Cancelled
            } else {
                error!(task_id = %task_id, error = %e, "task execution failed");
                TaskOutcome::Failed {
                    error: TaskError {
                        code: "EXECUTION_ERROR".to_string(),
                        message: e.to_string(),
                        retryable: true,
                        commit_state: CommitState::BeforeCommit,
                    },
                }
            }
        }
    };

    if let Err(e) = crate::application::complete::complete_task(
        &ctx,
        &agent_id,
        &task_id,
        lease_epoch,
        report_outcome,
    )
    .await
    {
        error!(task_id = %task_id, error = %e, "complete_task failed");
    }
}

/// Dispatch a task to the appropriate executor method and wrap the result in
/// a `TaskOutcome`.
async fn dispatch(
    executor: &Arc<dyn TaskExecutor>,
    assignment: &crate::application::poll::TaskAssignment,
    cancel: &CancellationToken,
) -> Result<TaskOutcome> {
    // Check cancellation before starting any operation.
    if cancel.is_cancelled() {
        return Ok(TaskOutcome::Cancelled);
    }

    match assignment.operation {
        TaskOperation::Sync => {
            let metrics = executor
                .execute_sync(&assignment.pipeline_yaml, cancel)
                .await?;
            Ok(TaskOutcome::Completed { metrics })
        }
        TaskOperation::CheckApply => {
            // Run in a select against the cancel token so cancellation
            // is observed even for operations that don't take a token parameter.
            tokio::select! {
                result = executor.execute_check_apply(&assignment.pipeline_yaml) => {
                    result?;
                }
                () = cancel.cancelled() => {
                    return Ok(TaskOutcome::Cancelled);
                }
            }
            Ok(TaskOutcome::Completed {
                metrics: empty_metrics(),
            })
        }
        TaskOperation::Teardown => {
            let reason = assignment
                .metadata
                .as_ref()
                .and_then(|m| m.get("reason"))
                .and_then(|v| v.as_str())
                .unwrap_or("api");
            tokio::select! {
                result = executor.execute_teardown(&assignment.pipeline_yaml, reason) => {
                    result?;
                }
                () = cancel.cancelled() => {
                    return Ok(TaskOutcome::Cancelled);
                }
            }
            Ok(TaskOutcome::Completed {
                metrics: empty_metrics(),
            })
        }
        TaskOperation::Assert => {
            tokio::select! {
                result = executor.execute_assert(&assignment.pipeline_yaml) => {
                    result?;
                }
                () = cancel.cancelled() => {
                    return Ok(TaskOutcome::Cancelled);
                }
            }
            Ok(TaskOutcome::Completed {
                metrics: empty_metrics(),
            })
        }
    }
}

fn empty_metrics() -> RunMetrics {
    RunMetrics {
        rows_read: 0,
        rows_written: 0,
        bytes_read: 0,
        bytes_written: 0,
        duration_ms: 0,
    }
}

// ---------------------------------------------------------------------------
// ImmediateExecutor (test-only)
// ---------------------------------------------------------------------------

/// Fake executor that completes immediately with dummy metrics.
#[cfg(test)]
pub struct ImmediateExecutor;

#[cfg(test)]
#[async_trait]
impl TaskExecutor for ImmediateExecutor {
    async fn execute_sync(&self, _yaml: &str, _cancel: &CancellationToken) -> Result<RunMetrics> {
        Ok(RunMetrics {
            rows_read: 100,
            rows_written: 100,
            bytes_read: 1024,
            bytes_written: 1024,
            duration_ms: 50,
        })
    }

    async fn execute_check_apply(&self, _yaml: &str) -> Result<()> {
        Ok(())
    }

    async fn execute_teardown(&self, _yaml: &str, _reason: &str) -> Result<()> {
        Ok(())
    }

    async fn execute_assert(&self, _yaml: &str) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RecordingExecutor (test-only)
// ---------------------------------------------------------------------------

/// Executor that records which methods were called and with what arguments.
/// Returns Ok for sync and teardown; returns Err for `check_apply` and assert,
/// matching the behaviour of the real `EngineTaskExecutor` for unsupported ops.
#[cfg(test)]
pub struct RecordingExecutor {
    calls: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
}

#[cfg(test)]
type CallLog = std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>;

#[cfg(test)]
impl RecordingExecutor {
    #[must_use]
    pub fn new() -> (Self, CallLog) {
        let calls: CallLog = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        (
            Self {
                calls: calls.clone(),
            },
            calls,
        )
    }
}

#[cfg(test)]
#[async_trait]
impl TaskExecutor for RecordingExecutor {
    async fn execute_sync(&self, yaml: &str, _cancel: &CancellationToken) -> Result<RunMetrics> {
        self.calls
            .lock()
            .unwrap()
            .push(("sync".into(), yaml.into()));
        Ok(RunMetrics {
            rows_read: 1,
            rows_written: 1,
            bytes_read: 1,
            bytes_written: 1,
            duration_ms: 1,
        })
    }

    async fn execute_check_apply(&self, yaml: &str) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(("check_apply".into(), yaml.into()));
        anyhow::bail!("check_apply not implemented")
    }

    async fn execute_teardown(&self, yaml: &str, reason: &str) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(("teardown".into(), format!("{yaml}|reason={reason}")));
        Ok(())
    }

    async fn execute_assert(&self, yaml: &str) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(("assert".into(), yaml.into()));
        anyhow::bail!("assert not implemented")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::query;
    use crate::application::submit::submit_pipeline;
    use crate::application::testing::fake_context;
    use crate::domain::run::RunState;
    use crate::domain::task::TaskOperation;

    #[tokio::test]
    async fn embedded_agent_executes_sync_task() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);

        // 1. Submit a pipeline run.
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let result = submit_pipeline(
            &ctx,
            None,
            yaml.to_string(),
            0,
            None,
            TaskOperation::Sync,
            None,
        )
        .await
        .unwrap();
        let run_id = result.run_id;

        // 2. Verify run is Pending.
        let run = query::get_run(&ctx, &run_id).await.unwrap();
        assert_eq!(run.state(), RunState::Pending);

        // 3. Start embedded agent.
        let executor: Arc<dyn TaskExecutor> = Arc::new(ImmediateExecutor);
        let cancel = CancellationToken::new();
        let agent_cancel = cancel.clone();
        let agent_ctx = Arc::clone(&ctx);

        let handle = tokio::spawn(async move {
            run_embedded_agent(
                agent_ctx,
                executor,
                EmbeddedAgentConfig {
                    poll_interval: Duration::from_millis(50),
                    max_concurrent_tasks: 1,
                },
                agent_cancel,
            )
            .await
        });

        // 4. Wait for the run to complete (with timeout).
        let check_ctx = Arc::clone(&ctx);
        let check_run_id = run_id.clone();
        let timeout = tokio::time::timeout(Duration::from_secs(5), async move {
            loop {
                let run = query::get_run(&check_ctx, &check_run_id).await.unwrap();
                if run.state() == RunState::Completed {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(timeout.is_ok(), "run did not complete within 5 seconds");

        // 5. Verify run is Completed with metrics.
        let run = query::get_run(&ctx, &run_id).await.unwrap();
        assert_eq!(run.state(), RunState::Completed);
        assert!(run.metrics().is_some());
        let m = run.metrics().unwrap();
        assert_eq!(m.rows_read, 100);
        assert_eq!(m.rows_written, 100);

        // 6. Shut down agent.
        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn embedded_agent_deregisters_on_cancel() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);

        let executor: Arc<dyn TaskExecutor> = Arc::new(ImmediateExecutor);
        let cancel = CancellationToken::new();
        let agent_cancel = cancel.clone();
        let agent_ctx = Arc::clone(&ctx);

        let handle = tokio::spawn(async move {
            run_embedded_agent(
                agent_ctx,
                executor,
                EmbeddedAgentConfig {
                    poll_interval: Duration::from_millis(50),
                    max_concurrent_tasks: 1,
                },
                agent_cancel,
            )
            .await
        });

        // Give the agent a moment to register.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel the agent.
        cancel.cancel();
        handle.await.unwrap().unwrap();

        // There should be no agents left in the repository.
        let agents = ctx.agents.find_stale(
            chrono::Duration::seconds(0),
            chrono::Utc::now() + chrono::Duration::hours(1),
        );
        let stale = agents.await.unwrap();
        assert!(
            stale.is_empty(),
            "expected no agents after deregister, got {count}",
            count = stale.len()
        );
    }

    /// Helper: start an agent with a `RecordingExecutor`, wait for the run to
    /// leave the `Pending` state (or reach a terminal state), then stop the agent.
    async fn run_agent_until_terminal(
        ctx: Arc<crate::application::context::AppContext>,
        run_id: &str,
    ) -> (CallLog, RunState) {
        let (executor, calls) = RecordingExecutor::new();
        let executor: Arc<dyn TaskExecutor> = Arc::new(executor);
        let cancel = CancellationToken::new();
        let agent_cancel = cancel.clone();
        let agent_ctx = Arc::clone(&ctx);

        let handle = tokio::spawn(async move {
            run_embedded_agent(
                agent_ctx,
                executor,
                EmbeddedAgentConfig {
                    poll_interval: Duration::from_millis(20),
                    max_concurrent_tasks: 1,
                },
                agent_cancel,
            )
            .await
        });

        // Wait for the run to leave Pending (i.e. the agent picked it up and
        // completed/failed it), with a 5-second guard.
        let check_ctx = Arc::clone(&ctx);
        let check_run_id = run_id.to_string();
        let final_state = tokio::time::timeout(Duration::from_secs(5), async move {
            loop {
                let run = query::get_run(&check_ctx, &check_run_id).await.unwrap();
                match run.state() {
                    RunState::Pending | RunState::Running => {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    terminal => return terminal,
                }
            }
        })
        .await
        .expect("run did not reach a terminal state within 5 seconds");

        cancel.cancel();
        handle.await.unwrap().unwrap();

        (calls, final_state)
    }

    #[tokio::test]
    async fn embedded_agent_executes_teardown_with_reason() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);

        // Submit a teardown task with a reason in the metadata.
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let metadata = serde_json::json!({ "reason": "pipeline_deleted" });
        let result = submit_pipeline(
            &ctx,
            None,
            yaml.to_string(),
            0,
            None,
            TaskOperation::Teardown,
            Some(metadata),
        )
        .await
        .unwrap();

        let (calls, final_state) = run_agent_until_terminal(Arc::clone(&ctx), &result.run_id).await;

        // Teardown with a successful executor should complete.
        assert_eq!(final_state, RunState::Completed);

        // The executor should have been called with `reason=pipeline_deleted`.
        let recorded = calls.lock().unwrap();
        assert!(
            recorded
                .iter()
                .any(|(method, args)| method == "teardown"
                    && args.contains("reason=pipeline_deleted")),
            "expected teardown call with reason, got: {recorded:?}"
        );
    }

    // Note: check_apply and assert tests were removed because the service layer
    // now returns 501 NotImplemented for those operations, so tasks are never
    // created and the agent never sees them. The embedded agent only advertises
    // Sync and Teardown operations.
}
