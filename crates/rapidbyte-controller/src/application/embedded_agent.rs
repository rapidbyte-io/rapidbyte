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
    // 0. Validate configuration.
    anyhow::ensure!(
        config.max_concurrent_tasks > 0,
        "max_concurrent_tasks must be > 0"
    );

    // 1. Generate stable agent id for this process lifetime.
    let agent_id = format!("embedded-{}", uuid::Uuid::new_v4());

    // 2. Register with the controller.
    let caps = AgentCapabilities {
        plugins: vec![],
        max_concurrent_tasks: config.max_concurrent_tasks,
    };
    crate::application::register::register(&app_ctx, &agent_id, caps).await?;
    info!(agent_id = %agent_id, "embedded agent registered");

    // 3. Semaphore enforces max_concurrent_tasks.
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_tasks as usize));

    // 4. Main poll loop.
    loop {
        // Exit immediately if cancelled.
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

        // Spawn execution.
        let ctx = Arc::clone(&app_ctx);
        let exec = Arc::clone(&executor);
        let task_cancel = cancel_token.clone();
        let agent_id_owned = agent_id.clone();

        tokio::spawn(async move {
            // The permit is held for the lifetime of the spawned task.
            let _permit = permit;

            let task_id = assignment.task_id.clone();
            let lease_epoch = assignment.lease_epoch;

            let outcome = dispatch(&exec, &assignment, &task_cancel).await;

            let report_outcome = match outcome {
                Ok(task_outcome) => task_outcome,
                Err(e) => {
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
            };

            if let Err(e) = crate::application::complete::complete_task(
                &ctx,
                &agent_id_owned,
                &task_id,
                lease_epoch,
                report_outcome,
            )
            .await
            {
                error!(
                    task_id = %task_id,
                    error = %e,
                    "complete_task failed"
                );
            }
        });
    }

    // 5. Drain in-flight tasks by waiting for the semaphore to reach full
    //    capacity (all permits returned), with a timeout to avoid hanging.
    let drain_timeout = Duration::from_secs(30);
    if tokio::time::timeout(
        drain_timeout,
        semaphore.acquire_many(config.max_concurrent_tasks),
    )
    .await
    .is_ok()
    {
        info!("embedded agent drained all tasks");
    } else {
        warn!("embedded agent drain timed out after {drain_timeout:?}");
    }

    // 6. Deregister.
    if let Err(e) = crate::application::register::deregister(&app_ctx, &agent_id).await {
        warn!(agent_id = %agent_id, error = %e, "deregister failed");
    } else {
        info!(agent_id = %agent_id, "embedded agent deregistered");
    }

    Ok(())
}

/// Dispatch a task to the appropriate executor method and wrap the result in
/// a `TaskOutcome`.
async fn dispatch(
    executor: &Arc<dyn TaskExecutor>,
    assignment: &crate::application::poll::TaskAssignment,
    cancel: &CancellationToken,
) -> Result<TaskOutcome> {
    match assignment.operation {
        TaskOperation::Sync => {
            let metrics = executor
                .execute_sync(&assignment.pipeline_yaml, cancel)
                .await?;
            Ok(TaskOutcome::Completed { metrics })
        }
        TaskOperation::CheckApply => {
            executor
                .execute_check_apply(&assignment.pipeline_yaml)
                .await?;
            Ok(TaskOutcome::Completed {
                metrics: empty_metrics(),
            })
        }
        TaskOperation::Teardown => {
            // TODO: Pass the actual teardown reason once Run entity supports metadata.
            // The reason is logged by the service layer at submission time.
            executor
                .execute_teardown(&assignment.pipeline_yaml, "api")
                .await?;
            Ok(TaskOutcome::Completed {
                metrics: empty_metrics(),
            })
        }
        TaskOperation::Assert => {
            executor.execute_assert(&assignment.pipeline_yaml).await?;
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
        let result = submit_pipeline(&ctx, None, yaml.to_string(), 0, None, TaskOperation::Sync)
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
}
