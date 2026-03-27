use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::domain::event::DomainEvent;
use crate::domain::run::{CommitState, RunError, RunMetrics, RunState};
use crate::domain::task::Task;

pub struct TaskError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub commit_state: CommitState,
}

pub enum TaskOutcome {
    Completed { metrics: RunMetrics },
    Failed { error: TaskError },
    Cancelled,
}

/// Report the outcome of a task execution.
///
/// Depending on the outcome the run is completed, retried, failed, or
/// cancelled. Retry decisions use the domain model's retry policy.
///
/// # Errors
///
/// Returns `AppError::NotFound` if the task or run does not exist,
/// `AppError::Domain` on lease validation or state-transition failures, or a
/// repository / event-bus error.
pub async fn complete_task(
    ctx: &AppContext,
    agent_id: &str,
    task_id: &str,
    lease_epoch: u64,
    outcome: TaskOutcome,
) -> Result<(), AppError> {
    // 1. Find task, validate lease
    let mut task = ctx.find_task(task_id).await?;
    let now = ctx.clock.now();
    task.validate_lease(agent_id, lease_epoch, now)?;

    // 2. Find run
    let mut run = ctx.find_run(task.run_id()).await?;

    let now = ctx.clock.now();

    match outcome {
        TaskOutcome::Completed { metrics } => {
            task.complete()?;
            run.complete(metrics.clone())?;
            ctx.store.complete_run(&task, &run).await?;
            ctx.event_bus
                .publish(DomainEvent::RunCompleted {
                    run_id: run.id().to_string(),
                    metrics,
                })
                .await?;
        }
        TaskOutcome::Failed { error } => {
            if run.can_retry_after_error(error.retryable, &error.commit_state)
                && !run.is_cancel_requested()
            {
                // Retryable (and not cancelled)
                task.fail()?;
                let new_attempt = run.retry()?;
                let new_task_id = uuid::Uuid::new_v4().to_string();
                let new_task = Task::new(
                    new_task_id,
                    run.id().to_string(),
                    new_attempt,
                    task.operation(),
                    now,
                );
                ctx.store.fail_and_retry(&task, &run, &new_task).await?;
                ctx.event_bus
                    .publish(DomainEvent::RunStateChanged {
                        run_id: run.id().to_string(),
                        state: RunState::Pending,
                        attempt: new_attempt,
                    })
                    .await?;
            } else {
                // Terminal failure
                task.fail()?;
                let run_error = RunError {
                    code: error.code,
                    message: error.message,
                };
                run.fail(run_error.clone())?;
                ctx.store.fail_run(&task, &run).await?;
                ctx.event_bus
                    .publish(DomainEvent::RunFailed {
                        run_id: run.id().to_string(),
                        error: run_error,
                    })
                    .await?;
            }
        }
        TaskOutcome::Cancelled => {
            task.cancel()?;
            run.cancel()?;
            ctx.store.cancel_run(&task, &run).await?;
            ctx.event_bus
                .publish(DomainEvent::RunCancelled {
                    run_id: run.id().to_string(),
                })
                .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::poll::poll_task;
    use crate::application::submit::submit_pipeline;
    use crate::application::testing::{fake_context, setup_running_task};
    use crate::domain::agent::{Agent, AgentCapabilities};
    use crate::domain::event::DomainEvent;
    use crate::domain::ports::clock::Clock;
    use crate::domain::run::{RunMetrics, RunState};
    use crate::domain::task::TaskState;

    fn sample_metrics() -> RunMetrics {
        RunMetrics {
            rows_read: 100,
            rows_written: 50,
            bytes_read: 1024,
            bytes_written: 512,
            duration_ms: 5000,
        }
    }

    #[tokio::test]
    async fn completed_task_and_run() {
        let tc = fake_context();
        let (task_id, run_id, epoch) = setup_running_task(&tc).await;

        complete_task(
            &tc.ctx,
            "agent-1",
            &task_id,
            epoch,
            TaskOutcome::Completed {
                metrics: sample_metrics(),
            },
        )
        .await
        .unwrap();

        // Task should be completed
        let task = tc.ctx.tasks.find_by_id(&task_id).await.unwrap().unwrap();
        assert_eq!(task.state(), TaskState::Completed);

        // Run should be completed with metrics
        let run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Completed);
        assert!(run.metrics().is_some());
        assert_eq!(run.metrics().unwrap().rows_read, 100);

        // Event published
        let events = tc.event_bus.published_events();
        let completed = events
            .iter()
            .find(|e| matches!(e, DomainEvent::RunCompleted { .. }));
        assert!(completed.is_some());
    }

    #[tokio::test]
    async fn failed_retryable_creates_new_task() {
        let tc = fake_context();
        let (task_id, run_id, epoch) = setup_running_task(&tc).await;

        complete_task(
            &tc.ctx,
            "agent-1",
            &task_id,
            epoch,
            TaskOutcome::Failed {
                error: TaskError {
                    code: "E001".to_string(),
                    message: "transient failure".to_string(),
                    retryable: true,
                    commit_state: CommitState::BeforeCommit,
                },
            },
        )
        .await
        .unwrap();

        // Original task should be failed
        let task = tc.ctx.tasks.find_by_id(&task_id).await.unwrap().unwrap();
        assert_eq!(task.state(), TaskState::Failed);

        // Run should be back to Pending
        let run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Pending);
        assert_eq!(run.current_attempt(), 2);

        // New task should exist
        let tasks = tc.storage.tasks.lock().unwrap();
        let new_task = tasks
            .values()
            .find(|t| t.run_id() == run_id && t.id() != task_id)
            .unwrap();
        assert_eq!(new_task.attempt(), 2);
        assert_eq!(new_task.state(), TaskState::Pending);

        // Event published: RunStateChanged to Pending
        let events = tc.event_bus.published_events();
        let retry_event = events.iter().find(|e| {
            matches!(
                e,
                DomainEvent::RunStateChanged {
                    state: RunState::Pending,
                    attempt: 2,
                    ..
                }
            )
        });
        assert!(retry_event.is_some());
    }

    #[tokio::test]
    async fn failed_not_retryable_terminal() {
        let tc = fake_context();
        let (task_id, run_id, epoch) = setup_running_task(&tc).await;

        complete_task(
            &tc.ctx,
            "agent-1",
            &task_id,
            epoch,
            TaskOutcome::Failed {
                error: TaskError {
                    code: "E002".to_string(),
                    message: "permanent failure".to_string(),
                    retryable: false,
                    commit_state: CommitState::BeforeCommit,
                },
            },
        )
        .await
        .unwrap();

        let task = tc.ctx.tasks.find_by_id(&task_id).await.unwrap().unwrap();
        assert_eq!(task.state(), TaskState::Failed);

        let run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Failed);
        assert_eq!(run.error().unwrap().code, "E002");

        let events = tc.event_bus.published_events();
        let failed = events
            .iter()
            .find(|e| matches!(e, DomainEvent::RunFailed { .. }));
        assert!(failed.is_some());
    }

    #[tokio::test]
    async fn failed_retries_exhausted() {
        let tc = fake_context();
        // Submit with max_retries=0, so no retries allowed
        let now = tc.clock.now();
        let agent = Agent::new(
            "agent-1".to_string(),
            AgentCapabilities {
                plugins: vec![],
                max_concurrent_tasks: 4,
            },
            now,
        );
        tc.ctx.agents.save(&agent).await.unwrap();

        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();
        let assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        complete_task(
            &tc.ctx,
            "agent-1",
            &assignment.task_id,
            assignment.lease_epoch,
            TaskOutcome::Failed {
                error: TaskError {
                    code: "E003".to_string(),
                    message: "failure with no retries".to_string(),
                    retryable: true,
                    commit_state: CommitState::BeforeCommit,
                },
            },
        )
        .await
        .unwrap();

        // Even though retryable, retries are exhausted
        let run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.state(), RunState::Failed);
    }

    #[tokio::test]
    async fn failed_after_commit_not_retryable() {
        let tc = fake_context();
        let (task_id, run_id, epoch) = setup_running_task(&tc).await;

        complete_task(
            &tc.ctx,
            "agent-1",
            &task_id,
            epoch,
            TaskOutcome::Failed {
                error: TaskError {
                    code: "E004".to_string(),
                    message: "post-commit".to_string(),
                    retryable: true,
                    commit_state: CommitState::AfterCommitConfirmed,
                },
            },
        )
        .await
        .unwrap();

        let run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Failed);
    }

    #[tokio::test]
    async fn cancelled_outcome() {
        let tc = fake_context();
        let (task_id, run_id, epoch) = setup_running_task(&tc).await;

        complete_task(&tc.ctx, "agent-1", &task_id, epoch, TaskOutcome::Cancelled)
            .await
            .unwrap();

        let task = tc.ctx.tasks.find_by_id(&task_id).await.unwrap().unwrap();
        assert_eq!(task.state(), TaskState::Cancelled);

        let run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Cancelled);

        let events = tc.event_bus.published_events();
        let cancelled = events
            .iter()
            .find(|e| matches!(e, DomainEvent::RunCancelled { .. }));
        assert!(cancelled.is_some());
    }

    #[tokio::test]
    async fn stale_lease_returns_error() {
        let tc = fake_context();
        let (task_id, _run_id, epoch) = setup_running_task(&tc).await;

        let result = complete_task(
            &tc.ctx,
            "agent-1",
            &task_id,
            epoch + 999,
            TaskOutcome::Completed {
                metrics: sample_metrics(),
            },
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn cancel_requested_prevents_retry_on_failure() {
        let tc = fake_context();
        let (task_id, run_id, epoch) = setup_running_task(&tc).await;

        // Request cancellation while task is running
        crate::application::cancel::cancel_run(&tc.ctx, &run_id)
            .await
            .unwrap();

        // Agent reports retryable failure
        complete_task(
            &tc.ctx,
            "agent-1",
            &task_id,
            epoch,
            TaskOutcome::Failed {
                error: TaskError {
                    code: "TRANSIENT".to_string(),
                    message: "temporary failure".to_string(),
                    retryable: true,
                    commit_state: crate::domain::run::CommitState::BeforeCommit,
                },
            },
        )
        .await
        .unwrap();

        // Run should be Failed (not retried), because cancellation was requested
        let run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Failed);
        // Should NOT have been retried (attempt should still be 1)
        assert_eq!(run.current_attempt(), 1);
    }

    #[tokio::test]
    async fn complete_unknown_task_not_found() {
        let tc = fake_context();
        let result = complete_task(
            &tc.ctx,
            "agent-1",
            "nonexistent-task",
            1,
            TaskOutcome::Completed {
                metrics: sample_metrics(),
            },
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AppError::NotFound { .. }));
    }

    #[tokio::test]
    async fn complete_unknown_agent_mismatch() {
        let tc = fake_context();
        let (task_id, _run_id, epoch) = setup_running_task(&tc).await;

        // Complete as "agent-2" when task is assigned to "agent-1"
        let result = complete_task(
            &tc.ctx,
            "agent-2",
            &task_id,
            epoch,
            TaskOutcome::Completed {
                metrics: sample_metrics(),
            },
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn complete_expired_lease_rejected() {
        let tc = fake_context();
        let (task_id, _run_id, epoch) = setup_running_task(&tc).await;

        // Advance clock past lease expiry (300s)
        tc.clock.advance(chrono::Duration::seconds(301));

        let result = complete_task(
            &tc.ctx,
            "agent-1",
            &task_id,
            epoch,
            TaskOutcome::Completed {
                metrics: sample_metrics(),
            },
        )
        .await;

        assert!(result.is_err());
    }
}
