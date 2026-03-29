use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::domain::event::DomainEvent;
use crate::domain::run::{Run, RunError, RunState};
use crate::domain::task::Task;

/// Handle a timed-out task: cancel if requested, retry if possible, otherwise fail.
///
/// # Errors
///
/// Returns a repository or event-bus error on failure.
pub async fn handle_task_timeout(
    ctx: &AppContext,
    task: &Task,
    run: &mut Run,
    error_code: &str,
    error_message: &str,
) -> Result<(), AppError> {
    let now = ctx.clock.now();

    if run.is_cancel_requested() {
        run.cancel()?;
        ctx.store.timeout_and_retry(task, run, None).await?;
        ctx.event_bus
            .publish(DomainEvent::RunCancelled {
                run_id: run.id().to_string(),
            })
            .await?;
    } else if run.can_retry_after_timeout() {
        let new_attempt = run.retry()?;
        let new_task_id = uuid::Uuid::new_v4().to_string();
        let new_task = Task::new(
            new_task_id,
            run.id().to_string(),
            new_attempt,
            task.operation(),
            now,
        );
        ctx.store
            .timeout_and_retry(task, run, Some(&new_task))
            .await?;
        ctx.event_bus
            .publish(DomainEvent::RunStateChanged {
                run_id: run.id().to_string(),
                state: RunState::Pending,
                attempt: new_attempt,
            })
            .await?;
    } else {
        let error = RunError {
            code: error_code.to_string(),
            message: error_message.to_string(),
        };
        run.fail(error.clone())?;
        ctx.store.timeout_and_retry(task, run, None).await?;
        ctx.event_bus
            .publish(DomainEvent::RunFailed {
                run_id: run.id().to_string(),
                error,
            })
            .await?;
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
    use crate::domain::ports::clock::Clock;
    use crate::domain::task::{TaskOperation, TaskState};

    #[tokio::test]
    async fn handle_timeout_cancel_requested() {
        let tc = fake_context();
        let (task_id, run_id, _epoch) = setup_running_task(&tc).await;

        // Set cancel_requested on the run
        {
            let mut run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
            run.request_cancel();
            tc.ctx.runs.save(&run).await.unwrap();
        }

        // Timeout the task
        let mut task = tc.ctx.tasks.find_by_id(&task_id).await.unwrap().unwrap();
        task.timeout().unwrap();

        let mut run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();

        handle_task_timeout(&tc.ctx, &task, &mut run, "LEASE_EXPIRED", "lease expired")
            .await
            .unwrap();

        // Run should be Cancelled
        let run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Cancelled);

        // RunCancelled event published
        let events = tc.event_bus.published_events();
        let cancelled = events
            .iter()
            .find(|e| matches!(e, DomainEvent::RunCancelled { .. }));
        assert!(cancelled.is_some());
    }

    #[tokio::test]
    async fn handle_timeout_retryable() {
        let tc = fake_context();
        let (task_id, run_id, _epoch) = setup_running_task(&tc).await;

        // Task has max_retries=2, so retry is allowed
        let mut task = tc.ctx.tasks.find_by_id(&task_id).await.unwrap().unwrap();
        task.timeout().unwrap();

        let mut run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();

        handle_task_timeout(&tc.ctx, &task, &mut run, "LEASE_EXPIRED", "lease expired")
            .await
            .unwrap();

        // Run should go back to Pending
        let run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Pending);
        assert_eq!(run.current_attempt(), 2);

        // New task should exist
        let tasks = tc.storage.tasks.lock().unwrap();
        let new_task = tasks
            .values()
            .find(|t| t.run_id() == run_id && t.id() != task_id);
        assert!(new_task.is_some());
        assert_eq!(new_task.unwrap().state(), TaskState::Pending);

        // RunStateChanged Pending event
        let events = tc.event_bus.published_events();
        let pending_event = events.iter().find(|e| {
            matches!(
                e,
                DomainEvent::RunStateChanged {
                    state: RunState::Pending,
                    attempt: 2,
                    ..
                }
            )
        });
        assert!(pending_event.is_some());
    }

    #[tokio::test]
    async fn handle_timeout_terminal_failure() {
        let tc = fake_context();

        // Submit with max_retries=0
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
        let submit = submit_pipeline(
            &tc.ctx,
            None,
            yaml.to_string(),
            0,
            None,
            TaskOperation::Sync,
        )
        .await
        .unwrap();
        let assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        let mut task = tc
            .ctx
            .tasks
            .find_by_id(&assignment.task_id)
            .await
            .unwrap()
            .unwrap();
        task.timeout().unwrap();

        let mut run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();

        handle_task_timeout(&tc.ctx, &task, &mut run, "LEASE_EXPIRED", "lease expired")
            .await
            .unwrap();

        // Run should be Failed
        let run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.state(), RunState::Failed);
        assert_eq!(run.error().unwrap().code, "LEASE_EXPIRED");

        // RunFailed event
        let events = tc.event_bus.published_events();
        let failed = events
            .iter()
            .find(|e| matches!(e, DomainEvent::RunFailed { .. }));
        assert!(failed.is_some());
    }

    #[tokio::test]
    async fn handle_timeout_cancel_takes_priority_over_retry() {
        let tc = fake_context();
        let (task_id, run_id, _epoch) = setup_running_task(&tc).await;

        // Run has max_retries=2 AND cancel_requested=true
        {
            let mut run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
            run.request_cancel();
            tc.ctx.runs.save(&run).await.unwrap();
        }

        let mut task = tc.ctx.tasks.find_by_id(&task_id).await.unwrap().unwrap();
        task.timeout().unwrap();

        let mut run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();

        handle_task_timeout(&tc.ctx, &task, &mut run, "LEASE_EXPIRED", "lease expired")
            .await
            .unwrap();

        // Should be Cancelled, NOT Pending (cancel takes priority over retry)
        let run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Cancelled);
    }

    #[tokio::test]
    async fn handle_timeout_error_message_preserved() {
        let tc = fake_context();

        // Submit with max_retries=0 so it goes terminal
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
        let submit = submit_pipeline(
            &tc.ctx,
            None,
            yaml.to_string(),
            0,
            None,
            TaskOperation::Sync,
        )
        .await
        .unwrap();
        let assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        let mut task = tc
            .ctx
            .tasks
            .find_by_id(&assignment.task_id)
            .await
            .unwrap()
            .unwrap();
        task.timeout().unwrap();

        let mut run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();

        handle_task_timeout(
            &tc.ctx,
            &task,
            &mut run,
            "MY_CUSTOM_CODE",
            "the detailed timeout message",
        )
        .await
        .unwrap();

        let run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();
        let error = run.error().unwrap();
        assert_eq!(error.code, "MY_CUSTOM_CODE");
        assert_eq!(error.message, "the detailed timeout message");
    }
}
