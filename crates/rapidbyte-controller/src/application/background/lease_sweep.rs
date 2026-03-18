use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::application::timeout::handle_task_timeout;

/// Sweep all tasks whose leases have expired and either retry or fail their runs.
///
/// # Errors
///
/// Returns a repository / event-bus error on failure.
pub async fn sweep_expired_leases(ctx: &AppContext) -> Result<(), AppError> {
    let now = ctx.clock.now();
    let expired_tasks = ctx.tasks.find_expired_leases(now).await?;

    for mut task in expired_tasks {
        task.timeout()?;

        let mut run =
            ctx.runs
                .find_by_id(task.run_id())
                .await?
                .ok_or_else(|| AppError::NotFound {
                    entity: "Run",
                    id: task.run_id().to_string(),
                })?;

        handle_task_timeout(
            ctx,
            &task,
            &mut run,
            "LEASE_EXPIRED",
            "Task lease expired without heartbeat",
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::poll::poll_task;
    use crate::application::register::register;
    use crate::application::submit::submit_pipeline;
    use crate::application::testing::fake_context;
    use crate::domain::agent::AgentCapabilities;
    use crate::domain::event::DomainEvent;
    use crate::domain::run::RunState;
    use crate::domain::task::TaskState;

    fn caps() -> AgentCapabilities {
        AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: 4,
        }
    }

    #[tokio::test]
    async fn expired_lease_retried() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, None)
            .await
            .unwrap();
        let assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        // Advance clock past lease expiry (default_lease_duration = 300s)
        tc.clock.advance(chrono::Duration::seconds(301));

        sweep_expired_leases(&tc.ctx).await.unwrap();

        // Task should be timed out
        let task = tc
            .ctx
            .tasks
            .find_by_id(&assignment.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.state(), TaskState::TimedOut);

        // Run should be retried (Pending)
        let run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.state(), RunState::Pending);
        assert_eq!(run.current_attempt(), 2);

        // New task should exist
        let tasks = tc.storage.tasks.lock().unwrap();
        let new_task = tasks
            .values()
            .find(|t| t.run_id() == submit.run_id && t.id() != assignment.task_id);
        assert!(new_task.is_some());
    }

    #[tokio::test]
    async fn expired_lease_no_retries() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();
        let _assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        tc.clock.advance(chrono::Duration::seconds(301));

        sweep_expired_leases(&tc.ctx).await.unwrap();

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
    async fn no_expired_leases_is_noop() {
        let tc = fake_context();
        // No tasks at all
        sweep_expired_leases(&tc.ctx).await.unwrap();

        let events = tc.event_bus.published_events();
        assert!(events.is_empty());
    }
}
