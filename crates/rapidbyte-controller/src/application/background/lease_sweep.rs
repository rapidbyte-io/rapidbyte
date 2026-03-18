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

        let mut run = ctx.find_run(task.run_id()).await?;

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

    #[tokio::test]
    async fn lease_sweep_cancel_requested_gets_cancelled() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, None)
            .await
            .unwrap();
        let _assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        // Request cancel
        {
            let mut run = tc
                .ctx
                .runs
                .find_by_id(&submit.run_id)
                .await
                .unwrap()
                .unwrap();
            run.request_cancel();
            tc.ctx.runs.save(&run).await.unwrap();
        }

        // Advance clock past lease expiry
        tc.clock.advance(chrono::Duration::seconds(301));

        sweep_expired_leases(&tc.ctx).await.unwrap();

        // Run should be Cancelled (not retried)
        let run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.state(), RunState::Cancelled);
    }

    #[tokio::test]
    async fn lease_sweep_multiple_expired_tasks() {
        let tc = fake_context();
        let multi_caps = AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: 4,
        };
        register(&tc.ctx, "agent-1", multi_caps).await.unwrap();

        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let s1 = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, None)
            .await
            .unwrap();
        let s2 = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, None)
            .await
            .unwrap();
        let s3 = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, None)
            .await
            .unwrap();

        let a1 = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();
        let a2 = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();
        let a3 = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        // Advance clock past lease expiry
        tc.clock.advance(chrono::Duration::seconds(301));

        sweep_expired_leases(&tc.ctx).await.unwrap();

        // All 3 tasks should be timed out
        let t1 = tc.ctx.tasks.find_by_id(&a1.task_id).await.unwrap().unwrap();
        let t2 = tc.ctx.tasks.find_by_id(&a2.task_id).await.unwrap().unwrap();
        let t3 = tc.ctx.tasks.find_by_id(&a3.task_id).await.unwrap().unwrap();
        assert_eq!(t1.state(), TaskState::TimedOut);
        assert_eq!(t2.state(), TaskState::TimedOut);
        assert_eq!(t3.state(), TaskState::TimedOut);

        // All 3 runs should be retried
        let r1 = tc.ctx.runs.find_by_id(&s1.run_id).await.unwrap().unwrap();
        let r2 = tc.ctx.runs.find_by_id(&s2.run_id).await.unwrap().unwrap();
        let r3 = tc.ctx.runs.find_by_id(&s3.run_id).await.unwrap().unwrap();
        assert_eq!(r1.state(), RunState::Pending);
        assert_eq!(r2.state(), RunState::Pending);
        assert_eq!(r3.state(), RunState::Pending);
    }
}
