use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::application::timeout::handle_task_timeout;
use crate::domain::agent::{Agent, AgentCapabilities};

/// Register (or re-register) an agent.
///
/// # Errors
///
/// Returns a repository error on failure.
pub async fn register(
    ctx: &AppContext,
    agent_id: &str,
    capabilities: AgentCapabilities,
) -> Result<(), AppError> {
    let now = ctx.clock.now();
    let agent = Agent::new(agent_id.to_string(), capabilities, now);
    ctx.agents.save(&agent).await?;
    Ok(())
}

/// Deregister an agent and handle any running tasks.
///
/// Running tasks are timed out and their runs are either retried or failed
/// depending on the retry policy.
///
/// # Errors
///
/// Returns `AppError::NotFound` if the agent does not exist, or a repository /
/// event-bus error on failure.
pub async fn deregister(ctx: &AppContext, agent_id: &str) -> Result<(), AppError> {
    // 1. Verify agent exists before proceeding
    ctx.find_agent(agent_id).await?;

    // 2. Find running tasks for this agent
    let running_tasks = ctx.tasks.find_running_by_agent_id(agent_id).await?;

    // 3. Handle each running task
    for mut task in running_tasks {
        task.timeout()?;

        let mut run = ctx.find_run(task.run_id()).await?;

        handle_task_timeout(
            ctx,
            &task,
            &mut run,
            "AGENT_DEREGISTERED",
            &format!("Agent {agent_id} deregistered while task was running"),
        )
        .await?;
    }

    // 4. Delete agent
    ctx.agents.delete(agent_id).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::poll::poll_task;
    use crate::application::submit::submit_pipeline;
    use crate::application::testing::fake_context;
    use crate::domain::agent::AgentCapabilities;
    use crate::domain::event::DomainEvent;
    use crate::domain::run::RunState;
    use crate::domain::task::TaskState;

    fn caps() -> AgentCapabilities {
        AgentCapabilities {
            plugins: vec!["source-postgres".to_string()],
            max_concurrent_tasks: 4,
        }
    }

    #[tokio::test]
    async fn register_creates_agent() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        let agent = tc.ctx.agents.find_by_id("agent-1").await.unwrap();
        assert!(agent.is_some());
        let agent = agent.unwrap();
        assert_eq!(agent.id(), "agent-1");
        assert_eq!(agent.capabilities().max_concurrent_tasks, 4);
    }

    #[tokio::test]
    async fn register_upserts_on_re_register() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        let new_caps = AgentCapabilities {
            plugins: vec!["dest-postgres".to_string()],
            max_concurrent_tasks: 8,
        };
        register(&tc.ctx, "agent-1", new_caps).await.unwrap();

        let agent = tc.ctx.agents.find_by_id("agent-1").await.unwrap().unwrap();
        assert_eq!(agent.capabilities().max_concurrent_tasks, 8);
    }

    #[tokio::test]
    async fn deregister_removes_agent_and_handles_tasks() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        // Submit and poll a task
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, None)
            .await
            .unwrap();
        let assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        deregister(&tc.ctx, "agent-1").await.unwrap();

        // Agent should be deleted
        let agent = tc.ctx.agents.find_by_id("agent-1").await.unwrap();
        assert!(agent.is_none());

        // Task should be timed out
        let task = tc
            .ctx
            .tasks
            .find_by_id(&assignment.task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.state(), TaskState::TimedOut);

        // Run should be retried (back to Pending)
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
    async fn deregister_no_tasks_removes_agent() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        deregister(&tc.ctx, "agent-1").await.unwrap();

        let agent = tc.ctx.agents.find_by_id("agent-1").await.unwrap();
        assert!(agent.is_none());
    }

    #[tokio::test]
    async fn deregister_unknown_returns_not_found() {
        let tc = fake_context();
        let result = deregister(&tc.ctx, "no-such-agent").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AppError::NotFound { .. }));
    }

    #[tokio::test]
    async fn deregister_no_retries_fails_run() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        // Submit with max_retries=0
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();
        let _assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        deregister(&tc.ctx, "agent-1").await.unwrap();

        // Run should be Failed
        let run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.state(), RunState::Failed);
        assert_eq!(run.error().unwrap().code, "AGENT_DEREGISTERED");

        // RunFailed event
        let events = tc.event_bus.published_events();
        let failed = events
            .iter()
            .find(|e| matches!(e, DomainEvent::RunFailed { .. }));
        assert!(failed.is_some());
    }

    #[tokio::test]
    async fn deregister_cancel_requested_run_gets_cancelled() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, None)
            .await
            .unwrap();
        let _assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        // Request cancel on the run
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

        deregister(&tc.ctx, "agent-1").await.unwrap();

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
    async fn register_with_zero_capacity() {
        let tc = fake_context();
        let zero_caps = AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: 0,
        };
        register(&tc.ctx, "agent-zero", zero_caps).await.unwrap();

        let agent = tc.ctx.agents.find_by_id("agent-zero").await.unwrap();
        assert!(agent.is_some());
        assert_eq!(agent.unwrap().capabilities().max_concurrent_tasks, 0);
    }

    #[tokio::test]
    async fn deregister_multiple_running_tasks() {
        let tc = fake_context();
        let multi_caps = AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: 3,
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

        deregister(&tc.ctx, "agent-1").await.unwrap();

        // All 3 tasks should be timed out
        let t1 = tc.ctx.tasks.find_by_id(&a1.task_id).await.unwrap().unwrap();
        let t2 = tc.ctx.tasks.find_by_id(&a2.task_id).await.unwrap().unwrap();
        let t3 = tc.ctx.tasks.find_by_id(&a3.task_id).await.unwrap().unwrap();
        assert_eq!(t1.state(), TaskState::TimedOut);
        assert_eq!(t2.state(), TaskState::TimedOut);
        assert_eq!(t3.state(), TaskState::TimedOut);

        // All 3 runs should be retried (back to Pending)
        let r1 = tc.ctx.runs.find_by_id(&s1.run_id).await.unwrap().unwrap();
        let r2 = tc.ctx.runs.find_by_id(&s2.run_id).await.unwrap().unwrap();
        let r3 = tc.ctx.runs.find_by_id(&s3.run_id).await.unwrap().unwrap();
        assert_eq!(r1.state(), RunState::Pending);
        assert_eq!(r2.state(), RunState::Pending);
        assert_eq!(r3.state(), RunState::Pending);
        assert_eq!(r1.current_attempt(), 2);
        assert_eq!(r2.current_attempt(), 2);
        assert_eq!(r3.current_attempt(), 2);
    }
}
