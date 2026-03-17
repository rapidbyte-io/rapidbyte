use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::domain::agent::{Agent, AgentCapabilities};
use crate::domain::event::DomainEvent;
use crate::domain::run::{RunError, RunState};
use crate::domain::task::Task;

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
    // 1. Find agent
    let _agent = ctx
        .agents
        .find_by_id(agent_id)
        .await?
        .ok_or_else(|| AppError::NotFound {
            entity: "Agent",
            id: agent_id.to_string(),
        })?;

    // 2. Find running tasks for this agent
    let running_tasks = ctx.tasks.find_running_by_agent_id(agent_id).await?;

    let now = ctx.clock.now();

    // 3. Handle each running task
    for mut task in running_tasks {
        task.timeout()?;

        let mut run =
            ctx.runs
                .find_by_id(task.run_id())
                .await?
                .ok_or_else(|| AppError::NotFound {
                    entity: "Run",
                    id: task.run_id().to_string(),
                })?;

        if run.can_retry_after_timeout() && !run.is_cancel_requested() {
            let new_attempt = run.retry()?;
            let new_task_id = uuid::Uuid::new_v4().to_string();
            let new_task = Task::new(new_task_id, run.id().to_string(), new_attempt, now);
            ctx.store
                .timeout_and_retry(&task, &run, Some(&new_task))
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
                code: "AGENT_DEREGISTERED".to_string(),
                message: format!("Agent {agent_id} deregistered while task was running"),
            };
            run.fail(error.clone())?;
            ctx.store.timeout_and_retry(&task, &run, None).await?;
            ctx.event_bus
                .publish(DomainEvent::RunFailed {
                    run_id: run.id().to_string(),
                    error,
                })
                .await?;
        }
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
}
