use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::application::register::deregister;

/// Reap agents that have not sent a heartbeat within the configured timeout.
///
/// For each stale agent, `deregister` is called which handles running tasks.
///
/// # Errors
///
/// Returns a repository / event-bus error on failure.
pub async fn reap_stale_agents(ctx: &AppContext) -> Result<(), AppError> {
    let now = ctx.clock.now();
    let timeout = chrono::Duration::from_std(ctx.config.agent_reap_timeout)
        .unwrap_or_else(|_| chrono::Duration::seconds(60));

    let stale_agents = ctx.agents.find_stale(timeout, now).await?;

    for agent in stale_agents {
        deregister(ctx, agent.id()).await?;
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
    use crate::domain::ports::clock::Clock;
    use crate::domain::run::RunState;
    use crate::domain::task::TaskState;

    fn caps() -> AgentCapabilities {
        AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: 4,
        }
    }

    #[tokio::test]
    async fn stale_agent_deregistered() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, None)
            .await
            .unwrap();
        let assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        // Advance clock past agent_reap_timeout (60s)
        tc.clock.advance(chrono::Duration::seconds(61));

        reap_stale_agents(&tc.ctx).await.unwrap();

        // Agent should be gone
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

        // Run should be retried
        let run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.state(), RunState::Pending);
        assert_eq!(run.current_attempt(), 2);
    }

    #[tokio::test]
    async fn no_stale_agents_is_noop() {
        let tc = fake_context();
        register(&tc.ctx, "agent-1", caps()).await.unwrap();

        // Don't advance clock — agent is fresh
        reap_stale_agents(&tc.ctx).await.unwrap();

        let agent = tc.ctx.agents.find_by_id("agent-1").await.unwrap();
        assert!(agent.is_some());
    }

    #[tokio::test]
    async fn reaper_doesnt_touch_fresh_agents() {
        let tc = fake_context();
        register(&tc.ctx, "agent-stale", caps()).await.unwrap();
        register(&tc.ctx, "agent-fresh", caps()).await.unwrap();

        // Advance clock by 50s (agent_reap_timeout=60s, so both are still fresh)
        tc.clock.advance(chrono::Duration::seconds(50));

        // Touch agent-fresh to keep it alive
        {
            let now = tc.clock.now();
            let mut agent = tc
                .ctx
                .agents
                .find_by_id("agent-fresh")
                .await
                .unwrap()
                .unwrap();
            agent.touch(now);
            tc.ctx.agents.save(&agent).await.unwrap();
        }

        // Advance clock another 11s (total 61s from start)
        // agent-stale last_seen_at = start -> 61s ago -> stale
        // agent-fresh last_seen_at = start+50s -> 11s ago -> fresh
        tc.clock.advance(chrono::Duration::seconds(11));

        reap_stale_agents(&tc.ctx).await.unwrap();

        // agent-stale should be removed
        let stale = tc.ctx.agents.find_by_id("agent-stale").await.unwrap();
        assert!(stale.is_none());

        // agent-fresh should still be present
        let fresh = tc.ctx.agents.find_by_id("agent-fresh").await.unwrap();
        assert!(fresh.is_some());
    }
}
