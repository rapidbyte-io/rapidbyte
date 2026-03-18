use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::domain::event::DomainEvent;
use crate::domain::run::RunState;
use crate::domain::task::TaskState;

#[derive(Debug)]
pub struct CancelResult {
    pub accepted: bool,
}

/// Request cancellation of a pipeline run.
///
/// - If the run is **Pending**, it is cancelled immediately along with its
///   pending task.
/// - If the run is **Running**, a cancellation flag is set and the agent will
///   learn about it at the next heartbeat.
/// - If the run is in a terminal state, the request is rejected.
///
/// # Errors
///
/// Returns `AppError::NotFound` if the run does not exist, or a repository /
/// event-bus error on failure.
pub async fn cancel_run(ctx: &AppContext, run_id: &str) -> Result<CancelResult, AppError> {
    // 1. Find run
    let mut run = ctx
        .runs
        .find_by_id(run_id)
        .await?
        .ok_or_else(|| AppError::NotFound {
            entity: "Run",
            id: run_id.to_string(),
        })?;

    match run.state() {
        // Terminal states: reject
        RunState::Completed | RunState::Failed | RunState::Cancelled => {
            Ok(CancelResult { accepted: false })
        }

        RunState::Pending => {
            // Find pending task for this run
            let tasks = ctx.tasks.find_by_run_id(run_id).await?;
            let pending_task = tasks.into_iter().find(|t| t.state() == TaskState::Pending);

            if let Some(mut pending_task) = pending_task {
                pending_task.cancel_pending()?;
                run.cancel()?;

                if ctx
                    .store
                    .cancel_pending_run(&run, &pending_task)
                    .await
                    .is_ok()
                {
                    ctx.event_bus
                        .publish(DomainEvent::RunCancelled {
                            run_id: run_id.to_string(),
                        })
                        .await?;
                    return Ok(CancelResult { accepted: true });
                }
            }

            // Run was concurrently assigned — set cancel flag instead.
            // Re-fetch once to get current state.
            let mut run = ctx
                .runs
                .find_by_id(run_id)
                .await?
                .ok_or_else(|| AppError::NotFound {
                    entity: "Run",
                    id: run_id.to_string(),
                })?;

            match run.state() {
                RunState::Running => {
                    run.request_cancel();
                    ctx.runs.save(&run).await?;
                    Ok(CancelResult { accepted: true })
                }
                RunState::Completed | RunState::Failed | RunState::Cancelled => {
                    Ok(CancelResult { accepted: false })
                }
                RunState::Pending => {
                    // Still pending but no pending task — shouldn't happen, but handle gracefully
                    run.request_cancel();
                    ctx.runs.save(&run).await?;
                    Ok(CancelResult { accepted: true })
                }
            }
        }

        // Running: set flag, agent learns via heartbeat
        RunState::Running => {
            run.request_cancel();
            ctx.runs.save(&run).await?;
            Ok(CancelResult { accepted: true })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::poll::poll_task;
    use crate::application::submit::submit_pipeline;
    use crate::application::testing::fake_context;
    use crate::domain::agent::{Agent, AgentCapabilities};
    use crate::domain::event::DomainEvent;
    use crate::domain::ports::clock::Clock;
    use crate::domain::run::RunState;
    use crate::domain::task::TaskState;

    #[tokio::test]
    async fn cancel_pending_run() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();

        let result = cancel_run(&tc.ctx, &submit.run_id).await.unwrap();
        assert!(result.accepted);

        // Run should be cancelled
        let run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.state(), RunState::Cancelled);

        // Task should be cancelled
        let tasks = tc.storage.tasks.lock().unwrap();
        let task = tasks.values().next().unwrap();
        assert_eq!(task.state(), TaskState::Cancelled);

        // Event published
        let events = tc.event_bus.published_events();
        let cancelled = events
            .iter()
            .find(|e| matches!(e, DomainEvent::RunCancelled { .. }));
        assert!(cancelled.is_some());
    }

    #[tokio::test]
    async fn cancel_running_sets_flag() {
        let tc = fake_context();
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
        let _assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        let result = cancel_run(&tc.ctx, &submit.run_id).await.unwrap();
        assert!(result.accepted);

        // Run should still be Running but with cancel_requested
        let run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.state(), RunState::Running);
        assert!(run.is_cancel_requested());

        // No RunCancelled event yet (agent needs to confirm)
        let events = tc.event_bus.published_events();
        let cancelled = events
            .iter()
            .filter(|e| matches!(e, DomainEvent::RunCancelled { .. }))
            .count();
        assert_eq!(cancelled, 0);
    }

    #[tokio::test]
    async fn cancel_terminal_rejected() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();

        // Cancel it first
        cancel_run(&tc.ctx, &submit.run_id).await.unwrap();

        // Try to cancel again (now in Cancelled state)
        let result = cancel_run(&tc.ctx, &submit.run_id).await.unwrap();
        assert!(!result.accepted);
    }

    #[tokio::test]
    async fn cancel_nonexistent_returns_not_found() {
        let tc = fake_context();
        let result = cancel_run(&tc.ctx, "no-such-run").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AppError::NotFound { .. }));
    }

    #[tokio::test]
    async fn cancel_pending_race_falls_through() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();

        // Manually change the task state to Running (simulating concurrent poll)
        {
            let now = tc.ctx.clock.now();
            let agent = Agent::new(
                "agent-1".to_string(),
                AgentCapabilities {
                    plugins: vec![],
                    max_concurrent_tasks: 4,
                },
                now,
            );
            tc.ctx.agents.save(&agent).await.unwrap();
            // Poll to actually assign the task (transitions task to Running and run to Running)
            let _assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();
        }

        // Now the run is Running but cancel_run sees it — should set cancel_requested
        let result = cancel_run(&tc.ctx, &submit.run_id).await.unwrap();
        assert!(result.accepted);

        let run = tc
            .ctx
            .runs
            .find_by_id(&submit.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.state(), RunState::Running);
        assert!(run.is_cancel_requested());
    }

    #[tokio::test]
    async fn cancel_running_then_complete_cancelled() {
        let tc = fake_context();
        let now = tc.ctx.clock.now();
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

        // Cancel (sets flag)
        let result = cancel_run(&tc.ctx, &submit.run_id).await.unwrap();
        assert!(result.accepted);

        // Complete with TaskOutcome::Cancelled
        crate::application::complete::complete_task(
            &tc.ctx,
            "agent-1",
            &assignment.task_id,
            assignment.lease_epoch,
            crate::application::complete::TaskOutcome::Cancelled,
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
        assert_eq!(run.state(), RunState::Cancelled);
    }

    #[tokio::test]
    async fn cancel_idempotent() {
        let tc = fake_context();
        let now = tc.ctx.clock.now();
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
        let _assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        // Cancel twice — both should succeed
        let first = cancel_run(&tc.ctx, &submit.run_id).await.unwrap();
        assert!(first.accepted);

        let second = cancel_run(&tc.ctx, &submit.run_id).await.unwrap();
        assert!(second.accepted);
    }
}
