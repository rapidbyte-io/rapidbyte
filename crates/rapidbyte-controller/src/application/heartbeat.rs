use chrono::{DateTime, Utc};

use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::domain::event::DomainEvent;

pub struct TaskHeartbeatInput {
    pub task_id: String,
    pub lease_epoch: u64,
    pub progress_message: Option<String>,
    pub progress_pct: Option<f64>,
}

#[derive(Debug)]
pub struct TaskDirectiveOutput {
    pub task_id: String,
    pub acknowledged: bool,
    pub cancel_requested: bool,
    pub lease_expires_at: DateTime<Utc>,
}

/// Process heartbeats from an agent for one or more tasks.
///
/// For each task the lease is validated and extended. If the run has a pending
/// cancellation the directive signals `cancel_requested = true`.
///
/// # Errors
///
/// Returns `AppError::NotFound` if the agent does not exist, or a repository /
/// event-bus error on failure.
pub async fn heartbeat(
    ctx: &AppContext,
    agent_id: &str,
    tasks: Vec<TaskHeartbeatInput>,
) -> Result<Vec<TaskDirectiveOutput>, AppError> {
    // 1. Find agent, touch, save
    let mut agent = ctx
        .agents
        .find_by_id(agent_id)
        .await?
        .ok_or_else(|| AppError::NotFound {
            entity: "Agent",
            id: agent_id.to_string(),
        })?;

    let now = ctx.clock.now();
    agent.touch(now);
    ctx.agents.save(&agent).await?;

    let lease_duration = chrono::Duration::from_std(ctx.config.default_lease_duration)
        .unwrap_or_else(|_| chrono::Duration::seconds(300));

    let mut directives = Vec::with_capacity(tasks.len());

    for input in tasks {
        // 2a. Find task
        let Some(task) = ctx.tasks.find_by_id(&input.task_id).await? else {
            directives.push(TaskDirectiveOutput {
                task_id: input.task_id,
                acknowledged: false,
                cancel_requested: false,
                lease_expires_at: now,
            });
            continue;
        };

        // 2b. Validate lease
        if task
            .validate_lease(agent_id, input.lease_epoch, now)
            .is_err()
        {
            directives.push(TaskDirectiveOutput {
                task_id: input.task_id,
                acknowledged: false,
                cancel_requested: false,
                lease_expires_at: now,
            });
            continue;
        }

        // 2c. Extend lease
        let Some(current_lease) = task.lease() else {
            directives.push(TaskDirectiveOutput {
                task_id: input.task_id,
                acknowledged: false,
                cancel_requested: false,
                lease_expires_at: now,
            });
            continue;
        };
        let new_lease = current_lease.extend(lease_duration, now);
        let new_expires_at = new_lease.expires_at();

        let mut task = task;
        task.set_lease(new_lease);
        task.set_updated_at(now);
        ctx.tasks.save(&task).await?;

        // 2d. Publish progress if provided
        if let Some(ref message) = input.progress_message {
            ctx.event_bus
                .publish(DomainEvent::ProgressReported {
                    run_id: task.run_id().to_string(),
                    message: message.clone(),
                    pct: input.progress_pct,
                })
                .await?;
        }

        // 2e. Check cancellation
        let cancel_requested = if let Some(run) = ctx.runs.find_by_id(task.run_id()).await? {
            run.is_cancel_requested()
        } else {
            false
        };

        directives.push(TaskDirectiveOutput {
            task_id: input.task_id,
            acknowledged: true,
            cancel_requested,
            lease_expires_at: new_expires_at,
        });
    }

    Ok(directives)
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

    async fn setup_running_task(
        tc: &crate::application::testing::TestContext,
    ) -> (String, String, u64) {
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
        let _submit = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, Some(60))
            .await
            .unwrap();

        let assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();
        (
            assignment.task_id,
            assignment.run_id,
            assignment.lease_epoch,
        )
    }

    #[tokio::test]
    async fn agent_liveness_updated() {
        let tc = fake_context();
        let (task_id, _run_id, epoch) = setup_running_task(&tc).await;

        let before = tc
            .ctx
            .agents
            .find_by_id("agent-1")
            .await
            .unwrap()
            .unwrap()
            .last_seen_at();

        tc.clock.advance(chrono::Duration::seconds(5));

        let _directives = heartbeat(
            &tc.ctx,
            "agent-1",
            vec![TaskHeartbeatInput {
                task_id,
                lease_epoch: epoch,
                progress_message: None,
                progress_pct: None,
            }],
        )
        .await
        .unwrap();

        let after = tc
            .ctx
            .agents
            .find_by_id("agent-1")
            .await
            .unwrap()
            .unwrap()
            .last_seen_at();

        assert!(after > before);
    }

    #[tokio::test]
    async fn lease_extended_for_active_task() {
        let tc = fake_context();
        let (task_id, _run_id, epoch) = setup_running_task(&tc).await;

        let directives = heartbeat(
            &tc.ctx,
            "agent-1",
            vec![TaskHeartbeatInput {
                task_id: task_id.clone(),
                lease_epoch: epoch,
                progress_message: None,
                progress_pct: None,
            }],
        )
        .await
        .unwrap();

        assert_eq!(directives.len(), 1);
        assert!(directives[0].acknowledged);
        assert!(!directives[0].cancel_requested);

        // Verify lease was actually extended in storage
        let task = tc.ctx.tasks.find_by_id(&task_id).await.unwrap().unwrap();
        assert_eq!(
            task.lease().unwrap().expires_at(),
            directives[0].lease_expires_at
        );
    }

    #[tokio::test]
    async fn stale_lease_rejected() {
        let tc = fake_context();
        let (task_id, _run_id, epoch) = setup_running_task(&tc).await;

        // Use wrong epoch
        let directives = heartbeat(
            &tc.ctx,
            "agent-1",
            vec![TaskHeartbeatInput {
                task_id: task_id.clone(),
                lease_epoch: epoch + 999,
                progress_message: None,
                progress_pct: None,
            }],
        )
        .await
        .unwrap();

        assert_eq!(directives.len(), 1);
        assert!(!directives[0].acknowledged);
    }

    #[tokio::test]
    async fn cancel_requested_when_run_has_flag() {
        let tc = fake_context();
        let (task_id, run_id, epoch) = setup_running_task(&tc).await;

        // Set cancel_requested on the run
        {
            let mut run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
            run.request_cancel();
            tc.ctx.runs.save(&run).await.unwrap();
        }

        let directives = heartbeat(
            &tc.ctx,
            "agent-1",
            vec![TaskHeartbeatInput {
                task_id,
                lease_epoch: epoch,
                progress_message: None,
                progress_pct: None,
            }],
        )
        .await
        .unwrap();

        assert_eq!(directives.len(), 1);
        assert!(directives[0].acknowledged);
        assert!(directives[0].cancel_requested);
    }

    #[tokio::test]
    async fn progress_event_published() {
        let tc = fake_context();
        let (task_id, _run_id, epoch) = setup_running_task(&tc).await;

        let events_before = tc.event_bus.published_events().len();

        let _directives = heartbeat(
            &tc.ctx,
            "agent-1",
            vec![TaskHeartbeatInput {
                task_id,
                lease_epoch: epoch,
                progress_message: Some("50% done".to_string()),
                progress_pct: Some(50.0),
            }],
        )
        .await
        .unwrap();

        let events = tc.event_bus.published_events();
        let progress_events: Vec<_> = events[events_before..]
            .iter()
            .filter(|e| matches!(e, DomainEvent::ProgressReported { .. }))
            .collect();
        assert_eq!(progress_events.len(), 1);
        if let DomainEvent::ProgressReported { message, pct, .. } = &progress_events[0] {
            assert_eq!(message, "50% done");
            assert_eq!(*pct, Some(50.0));
        }
    }

    #[tokio::test]
    async fn unknown_agent_returns_not_found() {
        let tc = fake_context();
        let result = heartbeat(&tc.ctx, "unknown-agent", vec![]).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AppError::NotFound { .. }));
    }

    #[tokio::test]
    async fn heartbeat_multiple_tasks() {
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
        submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();
        submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();

        let a1 = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();
        let a2 = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        let directives = heartbeat(
            &tc.ctx,
            "agent-1",
            vec![
                TaskHeartbeatInput {
                    task_id: a1.task_id.clone(),
                    lease_epoch: a1.lease_epoch,
                    progress_message: None,
                    progress_pct: None,
                },
                TaskHeartbeatInput {
                    task_id: a2.task_id.clone(),
                    lease_epoch: a2.lease_epoch,
                    progress_message: None,
                    progress_pct: None,
                },
            ],
        )
        .await
        .unwrap();

        assert_eq!(directives.len(), 2);
        assert!(directives[0].acknowledged);
        assert!(directives[1].acknowledged);
    }

    #[tokio::test]
    async fn heartbeat_unknown_task_acknowledged_false() {
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

        let directives = heartbeat(
            &tc.ctx,
            "agent-1",
            vec![TaskHeartbeatInput {
                task_id: "nonexistent-task".to_string(),
                lease_epoch: 1,
                progress_message: None,
                progress_pct: None,
            }],
        )
        .await
        .unwrap();

        assert_eq!(directives.len(), 1);
        assert!(!directives[0].acknowledged);
    }

    #[tokio::test]
    async fn heartbeat_expired_lease_rejected() {
        let tc = fake_context();
        let (task_id, _run_id, epoch) = setup_running_task(&tc).await;

        // Advance clock past lease expiry (default 300s)
        tc.clock.advance(chrono::Duration::seconds(301));

        let directives = heartbeat(
            &tc.ctx,
            "agent-1",
            vec![TaskHeartbeatInput {
                task_id: task_id.clone(),
                lease_epoch: epoch,
                progress_message: None,
                progress_pct: None,
            }],
        )
        .await
        .unwrap();

        assert_eq!(directives.len(), 1);
        assert!(!directives[0].acknowledged);
    }

    #[tokio::test]
    async fn heartbeat_without_tasks_touches_agent() {
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

        tc.clock.advance(chrono::Duration::seconds(10));

        let directives = heartbeat(&tc.ctx, "agent-1", vec![]).await.unwrap();
        assert!(directives.is_empty());

        // Agent's last_seen_at should be updated
        let updated = tc.ctx.agents.find_by_id("agent-1").await.unwrap().unwrap();
        assert!(updated.last_seen_at() > now);
    }
}
