use chrono::{DateTime, Utc};

use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::application::timeout::handle_task_timeout;

use crate::domain::event::DomainEvent;
use crate::domain::lease::Lease;
use crate::domain::run::RunState;

#[derive(Debug)]
pub struct TaskAssignment {
    pub task_id: String,
    pub run_id: String,
    pub pipeline_yaml: String,
    pub max_retries: u32,
    pub timeout_seconds: Option<u64>,
    pub lease_epoch: u64,
    pub lease_expires_at: DateTime<Utc>,
    pub attempt: u32,
}

/// Agent polls for a task to execute.
///
/// If a pending task exists it is atomically assigned to the agent and the
/// corresponding run transitions to `Running`.
///
/// # Errors
///
/// Returns `AppError::NotFound` if the agent does not exist, or a repository /
/// event-bus / secret-resolution error on failure.
///
/// # Panics
///
/// Panics if the assigned task has no lease (should never happen after
/// `assign_task`).
pub async fn poll_task(
    ctx: &AppContext,
    agent_id: &str,
) -> Result<Option<TaskAssignment>, AppError> {
    // 1. Find agent
    let mut agent = ctx.find_agent(agent_id).await?;

    // 2. Touch agent liveness
    let now = ctx.clock.now();
    agent.touch(now);
    ctx.agents.save(&agent).await?;

    // 3. Get next lease epoch
    let epoch = ctx.tasks.next_lease_epoch().await?;

    // 4. Calculate lease duration (use default config)
    let lease_duration = ctx.config.lease_duration_chrono();
    let expires_at = now + lease_duration;

    // 5. Create lease
    let lease = Lease::new(epoch, expires_at);

    // 6. Atomically assign task and transition run to Running
    let Some((task, mut run)) = ctx
        .store
        .assign_task(agent_id, agent.capabilities().max_concurrent_tasks, lease)
        .await?
    else {
        return Ok(None);
    };

    // 9. Resolve secrets
    let resolved_yaml = match ctx.secrets.resolve(run.pipeline_yaml()).await {
        Ok(yaml) => yaml,
        Err(e) => {
            // Compensation: timeout the task, retry or fail the run
            let mut task = task;
            task.timeout()?;

            handle_task_timeout(
                ctx,
                &task,
                &mut run,
                "SECRET_RESOLUTION_FAILED",
                &e.to_string(),
            )
            .await?;

            return Err(AppError::SecretResolution(e.to_string()));
        }
    };

    // 10. Publish event
    ctx.event_bus
        .publish(DomainEvent::RunStateChanged {
            run_id: run.id().to_string(),
            state: RunState::Running,
            attempt: run.current_attempt(),
        })
        .await?;

    // 11. Build assignment
    let lease_ref = task
        .lease()
        .expect("task should have a lease after assignment");
    Ok(Some(TaskAssignment {
        task_id: task.id().to_string(),
        run_id: run.id().to_string(),
        pipeline_yaml: resolved_yaml,
        max_retries: run.max_retries(),
        timeout_seconds: run.timeout_seconds(),
        lease_epoch: lease_ref.epoch(),
        lease_expires_at: lease_ref.expires_at(),
        attempt: task.attempt(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::submit::submit_pipeline;
    use crate::application::testing::fake_context;
    use crate::domain::agent::{Agent, AgentCapabilities};
    use crate::domain::event::DomainEvent;
    use crate::domain::ports::clock::Clock;
    use crate::domain::run::RunState;
    use crate::domain::task::TaskState;

    async fn setup_agent_and_run(
        tc: &crate::application::testing::TestContext,
    ) -> (String, String) {
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
        let result = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, Some(60))
            .await
            .unwrap();

        ("agent-1".to_string(), result.run_id)
    }

    #[tokio::test]
    async fn happy_path_gets_assignment() {
        let tc = fake_context();
        let (agent_id, run_id) = setup_agent_and_run(&tc).await;

        let assignment = poll_task(&tc.ctx, &agent_id).await.unwrap();
        assert!(assignment.is_some());

        let a = assignment.unwrap();
        assert_eq!(a.run_id, run_id);
        assert!(!a.task_id.is_empty());
        assert!(a.pipeline_yaml.contains("test-pipe"));
        assert_eq!(a.max_retries, 2);
        assert_eq!(a.timeout_seconds, Some(60));
        assert_eq!(a.attempt, 1);
        assert!(a.lease_epoch > 0);

        // Run should be Running
        let run = tc.ctx.runs.find_by_id(&run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Running);
    }

    #[tokio::test]
    async fn no_tasks_returns_none() {
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

        let result = poll_task(&tc.ctx, "agent-1").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn unknown_agent_returns_not_found() {
        let tc = fake_context();
        let result = poll_task(&tc.ctx, "unknown-agent").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AppError::NotFound { .. }));
    }

    #[tokio::test]
    async fn agent_liveness_updated() {
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

        // Advance clock
        tc.clock.advance(chrono::Duration::seconds(10));

        let _ = poll_task(&tc.ctx, "agent-1").await.unwrap();

        // Agent's last_seen_at should be updated
        let updated = tc.ctx.agents.find_by_id("agent-1").await.unwrap().unwrap();
        assert!(updated.last_seen_at() > now);
    }

    #[tokio::test]
    async fn event_published_on_poll() {
        let tc = fake_context();
        let (_agent_id, run_id) = setup_agent_and_run(&tc).await;

        let _ = poll_task(&tc.ctx, "agent-1").await.unwrap();

        let events = tc.event_bus.published_events();
        // First event is from submit (RunStateChanged Pending), second is from poll (RunStateChanged Running)
        assert!(events.len() >= 2);
        let running_event = events.iter().find(|e| {
            matches!(
                e,
                DomainEvent::RunStateChanged {
                    state: RunState::Running,
                    ..
                }
            )
        });
        assert!(running_event.is_some());
        if let Some(DomainEvent::RunStateChanged {
            run_id: eid, state, ..
        }) = running_event
        {
            assert_eq!(eid, &run_id);
            assert_eq!(*state, RunState::Running);
        }
    }

    #[tokio::test]
    async fn task_is_running_after_poll() {
        let tc = fake_context();
        let (_agent_id, _run_id) = setup_agent_and_run(&tc).await;

        let assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();

        let tasks = tc.storage.tasks.lock().unwrap();
        let task = tasks.get(&assignment.task_id).unwrap();
        assert_eq!(task.state(), TaskState::Running);
        assert_eq!(task.agent_id(), Some("agent-1"));
    }

    #[tokio::test]
    async fn poll_secret_resolution_failure_compensates() {
        use crate::application::context::{AppConfig, AppContext};
        use crate::application::testing::{
            FakeAgentRepository, FakeClock, FakeConnectionTester, FakeCursorStore, FakeEventBus,
            FakeLogStore, FakePipelineSource, FakePipelineStore, FakePluginRegistry,
            FakeRunRepository, FakeStorage, FakeTaskRepository,
        };
        use crate::domain::ports::secrets::{SecretError, SecretResolver};
        use std::sync::Arc;
        use std::time::Duration;

        struct FailingSecretResolver;
        #[async_trait::async_trait]
        impl SecretResolver for FailingSecretResolver {
            async fn resolve(&self, _yaml: &str) -> Result<String, SecretError> {
                Err(SecretError("vault unavailable".to_string()))
            }
        }

        let storage = FakeStorage::new();
        let event_bus = Arc::new(FakeEventBus::new());
        let clock = Arc::new(FakeClock::new(chrono::Utc::now()));

        let ctx = AppContext {
            runs: Arc::new(FakeRunRepository::new(storage.clone())),
            tasks: Arc::new(FakeTaskRepository::new(storage.clone())),
            agents: Arc::new(FakeAgentRepository::new(storage.clone())),
            store: Arc::new(FakePipelineStore::new(storage.clone())),
            event_bus: Arc::clone(&event_bus) as Arc<dyn crate::domain::ports::event_bus::EventBus>,
            secrets: Arc::new(FailingSecretResolver),
            clock: Arc::clone(&clock) as Arc<dyn crate::domain::ports::clock::Clock>,
            cursor_store: Arc::new(FakeCursorStore::new()),
            log_store: Arc::new(FakeLogStore::new()),
            connection_tester: Arc::new(FakeConnectionTester),
            plugin_registry: Arc::new(FakePluginRegistry),
            pipeline_source: Arc::new(FakePipelineSource::new()),
            config: AppConfig {
                default_lease_duration: Duration::from_secs(300),
                lease_check_interval: Duration::from_secs(30),
                agent_reap_timeout: Duration::from_secs(60),
                agent_reap_interval: Duration::from_secs(30),
                default_max_retries: 0,
                registry: None,
                allow_unauthenticated: true,
            },
        };

        // Register agent
        let now = clock.now();
        let agent = Agent::new(
            "agent-1".to_string(),
            AgentCapabilities {
                plugins: vec![],
                max_concurrent_tasks: 4,
            },
            now,
        );
        ctx.agents.save(&agent).await.unwrap();

        // Submit pipeline with max_retries=2
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let submit =
            crate::application::submit::submit_pipeline(&ctx, None, yaml.to_string(), 2, None)
                .await
                .unwrap();

        // Poll should return error due to secret resolution failure
        let result = poll_task(&ctx, "agent-1").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), AppError::SecretResolution(_)));

        // The run should have been compensated (retried back to Pending)
        let run = ctx.runs.find_by_id(&submit.run_id).await.unwrap().unwrap();
        assert_eq!(run.state(), RunState::Pending);
        assert_eq!(run.current_attempt(), 2);
    }

    #[tokio::test]
    async fn poll_returns_resolved_yaml() {
        let tc = fake_context();
        let (_agent_id, _run_id) = setup_agent_and_run(&tc).await;

        let assignment = poll_task(&tc.ctx, "agent-1").await.unwrap().unwrap();
        // FakeSecretResolver returns input unchanged
        assert!(assignment.pipeline_yaml.contains("test-pipe"));
        assert!(assignment.pipeline_yaml.contains("version"));
    }

    #[tokio::test]
    async fn poll_lease_epoch_increments() {
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

        assert!(a1.lease_epoch < a2.lease_epoch);
    }

    #[tokio::test]
    async fn poll_agent_at_capacity_returns_none() {
        let tc = fake_context();
        let now = tc.clock.now();
        let agent = Agent::new(
            "agent-1".to_string(),
            AgentCapabilities {
                plugins: vec![],
                max_concurrent_tasks: 1,
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

        let first = poll_task(&tc.ctx, "agent-1").await.unwrap();
        assert!(first.is_some());

        let second = poll_task(&tc.ctx, "agent-1").await.unwrap();
        assert!(second.is_none());
    }
}
