//! Worker lifecycle use-case: register, poll loop, heartbeat, graceful shutdown.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::adapter::AtomicProgressCollector;
use crate::domain::error::AgentError;
use crate::domain::ports::controller::RegistrationConfig;

use super::context::AgentContext;
use super::execute::execute_task;
use super::heartbeat::{heartbeat_loop, ActiveLeaseMap, LeaseEntry};

/// Run the agent worker loop.
///
/// Registers with the controller, spawns heartbeat and worker pool,
/// then polls for tasks until shutdown is signalled.
///
/// # Errors
///
/// Returns `AgentError` if registration fails or the worker pool encounters
/// a fatal error during polling or execution.
pub async fn run_agent(
    ctx: &AgentContext,
    registration: RegistrationConfig,
    adapters: &crate::adapter::AgentAdapters,
    shutdown: CancellationToken,
) -> Result<(), AgentError> {
    // 1. Register with controller
    let response = ctx.gateway.register(&registration).await?;
    let agent_id = response.agent_id;
    info!(agent_id, "Registered with controller");

    // 2. Controller response is authoritative for registry URL.
    //    Merge the registry info (preserving trust_policy/trusted_key_pems),
    //    or clear it if controller returned None (no registry configured).
    if let Some(registry_info) = response.registry {
        adapters
            .engine_executor
            .merge_registry_url(registry_info.url.as_deref(), registry_info.insecure)
            .await;
        info!(agent_id, "Updated registry URL from controller");
    } else {
        adapters
            .engine_executor
            .merge_registry_url(None, false)
            .await;
        info!(agent_id, "Controller returned no registry — cleared");
    }

    // 3. Shared active lease tracking
    let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));

    // 4. Spawn heartbeat loop
    let hb_gateway = ctx.gateway.clone();
    let hb_agent_id = agent_id.clone();
    let hb_config = ctx.config.clone();
    let hb_leases = active_leases.clone();
    let hb_shutdown = shutdown.clone();
    let heartbeat_handle = tokio::spawn(async move {
        heartbeat_loop(
            hb_gateway.as_ref(),
            &hb_agent_id,
            &hb_config,
            hb_leases,
            hb_shutdown,
        )
        .await;
    });

    // 5. Run worker pool: max_tasks concurrent poll-execute loops
    let pool_result =
        run_worker_pool(ctx, &agent_id, active_leases.clone(), shutdown.clone()).await;

    // 6. Graceful shutdown: cancel heartbeat, wait for it
    shutdown.cancel();
    let _ = heartbeat_handle.await;

    if let Err(ref e) = pool_result {
        error!(error = %e, "Worker pool exited with error");
    }

    info!("Agent stopped");
    pool_result
}

async fn run_worker_pool(
    ctx: &AgentContext,
    agent_id: &str,
    active_leases: ActiveLeaseMap,
    shutdown: CancellationToken,
) -> Result<(), AgentError> {
    let mut workers = JoinSet::new();

    for _ in 0..ctx.config.max_tasks.max(1) {
        let worker_ctx = ctx.clone();
        let agent_id = agent_id.to_owned();
        let leases = active_leases.clone();
        let shutdown = shutdown.clone();

        workers.spawn(async move { worker_loop(&worker_ctx, &agent_id, leases, shutdown).await });
    }

    while let Some(result) = workers.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(AgentError::ExecutionFailed(e.into())),
        }
    }

    Ok(())
}

async fn worker_loop(
    ctx: &AgentContext,
    agent_id: &str,
    active_leases: ActiveLeaseMap,
    shutdown: CancellationToken,
) -> Result<(), AgentError> {
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        // Poll is shutdown-aware so the agent can exit promptly.
        let poll_result = tokio::select! {
            result = ctx.gateway.poll(agent_id) => result,
            () = shutdown.cancelled() => return Ok(()),
        };

        let assignment = match poll_result {
            Ok(Some(task)) => task,
            Ok(None) => {
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                    () = shutdown.cancelled() => return Ok(()),
                }
                continue;
            }
            Err(e @ AgentError::ControllerNonRetryable(_)) => {
                error!(error = %e, "Poll rejected by controller (non-retryable)");
                return Err(e);
            }
            Err(e) => {
                warn!(error = %e, "Poll failed (transient), retrying");
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                    () = shutdown.cancelled() => return Ok(()),
                }
                continue;
            }
        };

        let cancel = shutdown.child_token();

        // Each task gets its own progress collector so concurrent tasks
        // don't overwrite each other's heartbeat progress.
        let task_progress = Arc::new(AtomicProgressCollector::new());

        // Register lease with per-task progress
        active_leases.write().await.insert(
            assignment.task_id.clone(),
            LeaseEntry {
                lease_epoch: assignment.lease_epoch,
                cancel: cancel.clone(),
                progress: task_progress.clone(),
            },
        );

        execute_task(
            ctx,
            agent_id,
            &assignment,
            &task_progress,
            cancel,
            &shutdown,
        )
        .await;

        // Always remove the lease after execution completes. If completion
        // reporting failed (retries exhausted), the agent stops heartbeating
        // this task, so the controller's lease timeout will fire and
        // reassign/fail the task. Keeping it would prevent timeout recovery
        // since heartbeats would keep extending the lease indefinitely.
        active_leases.write().await.remove(&assignment.task_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{AgentAdapters, AtomicProgressCollector, EngineExecutor};
    use crate::application::context::AgentAppConfig;
    use crate::application::testing::{fake_context, VALID_YAML};
    use crate::domain::ports::controller::{RegistrationResponse, TaskAssignment};
    use crate::domain::task::{TaskExecutionResult, TaskMetrics, TaskOutcomeKind};
    use std::time::Duration;

    fn test_adapters() -> AgentAdapters {
        AgentAdapters {
            engine_executor: Arc::new(EngineExecutor::new(
                rapidbyte_registry::RegistryConfig::default(),
            )),
            progress_collector: Arc::new(AtomicProgressCollector::new()),
        }
    }

    #[tokio::test]
    async fn run_agent_registers_with_controller() {
        let mut t = fake_context();
        t.ctx.config = AgentAppConfig {
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(3600), // long interval, won't fire
            completion_retry_delay: Duration::from_millis(1),
            ..AgentAppConfig::default()
        };

        // Enqueue registration success.
        t.gateway.enqueue_register(Ok(RegistrationResponse {
            agent_id: "agent-1".into(),
            registry: None,
        }));

        // Enqueue enough poll-None results so the worker doesn't panic before
        // the shutdown token is cancelled.
        for _ in 0..50 {
            t.gateway.enqueue_poll(Ok(None));
        }

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            sd.cancel();
        });

        let adapters = test_adapters();
        let result = run_agent(
            &t.ctx,
            RegistrationConfig { max_tasks: 1 },
            &adapters,
            shutdown,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn run_agent_executes_polled_task() {
        let mut t = fake_context();
        t.ctx.config = AgentAppConfig {
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(3600),
            completion_retry_delay: Duration::from_millis(1),
            ..AgentAppConfig::default()
        };

        // Registration.
        t.gateway.enqueue_register(Ok(RegistrationResponse {
            agent_id: "agent-1".into(),
            registry: None,
        }));

        // First poll: task assignment with valid YAML.
        t.gateway.enqueue_poll(Ok(Some(TaskAssignment {
            task_id: "task-1".into(),
            run_id: "run-1".into(),
            pipeline_yaml: VALID_YAML.into(),
            lease_epoch: 1,
            attempt: 1,
        })));

        // Enqueue executor result.
        t.executor.enqueue(Ok(TaskExecutionResult {
            outcome: TaskOutcomeKind::Completed,
            metrics: TaskMetrics {
                records_written: 100,
                ..TaskMetrics::default()
            },
        }));

        // Enqueue completion result.
        t.gateway.enqueue_complete(Ok(()));

        // After task completes, subsequent polls return None, then shutdown.
        for _ in 0..50 {
            t.gateway.enqueue_poll(Ok(None));
        }

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        // Allow the task execution to complete before cancelling.
        tokio::spawn(async move {
            // Yield multiple times to let the worker execute the task and poll again.
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            sd.cancel();
        });

        let adapters = test_adapters();
        let result = run_agent(
            &t.ctx,
            RegistrationConfig { max_tasks: 1 },
            &adapters,
            shutdown,
        )
        .await;

        assert!(result.is_ok());

        let payloads = t.gateway.completed_payloads();
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].task_id, "task-1");
        assert!(matches!(
            payloads[0].result.outcome,
            TaskOutcomeKind::Completed
        ));
    }

    #[tokio::test]
    async fn run_agent_stops_on_shutdown() {
        let mut t = fake_context();
        t.ctx.config = AgentAppConfig {
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(3600),
            completion_retry_delay: Duration::from_millis(1),
            ..AgentAppConfig::default()
        };

        // Registration.
        t.gateway.enqueue_register(Ok(RegistrationResponse {
            agent_id: "agent-1".into(),
            registry: None,
        }));

        // A few polls in case the worker gets one iteration in.
        for _ in 0..10 {
            t.gateway.enqueue_poll(Ok(None));
        }

        // Cancel immediately after registration by using an already-cancelled token.
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let adapters = test_adapters();
        let result = run_agent(
            &t.ctx,
            RegistrationConfig { max_tasks: 1 },
            &adapters,
            shutdown,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn registry_update_preserves_trust_policy() {
        let initial_config = rapidbyte_registry::RegistryConfig {
            trust_policy: rapidbyte_registry::TrustPolicy::Verify,
            trusted_key_pems: vec!["PEM-KEY-1".into()],
            ..Default::default()
        };

        let executor = Arc::new(EngineExecutor::new(initial_config));

        // Simulate controller providing a registry URL
        executor
            .merge_registry_url(Some("https://registry.example.com"), false)
            .await;

        // Read back the config and verify trust settings are preserved
        let config = executor.registry_config_snapshot().await;
        // normalize_registry_url_option strips the scheme
        assert!(config.default_registry.is_some());
        assert!(!config.insecure);
        assert_eq!(config.trust_policy, rapidbyte_registry::TrustPolicy::Verify);
        assert_eq!(config.trusted_key_pems, vec!["PEM-KEY-1".to_string()]);
    }

    #[tokio::test]
    async fn non_retryable_poll_error_terminates_agent() {
        let mut t = fake_context();
        t.ctx.config = AgentAppConfig {
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(3600),
            ..AgentAppConfig::default()
        };

        t.gateway.enqueue_register(Ok(RegistrationResponse {
            agent_id: "agent-1".into(),
            registry: None,
        }));

        // Poll returns a non-retryable error (e.g., auth failure)
        t.gateway
            .enqueue_poll(Err(AgentError::ControllerNonRetryable(
                "unauthenticated".into(),
            )));

        let shutdown = CancellationToken::new();
        let adapters = test_adapters();
        let result = run_agent(
            &t.ctx,
            RegistrationConfig { max_tasks: 1 },
            &adapters,
            shutdown,
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, AgentError::ControllerNonRetryable(_)),
            "expected ControllerNonRetryable, got {err:?}"
        );
    }

    #[tokio::test]
    async fn registry_none_clears_executor_url() {
        let initial_config = rapidbyte_registry::RegistryConfig {
            default_registry: Some("https://initial.example.com".into()),
            insecure: true,
            ..Default::default()
        };

        let executor = Arc::new(EngineExecutor::new(initial_config));

        // Simulate controller returning registry: None
        executor.merge_registry_url(None, false).await;

        let config = executor.registry_config_snapshot().await;
        assert!(
            config.default_registry.is_none(),
            "expected registry cleared, got {:?}",
            config.default_registry
        );
        assert!(!config.insecure);
    }
}
