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

    // 2. Update registry config from controller response
    if let Some(registry_info) = response.registry {
        let new_config = rapidbyte_registry::RegistryConfig {
            insecure: registry_info.insecure,
            default_registry: rapidbyte_registry::normalize_registry_url_option(
                registry_info.url.as_deref(),
            ),
            ..Default::default()
        };
        adapters
            .engine_executor
            .update_registry_config(new_config)
            .await;
        info!(agent_id, "Updated registry config from controller");
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
    let pool_result = run_worker_pool(
        ctx,
        &agent_id,
        &adapters.progress_collector,
        active_leases.clone(),
        shutdown.clone(),
    )
    .await;

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
    progress_collector: &Arc<AtomicProgressCollector>,
    active_leases: ActiveLeaseMap,
    shutdown: CancellationToken,
) -> Result<(), AgentError> {
    let mut workers = JoinSet::new();

    for _ in 0..ctx.config.max_tasks.max(1) {
        let gateway = ctx.gateway.clone();
        let executor = ctx.executor.clone();
        let progress = ctx.progress.clone();
        let metrics = ctx.metrics.clone();
        let clock = ctx.clock.clone();
        let config = ctx.config.clone();
        let agent_id = agent_id.to_owned();
        let leases = active_leases.clone();
        let shutdown = shutdown.clone();
        let pc = progress_collector.clone();

        workers.spawn(async move {
            let worker_ctx = AgentContext {
                gateway,
                executor,
                progress,
                metrics,
                clock,
                config,
            };
            worker_loop(&worker_ctx, &agent_id, &pc, leases, shutdown).await
        });
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
    progress_collector: &Arc<AtomicProgressCollector>,
    active_leases: ActiveLeaseMap,
    shutdown: CancellationToken,
) -> Result<(), AgentError> {
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        let assignment = match ctx.gateway.poll(agent_id).await {
            Ok(Some(task)) => task,
            Ok(None) => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                continue;
            }
            Err(e) => {
                warn!(error = %e, "Poll failed");
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        let cancel = CancellationToken::new();
        let child_cancel = cancel.clone();

        // Register lease
        active_leases.write().await.insert(
            assignment.task_id.clone(),
            LeaseEntry {
                run_id: assignment.run_id.clone(),
                lease_epoch: assignment.lease_epoch,
                cancel: cancel.clone(),
                progress: ctx.progress.clone(),
            },
        );

        // Link task cancellation to shutdown
        let shutdown_for_cancel = shutdown.clone();
        let cancel_for_shutdown = cancel.clone();
        tokio::spawn(async move {
            shutdown_for_cancel.cancelled().await;
            cancel_for_shutdown.cancel();
        });

        execute_task(ctx, agent_id, &assignment, progress_collector, child_cancel).await;

        // Remove lease after execution
        active_leases.write().await.remove(&assignment.task_id);
    }
}
