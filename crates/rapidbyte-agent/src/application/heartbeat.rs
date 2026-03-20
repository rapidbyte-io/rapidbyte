//! Heartbeat loop use-case: periodic progress reporting and cancel handling.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::domain::ports::controller::{ControllerGateway, HeartbeatPayload, TaskHeartbeat};
use crate::domain::ports::progress::ProgressCollector;

use super::context::AgentAppConfig;

/// Active lease entry tracked by the worker.
pub struct LeaseEntry {
    pub lease_epoch: u64,
    pub cancel: CancellationToken,
    pub progress: Arc<dyn ProgressCollector>,
}

/// Shared map of active task leases.
pub type ActiveLeaseMap = Arc<RwLock<HashMap<String, LeaseEntry>>>;

/// Run the periodic heartbeat loop until shutdown is signalled.
pub async fn heartbeat_loop(
    gateway: &dyn ControllerGateway,
    agent_id: &str,
    config: &AgentAppConfig,
    active_leases: ActiveLeaseMap,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(config.heartbeat_interval);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _tick = ticker.tick() => {}
        }

        let tasks = build_heartbeats(&active_leases).await;
        if tasks.is_empty() {
            continue;
        }

        let payload = HeartbeatPayload {
            agent_id: agent_id.to_owned(),
            tasks,
        };

        match gateway.heartbeat(payload).await {
            Ok(response) => {
                for directive in response.directives {
                    if directive.cancel_requested {
                        warn!(task_id = directive.task_id, "Received cancel directive");
                        let leases = active_leases.read().await;
                        if let Some(entry) = leases.get(&directive.task_id) {
                            entry.cancel.cancel();
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Heartbeat failed");
            }
        }
    }
}

async fn build_heartbeats(active_leases: &ActiveLeaseMap) -> Vec<TaskHeartbeat> {
    // Collect refs under the outer lock, then release before calling .latest()
    // to avoid holding the lease RwLock while acquiring the progress RwLock.
    let snapshot: Vec<_> = {
        let leases = active_leases.read().await;
        leases
            .iter()
            .map(|(task_id, entry)| {
                (
                    task_id.clone(),
                    entry.lease_epoch,
                    Arc::clone(&entry.progress),
                )
            })
            .collect()
    };

    snapshot
        .into_iter()
        .map(|(task_id, lease_epoch, progress)| TaskHeartbeat {
            task_id,
            lease_epoch,
            progress: progress.latest(),
        })
        .collect()
}
