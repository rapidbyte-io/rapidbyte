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
    pub run_id: String,
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
    let leases = active_leases.read().await;
    leases
        .iter()
        .map(|(task_id, entry)| TaskHeartbeat {
            task_id: task_id.clone(),
            lease_epoch: entry.lease_epoch,
            progress: entry.progress.latest(),
        })
        .collect()
}
