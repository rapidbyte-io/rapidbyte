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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::context::AgentAppConfig;
    use crate::application::testing::{FakeControllerGateway, FakeProgressCollector};
    use crate::domain::ports::controller::{HeartbeatResponse, TaskDirective};
    use std::time::Duration;

    fn test_config() -> AgentAppConfig {
        AgentAppConfig {
            heartbeat_interval: Duration::from_millis(10),
            ..AgentAppConfig::default()
        }
    }

    /// Yield control to the runtime repeatedly so spawned tasks can make progress.
    async fn yield_many(n: usize) {
        for _ in 0..n {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_forwards_progress_to_gateway() {
        let gateway = Arc::new(FakeControllerGateway::new());
        let config = test_config();
        let shutdown = CancellationToken::new();
        let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));

        // Set up a lease with a progress message.
        let progress = Arc::new(FakeProgressCollector::new());
        progress.set_message("processing (1024 bytes)");

        active_leases.write().await.insert(
            "task-1".into(),
            LeaseEntry {
                lease_epoch: 5,
                cancel: CancellationToken::new(),
                progress: progress as Arc<dyn ProgressCollector>,
            },
        );

        // Enqueue a heartbeat response (empty directives).
        gateway.enqueue_heartbeat(Ok(HeartbeatResponse { directives: vec![] }));

        let gw = gateway.clone();
        let leases = active_leases.clone();
        let sd = shutdown.clone();
        let handle = tokio::spawn(async move {
            heartbeat_loop(gw.as_ref(), "agent-1", &config, leases, sd).await;
        });

        // Advance past one heartbeat interval to trigger the first tick and let
        // the spawned task process the heartbeat RPC through multiple await points.
        tokio::time::advance(Duration::from_millis(15)).await;
        yield_many(20).await;

        shutdown.cancel();
        handle.await.unwrap();

        let payloads = gateway.heartbeat_payloads();
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0].agent_id, "agent-1");
        assert_eq!(payloads[0].tasks.len(), 1);
        assert_eq!(payloads[0].tasks[0].task_id, "task-1");
        assert_eq!(payloads[0].tasks[0].lease_epoch, 5);
        assert_eq!(
            payloads[0].tasks[0].progress.message.as_deref(),
            Some("processing (1024 bytes)")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_cancels_task_on_directive() {
        let gateway = Arc::new(FakeControllerGateway::new());
        let config = test_config();
        let shutdown = CancellationToken::new();
        let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));

        let task_cancel = CancellationToken::new();
        let progress = Arc::new(FakeProgressCollector::new());

        active_leases.write().await.insert(
            "task-42".into(),
            LeaseEntry {
                lease_epoch: 1,
                cancel: task_cancel.clone(),
                progress: progress as Arc<dyn ProgressCollector>,
            },
        );

        // Heartbeat response with a cancel directive for task-42.
        gateway.enqueue_heartbeat(Ok(HeartbeatResponse {
            directives: vec![TaskDirective {
                task_id: "task-42".into(),
                cancel_requested: true,
            }],
        }));

        let gw = gateway.clone();
        let leases = active_leases.clone();
        let sd = shutdown.clone();
        let handle = tokio::spawn(async move {
            heartbeat_loop(gw.as_ref(), "agent-1", &config, leases, sd).await;
        });

        // Advance time to trigger the first tick and let the spawned task process
        // the heartbeat response (including the cancel directive).
        tokio::time::advance(Duration::from_millis(15)).await;
        yield_many(20).await;

        shutdown.cancel();
        handle.await.unwrap();

        assert!(task_cancel.is_cancelled());
    }

    #[tokio::test]
    async fn heartbeat_stops_on_shutdown() {
        let gateway = Arc::new(FakeControllerGateway::new());
        let config = test_config();
        let shutdown = CancellationToken::new();
        let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));

        // Cancel before starting — the loop should exit immediately.
        shutdown.cancel();

        heartbeat_loop(
            gateway.as_ref(),
            "agent-1",
            &config,
            active_leases,
            shutdown,
        )
        .await;

        // No heartbeat calls should have been made.
        assert!(gateway.heartbeat_payloads().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_skips_rpc_when_no_leases() {
        let gateway = Arc::new(FakeControllerGateway::new());
        let config = test_config();
        let shutdown = CancellationToken::new();
        let active_leases: ActiveLeaseMap = Arc::new(RwLock::new(HashMap::new()));
        // No leases, no heartbeat results enqueued — the loop should skip the RPC.

        let gw = gateway.clone();
        let leases = active_leases.clone();
        let sd = shutdown.clone();
        let handle = tokio::spawn(async move {
            heartbeat_loop(gw.as_ref(), "agent-1", &config, leases, sd).await;
        });

        // Advance past one tick.
        tokio::time::advance(Duration::from_millis(15)).await;
        yield_many(20).await;

        shutdown.cancel();
        handle.await.unwrap();

        // No heartbeat calls — RPC was skipped because there are no active leases.
        assert!(gateway.heartbeat_payloads().is_empty());
    }
}
