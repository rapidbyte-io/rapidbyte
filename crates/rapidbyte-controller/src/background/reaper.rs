//! Agent reaper background task.

use std::time::Duration;

use tracing::info;

use crate::state::ControllerState;

pub fn spawn_reaper(
    state: ControllerState,
    interval: Duration,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval_timer = tokio::time::interval(interval);
        loop {
            interval_timer.tick().await;
            let dead = state.registry.write().await.reap_dead(timeout);
            for agent_id in &dead {
                if let Err(error) = state.delete_agent(agent_id).await {
                    tracing::error!(agent_id, ?error, "failed to delete reaped agent");
                }
                info!(agent_id, "Reaped dead agent");
                rapidbyte_metrics::instruments::controller::heartbeat_timeouts().add(1, &[]);
                rapidbyte_metrics::instruments::controller::active_agents().add(-1, &[]);
            }
        }
    })
}
