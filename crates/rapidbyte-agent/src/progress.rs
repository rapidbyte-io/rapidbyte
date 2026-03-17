//! `ProgressEvent` to v2 `AgentMessage::Progress` forwarding.

use rapidbyte_engine::progress::ProgressEvent;
use tokio::sync::mpsc;
use tracing::warn;

use crate::proto::rapidbyte::v2::agent_message;
use crate::proto::rapidbyte::v2::{AgentMessage, Progress};

/// Forward engine progress events to the controller session.
///
/// Runs until the receiver is closed (engine finished).
pub async fn forward_progress(
    mut rx: mpsc::UnboundedReceiver<ProgressEvent>,
    session_outbound: mpsc::UnboundedSender<AgentMessage>,
    agent_id: String,
    task_id: String,
    lease_epoch: u64,
) {
    while let Some(event) = rx.recv().await {
        let (records, bytes) = match event {
            ProgressEvent::BatchEmitted { records, bytes } => (records, bytes),
            ProgressEvent::StreamCompleted { .. } | ProgressEvent::PhaseChange { .. } => (0, 0),
            ProgressEvent::Retry { .. } => continue,
        };

        let message = AgentMessage {
            agent_id: agent_id.clone(),
            payload: Some(agent_message::Payload::Progress(Progress {
                task_id: task_id.clone(),
                lease_epoch,
                records,
                bytes,
            })),
        };
        if session_outbound.send(message).is_err() {
            warn!("Failed to send progress on controller session stream");
            break;
        }
    }
}
