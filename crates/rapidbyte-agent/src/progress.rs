//! Progress event collection for heartbeat-based reporting.
//!
//! The `ReportProgress` RPC has been removed from the proto.  Progress is now
//! sent to the controller as part of the periodic heartbeat via
//! `TaskHeartbeat.progress_message` / `TaskHeartbeat.progress_pct`.
//!
//! This module collects the latest progress event from the engine and exposes
//! it for the heartbeat loop to include in the next heartbeat.

use rapidbyte_engine::progress::ProgressEvent;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

/// Latest progress snapshot for a running task, included in the next heartbeat.
#[derive(Debug, Clone, Default)]
pub struct ProgressSnapshot {
    pub message: Option<String>,
    pub progress_pct: Option<f64>,
}

/// Collect engine progress events and maintain a latest snapshot.
///
/// Runs until the receiver is closed (engine finished).
pub async fn collect_progress(
    mut rx: mpsc::UnboundedReceiver<ProgressEvent>,
    snapshot: Arc<RwLock<ProgressSnapshot>>,
) {
    while let Some(event) = rx.recv().await {
        let update = match &event {
            ProgressEvent::BatchEmitted { bytes } => ProgressSnapshot {
                message: Some(format!("processing ({bytes} bytes)")),
                progress_pct: None,
            },
            ProgressEvent::StreamCompleted { stream } => ProgressSnapshot {
                message: Some(format!("stream {stream} completed")),
                progress_pct: None,
            },
            ProgressEvent::PhaseChange { phase } => ProgressSnapshot {
                message: Some(format!("{phase:?}").to_lowercase()),
                progress_pct: None,
            },
            ProgressEvent::Retry { .. } => continue,
        };
        *snapshot.write().await = update;
    }
}
