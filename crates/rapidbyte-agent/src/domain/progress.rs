//! Progress snapshot for heartbeat reporting.

/// Latest progress snapshot for a running task, included in the next heartbeat.
#[derive(Debug, Clone, Default)]
pub struct ProgressSnapshot {
    pub message: Option<String>,
    pub progress_pct: Option<f64>,
}
