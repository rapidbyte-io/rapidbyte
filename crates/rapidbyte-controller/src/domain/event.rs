use serde::{Deserialize, Serialize};

use super::run::{RunError, RunMetrics, RunState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DomainEvent {
    RunStateChanged {
        run_id: String,
        state: RunState,
        attempt: u32,
    },
    ProgressReported {
        run_id: String,
        message: String,
        pct: Option<f64>,
    },
    RunCompleted {
        run_id: String,
        metrics: RunMetrics,
    },
    RunFailed {
        run_id: String,
        error: RunError,
    },
    RunCancelled {
        run_id: String,
    },
}
