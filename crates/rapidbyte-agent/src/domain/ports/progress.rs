//! Outbound port for progress collection.

use crate::domain::progress::ProgressSnapshot;

/// Read-side interface for collecting progress from a running task.
///
/// The heartbeat loop reads the latest snapshot; the execute use-case
/// resets it between tasks.
pub trait ProgressCollector: Send + Sync {
    /// Get the latest progress snapshot for heartbeating.
    fn latest(&self) -> ProgressSnapshot;

    /// Reset progress (when starting a new task).
    fn reset(&self);
}
