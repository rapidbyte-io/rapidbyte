//! Outbound port for progress collection.

use crate::domain::progress::ProgressSnapshot;

/// Read-side interface for collecting progress from a running task.
///
/// The heartbeat loop reads the latest snapshot; the execute use-case
/// resets it between tasks.
pub trait ProgressCollector: Send + Sync {
    /// Get the latest progress snapshot for heartbeating.
    fn latest(&self) -> ProgressSnapshot;

    /// Take the latest progress snapshot, replacing it with an empty one.
    ///
    /// Used by the heartbeat loop to consume progress so unchanged
    /// snapshots are not re-sent on subsequent ticks.
    fn take(&self) -> ProgressSnapshot;

    /// Write a progress snapshot (used by the bridge task and heartbeat restore).
    fn update(&self, snapshot: ProgressSnapshot);

    /// Reset progress (when starting a new task).
    fn reset(&self);
}
