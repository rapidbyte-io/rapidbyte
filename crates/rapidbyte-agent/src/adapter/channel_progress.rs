//! Concrete progress collector backed by `RwLock<ProgressSnapshot>`.
//!
//! The write-side (`update`) is used by the progress bridge in `execute_task`.
//! The read-side implements the `ProgressCollector` port for heartbeating.

use std::sync::RwLock;

use crate::domain::ports::progress::ProgressCollector;
use crate::domain::progress::ProgressSnapshot;

/// Thread-safe progress collector that stores the latest snapshot.
pub struct AtomicProgressCollector {
    snapshot: RwLock<ProgressSnapshot>,
}

impl Default for AtomicProgressCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicProgressCollector {
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot: RwLock::new(ProgressSnapshot::default()),
        }
    }

    /// Write a new progress snapshot (called by the bridge task).
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    pub fn update(&self, snapshot: ProgressSnapshot) {
        *self.snapshot.write().unwrap() = snapshot;
    }
}

impl ProgressCollector for AtomicProgressCollector {
    fn latest(&self) -> ProgressSnapshot {
        self.snapshot.read().unwrap().clone()
    }

    fn reset(&self) {
        *self.snapshot.write().unwrap() = ProgressSnapshot::default();
    }
}
