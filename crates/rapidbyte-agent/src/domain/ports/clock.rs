//! Time abstraction for testability.

use std::time::Instant;

/// Monotonic clock for backoff and interval timing.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}
