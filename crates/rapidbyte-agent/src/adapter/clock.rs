//! System clock adapter.

use std::time::Instant;

use crate::domain::ports::clock::Clock;

/// Real monotonic clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}
