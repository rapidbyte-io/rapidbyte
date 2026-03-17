//! In-memory/system clock adapter.

#[derive(Debug, Default)]
pub struct SystemClock;

impl crate::ports::clock::Clock for SystemClock {}
