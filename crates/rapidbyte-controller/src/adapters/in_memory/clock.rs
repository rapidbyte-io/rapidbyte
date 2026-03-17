//! In-memory/system clock adapter.

#[derive(Debug, Default)]
pub(crate) struct SystemClock;

impl crate::ports::clock::Clock for SystemClock {}
