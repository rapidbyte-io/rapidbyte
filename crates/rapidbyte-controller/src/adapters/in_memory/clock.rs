//! In-memory/system clock adapter.

#[derive(Debug, Default)]
#[allow(dead_code)]
pub(crate) struct SystemClock;

impl crate::ports::clock::Clock for SystemClock {}
