//! Domain model: port traits for the engine's hexagonal architecture.
//!
//! All external dependencies (plugin runtime, state backend, metrics) are
//! expressed as trait interfaces here so the core orchestration logic can
//! be tested in isolation.

pub mod error;
pub mod outcome;
pub mod ports;
pub mod progress;
pub mod retry;
