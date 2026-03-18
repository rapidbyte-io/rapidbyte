//! Domain model: port traits for the engine's hexagonal architecture.
//!
//! All external dependencies (plugin runtime, state backend, metrics) are
//! expressed as trait interfaces here so the core orchestration logic can
//! be tested in isolation.

pub mod ports;
