//! Core runtime/orchestration crate for Rapidbyte pipeline execution.

pub mod arrow;
pub mod checkpoint;
pub(crate) mod dlq;
pub mod errors;
pub mod orchestrator;
pub mod config;
pub mod runner;

// Re-export public API for convenience
pub use errors::PipelineError;
pub use orchestrator::{check_pipeline, discover_connector, run_pipeline};
pub use runner::{CheckResult, PipelineResult};
