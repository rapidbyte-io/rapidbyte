//! Engine layer: orchestration, connector runners, checkpoints, and error policy.

pub mod checkpoint;
pub(crate) mod compression;
pub(crate) mod dlq;
pub mod errors;
pub mod orchestrator;
pub mod runner;

// Re-export public API for convenience
pub use errors::PipelineError;
pub use orchestrator::{check_pipeline, discover_connector, run_pipeline};
pub use runner::{CheckResult, PipelineResult};
