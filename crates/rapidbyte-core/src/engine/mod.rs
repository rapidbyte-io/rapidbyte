pub mod checkpoint;
pub(crate) mod compression;
pub mod errors;
pub mod orchestrator;
pub mod runner;

// Re-export public API for convenience
pub use errors::PipelineError;
pub use orchestrator::{check_pipeline, discover_connector, run_pipeline};
pub use runner::{CheckResult, PipelineResult};
