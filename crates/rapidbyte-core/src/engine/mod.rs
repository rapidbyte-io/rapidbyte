pub mod checkpoint;
pub mod errors;
pub mod orchestrator;
pub mod runner;
pub mod vm_factory;

// Re-export public API for convenience
pub use errors::PipelineError;
pub use orchestrator::{check_pipeline, run_pipeline};
pub use runner::{CheckResult, PipelineResult};
