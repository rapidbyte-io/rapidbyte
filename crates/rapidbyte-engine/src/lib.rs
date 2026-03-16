//! Pipeline orchestration engine for Rapidbyte.
//!
//! Wires together config parsing, validation, plugin runners,
//! and the state backend to execute data pipelines.
//!
//! # Crate structure
//!
//! | Module         | Responsibility |
//! |----------------|----------------|
//! | `arrow`        | Arrow IPC encode/decode utilities |
//! | `config`       | Pipeline YAML config types, parsing, validation |
//! | `error`        | Pipeline error types and retry policy |
//! | `finalizers`   | Checkpoint correlation, DLQ persistence, run finalization (internal) |
//! | `execution`    | Runtime execution mode types (dry-run, limits) |
//! | `orchestrator` | Pipeline execution, retry, stream dispatch |
//! | `progress`     | Progress event types for live CLI updates |
//! | `plugin`       | Plugin lifecycle management: resolution, manifest validation, sandbox (internal) |
//! | `result`       | Pipeline execution result types and timing breakdowns |
//! | `runner`       | Individual plugin runners (source, dest, transform) |

#![warn(clippy::pedantic)]

pub mod arrow;
pub mod autotune;
pub mod config;
pub mod error;
pub mod execution;
pub(crate) mod finalizers;
pub mod orchestrator;
pub mod plugin;
pub mod progress;
pub mod result;
pub mod runner;

// Top-level re-exports for convenience.
pub use config::parser::parse_pipeline;
pub use config::types::PipelineConfig;
pub use config::validator::validate_pipeline;
pub use error::PipelineError;
pub use execution::{DryRunResult, DryRunStreamResult, ExecutionOptions, PipelineOutcome};
pub use orchestrator::{check_pipeline, discover_plugin, run_pipeline};
pub use progress::{Phase, ProgressEvent};
pub use result::{CheckItemResult, CheckResult, PipelineResult};
