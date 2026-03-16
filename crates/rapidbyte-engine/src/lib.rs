//! Pipeline orchestration engine for Rapidbyte.
//!
//! Wires together config parsing, validation, plugin runners,
//! and the state backend to execute data pipelines.
//!
//! # Crate structure
//!
//! | Module             | Responsibility |
//! |--------------------|----------------|
//! | `arrow`            | Arrow IPC encode/decode utilities |
//! | `config`           | Pipeline YAML config types, parsing, validation |
//! | `error`            | Pipeline error types, retry policy, convenience constructors |
//! | `execution`        | Runtime execution mode types (dry-run, limits) |
//! | `finalizers`       | Post-execution: checkpoint correlation, DLQ, run finalization |
//! | `orchestrator`     | Top-level pipeline coordination (run, check, discover) |
//! | `pipeline`         | Pipeline execution: planning, autotune, scheduling, stream execution |
//! | `plugin`           | Plugin resolution, manifest validation, module loading |
//! | `progress`         | Progress event types and ProgressSender for live CLI updates |
//! | `result`           | Pipeline result types, timing breakdowns |
//! | `runner`           | Per-kind plugin runners (source, dest, transform, validate) |

#![warn(clippy::pedantic)]

pub mod arrow;
pub mod config;
pub mod error;
pub mod execution;
pub(crate) mod finalizers;
pub mod orchestrator;
pub(crate) mod pipeline;
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
pub use progress::{Phase, ProgressEvent, ProgressSender};
pub use result::{CheckResult, CheckStatus, PipelineResult};
