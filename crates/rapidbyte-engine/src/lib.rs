//! Pipeline orchestration engine for Rapidbyte.
//!
//! Wires together config parsing, validation, connector runners,
//! and the state backend to execute data pipelines.
//!
//! # Crate structure
//!
//! | Module         | Responsibility |
//! |----------------|----------------|
//! | `config`       | Pipeline YAML config types, parsing, validation |
//! | `orchestrator` | Pipeline execution, retry, stream dispatch |
//! | `runner`       | Individual connector runners (source, dest, transform) |
//! | `error`        | Pipeline error types and retry policy |
//! | `checkpoint`   | Cursor correlation and persistence |
//! | `arrow`        | Arrow IPC encode/decode utilities |

#![warn(clippy::pedantic)]

pub mod arrow;
pub mod checkpoint;
pub mod config;
pub(crate) mod dlq;
pub mod error;
pub mod orchestrator;
pub mod result;
pub mod runner;

// Top-level re-exports for convenience.
pub use config::parser::parse_pipeline;
pub use config::types::PipelineConfig;
pub use config::validator::validate_pipeline;
pub use error::PipelineError;
pub use orchestrator::{check_pipeline, discover_connector, run_pipeline};
pub use result::{CheckResult, PipelineResult};
