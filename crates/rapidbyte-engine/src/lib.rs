//! Pipeline orchestration engine for Rapidbyte.
//!
//! Wires together config parsing, validation, plugin runners,
//! and the state backend to execute data pipelines.
//!
//! # Crate structure
//!
//! | Module             | Responsibility |
//! |--------------------|----------------|
//! | `adapter`          | Concrete adapter implementations for port traits |
//! | `application`      | DI context, use-case orchestration, testing fakes |
//! | `arrow`            | Arrow IPC encode/decode utilities |
//! | `config`           | Pipeline YAML config types, parsing, validation |
//! | `domain`           | Port traits, domain errors, outcomes, progress, retry |
//! | `error`            | (legacy) Pipeline error types, retry policy, convenience constructors |
//! | `finalizers`       | (legacy) Post-execution: checkpoint correlation, DLQ, run finalization |
//! | `orchestrator`     | (legacy) Top-level pipeline coordination (run, check, discover) |
//! | `outcome`          | (legacy) Pipeline operation types: results, timings, options, check statuses |
//! | `pipeline`         | (legacy) Pipeline execution: planning, scheduling, stream execution |
//! | `plugin`           | (legacy) Plugin resolution, manifest validation, module loading |
//! | `progress`         | (legacy) Progress event types and ProgressSender for live CLI updates |
//! | `runner`           | (legacy) Per-kind plugin runners (source, dest, transform, validate) |

#![warn(clippy::pedantic)]

pub mod adapter;
pub mod application;
pub mod arrow;
pub mod config;
pub mod domain;
pub mod error;
pub(crate) mod finalizers;
pub mod orchestrator;
pub mod outcome;
pub(crate) mod pipeline;
pub mod plugin;
pub mod progress;
pub mod runner;

// ---------------------------------------------------------------------------
// Legacy top-level re-exports (will be removed during cleanup)
// ---------------------------------------------------------------------------
pub use config::parser::parse_pipeline;
pub use config::types::PipelineConfig;
pub use config::validator::validate_pipeline;
pub use error::PipelineError;
pub use orchestrator::{check_pipeline, discover_plugin, run_pipeline};
pub use outcome::{
    CheckResult, CheckStatus, DryRunResult, DryRunStreamResult, ExecutionOptions, PipelineOutcome,
    PipelineResult,
};
pub use progress::{Phase, ProgressEvent, ProgressSender};

// ---------------------------------------------------------------------------
// New hexagonal API re-exports
// ---------------------------------------------------------------------------

// Application layer
pub use application::check::check_pipeline as domain_check_pipeline;
pub use application::context::{EngineConfig, EngineContext};
pub use application::discover::discover_plugin as domain_discover_plugin;
pub use application::run::run_pipeline as domain_run_pipeline;

// Domain errors (aliased to avoid conflict with legacy PipelineError)
pub use domain::error::PipelineError as DomainPipelineError;

// Domain outcomes (aliased to avoid conflict with legacy outcome types)
pub use domain::outcome::{
    CheckResult as DomainCheckResult, ExecutionOptions as DomainExecutionOptions,
    PipelineOutcome as DomainPipelineOutcome, PipelineResult as DomainPipelineResult,
};

// Domain progress (aliased to avoid conflict with legacy Phase / ProgressEvent)
pub use domain::progress::{
    Phase as DomainPhase, ProgressEvent as DomainProgressEvent, ProgressReporter,
};

// Domain port traits
pub use domain::ports::{
    CursorRepository, DlqRepository, MetricsSnapshot, PluginResolver, PluginRunner,
    RepositoryError, RunRecordRepository,
};
