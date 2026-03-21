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
//! | `domain`           | Port traits, domain errors, outcomes, progress, retry |

#![warn(clippy::pedantic)]

pub mod adapter;
pub mod application;
pub mod domain;

// ---------------------------------------------------------------------------
// Public re-exports — canonical API surface
// ---------------------------------------------------------------------------

// Application layer
pub use application::check::check_pipeline;
pub use application::context::{EngineConfig, EngineContext};
pub use application::discover::discover_plugin;
pub use application::run::run_pipeline;
pub use application::teardown::teardown_pipeline;

// Domain errors
pub use domain::error::PipelineError;

// Domain outcomes
pub use domain::outcome::{
    CheckResult, CheckStatus, DestTiming, PipelineCounts, PipelineResult, SourceTiming,
    StreamNegotiationResult, StreamShardMetric,
};

// Domain progress
pub use domain::progress::{Phase, ProgressEvent, ProgressReporter};

// Domain port traits
pub use domain::ports::{
    CursorRepository, DlqRepository, MetricsSnapshot, PluginResolver, PluginRunner,
    RepositoryError, RunRecordRepository,
};

// Adapter implementations
pub use adapter::engine_factory::{
    build_discover_context, build_lightweight_context, build_run_context,
};
pub use adapter::metrics::OtelMetricsSnapshot;
pub use adapter::postgres::PgBackend;
pub use adapter::progress::ChannelProgressReporter;
pub use adapter::registry_resolver::RegistryPluginResolver;
pub use adapter::wasm_runner::WasmPluginRunner;
