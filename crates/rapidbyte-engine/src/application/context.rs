//! DI container and configuration for the engine application layer.
//!
//! [`EngineContext`] bundles all port trait objects that the orchestrator
//! needs, so callers wire up concrete adapters once and pass a single
//! context into use-case functions.

use std::sync::Arc;

use crate::domain::ports::*;
use crate::domain::progress::ProgressReporter;

/// Tunable engine knobs that are not per-pipeline.
pub struct EngineConfig {
    /// Maximum number of automatic retries for transient plugin errors.
    pub max_retries: u32,
    /// Capacity of the `mpsc` channel between pipeline stages.
    pub channel_capacity: usize,
}

/// Application-level dependency container.
///
/// Holds `Arc`-wrapped trait objects for every external dependency the
/// engine orchestrator needs. Constructed once at startup and shared
/// (immutably) across all pipeline runs.
pub struct EngineContext {
    /// Plugin execution (source / transform / destination / validate / discover).
    pub runner: Arc<dyn PluginRunner>,
    /// Plugin resolution (OCI registry, filesystem).
    pub resolver: Arc<dyn PluginResolver>,
    /// Incremental-sync cursor persistence.
    pub cursors: Arc<dyn CursorRepository>,
    /// Run lifecycle persistence.
    pub runs: Arc<dyn RunRecordRepository>,
    /// Dead-letter queue persistence.
    pub dlq: Arc<dyn DlqRepository>,
    /// Progress event delivery.
    pub progress: Arc<dyn ProgressReporter>,
    /// Point-in-time pipeline metrics.
    pub metrics: Arc<dyn MetricsSnapshot>,
    /// Engine configuration knobs.
    pub config: EngineConfig,
}
