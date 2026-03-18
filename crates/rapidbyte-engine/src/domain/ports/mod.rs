//! Port traits for the engine's hexagonal architecture.
//!
//! Each module defines a trait representing an external dependency.
//! The orchestrator depends on these traits; concrete implementations
//! live in the adapter layer.
//!
//! | Module        | Trait                  | Purpose |
//! |---------------|------------------------|---------|
//! | `cursor`      | `CursorRepository`     | Incremental sync cursor persistence |
//! | `dlq`         | `DlqRepository`        | Dead-letter queue record persistence |
//! | `metrics`     | `MetricsSnapshot`      | Point-in-time pipeline metrics |
//! | `resolver`    | `PluginResolver`       | Plugin reference resolution |
//! | `run_record`  | `RunRecordRepository`  | Run lifecycle persistence |
//! | `runner`      | `PluginRunner`         | Plugin execution (source/transform/dest) |

pub mod cursor;
pub mod dlq;
pub mod metrics;
pub mod resolver;
pub mod run_record;
pub mod runner;

// ---------------------------------------------------------------------------
// Shared error type for repository ports
// ---------------------------------------------------------------------------

/// Error type shared by all repository port traits.
///
/// Mirrors the controller pattern: a typed `Conflict` variant for
/// expected concurrency races, and an opaque `Other` variant for
/// unexpected storage failures.
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    /// A state conflict occurred (e.g., row was concurrently modified).
    #[error("conflict: {0}")]
    Conflict(String),
    /// Any other storage error.
    #[error("{0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl RepositoryError {
    /// Returns `true` if this is a state conflict (expected race),
    /// not a real failure.
    #[must_use]
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict(_))
    }

    /// Wrap an arbitrary error into the `Other` variant.
    pub fn other(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Other(Box::new(err))
    }
}

// ---------------------------------------------------------------------------
// Re-exports for convenience
// ---------------------------------------------------------------------------

pub use cursor::CursorRepository;
pub use dlq::DlqRepository;
pub use metrics::MetricsSnapshot;
pub use resolver::{PluginResolver, ResolvedPlugin};
pub use run_record::RunRecordRepository;
pub use runner::{
    CheckComponentStatus, DestinationOutcome, DestinationRunParams, DiscoverParams,
    DiscoveredStream, PluginRunner, SourceOutcome, SourceRunParams, TransformOutcome,
    TransformRunParams, ValidateParams,
};
