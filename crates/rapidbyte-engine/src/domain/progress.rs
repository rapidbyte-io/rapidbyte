//! Progress events and reporting trait for pipeline execution.
//!
//! [`ProgressReporter`] is a port trait that decouples the engine from
//! specific progress delivery mechanisms (channels, callbacks, etc.).

use std::time::Duration;

/// Execution phase of the pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase {
    /// Resolving plugins and configuration.
    Resolving,
    /// Loading WASM modules.
    Loading,
    /// Executing the pipeline stages.
    Running,
    /// Finalizing results and cleanup.
    Finalizing,
}

/// Progress event emitted during pipeline execution.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// Pipeline entered a new execution phase.
    PhaseChanged { phase: Phase },
    /// A stream started processing.
    StreamStarted { stream: String },
    /// A batch was emitted by the source plugin.
    BatchEmitted { stream: String, rows: u64 },
    /// A stream finished processing.
    StreamCompleted { stream: String },
    /// A retryable error occurred; pipeline will retry after delay.
    RetryScheduled { attempt: u32, delay: Duration },
}

/// Trait for receiving progress events during pipeline execution.
///
/// Implementations might send events over a channel, log them, or
/// drive a progress bar.
pub trait ProgressReporter: Send + Sync {
    /// Report a progress event. Implementations should be non-blocking.
    fn report(&self, event: ProgressEvent);
}
