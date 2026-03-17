//! Lightweight progress events emitted during pipeline execution.

/// Newtype wrapper around the progress channel sender.
#[derive(Clone)]
pub struct ProgressSender(Option<tokio::sync::mpsc::UnboundedSender<ProgressEvent>>);

impl ProgressSender {
    /// Create from an optional channel sender.
    #[must_use]
    pub fn new(tx: Option<tokio::sync::mpsc::UnboundedSender<ProgressEvent>>) -> Self {
        Self(tx)
    }

    /// Emit a progress event. No-op if no channel is connected.
    pub fn emit(&self, event: ProgressEvent) {
        if let Some(tx) = &self.0 {
            let _ = tx.send(event);
        }
    }
}

/// Execution phase of the pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase {
    Resolving,
    Loading,
    Running,
    Finished,
}

/// Progress event sent from engine to CLI during execution.
#[derive(Debug, Clone)]
pub enum ProgressEvent {
    /// Pipeline entered a new execution phase.
    PhaseChange { phase: Phase },
    /// A batch was emitted by the source plugin.
    BatchEmitted { records: u64, bytes: u64 },
    /// A stream finished processing.
    StreamCompleted { stream: String },
    /// A retryable error occurred; pipeline will retry after delay.
    Retry {
        attempt: u32,
        max_retries: u32,
        message: String,
        delay_secs: f64,
    },
}
