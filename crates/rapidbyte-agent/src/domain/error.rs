//! Agent-level error types.

/// Errors that can occur during agent operations.
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    /// Pipeline YAML could not be parsed or validated.
    #[error("invalid pipeline: {0}")]
    InvalidPipeline(String),

    /// Pipeline execution failed with an infrastructure error.
    #[error("execution failed: {0}")]
    ExecutionFailed(#[from] anyhow::Error),

    /// Controller communication failed.
    #[error("controller error: {0}")]
    Controller(String),

    /// Task was cancelled before or during execution.
    #[error("cancelled")]
    Cancelled,
}
