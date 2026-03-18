//! Legacy pipeline error model — used internally by the runner and plugin modules.
//!
//! The canonical domain error is [`crate::domain::error::PipelineError`].
//! This module exists solely because the WASM runner functions return this
//! type, and the adapter layer converts it to the domain error.

use rapidbyte_types::error::PluginError;

// ---------------------------------------------------------------------------
// PipelineError — categorised errors for retry decisions
// ---------------------------------------------------------------------------

/// Categorized pipeline error for retry decisions.
///
/// `Plugin` wraps a typed `PluginError` with retry metadata
/// (`retryable`, `backoff_class`, `retry_after_ms`, etc.).
///
/// `Infrastructure` wraps opaque host-side errors (WASM load failures,
/// channel errors, state backend issues, etc.) that are never retryable
/// at the plugin level.
#[derive(Debug)]
pub enum PipelineError {
    /// Typed plugin error with retry metadata.
    Plugin(PluginError),
    /// Infrastructure error (WASM load, channel, state backend, etc.)
    Infrastructure(anyhow::Error),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plugin(e) => write!(f, "{e}"),
            Self::Infrastructure(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for PipelineError {}

impl From<anyhow::Error> for PipelineError {
    fn from(e: anyhow::Error) -> Self {
        Self::Infrastructure(e)
    }
}

impl PipelineError {
    /// Shorthand for Infrastructure variant from a string message.
    pub fn infra(msg: impl Into<String>) -> Self {
        Self::Infrastructure(anyhow::anyhow!("{}", msg.into()))
    }

    /// Lock a mutex, converting poison errors to Infrastructure errors.
    ///
    /// # Errors
    ///
    /// Returns a `PipelineError::Infrastructure` if the mutex is poisoned.
    pub fn lock_or_infra<'a, T>(
        mutex: &'a std::sync::Mutex<T>,
        name: &str,
    ) -> Result<std::sync::MutexGuard<'a, T>, PipelineError> {
        mutex
            .lock()
            .map_err(|_| PipelineError::infra(format!("{name} mutex poisoned")))
    }

    /// Cancelled pipeline error with safe-to-retry metadata.
    #[must_use]
    pub fn cancelled(message: &str) -> Self {
        use rapidbyte_types::error::CommitState;
        let mut error = PluginError::internal("CANCELLED", message);
        error.retryable = true;
        error.safe_to_retry = true;
        error.commit_state = Some(CommitState::BeforeCommit);
        Self::Plugin(error)
    }
}
