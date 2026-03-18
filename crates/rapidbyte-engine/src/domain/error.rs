//! Domain error types for pipeline operations.
//!
//! [`PipelineError`] classifies errors into plugin, infrastructure, and
//! cancellation variants. Plugin errors carry retry metadata extracted
//! via [`RetryParams`].

use std::time::Duration;

use rapidbyte_types::error::{BackoffClass, CommitState, PluginError};
use thiserror::Error;

/// Categorized pipeline error for retry decisions.
///
/// - `Plugin` wraps a typed [`PluginError`] with retry metadata.
/// - `Infrastructure` wraps opaque host-side errors (never retryable).
/// - `Cancelled` signals a graceful pipeline cancellation.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// Typed plugin error with retry metadata.
    #[error("plugin error: {0}")]
    Plugin(PluginError),

    /// Infrastructure error (WASM load, channel, state backend, etc.).
    #[error("infrastructure error: {0}")]
    Infrastructure(#[from] anyhow::Error),

    /// Pipeline was cancelled.
    #[error("pipeline cancelled")]
    Cancelled,
}

impl PipelineError {
    /// Shorthand for an `Infrastructure` variant from a string message.
    pub fn infra(msg: impl Into<String>) -> Self {
        Self::Infrastructure(anyhow::anyhow!("{}", msg.into()))
    }

    /// Returns the typed plugin error if this is a `Plugin` variant.
    #[must_use]
    pub fn plugin_error(&self) -> Option<&PluginError> {
        match self {
            Self::Plugin(e) => Some(e),
            _ => None,
        }
    }

    /// Extract retry parameters from a plugin error.
    ///
    /// Returns `None` for infrastructure errors and cancellations.
    #[must_use]
    pub fn retry_params(&self) -> Option<RetryParams> {
        match self {
            Self::Plugin(e) => Some(RetryParams {
                retryable: e.retryable,
                safe_to_retry: e.safe_to_retry,
                backoff_class: e.category.default_backoff(),
                retry_after: e.retry_after_ms.map(Duration::from_millis),
                commit_state: e.commit_state,
            }),
            _ => None,
        }
    }
}

/// Retry parameters extracted from a [`PluginError`].
#[derive(Debug)]
pub struct RetryParams {
    /// Whether the error is retryable at all.
    pub retryable: bool,
    /// Whether it is safe to retry (no partial commit, etc.).
    pub safe_to_retry: bool,
    /// Backoff class derived from the error category.
    pub backoff_class: BackoffClass,
    /// Plugin-specified retry-after hint.
    pub retry_after: Option<Duration>,
    /// Transaction commit state at time of error.
    pub commit_state: Option<CommitState>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_types::error::PluginError;

    #[test]
    fn infra_error_is_not_retryable() {
        let err = PipelineError::infra("WASM module load failed");
        assert!(err.retry_params().is_none());
    }

    #[test]
    fn cancelled_is_not_retryable() {
        let err = PipelineError::Cancelled;
        assert!(err.retry_params().is_none());
    }

    #[test]
    fn plugin_error_extracts_retry_params() {
        let plugin_err = PluginError::transient_network("CONN_RESET", "connection reset");
        let err = PipelineError::Plugin(plugin_err);

        let params = err.retry_params().expect("should have retry params");
        assert!(params.retryable);
        assert!(params.safe_to_retry);
        assert_eq!(params.backoff_class, BackoffClass::Normal);
        assert!(params.retry_after.is_none());
        assert!(params.commit_state.is_none());
    }

    #[test]
    fn plugin_error_with_retry_after() {
        let plugin_err = PluginError::rate_limit("THROTTLED", "slow down", Some(5000));
        let err = PipelineError::Plugin(plugin_err);

        let params = err.retry_params().expect("should have retry params");
        assert!(params.retryable);
        assert_eq!(params.retry_after, Some(Duration::from_millis(5000)));
        assert_eq!(params.backoff_class, BackoffClass::Slow);
    }

    #[test]
    fn plugin_error_returns_reference() {
        let plugin_err = PluginError::config("BAD_HOST", "host invalid");
        let err = PipelineError::Plugin(plugin_err);

        let pe = err.plugin_error().expect("should have plugin error");
        assert_eq!(pe.code, "BAD_HOST");
    }

    #[test]
    fn infrastructure_has_no_plugin_error() {
        let err = PipelineError::infra("something broke");
        assert!(err.plugin_error().is_none());
    }

    #[test]
    fn infra_error_displays_message() {
        let err = PipelineError::infra("db down");
        assert!(err.to_string().contains("db down"));
    }

    #[test]
    fn cancelled_displays_message() {
        let err = PipelineError::Cancelled;
        assert_eq!(err.to_string(), "pipeline cancelled");
    }
}
