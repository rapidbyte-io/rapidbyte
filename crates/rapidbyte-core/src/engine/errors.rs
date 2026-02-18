use std::time::Duration;

use rapidbyte_sdk::errors::{BackoffClass, ConnectorErrorV1};

// ---------------------------------------------------------------------------
// PipelineError — categorised errors for retry decisions
// ---------------------------------------------------------------------------

/// Categorized pipeline error for retry decisions.
///
/// `Connector` wraps a typed `ConnectorErrorV1` with retry metadata
/// (`retryable`, `backoff_class`, `retry_after_ms`, etc.).
///
/// `Infrastructure` wraps opaque host-side errors (WASM load failures,
/// channel errors, state backend issues, etc.) that are never retryable
/// at the connector level.
#[derive(Debug)]
pub enum PipelineError {
    /// Typed connector error with retry metadata.
    Connector(ConnectorErrorV1),
    /// Infrastructure error (WASM load, channel, state backend, etc.)
    Infrastructure(anyhow::Error),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connector(e) => write!(f, "{}", e),
            Self::Infrastructure(e) => write!(f, "{}", e),
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
    /// Returns `true` if this is a typed connector error that the connector
    /// has marked as retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connector(e) => e.retryable,
            Self::Infrastructure(_) => false,
        }
    }

    /// Returns the typed connector error if this is a `Connector` variant.
    pub fn as_connector_error(&self) -> Option<&ConnectorErrorV1> {
        match self {
            Self::Connector(e) => Some(e),
            Self::Infrastructure(_) => None,
        }
    }
}

/// Compute retry delay based on error hints and attempt number.
pub(crate) fn compute_backoff(err: &ConnectorErrorV1, attempt: u32) -> Duration {
    // If connector specified a retry_after, use it
    if let Some(ms) = err.retry_after_ms {
        return Duration::from_millis(ms);
    }

    // Exponential backoff based on backoff_class
    let base_ms: u64 = match err.backoff_class {
        BackoffClass::Fast => 100,
        BackoffClass::Normal => 1000,
        BackoffClass::Slow => 5000,
    };

    let delay_ms = base_ms.saturating_mul(2u64.pow(attempt.saturating_sub(1)));
    let max_ms: u64 = 60_000; // cap at 60s
    Duration::from_millis(delay_ms.min(max_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::errors::ErrorCategory;

    // -----------------------------------------------------------------------
    // PipelineError tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_pipeline_error_connector_is_retryable() {
        let err = PipelineError::Connector(ConnectorErrorV1::transient_network(
            "CONN_RESET",
            "connection reset by peer",
        ));
        assert!(err.is_retryable());
        let ce = err.as_connector_error().unwrap();
        assert_eq!(ce.category, ErrorCategory::TransientNetwork);
        assert_eq!(ce.backoff_class, BackoffClass::Normal);
    }

    #[test]
    fn test_pipeline_error_connector_not_retryable() {
        let err = PipelineError::Connector(ConnectorErrorV1::config(
            "MISSING_HOST",
            "host is required",
        ));
        assert!(!err.is_retryable());
        let ce = err.as_connector_error().unwrap();
        assert_eq!(ce.category, ErrorCategory::Config);
    }

    #[test]
    fn test_pipeline_error_infrastructure_not_retryable() {
        let err = PipelineError::Infrastructure(anyhow::anyhow!("WASM module load failed"));
        assert!(!err.is_retryable());
        assert!(err.as_connector_error().is_none());
    }

    #[test]
    fn test_pipeline_error_from_anyhow() {
        let anyhow_err = anyhow::anyhow!("something went wrong");
        let pe: PipelineError = anyhow_err.into();
        assert!(matches!(pe, PipelineError::Infrastructure(_)));
        assert!(!pe.is_retryable());
    }

    #[test]
    fn test_pipeline_error_display_connector() {
        let err = PipelineError::Connector(ConnectorErrorV1::rate_limit(
            "TOO_MANY",
            "slow down",
            Some(5000),
        ));
        let msg = format!("{}", err);
        assert!(msg.contains("rate_limit"));
        assert!(msg.contains("TOO_MANY"));
        assert!(msg.contains("retryable"));
    }

    #[test]
    fn test_pipeline_error_display_infrastructure() {
        let err = PipelineError::Infrastructure(anyhow::anyhow!("Store::new failed"));
        let msg = format!("{}", err);
        assert!(msg.contains("Store::new failed"));
    }

    // -----------------------------------------------------------------------
    // compute_backoff tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_backoff_fast() {
        let mut err = ConnectorErrorV1::transient_network("X", "y");
        err.backoff_class = BackoffClass::Fast;
        assert_eq!(compute_backoff(&err, 1), Duration::from_millis(100));
        assert_eq!(compute_backoff(&err, 2), Duration::from_millis(200));
        assert_eq!(compute_backoff(&err, 3), Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_normal() {
        let err = ConnectorErrorV1::transient_network("X", "y");
        assert_eq!(compute_backoff(&err, 1), Duration::from_millis(1000));
        assert_eq!(compute_backoff(&err, 2), Duration::from_millis(2000));
    }

    #[test]
    fn test_backoff_slow() {
        let err = ConnectorErrorV1::rate_limit("X", "y", None);
        assert_eq!(compute_backoff(&err, 1), Duration::from_millis(5000));
        assert_eq!(compute_backoff(&err, 2), Duration::from_millis(10000));
    }

    #[test]
    fn test_backoff_respects_retry_after() {
        let err = ConnectorErrorV1::rate_limit("X", "y", Some(7500));
        assert_eq!(compute_backoff(&err, 1), Duration::from_millis(7500));
        assert_eq!(compute_backoff(&err, 5), Duration::from_millis(7500));
    }

    #[test]
    fn test_backoff_capped_at_60s() {
        let err = ConnectorErrorV1::transient_db("X", "y");
        assert_eq!(compute_backoff(&err, 20), Duration::from_millis(60_000));
    }
}
