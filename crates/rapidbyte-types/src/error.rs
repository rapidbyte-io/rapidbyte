//! Structured error model for connector operations.
//!
//! [`ConnectorError`] carries classification, retry metadata, and optional
//! diagnostic details. Construct via category-specific factory methods.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Broad classification of a connector error.
///
/// Determines default retry behavior and operator-facing categorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    /// Invalid connector configuration.
    Config,
    /// Authentication failure.
    Auth,
    /// Insufficient permissions.
    Permission,
    /// Rate limit exceeded (retryable).
    RateLimit,
    /// Transient network error (retryable).
    TransientNetwork,
    /// Transient database error (retryable).
    TransientDb,
    /// Invalid or corrupt data.
    Data,
    /// Schema mismatch or incompatibility.
    Schema,
    /// Internal connector error.
    Internal,
    /// Frame lifecycle error (alloc/write/seal/read).
    Frame,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Config => "config",
            Self::Auth => "auth",
            Self::Permission => "permission",
            Self::RateLimit => "rate_limit",
            Self::TransientNetwork => "transient_network",
            Self::TransientDb => "transient_db",
            Self::Data => "data",
            Self::Schema => "schema",
            Self::Internal => "internal",
            Self::Frame => "frame",
        };
        f.write_str(s)
    }
}

/// Blast radius of an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorScope {
    /// Affects the entire stream.
    Stream,
    /// Affects a single batch.
    Batch,
    /// Affects an individual record.
    Record,
}

impl fmt::Display for ErrorScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Stream => "stream",
            Self::Batch => "batch",
            Self::Record => "record",
        };
        f.write_str(s)
    }
}

/// Retry backoff strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackoffClass {
    /// Millisecond-scale retry.
    Fast,
    /// Second-scale retry.
    Normal,
    /// Minute-scale retry.
    Slow,
}

/// Transaction commit state at the time of error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitState {
    /// Error occurred before any commit attempt.
    BeforeCommit,
    /// Commit was attempted but outcome is unknown.
    AfterCommitUnknown,
    /// Commit was confirmed successful before the error.
    AfterCommitConfirmed,
}

/// Validation check outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    Success,
    Failed,
    Warning,
}

/// Result of a connector validation check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationResult {
    pub status: ValidationStatus,
    pub message: String,
}

/// Structured error from a connector operation.
///
/// Carries classification, retry metadata, and optional diagnostic details.
/// Construct via category-specific factory methods (e.g., [`ConnectorError::config`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("[{category}] {code}: {message}")]
pub struct ConnectorError {
    pub category: ErrorCategory,
    pub scope: ErrorScope,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    pub backoff_class: BackoffClass,
    pub safe_to_retry: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_state: Option<CommitState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ConnectorError {
    fn new(
        category: ErrorCategory,
        scope: ErrorScope,
        retryable: bool,
        backoff_class: BackoffClass,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            category,
            scope,
            code: code.into(),
            message: message.into(),
            retryable,
            retry_after_ms: None,
            backoff_class,
            safe_to_retry: retryable,
            commit_state: None,
            details: None,
        }
    }

    /// Configuration error (not retryable).
    #[must_use]
    pub fn config(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Config, ErrorScope::Stream, false, BackoffClass::Normal, code, message)
    }

    /// Authentication error (not retryable).
    #[must_use]
    pub fn auth(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Auth, ErrorScope::Stream, false, BackoffClass::Normal, code, message)
    }

    /// Permission error (not retryable).
    #[must_use]
    pub fn permission(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Permission, ErrorScope::Stream, false, BackoffClass::Normal, code, message)
    }

    /// Rate limit error (retryable, slow backoff).
    #[must_use]
    pub fn rate_limit(
        code: impl Into<String>,
        message: impl Into<String>,
        retry_after_ms: Option<u64>,
    ) -> Self {
        let mut err = Self::new(
            ErrorCategory::RateLimit, ErrorScope::Stream, true, BackoffClass::Slow, code, message,
        );
        err.retry_after_ms = retry_after_ms;
        err
    }

    /// Transient network error (retryable, normal backoff).
    #[must_use]
    pub fn transient_network(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::TransientNetwork, ErrorScope::Stream, true, BackoffClass::Normal, code, message)
    }

    /// Transient database error (retryable, normal backoff).
    #[must_use]
    pub fn transient_db(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::TransientDb, ErrorScope::Stream, true, BackoffClass::Normal, code, message)
    }

    /// Data validation error (not retryable, record scope).
    #[must_use]
    pub fn data(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Data, ErrorScope::Record, false, BackoffClass::Normal, code, message)
    }

    /// Schema mismatch error (not retryable).
    #[must_use]
    pub fn schema(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Schema, ErrorScope::Stream, false, BackoffClass::Normal, code, message)
    }

    /// Internal connector error (not retryable).
    #[must_use]
    pub fn internal(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Internal, ErrorScope::Stream, false, BackoffClass::Normal, code, message)
    }

    /// Frame lifecycle error (not retryable).
    #[must_use]
    pub fn frame(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Frame, ErrorScope::Batch, false, BackoffClass::Normal, code, message)
    }

    /// Attach structured diagnostic details.
    #[must_use]
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// Record transaction commit state at time of error.
    ///
    /// Setting [`CommitState::AfterCommitUnknown`] also sets `safe_to_retry = false`.
    #[must_use]
    pub fn with_commit_state(mut self, state: CommitState) -> Self {
        if state == CommitState::AfterCommitUnknown {
            self.safe_to_retry = false;
        }
        self.commit_state = Some(state);
        self
    }

    /// Override the default error scope.
    #[must_use]
    pub fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = scope;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_error_defaults() {
        let err = ConnectorError::config("MISSING_HOST", "host is required");
        assert_eq!(err.category, ErrorCategory::Config);
        assert_eq!(err.scope, ErrorScope::Stream);
        assert!(!err.retryable);
        assert!(!err.safe_to_retry);
        assert_eq!(err.backoff_class, BackoffClass::Normal);
    }

    #[test]
    fn transient_errors_are_retryable() {
        let net = ConnectorError::transient_network("TIMEOUT", "timed out");
        assert!(net.retryable);
        assert!(net.safe_to_retry);

        let db = ConnectorError::transient_db("DEADLOCK", "deadlock");
        assert!(db.retryable);
        assert!(db.safe_to_retry);
    }

    #[test]
    fn after_commit_unknown_disables_safe_retry() {
        let err = ConnectorError::transient_db("UNKNOWN", "commit unknown")
            .with_commit_state(CommitState::AfterCommitUnknown);
        assert!(err.retryable);
        assert!(!err.safe_to_retry);
    }

    #[test]
    fn serde_roundtrip() {
        let err = ConnectorError::rate_limit("THROTTLED", "slow down", Some(5000))
            .with_details(serde_json::json!({"endpoint": "/api/data"}));
        let json = serde_json::to_string(&err).unwrap();
        let back: ConnectorError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, back);
    }

    #[test]
    fn display_format() {
        let err = ConnectorError::config("BAD_PORT", "port must be positive");
        assert_eq!(err.to_string(), "[config] BAD_PORT: port must be positive");
    }
}
