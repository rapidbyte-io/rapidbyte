//! Typed connector error model shared across SDK and host runtime.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    Success,
    Failed,
    Warning,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ValidationResult {
    pub status: ValidationStatus,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Error, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    #[error("config")]
    Config,
    #[error("auth")]
    Auth,
    #[error("permission")]
    Permission,
    #[error("rate_limit")]
    RateLimit,
    #[error("transient_network")]
    TransientNetwork,
    #[error("transient_db")]
    TransientDb,
    #[error("data")]
    Data,
    #[error("schema")]
    Schema,
    #[error("internal")]
    Internal,
}

#[derive(Debug, Clone, Copy, Error, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ErrorScope {
    #[error("stream")]
    Stream,
    #[error("batch")]
    Batch,
    #[error("record")]
    Record,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum BackoffClass {
    Fast,
    Normal,
    Slow,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CommitState {
    BeforeCommit,
    AfterCommitUnknown,
    AfterCommitConfirmed,
}

/// Opaque error code following SCREAMING_SNAKE_CASE convention.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ErrorCode(pub String);

impl ErrorCode {
    pub fn new(code: impl Into<String>) -> Self {
        Self(code.into())
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ErrorCode {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ErrorCode {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Error, Serialize, Deserialize, PartialEq)]
#[error(
    "[{category}/{scope}] {code} ({retryability}): {message}",
    retryability = if *.retryable { "retryable" } else { "fatal" }
)]
pub struct ConnectorError {
    pub category: ErrorCategory,
    pub scope: ErrorScope,
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
    pub backoff_class: BackoffClass,
    pub safe_to_retry: bool,
    pub commit_state: Option<CommitState>,
    pub details: Option<serde_json::Value>,
}

impl ConnectorError {
    /// Configuration error (not retryable).
    pub fn config(code: impl Into<ErrorCode>, message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::Config,
            scope: ErrorScope::Stream,
            code: code.into(),
            message: message.into(),
            retryable: false,
            retry_after_ms: None,
            backoff_class: BackoffClass::Normal,
            safe_to_retry: false,
            commit_state: None,
            details: None,
        }
    }

    /// Authentication error (not retryable).
    pub fn auth(code: impl Into<ErrorCode>, message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::Auth,
            scope: ErrorScope::Stream,
            code: code.into(),
            message: message.into(),
            retryable: false,
            retry_after_ms: None,
            backoff_class: BackoffClass::Normal,
            safe_to_retry: false,
            commit_state: None,
            details: None,
        }
    }

    /// Permission error (not retryable).
    pub fn permission(code: impl Into<ErrorCode>, message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::Permission,
            scope: ErrorScope::Stream,
            code: code.into(),
            message: message.into(),
            retryable: false,
            retry_after_ms: None,
            backoff_class: BackoffClass::Normal,
            safe_to_retry: false,
            commit_state: None,
            details: None,
        }
    }

    /// Rate limit error (retryable, slow backoff).
    pub fn rate_limit(
        code: impl Into<ErrorCode>,
        message: impl Into<String>,
        retry_after_ms: Option<u64>,
    ) -> Self {
        Self {
            category: ErrorCategory::RateLimit,
            scope: ErrorScope::Stream,
            code: code.into(),
            message: message.into(),
            retryable: true,
            retry_after_ms,
            backoff_class: BackoffClass::Slow,
            safe_to_retry: true,
            commit_state: None,
            details: None,
        }
    }

    /// Transient network error (retryable, normal backoff).
    pub fn transient_network(code: impl Into<ErrorCode>, message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::TransientNetwork,
            scope: ErrorScope::Stream,
            code: code.into(),
            message: message.into(),
            retryable: true,
            retry_after_ms: None,
            backoff_class: BackoffClass::Normal,
            safe_to_retry: true,
            commit_state: None,
            details: None,
        }
    }

    /// Transient database error (retryable, normal backoff).
    pub fn transient_db(code: impl Into<ErrorCode>, message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::TransientDb,
            scope: ErrorScope::Stream,
            code: code.into(),
            message: message.into(),
            retryable: true,
            retry_after_ms: None,
            backoff_class: BackoffClass::Normal,
            safe_to_retry: true,
            commit_state: None,
            details: None,
        }
    }

    /// Data error (not retryable, record scope).
    pub fn data(code: impl Into<ErrorCode>, message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::Data,
            scope: ErrorScope::Record,
            code: code.into(),
            message: message.into(),
            retryable: false,
            retry_after_ms: None,
            backoff_class: BackoffClass::Normal,
            safe_to_retry: false,
            commit_state: None,
            details: None,
        }
    }

    /// Schema error (not retryable).
    pub fn schema(code: impl Into<ErrorCode>, message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::Schema,
            scope: ErrorScope::Stream,
            code: code.into(),
            message: message.into(),
            retryable: false,
            retry_after_ms: None,
            backoff_class: BackoffClass::Normal,
            safe_to_retry: false,
            commit_state: None,
            details: None,
        }
    }

    /// Internal error (not retryable).
    pub fn internal(code: impl Into<ErrorCode>, message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::Internal,
            scope: ErrorScope::Stream,
            code: code.into(),
            message: message.into(),
            retryable: false,
            retry_after_ms: None,
            backoff_class: BackoffClass::Normal,
            safe_to_retry: false,
            commit_state: None,
            details: None,
        }
    }

    /// Attach structured details.
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// Attach commit state information.
    ///
    /// When `AfterCommitUnknown`, also sets `safe_to_retry = false` because
    /// the server-side commit outcome is unknown (the data may already be
    /// persisted, so a blind retry could produce duplicates).
    pub fn with_commit_state(mut self, state: CommitState) -> Self {
        self.commit_state = Some(state);
        if state == CommitState::AfterCommitUnknown {
            self.safe_to_retry = false;
        }
        self
    }

    /// Override the default scope.
    pub fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = scope;
        self
    }
}

/// Result type with typed errors including retry metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ConnectorResult<T> {
    Ok { data: T },
    Err { error: ConnectorError },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_result_roundtrip() {
        let result = ValidationResult {
            status: ValidationStatus::Success,
            message: "Connection successful".to_string(),
        };
        let json = serde_json::to_string(&result).expect("serialize");
        let back: ValidationResult = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(result, back);
    }

    #[test]
    fn test_config_error_not_retryable() {
        let err = ConnectorError::config("MISSING_HOST", "host is required");
        assert_eq!(err.category, ErrorCategory::Config);
        assert!(!err.retryable);
        assert!(!err.safe_to_retry);
    }

    #[test]
    fn test_auth_error_not_retryable() {
        let err = ConnectorError::auth("INVALID_CREDS", "bad password");
        assert_eq!(err.category, ErrorCategory::Auth);
        assert!(!err.retryable);
    }

    #[test]
    fn test_permission_error_not_retryable() {
        let err = ConnectorError::permission("NO_SELECT", "no SELECT privilege");
        assert_eq!(err.category, ErrorCategory::Permission);
        assert!(!err.retryable);
    }

    #[test]
    fn test_rate_limit_retryable_slow_backoff() {
        let err = ConnectorError::rate_limit("TOO_MANY_REQUESTS", "slow down", Some(5000));
        assert_eq!(err.category, ErrorCategory::RateLimit);
        assert!(err.retryable);
        assert!(err.safe_to_retry);
        assert_eq!(err.backoff_class, BackoffClass::Slow);
        assert_eq!(err.retry_after_ms, Some(5000));
    }

    #[test]
    fn test_transient_network_retryable() {
        let err = ConnectorError::transient_network("CONN_RESET", "connection reset");
        assert_eq!(err.category, ErrorCategory::TransientNetwork);
        assert!(err.retryable);
        assert_eq!(err.backoff_class, BackoffClass::Normal);
    }

    #[test]
    fn test_transient_db_retryable() {
        let err = ConnectorError::transient_db("DEADLOCK", "deadlock detected");
        assert_eq!(err.category, ErrorCategory::TransientDb);
        assert!(err.retryable);
        assert_eq!(err.backoff_class, BackoffClass::Normal);
    }

    #[test]
    fn test_data_error_record_scope() {
        let err = ConnectorError::data("INVALID_TYPE", "expected int, got string");
        assert_eq!(err.category, ErrorCategory::Data);
        assert_eq!(err.scope, ErrorScope::Record);
        assert!(!err.retryable);
    }

    #[test]
    fn test_schema_error_not_retryable() {
        let err = ConnectorError::schema("COLUMN_MISSING", "column 'foo' not found");
        assert_eq!(err.category, ErrorCategory::Schema);
        assert!(!err.retryable);
    }

    #[test]
    fn test_internal_error_not_retryable() {
        let err = ConnectorError::internal("BUG", "unexpected state");
        assert_eq!(err.category, ErrorCategory::Internal);
        assert!(!err.retryable);
    }

    #[test]
    fn test_builder_with_details() {
        let err = ConnectorError::data("INVALID_TYPE", "bad value")
            .with_details(serde_json::json!({"column": "age", "value": "abc"}));
        assert!(err.details.is_some());
        assert_eq!(err.details.expect("details")["column"], "age");
    }

    #[test]
    fn test_builder_with_commit_state() {
        let err = ConnectorError::transient_db("TIMEOUT", "query timeout")
            .with_commit_state(CommitState::BeforeCommit);
        assert_eq!(err.commit_state, Some(CommitState::BeforeCommit));
    }

    #[test]
    fn test_builder_with_scope() {
        let err = ConnectorError::data("INVALID_TYPE", "bad value").with_scope(ErrorScope::Batch);
        assert_eq!(err.scope, ErrorScope::Batch);
    }

    #[test]
    fn test_display_format() {
        let err = ConnectorError::config("MISSING_HOST", "host is required");
        let s = format!("{}", err);
        assert!(s.contains("config"));
        assert!(s.contains("MISSING_HOST"));
        assert!(s.contains("fatal"));
        assert!(s.contains("host is required"));
    }

    #[test]
    fn test_display_retryable() {
        let err = ConnectorError::transient_network("CONN_RESET", "connection reset");
        let s = format!("{}", err);
        assert!(s.contains("retryable"));
    }

    #[test]
    fn test_connector_result_ok_roundtrip() {
        let result: ConnectorResult<String> = ConnectorResult::Ok {
            data: "hello".to_string(),
        };
        let json = serde_json::to_string(&result).expect("serialize");
        let back: ConnectorResult<String> = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(result, back);
        assert!(json.contains("\"status\":\"ok\""));
    }

    #[test]
    fn test_connector_result_err_roundtrip() {
        let result: ConnectorResult<()> = ConnectorResult::Err {
            error: ConnectorError::config("BAD_HOST", "invalid host"),
        };
        let json = serde_json::to_string(&result).expect("serialize");
        let back: ConnectorResult<()> = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(result, back);
        assert!(json.contains("\"status\":\"err\""));
        assert!(json.contains("\"category\":\"config\""));
    }

    #[test]
    fn test_connector_error_roundtrip() {
        let err = ConnectorError::rate_limit("TOO_MANY", "slow down", Some(1000))
            .with_details(serde_json::json!({"endpoint": "/api/data"}))
            .with_commit_state(CommitState::BeforeCommit);
        let json = serde_json::to_string(&err).expect("serialize");
        let back: ConnectorError = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(err, back);
    }
}
