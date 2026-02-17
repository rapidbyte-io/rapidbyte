use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// Validation types (unchanged)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// V0 error types (renamed from originals, preserved for compatibility)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorErrorV0 {
    pub code: String,
    pub message: String,
}

/// Result type returned by connector protocol functions.
/// Serialized as JSON for host-guest transport.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ConnectorResultV0<T> {
    Ok { data: T },
    Err { error: ConnectorErrorV0 },
}

/// Alias so existing code using `ConnectorError` continues to compile.
pub type ConnectorError = ConnectorErrorV0;

/// Alias so existing code using `ConnectorResult<T>` continues to compile.
pub type ConnectorResult<T> = ConnectorResultV0<T>;

// ---------------------------------------------------------------------------
// V1 typed error model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    Config,
    Auth,
    Permission,
    RateLimit,
    TransientNetwork,
    TransientDb,
    Data,
    Schema,
    Internal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ErrorScope {
    Stream,
    Batch,
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

impl std::fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config => write!(f, "config"),
            Self::Auth => write!(f, "auth"),
            Self::Permission => write!(f, "permission"),
            Self::RateLimit => write!(f, "rate_limit"),
            Self::TransientNetwork => write!(f, "transient_network"),
            Self::TransientDb => write!(f, "transient_db"),
            Self::Data => write!(f, "data"),
            Self::Schema => write!(f, "schema"),
            Self::Internal => write!(f, "internal"),
        }
    }
}

impl std::fmt::Display for ErrorScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream => write!(f, "stream"),
            Self::Batch => write!(f, "batch"),
            Self::Record => write!(f, "record"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorErrorV1 {
    pub category: ErrorCategory,
    pub scope: ErrorScope,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
    pub backoff_class: BackoffClass,
    pub safe_to_retry: bool,
    pub commit_state: Option<CommitState>,
    pub details: Option<serde_json::Value>,
}

impl ConnectorErrorV1 {
    // -- Category constructors that enforce invariants --

    /// Configuration error (not retryable).
    pub fn config(code: impl Into<String>, message: impl Into<String>) -> Self {
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
    pub fn auth(code: impl Into<String>, message: impl Into<String>) -> Self {
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
    pub fn permission(code: impl Into<String>, message: impl Into<String>) -> Self {
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
        code: impl Into<String>,
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
    pub fn transient_network(code: impl Into<String>, message: impl Into<String>) -> Self {
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
    pub fn transient_db(code: impl Into<String>, message: impl Into<String>) -> Self {
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
    pub fn data(code: impl Into<String>, message: impl Into<String>) -> Self {
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
    pub fn schema(code: impl Into<String>, message: impl Into<String>) -> Self {
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
    pub fn internal(code: impl Into<String>, message: impl Into<String>) -> Self {
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

    // -- Builder methods --

    /// Attach structured details.
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// Attach commit state information.
    pub fn with_commit_state(mut self, state: CommitState) -> Self {
        self.commit_state = Some(state);
        self
    }

    /// Override the default scope.
    pub fn with_scope(mut self, scope: ErrorScope) -> Self {
        self.scope = scope;
        self
    }
}

impl fmt::Display for ConnectorErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}/{}] {} ({}): {}",
            self.category, self.scope, self.code, if self.retryable { "retryable" } else { "fatal" }, self.message
        )
    }
}

impl std::error::Error for ConnectorErrorV1 {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_result_roundtrip() {
        let result = ValidationResult {
            status: ValidationStatus::Success,
            message: "Connection successful".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: ValidationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(result, back);
    }

    #[test]
    fn test_connector_result_ok() {
        let result: ConnectorResult<String> = ConnectorResult::Ok {
            data: "hello".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"status\":\"ok\""));
        let back: ConnectorResult<String> = serde_json::from_str(&json).unwrap();
        assert_eq!(result, back);
    }

    #[test]
    fn test_connector_result_err() {
        let result: ConnectorResult<String> = ConnectorResult::Err {
            error: ConnectorError {
                code: "CONNECTION_FAILED".to_string(),
                message: "Could not connect to database".to_string(),
            },
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"status\":\"err\""));
        let back: ConnectorResult<String> = serde_json::from_str(&json).unwrap();
        assert_eq!(result, back);
    }

    // -----------------------------------------------------------------------
    // V1 error model tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_config_error_not_retryable() {
        let err = ConnectorErrorV1::config("MISSING_HOST", "host is required");
        assert_eq!(err.category, ErrorCategory::Config);
        assert!(!err.retryable);
        assert!(!err.safe_to_retry);
    }

    #[test]
    fn test_auth_error_not_retryable() {
        let err = ConnectorErrorV1::auth("INVALID_CREDS", "bad password");
        assert_eq!(err.category, ErrorCategory::Auth);
        assert!(!err.retryable);
    }

    #[test]
    fn test_permission_error_not_retryable() {
        let err = ConnectorErrorV1::permission("NO_SELECT", "no SELECT privilege");
        assert_eq!(err.category, ErrorCategory::Permission);
        assert!(!err.retryable);
    }

    #[test]
    fn test_rate_limit_retryable_slow_backoff() {
        let err = ConnectorErrorV1::rate_limit("TOO_MANY_REQUESTS", "slow down", Some(5000));
        assert_eq!(err.category, ErrorCategory::RateLimit);
        assert!(err.retryable);
        assert!(err.safe_to_retry);
        assert_eq!(err.backoff_class, BackoffClass::Slow);
        assert_eq!(err.retry_after_ms, Some(5000));
    }

    #[test]
    fn test_transient_network_retryable() {
        let err = ConnectorErrorV1::transient_network("CONN_RESET", "connection reset");
        assert_eq!(err.category, ErrorCategory::TransientNetwork);
        assert!(err.retryable);
        assert_eq!(err.backoff_class, BackoffClass::Normal);
    }

    #[test]
    fn test_transient_db_retryable() {
        let err = ConnectorErrorV1::transient_db("DEADLOCK", "deadlock detected");
        assert_eq!(err.category, ErrorCategory::TransientDb);
        assert!(err.retryable);
        assert_eq!(err.backoff_class, BackoffClass::Normal);
    }

    #[test]
    fn test_data_error_record_scope() {
        let err = ConnectorErrorV1::data("INVALID_TYPE", "expected int, got string");
        assert_eq!(err.category, ErrorCategory::Data);
        assert_eq!(err.scope, ErrorScope::Record);
        assert!(!err.retryable);
    }

    #[test]
    fn test_schema_error_not_retryable() {
        let err = ConnectorErrorV1::schema("COLUMN_MISSING", "column 'foo' not found");
        assert_eq!(err.category, ErrorCategory::Schema);
        assert!(!err.retryable);
    }

    #[test]
    fn test_internal_error_not_retryable() {
        let err = ConnectorErrorV1::internal("BUG", "unexpected state");
        assert_eq!(err.category, ErrorCategory::Internal);
        assert!(!err.retryable);
    }

    #[test]
    fn test_builder_with_details() {
        let err = ConnectorErrorV1::data("INVALID_TYPE", "bad value")
            .with_details(serde_json::json!({"column": "age", "value": "abc"}));
        assert!(err.details.is_some());
        assert_eq!(err.details.unwrap()["column"], "age");
    }

    #[test]
    fn test_builder_with_commit_state() {
        let err = ConnectorErrorV1::transient_db("TIMEOUT", "query timeout")
            .with_commit_state(CommitState::BeforeCommit);
        assert_eq!(err.commit_state, Some(CommitState::BeforeCommit));
    }

    #[test]
    fn test_builder_with_scope() {
        let err = ConnectorErrorV1::data("INVALID_TYPE", "bad value")
            .with_scope(ErrorScope::Batch);
        assert_eq!(err.scope, ErrorScope::Batch);
    }

    #[test]
    fn test_display_format() {
        let err = ConnectorErrorV1::config("MISSING_HOST", "host is required");
        let s = format!("{}", err);
        assert!(s.contains("config"));
        assert!(s.contains("MISSING_HOST"));
        assert!(s.contains("fatal"));
        assert!(s.contains("host is required"));
    }

    #[test]
    fn test_display_retryable() {
        let err = ConnectorErrorV1::transient_network("CONN_RESET", "connection reset");
        let s = format!("{}", err);
        assert!(s.contains("retryable"));
    }

    #[test]
    fn test_connector_error_v1_roundtrip() {
        let err = ConnectorErrorV1::rate_limit("TOO_MANY", "slow down", Some(1000))
            .with_details(serde_json::json!({"endpoint": "/api/data"}))
            .with_commit_state(CommitState::BeforeCommit);
        let json = serde_json::to_string(&err).unwrap();
        let back: ConnectorErrorV1 = serde_json::from_str(&json).unwrap();
        assert_eq!(err, back);
    }

    #[test]
    fn test_v0_aliases_work() {
        // Verify that the type aliases compile and work with pattern matching
        let result: ConnectorResult<String> = ConnectorResult::Ok {
            data: "test".to_string(),
        };
        match result {
            ConnectorResult::Ok { data } => assert_eq!(data, "test"),
            ConnectorResult::Err { .. } => panic!("unexpected error"),
        }

        let err = ConnectorError {
            code: "TEST".to_string(),
            message: "test".to_string(),
        };
        assert_eq!(err.code, "TEST");
    }
}
