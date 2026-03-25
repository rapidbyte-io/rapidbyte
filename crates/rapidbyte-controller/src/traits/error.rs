use serde::Serialize;
use std::fmt;

/// Error type returned by all driving-port service traits.
/// Adapters (REST, gRPC) map these to transport-specific error responses.
#[derive(Debug)]
pub enum ServiceError {
    NotFound { resource: String, id: String },
    Conflict { message: String },
    ValidationFailed { details: Vec<FieldError> },
    Unauthorized,
    Internal { message: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldError {
    pub field: String,
    pub reason: String,
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { resource, id } => write!(f, "{resource} '{id}' not found"),
            Self::Conflict { message } => write!(f, "conflict: {message}"),
            Self::ValidationFailed { details } => {
                write!(f, "validation failed: {} error(s)", details.len())
            }
            Self::Unauthorized => write!(f, "unauthorized"),
            Self::Internal { message } => write!(f, "internal error: {message}"),
        }
    }
}

impl std::error::Error for ServiceError {}

/// Paginated list response used by all list endpoints.
#[derive(Debug, Clone, Serialize)]
pub struct PaginatedList<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// A stream of events for SSE endpoints.
pub type EventStream<T> = std::pin::Pin<Box<dyn futures::Stream<Item = T> + Send>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_display() {
        let err = ServiceError::NotFound {
            resource: "pipeline".into(),
            id: "foo".into(),
        };
        assert_eq!(err.to_string(), "pipeline 'foo' not found");
    }

    #[test]
    fn validation_failed_display() {
        let err = ServiceError::ValidationFailed {
            details: vec![FieldError {
                field: "name".into(),
                reason: "required".into(),
            }],
        };
        assert_eq!(err.to_string(), "validation failed: 1 error(s)");
    }
}
