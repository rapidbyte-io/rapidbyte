use rapidbyte_engine::PipelineError;
use serde::Serialize;

/// API-layer error type. Service implementations return this.
/// The axum adapter maps it to HTTP status codes.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("not found: {message}")]
    NotFound { code: String, message: String },

    #[error("conflict: {message}")]
    Conflict { code: String, message: String },

    #[error("validation failed: {message}")]
    ValidationFailed {
        message: String,
        details: Vec<FieldError>,
    },

    #[error("not implemented: {message}")]
    NotImplemented { message: String },

    #[error("unauthorized: {message}")]
    Unauthorized { message: String },

    #[error("internal error: {message}")]
    Internal { message: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldError {
    pub field: String,
    pub reason: String,
}

impl ApiError {
    pub fn not_found(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::NotFound {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn conflict(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Conflict {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::NotImplemented {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

impl From<PipelineError> for ApiError {
    fn from(err: PipelineError) -> Self {
        match &err {
            PipelineError::Cancelled => Self::Conflict {
                code: "run_cancelled".into(),
                message: "Pipeline run was cancelled".into(),
            },
            PipelineError::Plugin(_) | PipelineError::Infrastructure(_) => Self::Internal {
                message: err.to_string(),
            },
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self::Internal {
            message: err.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_cancelled_maps_to_conflict() {
        let err: ApiError = PipelineError::Cancelled.into();
        assert!(matches!(err, ApiError::Conflict { .. }));
    }

    #[test]
    fn infrastructure_error_maps_to_internal() {
        let err: ApiError =
            PipelineError::Infrastructure(anyhow::anyhow!("db connection failed")).into();
        assert!(matches!(err, ApiError::Internal { .. }));
        assert!(err.to_string().contains("db connection failed"));
    }

    #[test]
    fn not_found_helper() {
        let err = ApiError::not_found("pipeline_not_found", "Pipeline 'foo' does not exist");
        assert!(
            matches!(err, ApiError::NotFound { code, message } if code == "pipeline_not_found" && message.contains("foo"))
        );
    }

    #[test]
    fn not_implemented_helper() {
        let err = ApiError::not_implemented("batch sync");
        assert!(matches!(err, ApiError::NotImplemented { message } if message == "batch sync"));
    }
}
