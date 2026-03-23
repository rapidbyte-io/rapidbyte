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
            PipelineError::Plugin(_) => Self::Internal {
                message: err.to_string(),
            },
            PipelineError::Infrastructure(_) => Self::Internal {
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
