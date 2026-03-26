use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

use crate::adapter::auth::AuthError;
use crate::traits::error::{FieldError, ServiceError};

/// Wrapper for REST error responses. Converts [`ServiceError`] and [`AuthError`]
/// into JSON error bodies with appropriate HTTP status codes.
pub struct RestError(ServiceError);

impl From<ServiceError> for RestError {
    fn from(err: ServiceError) -> Self {
        Self(err)
    }
}

impl From<AuthError> for RestError {
    fn from(err: AuthError) -> Self {
        match err {
            AuthError::MissingToken | AuthError::InvalidToken => Self(ServiceError::Unauthorized),
        }
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Vec<FieldError>>,
}

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        let (status, code, message, details) = match &self.0 {
            ServiceError::NotFound { resource, id } => (
                StatusCode::NOT_FOUND,
                format!("{resource}_not_found"),
                format!("{resource} '{id}' does not exist"),
                None,
            ),
            ServiceError::Conflict { message } => (
                StatusCode::CONFLICT,
                "conflict".into(),
                message.clone(),
                None,
            ),
            ServiceError::ValidationFailed { details } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_failed".into(),
                "Validation failed".into(),
                Some(details.clone()),
            ),
            ServiceError::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized".into(),
                "Missing or invalid bearer token".into(),
                None,
            ),
            ServiceError::Internal { message } => {
                // Log the real error for operators but don't leak internals to clients.
                tracing::error!(error = %message, "internal service error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error".into(),
                    "An internal error occurred".into(),
                    None,
                )
            }
            ServiceError::NotImplemented { feature } => (
                StatusCode::NOT_IMPLEMENTED,
                "not_implemented".into(),
                format!("{feature} is not yet available"),
                None,
            ),
        };

        let body = ErrorResponse {
            error: ErrorBody {
                code,
                message,
                details,
            },
        };

        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn response_body(resp: Response) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn not_found_returns_404() {
        let err = RestError::from(ServiceError::NotFound {
            resource: "pipeline".into(),
            id: "foo".into(),
        });
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = response_body(resp).await;
        assert_eq!(body["error"]["code"], "pipeline_not_found");
    }

    #[tokio::test]
    async fn unauthorized_returns_401() {
        let err = RestError::from(ServiceError::Unauthorized);
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn validation_includes_details() {
        let err = RestError::from(ServiceError::ValidationFailed {
            details: vec![FieldError {
                field: "name".into(),
                reason: "required".into(),
            }],
        });
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = response_body(resp).await;
        assert_eq!(body["error"]["details"][0]["field"], "name");
    }
}
