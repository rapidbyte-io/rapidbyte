use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use rapidbyte_api::ApiError;
use serde::Serialize;

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Vec<rapidbyte_api::FieldError>>,
}

/// Wrapper to implement `IntoResponse` for `ApiError`.
pub struct ApiErrorResponse(pub ApiError);

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        let (status, code, message, details) = match self.0 {
            ApiError::NotFound { code, message } => (StatusCode::NOT_FOUND, code, message, None),
            ApiError::Conflict { code, message } => (StatusCode::CONFLICT, code, message, None),
            ApiError::ValidationFailed { message, details } => (
                StatusCode::BAD_REQUEST,
                "validation_failed".into(),
                message,
                Some(details),
            ),
            ApiError::NotImplemented { message } => (
                StatusCode::NOT_IMPLEMENTED,
                "not_implemented".into(),
                message,
                None,
            ),
            ApiError::Unauthorized { message } => (
                StatusCode::UNAUTHORIZED,
                "unauthorized".into(),
                message,
                None,
            ),
            ApiError::Internal { message } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error".into(),
                message,
                None,
            ),
        };

        let body = ErrorEnvelope {
            error: ErrorBody {
                code,
                message,
                details,
            },
        };

        (status, Json(body)).into_response()
    }
}

impl From<ApiError> for ApiErrorResponse {
    fn from(err: ApiError) -> Self {
        Self(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use http::StatusCode;

    #[tokio::test]
    async fn not_found_returns_404() {
        let err = ApiError::not_found("pipeline_not_found", "Pipeline 'foo' not found");
        let resp = ApiErrorResponse(err).into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn not_implemented_returns_501() {
        let err = ApiError::not_implemented("batch sync");
        let resp = ApiErrorResponse(err).into_response();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn unauthorized_returns_401() {
        let err = ApiError::Unauthorized {
            message: "bad token".into(),
        };
        let resp = ApiErrorResponse(err).into_response();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn conflict_returns_409() {
        let err = ApiError::conflict("run_exists", "Run already in progress");
        let resp = ApiErrorResponse(err).into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn validation_failed_returns_400() {
        let err = ApiError::ValidationFailed {
            message: "invalid input".into(),
            details: vec![rapidbyte_api::FieldError {
                field: "name".into(),
                reason: "required".into(),
            }],
        };
        let resp = ApiErrorResponse(err).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn internal_returns_500() {
        let err = ApiError::internal("something broke");
        let resp = ApiErrorResponse(err).into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn error_body_has_correct_json_shape() {
        let err = ApiError::not_found("test_code", "test message");
        let resp = ApiErrorResponse(err).into_response();
        let body = to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "test_code");
        assert_eq!(json["error"]["message"], "test message");
        assert!(json["error"]["details"].is_null());
    }

    #[tokio::test]
    async fn validation_body_includes_details() {
        let err = ApiError::ValidationFailed {
            message: "bad fields".into(),
            details: vec![rapidbyte_api::FieldError {
                field: "pipeline".into(),
                reason: "cannot be empty".into(),
            }],
        };
        let resp = ApiErrorResponse(err).into_response();
        let body = to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["details"][0]["field"], "pipeline");
        assert_eq!(json["error"]["details"][0]["reason"], "cannot be empty");
    }

    #[tokio::test]
    async fn from_api_error_conversion() {
        let err = ApiError::not_found("x", "y");
        let resp: ApiErrorResponse = err.into();
        let response = resp.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
