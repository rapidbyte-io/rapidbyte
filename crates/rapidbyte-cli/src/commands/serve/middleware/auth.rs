use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// Configuration for bearer-token authentication.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Whether authentication is required.
    pub required: bool,
    /// Set of valid bearer tokens.
    pub tokens: Vec<String>,
}

#[derive(Serialize)]
struct AuthErrorEnvelope {
    error: AuthErrorBody,
}

#[derive(Serialize)]
struct AuthErrorBody {
    code: &'static str,
    message: String,
}

fn unauthorized_response(message: impl Into<String>) -> Response {
    let body = AuthErrorEnvelope {
        error: AuthErrorBody {
            code: "unauthorized",
            message: message.into(),
        },
    };
    (StatusCode::UNAUTHORIZED, Json(body)).into_response()
}

/// Axum middleware function for bearer-token authentication.
///
/// Use with `axum::middleware::from_fn_with_state`.
///
/// Behaviour:
/// - Health endpoint (`/api/v1/server/health`) always passes through.
/// - If `config.required` is `false`, all requests pass through.
/// - Otherwise, extracts `Authorization: Bearer <token>` and validates
///   against `config.tokens`.
pub async fn auth_middleware(
    State(config): State<Arc<AuthConfig>>,
    request: Request,
    next: Next,
) -> Response {
    // Health endpoint always passes through.
    if request.uri().path() == "/api/v1/server/health" {
        return next.run(request).await;
    }

    // If auth is not required, pass through.
    if !config.required {
        return next.run(request).await;
    }

    // Extract the Authorization header.
    let auth_header = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let Some(header_value) = auth_header else {
        return unauthorized_response("missing Authorization header");
    };

    let Some(token) = header_value.strip_prefix("Bearer ") else {
        return unauthorized_response("Authorization header must use Bearer scheme");
    };

    if !config.tokens.iter().any(|t| t == token) {
        return unauthorized_response("invalid bearer token");
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use axum::Router;
    use http::Request as HttpRequest;
    use tower::ServiceExt;

    fn test_config(required: bool, tokens: Vec<&str>) -> Arc<AuthConfig> {
        Arc::new(AuthConfig {
            required,
            tokens: tokens.into_iter().map(String::from).collect(),
        })
    }

    fn app(config: Arc<AuthConfig>) -> Router {
        Router::new()
            .route("/api/v1/pipelines", get(|| async { "ok" }))
            .route("/api/v1/server/health", get(|| async { "healthy" }))
            .layer(from_fn_with_state(config.clone(), auth_middleware))
            .with_state(config)
    }

    #[tokio::test]
    async fn valid_token_returns_200() {
        let config = test_config(true, vec!["secret123"]);
        let resp = app(config)
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/v1/pipelines")
                    .header("Authorization", "Bearer secret123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn missing_token_returns_401() {
        let config = test_config(true, vec!["secret123"]);
        let resp = app(config)
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/v1/pipelines")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn invalid_token_returns_401() {
        let config = test_config(true, vec!["secret123"]);
        let resp = app(config)
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/v1/pipelines")
                    .header("Authorization", "Bearer wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn non_bearer_scheme_returns_401() {
        let config = test_config(true, vec!["secret123"]);
        let resp = app(config)
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/v1/pipelines")
                    .header("Authorization", "Basic c2VjcmV0MTIz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_not_required_allows_all() {
        let config = test_config(false, vec![]);
        let resp = app(config)
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/v1/pipelines")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_endpoint_skips_auth() {
        let config = test_config(true, vec!["secret123"]);
        let resp = app(config)
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/v1/server/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn error_body_has_json_shape() {
        let config = test_config(true, vec!["secret123"]);
        let resp = app(config)
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/v1/pipelines")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "unauthorized");
        assert!(json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("missing"));
    }

    #[tokio::test]
    async fn multiple_valid_tokens() {
        let config = test_config(true, vec!["token_a", "token_b"]);
        let resp = app(config)
            .oneshot(
                HttpRequest::builder()
                    .uri("/api/v1/pipelines")
                    .header("Authorization", "Bearer token_b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
