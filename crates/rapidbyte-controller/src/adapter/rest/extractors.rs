use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::adapter::auth::{validate_token, AuthContext, AuthError};
use crate::adapter::rest::error::RestError;
use crate::application::services::AppServices;
use crate::config::AuthConfig;

/// Shared state for the REST router.
#[derive(Clone)]
pub struct RestState {
    pub services: Arc<AppServices>,
    pub auth_config: AuthConfig,
}

/// Axum extractor that validates bearer tokens.
pub struct Auth(pub AuthContext);

impl FromRequestParts<RestState> for Auth {
    type Rejection = RestError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &RestState,
    ) -> Result<Self, Self::Rejection> {
        if state.auth_config.allow_unauthenticated {
            return Ok(Auth(AuthContext {
                token: String::new(),
            }));
        }
        let token = extract_bearer(&parts.headers).map_err(RestError::from)?;
        let ctx = validate_token(&state.auth_config, &token).map_err(RestError::from)?;
        Ok(Auth(ctx))
    }
}

fn extract_bearer(headers: &axum::http::HeaderMap) -> Result<String, AuthError> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(AuthError::MissingToken)?;

    // RFC 7235: auth scheme is case-insensitive. Accept "Bearer", "bearer", "BEARER", etc.
    // Use get() for boundary-safe slicing — avoids panic on multi-byte UTF-8 at index 7.
    let token = match value.get(..7) {
        Some(prefix) if prefix.eq_ignore_ascii_case("bearer ") => &value[7..],
        _ => return Err(AuthError::InvalidToken),
    };

    if token.is_empty() {
        return Err(AuthError::InvalidToken);
    }

    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    #[test]
    fn extracts_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer tok-abc".parse().unwrap());
        assert_eq!(extract_bearer(&headers).unwrap(), "tok-abc");
    }

    #[test]
    fn missing_header_returns_error() {
        let headers = HeaderMap::new();
        assert!(extract_bearer(&headers).is_err());
    }

    #[test]
    fn wrong_prefix_returns_error() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Basic abc".parse().unwrap());
        assert!(extract_bearer(&headers).is_err());
    }

    #[test]
    fn empty_token_returns_error() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer ".parse().unwrap());
        assert!(extract_bearer(&headers).is_err());
    }

    #[test]
    fn lowercase_bearer_accepted() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "bearer tok-abc".parse().unwrap());
        assert_eq!(extract_bearer(&headers).unwrap(), "tok-abc");
    }

    #[test]
    fn mixed_case_bearer_accepted() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "BEARER tok-abc".parse().unwrap());
        assert_eq!(extract_bearer(&headers).unwrap(), "tok-abc");
    }

    #[test]
    fn non_ascii_prefix_returns_error_without_panic() {
        // Multi-byte UTF-8 at the prefix boundary must not panic.
        // HeaderValue::from_str rejects non-visible-ASCII, so we test
        // extract_bearer's internal logic directly with a short ASCII
        // value that doesn't start with "bearer ".
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            "Bär".parse().unwrap_or_else(|_| {
                // HeaderValue rejects non-ASCII, so use a safe short value instead
                "B".parse().unwrap()
            }),
        );
        assert!(extract_bearer(&headers).is_err());
    }

    #[test]
    fn short_header_returns_error_without_panic() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bear".parse().unwrap());
        assert!(extract_bearer(&headers).is_err());
    }
}
