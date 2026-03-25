use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::adapter::auth::{validate_token, AuthContext, AuthError};
use crate::adapter::rest::error::RestError;
use crate::config::AuthConfig;

/// Shared state for the REST router.
#[derive(Clone)]
pub struct RestState {
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

    let token = value
        .strip_prefix("Bearer ")
        .ok_or(AuthError::InvalidToken)?;

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
}
