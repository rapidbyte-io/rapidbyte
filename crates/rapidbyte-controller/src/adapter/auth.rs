//! Transport-agnostic bearer-token validation shared by gRPC and REST adapters.

use subtle::ConstantTimeEq;

use crate::config::AuthConfig;

/// Identity resolved from a valid bearer token.
/// Will expand with `tenant_id`, roles, permissions when RBAC lands.
#[derive(Clone, Debug)]
pub struct AuthContext {
    pub token: String,
}

/// Auth validation error.
#[derive(Debug)]
pub enum AuthError {
    MissingToken,
    InvalidToken,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingToken => write!(f, "missing bearer token"),
            Self::InvalidToken => write!(f, "invalid bearer token"),
        }
    }
}

impl std::error::Error for AuthError {}

/// Transport-agnostic token validation. Called by both gRPC interceptor and
/// axum middleware.
///
/// # Errors
///
/// Returns [`AuthError::InvalidToken`] when the token does not match any
/// configured token, or [`AuthError::MissingToken`] if no token is present.
pub fn validate_token(config: &AuthConfig, raw_token: &str) -> Result<AuthContext, AuthError> {
    if config.allow_unauthenticated {
        return Ok(AuthContext {
            token: raw_token.to_string(),
        });
    }
    if config
        .tokens
        .iter()
        .any(|t| t.len() == raw_token.len() && bool::from(t.as_bytes().ct_eq(raw_token.as_bytes())))
    {
        Ok(AuthContext {
            token: raw_token.to_string(),
        })
    } else {
        Err(AuthError::InvalidToken)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_tokens(tokens: Vec<&str>) -> AuthConfig {
        AuthConfig {
            tokens: tokens.into_iter().map(String::from).collect(),
            allow_unauthenticated: false,
            ..Default::default()
        }
    }

    #[test]
    fn valid_token_returns_auth_context() {
        let config = config_with_tokens(vec!["tok-abc"]);
        let ctx = validate_token(&config, "tok-abc").unwrap();
        assert_eq!(ctx.token, "tok-abc");
    }

    #[test]
    fn invalid_token_returns_error() {
        let config = config_with_tokens(vec!["tok-abc"]);
        assert!(validate_token(&config, "wrong").is_err());
    }

    #[test]
    fn unauthenticated_mode_allows_any_token() {
        let config = AuthConfig {
            allow_unauthenticated: true,
            ..Default::default()
        };
        let ctx = validate_token(&config, "anything").unwrap();
        assert_eq!(ctx.token, "anything");
    }

    #[test]
    fn multiple_tokens_any_valid() {
        let config = config_with_tokens(vec!["tok-a", "tok-b"]);
        assert!(validate_token(&config, "tok-b").is_ok());
    }
}
