//! Bearer-token authentication interceptor for gRPC services.

use tonic::{service::Interceptor, Request, Status};

use crate::adapter::auth::{validate_token, AuthError};
use crate::config::AuthConfig;

/// A tonic interceptor that validates bearer tokens from the `authorization`
/// metadata header.
#[derive(Clone)]
pub struct BearerAuthInterceptor {
    auth_config: AuthConfig,
}

impl BearerAuthInterceptor {
    #[must_use]
    pub fn new(auth_config: AuthConfig) -> Self {
        Self { auth_config }
    }
}

impl Interceptor for BearerAuthInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        // RFC 7235: auth scheme is case-insensitive.
        // Use get() for boundary-safe slicing — avoids panic on multi-byte UTF-8.
        let token = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| match s.get(..7) {
                Some(prefix) if prefix.eq_ignore_ascii_case("bearer ") => Some(&s[7..]),
                _ => None,
            });

        match token {
            Some(raw) => validate_token(&self.auth_config, raw)
                .map(|_| req)
                .map_err(|e| Status::unauthenticated(e.to_string())),
            None => {
                if self.auth_config.allow_unauthenticated {
                    Ok(req)
                } else {
                    Err(Status::unauthenticated(AuthError::MissingToken.to_string()))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn make_request(auth_header: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(header) = auth_header {
            req.metadata_mut()
                .insert("authorization", MetadataValue::try_from(header).unwrap());
        }
        req
    }

    fn interceptor_with_tokens(
        tokens: Vec<&str>,
        allow_unauthenticated: bool,
    ) -> BearerAuthInterceptor {
        BearerAuthInterceptor::new(AuthConfig {
            tokens: tokens.into_iter().map(String::from).collect(),
            allow_unauthenticated,
            ..Default::default()
        })
    }

    #[test]
    fn allow_unauthenticated_passes_all() {
        let mut interceptor = interceptor_with_tokens(vec![], true);
        let req = make_request(None);
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn valid_token_passes() {
        let mut interceptor = interceptor_with_tokens(vec!["secret-token"], false);
        let req = make_request(Some("Bearer secret-token"));
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn invalid_token_rejected() {
        let mut interceptor = interceptor_with_tokens(vec!["secret-token"], false);
        let req = make_request(Some("Bearer wrong-token"));
        let result = interceptor.call(req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn missing_header_rejected() {
        let mut interceptor = interceptor_with_tokens(vec!["secret-token"], false);
        let req = make_request(None);
        let result = interceptor.call(req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn missing_bearer_prefix_rejected() {
        let mut interceptor = interceptor_with_tokens(vec!["secret-token"], false);
        let req = make_request(Some("secret-token"));
        let result = interceptor.call(req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn multiple_tokens_any_valid() {
        let mut interceptor = interceptor_with_tokens(vec!["token-a", "token-b"], false);
        let req = make_request(Some("Bearer token-b"));
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn empty_tokens_rejects_all() {
        let mut interceptor = interceptor_with_tokens(vec![], false);
        let req = make_request(Some("Bearer something"));
        let result = interceptor.call(req);
        assert!(result.is_err());
    }

    #[test]
    fn lowercase_bearer_accepted() {
        let mut interceptor = interceptor_with_tokens(vec!["secret-token"], false);
        let req = make_request(Some("bearer secret-token"));
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn mixed_case_bearer_accepted() {
        let mut interceptor = interceptor_with_tokens(vec!["secret-token"], false);
        let req = make_request(Some("BEARER secret-token"));
        assert!(interceptor.call(req).is_ok());
    }
}
