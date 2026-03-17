//! Bearer-token authentication interceptor for gRPC services.

use tonic::{service::Interceptor, Request, Status};

/// A tonic interceptor that validates bearer tokens from the `authorization`
/// metadata header.
#[derive(Clone)]
pub struct BearerAuthInterceptor {
    tokens: Vec<String>,
    allow_unauthenticated: bool,
}

impl BearerAuthInterceptor {
    #[must_use]
    pub fn new(tokens: Vec<String>, allow_unauthenticated: bool) -> Self {
        Self {
            tokens,
            allow_unauthenticated,
        }
    }
}

impl Interceptor for BearerAuthInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        if self.allow_unauthenticated {
            return Ok(req);
        }

        let auth = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));

        match auth {
            Some(token) if self.tokens.iter().any(|t| t == token) => Ok(req),
            _ => Err(Status::unauthenticated("invalid or missing bearer token")),
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

    #[test]
    fn allow_unauthenticated_passes_all() {
        let mut interceptor = BearerAuthInterceptor::new(vec![], true);
        let req = make_request(None);
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn valid_token_passes() {
        let mut interceptor = BearerAuthInterceptor::new(vec!["secret-token".to_string()], false);
        let req = make_request(Some("Bearer secret-token"));
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn invalid_token_rejected() {
        let mut interceptor = BearerAuthInterceptor::new(vec!["secret-token".to_string()], false);
        let req = make_request(Some("Bearer wrong-token"));
        let result = interceptor.call(req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn missing_header_rejected() {
        let mut interceptor = BearerAuthInterceptor::new(vec!["secret-token".to_string()], false);
        let req = make_request(None);
        let result = interceptor.call(req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn missing_bearer_prefix_rejected() {
        let mut interceptor = BearerAuthInterceptor::new(vec!["secret-token".to_string()], false);
        let req = make_request(Some("secret-token"));
        let result = interceptor.call(req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn multiple_tokens_any_valid() {
        let mut interceptor =
            BearerAuthInterceptor::new(vec!["token-a".to_string(), "token-b".to_string()], false);
        let req = make_request(Some("Bearer token-b"));
        assert!(interceptor.call(req).is_ok());
    }

    #[test]
    fn empty_tokens_rejects_all() {
        let mut interceptor = BearerAuthInterceptor::new(vec![], false);
        let req = make_request(Some("Bearer something"));
        let result = interceptor.call(req);
        assert!(result.is_err());
    }
}
