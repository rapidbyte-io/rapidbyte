//! Shared request helpers for authenticated controller RPCs.

use tonic::metadata::errors::InvalidMetadataValue;
use tonic::Request;

pub(crate) fn request_with_bearer<T>(
    message: T,
    auth_token: Option<&str>,
) -> Result<Request<T>, InvalidMetadataValue> {
    let mut request = Request::new(message);
    if let Some(token) = auth_token {
        let metadata = format!("Bearer {token}").parse()?;
        request.metadata_mut().insert("authorization", metadata);
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn request_with_bearer_adds_authorization_metadata() {
        let request = request_with_bearer("payload", Some("secret")).unwrap();
        assert_eq!(
            request.metadata().get("authorization"),
            Some(&MetadataValue::from_static("Bearer secret"))
        );
    }

    #[test]
    fn request_with_bearer_is_noop_without_token() {
        let request = request_with_bearer("payload", None).unwrap();
        assert!(request.metadata().get("authorization").is_none());
    }

    #[test]
    fn request_with_bearer_rejects_invalid_token() {
        let err = request_with_bearer("payload", Some("bad\nvalue")).unwrap_err();
        assert!(!err.to_string().is_empty());
    }
}
