//! Shared request helpers for authenticated controller RPCs.

use tonic::Request;

pub(crate) fn request_with_bearer<T>(message: T, auth_token: Option<&str>) -> Request<T> {
    let mut request = Request::new(message);
    if let Some(token) = auth_token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn request_with_bearer_adds_authorization_metadata() {
        let request = request_with_bearer("payload", Some("secret"));
        assert_eq!(
            request.metadata().get("authorization"),
            Some(&MetadataValue::from_static("Bearer secret"))
        );
    }

    #[test]
    fn request_with_bearer_is_noop_without_token() {
        let request = request_with_bearer("payload", None);
        assert!(request.metadata().get("authorization").is_none());
    }
}
