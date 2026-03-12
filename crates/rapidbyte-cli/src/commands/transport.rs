//! Shared transport helpers for distributed controller and Flight clients.

use std::path::PathBuf;

use anyhow::{Context, Result};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsClientConfig {
    pub ca_cert_path: Option<PathBuf>,
    pub domain_name: Option<String>,
}

impl TlsClientConfig {
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.ca_cert_path.is_some() || self.domain_name.is_some()
    }
}

pub fn build_endpoint(url: &str, tls: Option<&TlsClientConfig>) -> Result<Endpoint> {
    let mut endpoint = Endpoint::from_shared(url.to_string())?;
    if url.starts_with("https://") || tls.is_some() {
        let mut tls_config = ClientTlsConfig::new();
        if let Some(tls) = tls {
            if let Some(path) = &tls.ca_cert_path {
                let pem = std::fs::read(path).with_context(|| {
                    format!("Failed to read TLS CA certificate {}", path.display())
                })?;
                tls_config = tls_config.ca_certificate(Certificate::from_pem(pem));
            }
            if let Some(domain_name) = &tls.domain_name {
                tls_config = tls_config.domain_name(domain_name.clone());
            }
        }
        endpoint = endpoint.tls_config(tls_config)?;
    }
    Ok(endpoint)
}

pub async fn connect_channel(url: &str, tls: Option<&TlsClientConfig>) -> Result<Channel> {
    Ok(build_endpoint(url, tls)?.connect().await?)
}

pub fn request_with_bearer<T>(message: T, auth_token: Option<&str>) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(token) = auth_token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    request
}
