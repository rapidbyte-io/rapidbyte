//! Controller server subcommand.

use anyhow::Result;
use std::path::Path;

pub async fn execute(
    listen: &str,
    signing_key: Option<&str>,
    auth_token: Option<&str>,
    tls_cert: Option<&Path>,
    tls_key: Option<&Path>,
) -> Result<()> {
    let config = build_config(listen, signing_key, auth_token, tls_cert, tls_key)?;
    rapidbyte_controller::run(config).await
}

fn build_config(
    listen: &str,
    signing_key: Option<&str>,
    auth_token: Option<&str>,
    tls_cert: Option<&Path>,
    tls_key: Option<&Path>,
) -> Result<rapidbyte_controller::ControllerConfig> {
    let addr = listen
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid listen address: {e}"))?;
    let mut config = rapidbyte_controller::ControllerConfig {
        listen_addr: addr,
        ..Default::default()
    };
    if let Some(key) = signing_key {
        config.signing_key = key.as_bytes().to_vec();
    }
    if let Some(token) = auth_token {
        config.auth_tokens = vec![token.to_string()];
    }
    match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => {
            config.tls = Some(rapidbyte_controller::ServerTlsConfig {
                cert_pem: std::fs::read(cert)?,
                key_pem: std::fs::read(key)?,
            });
        }
        (None, None) => {}
        _ => anyhow::bail!("controller TLS requires both --tls-cert and --tls-key"),
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn controller_execute_uses_auth_token() {
        let config =
            build_config("[::]:9090", Some("signing"), Some("secret"), None, None).unwrap();
        assert_eq!(config.auth_tokens, vec!["secret".to_string()]);
        assert_eq!(config.signing_key, b"signing".to_vec());
    }

    #[test]
    fn controller_execute_wires_tls() {
        let dir = tempdir().unwrap();
        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");
        std::fs::write(&cert_path, b"cert-pem").unwrap();
        std::fs::write(&key_path, b"key-pem").unwrap();

        let config = build_config(
            "[::]:9090",
            None,
            None,
            Some(cert_path.as_path()),
            Some(key_path.as_path()),
        )
        .unwrap();

        assert_eq!(config.tls.as_ref().unwrap().cert_pem, b"cert-pem");
        assert_eq!(config.tls.as_ref().unwrap().key_pem, b"key-pem");
    }
}
