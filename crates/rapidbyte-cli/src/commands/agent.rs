//! Agent worker subcommand.

use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::info;

#[allow(clippy::too_many_arguments)]
pub async fn execute(
    controller: &str,
    max_tasks: u32,
    auth_token: Option<&str>,
    controller_ca_cert: Option<&Path>,
    controller_tls_domain: Option<&str>,
    metrics_listen: Option<&str>,
    registry_url: Option<&str>,
    registry_insecure: bool,
    trust_policy: &str,
    trusted_key_pems: Vec<String>,
    otel_guard: rapidbyte_metrics::OtelGuard,
) -> Result<()> {
    let config = build_config(
        controller,
        max_tasks,
        auth_token,
        controller_ca_cert,
        controller_tls_domain,
        metrics_listen,
        registry_url,
        registry_insecure,
        trust_policy,
        trusted_key_pems,
    )?;

    let otel_guard = Arc::new(otel_guard);
    let (ctx, registration, adapters) =
        rapidbyte_agent::build_agent_context(&config, otel_guard).await?;

    // Set up graceful shutdown on SIGINT / SIGTERM
    let shutdown = CancellationToken::new();
    let shutdown_signal = shutdown.clone();

    tokio::spawn(async move {
        shutdown_signal_wait().await;
        info!("Shutdown signal received, stopping agent ...");
        shutdown_signal.cancel();
    });

    rapidbyte_agent::run_agent(&ctx, registration, &adapters, shutdown).await?;
    Ok(())
}

async fn shutdown_signal_wait() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl-c");
    }
}

#[allow(clippy::too_many_arguments)]
fn build_config(
    controller: &str,
    max_tasks: u32,
    auth_token: Option<&str>,
    controller_ca_cert: Option<&Path>,
    controller_tls_domain: Option<&str>,
    metrics_listen: Option<&str>,
    registry_url: Option<&str>,
    registry_insecure: bool,
    trust_policy: &str,
    trusted_key_pems: Vec<String>,
) -> Result<rapidbyte_agent::AgentConfig> {
    let controller_tls = if controller_ca_cert.is_some() || controller_tls_domain.is_some() {
        Some(rapidbyte_agent::ClientTlsConfig {
            ca_cert_pem: match controller_ca_cert {
                Some(ca_cert) => std::fs::read(ca_cert)?,
                None => Vec::new(),
            },
            domain_name: controller_tls_domain.map(str::to_owned),
        })
    } else {
        None
    };

    Ok(rapidbyte_agent::AgentConfig {
        controller_url: controller.into(),
        max_tasks,
        auth_token: auth_token.map(str::to_owned),
        controller_tls,
        metrics_listen: metrics_listen.map(str::to_owned),
        registry_url: registry_url.map(str::to_owned),
        registry_insecure,
        trust_policy: trust_policy.to_owned(),
        trusted_key_pems,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn agent_execute_wires_tls() {
        let dir = tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, b"ca-pem").unwrap();

        let config = build_config(
            "https://controller.example:9090",
            4,
            Some("secret"),
            Some(ca_path.as_path()),
            Some("controller.example"),
            None,
            None,
            false,
            "skip",
            vec![],
        )
        .unwrap();

        assert_eq!(
            config.controller_tls.as_ref().unwrap().ca_cert_pem,
            b"ca-pem"
        );
        assert_eq!(
            config
                .controller_tls
                .as_ref()
                .unwrap()
                .domain_name
                .as_deref(),
            Some("controller.example")
        );
        assert_eq!(config.auth_token.as_deref(), Some("secret"));
    }

    #[test]
    fn agent_execute_no_tls() {
        let config = build_config(
            "http://controller.example:9090",
            1,
            None,
            None,
            None,
            None,
            None,
            false,
            "skip",
            vec![],
        )
        .unwrap();

        assert!(config.controller_tls.is_none());
        assert!(config.auth_token.is_none());
    }
}
