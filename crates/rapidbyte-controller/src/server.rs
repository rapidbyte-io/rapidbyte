//! gRPC server startup and wiring.

use std::sync::Arc;

use tonic::transport::{Identity, Server, ServerTlsConfig as TonicServerTlsConfig};
use tracing::info;

use crate::adapters::grpc::agent_bridge::AgentHandler;
use crate::adapters::grpc::agent_session::AgentSessionHandler;
use crate::adapters::grpc::control_plane::ControlPlaneHandler;
use crate::background;
use crate::config::{initialize_metadata_store, validate, ControllerConfig, DEFAULT_SIGNING_KEY};
use crate::middleware::BearerAuthInterceptor;
use crate::proto::rapidbyte::v2::agent_session_server::AgentSessionServer;
use crate::proto::rapidbyte::v2::control_plane_server::ControlPlaneServer;
use crate::state::ControllerState;

/// Start the controller gRPC server.
///
/// # Errors
///
/// Returns an error if the gRPC server fails to bind or encounters a
/// transport-level failure.
///
pub async fn run(
    config: ControllerConfig,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
    secrets: rapidbyte_secrets::SecretProviders,
) -> anyhow::Result<()> {
    validate(&config)?;

    if let Some(ref metrics_addr) = config.metrics_listen {
        tracing::info!("Prometheus metrics endpoint at {metrics_addr}");
        let metrics_listener = rapidbyte_metrics::bind_prometheus(metrics_addr).await?;
        tokio::spawn(rapidbyte_metrics::serve_prometheus(
            otel_guard.clone(),
            metrics_listener,
        ));
    }

    let metadata_store = initialize_metadata_store(&config).await?;

    if config.auth.signing_key == DEFAULT_SIGNING_KEY {
        tracing::warn!(
            "Using default signing key — set RAPIDBYTE_SIGNING_KEY for production deployments"
        );
    }

    let state =
        ControllerState::from_metadata_store(&config.auth.signing_key, metadata_store).await?;
    let state = state.with_secrets(secrets);

    let reaper_task = background::spawn_reaper(
        state.clone(),
        config.timers.agent_reap_interval,
        config.timers.agent_reap_timeout,
    );
    let lease_sweep_task = background::spawn_lease_sweep(
        state.clone(),
        config.timers.lease_check_interval,
        config.timers.reconciliation_timeout,
    );
    let preview_cleanup_task =
        background::spawn_preview_cleanup(state.clone(), config.timers.preview_cleanup_interval);
    let background_tasks = (reaper_task, lease_sweep_task, preview_cleanup_task);

    let auth = BearerAuthInterceptor::new(config.auth.tokens.clone());

    let control_plane_service =
        ControlPlaneServer::with_interceptor(ControlPlaneHandler::new(state.clone()), auth.clone());

    // Read trusted key PEM contents from files
    let mut trusted_key_pems: Vec<String> = Vec::new();
    for path in &config.trust.trusted_key_paths {
        let pem = std::fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!("Failed to read trusted key file {}: {e}", path.display())
        })?;
        trusted_key_pems.push(pem);
    }

    let agent_handler = AgentHandler::with_trust_config(
        state,
        config.registry.url.clone().unwrap_or_default(),
        config.registry.insecure,
        config.trust.policy.clone(),
        trusted_key_pems,
    );
    let agent_session_service =
        AgentSessionServer::with_interceptor(AgentSessionHandler::new(agent_handler), auth.clone());

    info!(addr = %config.listen_addr, "Controller listening");

    let mut server = Server::builder().layer(rapidbyte_metrics::grpc_layer::GrpcMetricsLayer);
    if let Some(tls) = &config.tls {
        server = server.tls_config(TonicServerTlsConfig::new().identity(Identity::from_pem(
            tls.cert_pem.clone(),
            tls.key_pem.clone(),
        )))?;
    }

    server
        .add_service(control_plane_service)
        .add_service(agent_session_service)
        .serve(config.listen_addr)
        .await?;

    drop(background_tasks);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ControllerConfig;

    #[tokio::test]
    async fn run_fails_when_metrics_listener_is_unavailable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let guard =
            Arc::new(rapidbyte_metrics::init("test-controller").expect("otel init should succeed"));
        let err = run(
            ControllerConfig {
                auth: crate::config::AuthConfig {
                    tokens: vec!["secret".into()],
                    signing_key: b"test-signing-key".to_vec(),
                },
                metrics_listen: Some(addr.to_string()),
                ..Default::default()
            },
            guard,
            rapidbyte_secrets::SecretProviders::new(),
        )
        .await
        .unwrap_err();

        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("address already in use") || msg.contains("addrinuse"),
            "unexpected error: {err:#}"
        );
    }
}
