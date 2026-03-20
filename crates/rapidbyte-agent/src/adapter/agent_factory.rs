//! Composition root: wires all adapters into an `AgentContext`.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::application::context::{AgentAppConfig, AgentContext};
use crate::domain::ports::controller::RegistrationConfig;

use super::channel_progress::AtomicProgressCollector;
use super::engine_executor::EngineExecutor;
use super::grpc_controller::{ClientTlsConfig, GrpcControllerGateway};

/// Full infrastructure configuration for agent startup.
#[derive(Clone)]
pub struct AgentConfig {
    pub controller_url: String,
    pub max_tasks: u32,
    pub heartbeat_interval: Duration,
    pub auth_token: Option<String>,
    pub controller_tls: Option<ClientTlsConfig>,
    pub metrics_listen: Option<String>,
    pub registry_url: Option<String>,
    pub registry_insecure: bool,
    pub trust_policy: String,
    pub trusted_key_pems: Vec<String>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            controller_url: "http://[::]:9090".into(),
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(10),
            auth_token: None,
            controller_tls: None,
            metrics_listen: None,
            registry_url: None,
            registry_insecure: false,
            trust_policy: "skip".into(),
            trusted_key_pems: Vec::new(),
        }
    }
}

/// Concrete adapter handles returned alongside the context.
///
/// Provides access to concrete adapter types that have methods beyond their
/// port trait interfaces (e.g., `EngineExecutor::update_registry_config`,
/// `AtomicProgressCollector::update`).
pub struct AgentAdapters {
    /// Engine executor adapter (concrete type for registry config updates).
    pub engine_executor: Arc<EngineExecutor>,
    /// Progress collector adapter (concrete type for write-side updates).
    pub progress_collector: Arc<AtomicProgressCollector>,
}

/// Build the agent context from infrastructure config.
///
/// Connects to the controller, initialises metrics, and wires all adapters.
/// Returns the DI context, registration config, and concrete adapter handles.
///
/// # Errors
///
/// Returns an error if the gRPC connection or Prometheus bind fails.
pub async fn build_agent_context(
    config: &AgentConfig,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
) -> Result<(AgentContext, RegistrationConfig, AgentAdapters), anyhow::Error> {
    // Start Prometheus endpoint if configured
    if let Some(ref addr) = config.metrics_listen {
        info!("Prometheus metrics endpoint at {addr}");
        let listener = rapidbyte_metrics::bind_prometheus(addr).await?;
        tokio::spawn(rapidbyte_metrics::serve_prometheus(
            otel_guard.clone(),
            listener,
        ));
    }

    let gateway = Arc::new(
        GrpcControllerGateway::connect(
            &config.controller_url,
            config.controller_tls.as_ref(),
            config.auth_token.as_deref(),
        )
        .await?,
    );

    let trust_policy = rapidbyte_registry::TrustPolicy::from_str_name(&config.trust_policy)
        .map_err(|e| anyhow::anyhow!("invalid trust_policy '{}': {e}", config.trust_policy))?;

    let registry_config = rapidbyte_registry::RegistryConfig {
        insecure: config.registry_insecure,
        default_registry: rapidbyte_registry::normalize_registry_url_option(
            config.registry_url.as_deref(),
        ),
        trust_policy,
        trusted_key_pems: config.trusted_key_pems.clone(),
        ..Default::default()
    };

    let executor = Arc::new(EngineExecutor::new(registry_config));
    let progress = Arc::new(AtomicProgressCollector::new());

    let ctx = AgentContext {
        gateway,
        executor: executor.clone(),
        progress: progress.clone(),
        config: AgentAppConfig {
            max_tasks: config.max_tasks,
            heartbeat_interval: config.heartbeat_interval,
            ..AgentAppConfig::default()
        },
    };

    let registration = RegistrationConfig {
        max_tasks: config.max_tasks,
    };

    let adapters = AgentAdapters {
        engine_executor: executor,
        progress_collector: progress,
    };

    Ok((ctx, registration, adapters))
}
