use std::sync::Arc;

use anyhow::Result;
use rapidbyte_secrets::SecretProviders;
use sqlx::PgPool;
use tonic::transport::Server;

use crate::adapter::clock::SystemClock;
use crate::adapter::grpc::agent::AgentGrpcService;
use crate::adapter::grpc::auth::BearerAuthInterceptor;
use crate::adapter::grpc::pipeline::PipelineGrpcService;
use crate::adapter::postgres::agent::PgAgentRepository;
use crate::adapter::postgres::cursor_store::PgCursorStore;
use crate::adapter::postgres::event_bus::PgEventBus;
use crate::adapter::postgres::log_store::PgLogStore;
use crate::adapter::postgres::run::PgRunRepository;
use crate::adapter::postgres::store::PgPipelineStore;
use crate::adapter::postgres::task::PgTaskRepository;
use crate::adapter::rest::extractors::RestState;
use crate::adapter::secrets::VaultSecretResolver;
use crate::application::context::{AppConfig, AppContext, RegistryConfig};
use crate::application::services::AppServices;
use crate::config::ControllerConfig;
use crate::domain::ports::connection_tester::{
    ConnectionTestError, ConnectionTester, DiscoveryResult, TestResult,
};
use crate::domain::ports::plugin_registry::{
    InstalledPlugin, PluginMetadata, PluginRegistry, RegistryEntry, RegistryError,
};
use crate::proto::rapidbyte::v1::agent_service_server::AgentServiceServer;
use crate::proto::rapidbyte::v1::pipeline_service_server::PipelineServiceServer;

// ---------------------------------------------------------------------------
// NoOpConnectionTester
// ---------------------------------------------------------------------------

/// Placeholder `ConnectionTester` used by the standalone controller server.
///
/// The real implementation lives in the CLI crate where Wasmtime plugins are
/// available.  This no-op returns a `Plugin` error so callers know that
/// connection testing requires engine context.
struct NoOpConnectionTester;

#[async_trait::async_trait]
impl ConnectionTester for NoOpConnectionTester {
    async fn test(&self, _config: &serde_json::Value) -> Result<TestResult, ConnectionTestError> {
        Err(ConnectionTestError::Plugin(
            "connection testing requires engine context".into(),
        ))
    }

    async fn discover(
        &self,
        _config: &serde_json::Value,
        _table: Option<&str>,
    ) -> Result<DiscoveryResult, ConnectionTestError> {
        Err(ConnectionTestError::Plugin(
            "connection discovery requires engine context".into(),
        ))
    }
}

// ---------------------------------------------------------------------------
// NoOpPluginRegistry
// ---------------------------------------------------------------------------

/// Placeholder `PluginRegistry` used by the standalone controller server.
///
/// The real implementation lives in the CLI crate where the local plugin
/// store and remote registry are accessible.  This no-op returns a `Registry`
/// error so callers know that plugin operations require engine context.
struct NoOpPluginRegistry;

#[async_trait::async_trait]
impl PluginRegistry for NoOpPluginRegistry {
    async fn list_installed(&self) -> Result<Vec<InstalledPlugin>, RegistryError> {
        Ok(vec![])
    }

    async fn search(
        &self,
        _query: &str,
        _plugin_type: Option<&str>,
    ) -> Result<Vec<RegistryEntry>, RegistryError> {
        Ok(vec![])
    }

    async fn info(&self, _plugin_ref: &str) -> Result<PluginMetadata, RegistryError> {
        Err(RegistryError::Unavailable(
            "plugin info requires engine context".into(),
        ))
    }

    async fn install(&self, _plugin_ref: &str) -> Result<InstalledPlugin, RegistryError> {
        Err(RegistryError::Unavailable(
            "plugin installation requires engine context".into(),
        ))
    }

    async fn remove(&self, _plugin_ref: &str) -> Result<(), RegistryError> {
        Err(RegistryError::Unavailable(
            "plugin removal requires engine context".into(),
        ))
    }
}

// ---------------------------------------------------------------------------

/// Shared server components produced by [`setup`].
struct ServerComponents {
    ctx: Arc<AppContext>,
    config: ControllerConfig,
    started_at: chrono::DateTime<chrono::Utc>,
}

/// Perform shared setup: metrics, auth validation, DB pool + migrations, adapters,
/// event bus listener, `AppContext`, and background tasks.
///
/// # Errors
///
/// Returns an error if auth config is invalid, the database connection or
/// migration fails, or the event bus listener cannot be started.
#[allow(clippy::too_many_lines)]
async fn setup(
    config: ControllerConfig,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
    secrets: SecretProviders,
) -> Result<ServerComponents> {
    let started_at = chrono::Utc::now();

    // 0. Optionally bind Prometheus metrics endpoint.
    //    The otel_guard is kept alive for the server lifetime regardless.
    let _otel_keep = otel_guard.clone();
    if let Some(ref metrics_addr) = config.metrics_listen {
        tracing::info!(addr = %metrics_addr, "Prometheus metrics endpoint");
        let metrics_listener = rapidbyte_metrics::bind_prometheus(metrics_addr).await?;
        tokio::spawn(rapidbyte_metrics::serve_prometheus(
            otel_guard,
            metrics_listener,
        ));
    }

    // 0.5. Validate configuration
    if !config.auth.allow_unauthenticated {
        if config.auth.tokens.is_empty() {
            anyhow::bail!(
                "controller misconfigured: auth tokens are empty and allow_unauthenticated is false — all requests would be rejected"
            );
        }
        if config.auth.tokens.iter().any(|t| t.trim().is_empty()) {
            anyhow::bail!(
                "controller misconfigured: auth tokens contain empty or whitespace-only entries"
            );
        }
    }
    if !config.auth.allow_insecure_default_signing_key {
        let default_key = ControllerConfig::default().auth.signing_key;
        if config.auth.signing_key.is_empty() || config.auth.signing_key == default_key {
            anyhow::bail!(
                "controller misconfigured: signing key is missing or uses the insecure default — \
                 set --signing-key or use --allow-insecure-signing-key for development"
            );
        }
    }

    // 1. Connect to Postgres
    let database_url = config
        .metadata_database_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("metadata_database_url is required"))?;
    // Use from_str to handle both postgres:// URLs and libpq-style connection strings
    let connect_options: sqlx::postgres::PgConnectOptions = database_url
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid database URL: {e}"))?;
    let pool = PgPool::connect_with(connect_options).await?;
    sqlx::migrate!("./migrations")
        .set_ignore_missing(true)
        .run(&pool)
        .await?;

    // 2. Build adapters
    let runs = Arc::new(PgRunRepository::new(pool.clone()));
    let tasks = Arc::new(PgTaskRepository::new(pool.clone()));
    let agents = Arc::new(PgAgentRepository::new(pool.clone()));
    let store = Arc::new(PgPipelineStore::new(pool.clone()));
    let event_bus = Arc::new(PgEventBus::new(pool.clone()));
    let cursor_store = Arc::new(PgCursorStore::new(pool.clone()));
    let log_store = Arc::new(PgLogStore::new(pool.clone()));
    let secrets_resolver = Arc::new(VaultSecretResolver::new(secrets));
    let clock = Arc::new(SystemClock);

    // 3. Start PG LISTEN listener for real-time event dispatch
    event_bus.start_listener().await?;

    // 4. Build AppConfig from ControllerConfig
    let registry = config.registry.url.clone().map(|url| RegistryConfig {
        url,
        insecure: config.registry.insecure,
    });

    let app_config = AppConfig {
        default_lease_duration: config.timers.default_lease_duration,
        lease_check_interval: config.timers.lease_check_interval,
        agent_reap_timeout: config.timers.agent_reap_timeout,
        agent_reap_interval: config.timers.agent_reap_interval,
        default_max_retries: config.timers.default_max_retries,
        registry,
        allow_unauthenticated: config.auth.allow_unauthenticated,
    };

    // 5. Compose AppContext
    let ctx = Arc::new(AppContext {
        runs,
        tasks,
        agents,
        store,
        event_bus,
        secrets: secrets_resolver,
        clock,
        cursor_store,
        log_store,
        connection_tester: Arc::new(NoOpConnectionTester),
        plugin_registry: Arc::new(NoOpPluginRegistry),
        config: app_config,
    });

    // 6. Spawn background tasks
    let ctx_sweep = ctx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(ctx_sweep.config.lease_check_interval);
        loop {
            interval.tick().await;
            if let Err(e) =
                crate::application::background::lease_sweep::sweep_expired_leases(&ctx_sweep).await
            {
                tracing::error!(error = %e, "lease sweep failed");
            }
        }
    });

    let ctx_reaper = ctx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(ctx_reaper.config.agent_reap_interval);
        loop {
            interval.tick().await;
            if let Err(e) =
                crate::application::background::agent_reaper::reap_stale_agents(&ctx_reaper).await
            {
                tracing::error!(error = %e, "agent reaper failed");
            }
        }
    });

    Ok(ServerComponents {
        ctx,
        config,
        started_at,
    })
}

/// Build the gRPC server future from shared components.
fn build_grpc_server(
    components: &ServerComponents,
) -> Result<impl std::future::Future<Output = Result<(), tonic::transport::Error>>> {
    let auth = BearerAuthInterceptor::new(components.config.auth.clone());

    // Use a placeholder address for gRPC-only mode; the listen_addr is only
    // relevant for REST URL generation which is not used by the gRPC adapter.
    let grpc_services = Arc::new(AppServices::new(
        components.ctx.clone(),
        components.started_at,
        components.config.listen_addr,
    ));

    let pipeline_svc = PipelineServiceServer::with_interceptor(
        PipelineGrpcService::new(grpc_services),
        auth.clone(),
    );
    let agent_svc =
        AgentServiceServer::with_interceptor(AgentGrpcService::new(components.ctx.clone()), auth);

    let mut builder = Server::builder();
    if let Some(ref tls) = components.config.tls {
        let identity =
            tonic::transport::Identity::from_pem(tls.cert_pem.clone(), tls.key_pem.clone());
        builder =
            builder.tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?;
    }

    let listen_addr = components.config.listen_addr;
    tracing::info!(addr = %listen_addr, "controller gRPC listening");
    Ok(builder
        .add_service(pipeline_svc)
        .add_service(agent_svc)
        .serve(listen_addr))
}

/// Start the controller gRPC server.
///
/// This is the composition root: it builds adapters from configuration, wires
/// them into the application context, spawns background tasks, and starts
/// serving gRPC requests.
///
/// # Errors
///
/// Returns an error if the database connection, migration, TLS setup, or gRPC
/// server startup fails.
#[allow(clippy::similar_names, clippy::too_many_lines)]
pub async fn run(
    config: ControllerConfig,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
    secrets: SecretProviders,
) -> Result<()> {
    let components = setup(config, otel_guard, secrets).await?;
    let grpc_server = build_grpc_server(&components)?;
    grpc_server.await?;
    Ok(())
}

/// Start the controller gRPC server and REST API server.
///
/// This is the full composition root for `rapidbyte serve`: it wires all
/// adapters, spawns background tasks, and starts both the gRPC server and
/// the REST HTTP server in parallel. If `config.rest_listen_addr` is `None`,
/// only the gRPC server is started (identical to [`run`]).
///
/// # Errors
///
/// Returns an error if the database connection, migration, TLS setup, gRPC
/// server startup, or REST server startup fails.
#[allow(clippy::similar_names, clippy::too_many_lines)]
pub async fn serve(
    config: ControllerConfig,
    otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
    secrets: SecretProviders,
) -> Result<()> {
    let components = setup(config, otel_guard, secrets).await?;
    let grpc_server = build_grpc_server(&components)?;

    if let Some(rest_addr) = components.config.rest_listen_addr {
        let services = Arc::new(AppServices::new(
            components.ctx.clone(),
            components.started_at,
            rest_addr,
        ));
        let rest_state = RestState {
            services,
            auth_config: components.config.auth.clone(),
        };
        let rest_router = crate::adapter::rest::router(rest_state);
        let rest_listener = tokio::net::TcpListener::bind(rest_addr).await?;
        tracing::info!(addr = %rest_addr, "controller REST listening");

        tokio::select! {
            result = grpc_server => result?,
            result = axum::serve(rest_listener, rest_router) => result?,
        }
    } else {
        grpc_server.await?;
    }

    Ok(())
}
