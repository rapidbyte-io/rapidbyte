use std::sync::Arc;

use anyhow::Result;
use rapidbyte_secrets::SecretProviders;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::adapter::clock::SystemClock;
use crate::adapter::grpc::agent::AgentGrpcService;
use crate::adapter::grpc::auth::BearerAuthInterceptor;
use crate::adapter::grpc::pipeline::PipelineGrpcService;
use crate::adapter::noop::{NoOpConnectionTester, NoOpPipelineInspector, NoOpPluginRegistry};
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
use crate::application::embedded_agent::{EmbeddedAgentConfig, TaskExecutor};
use crate::application::services::AppServices;
use crate::config::ControllerConfig;
use crate::domain::ports::connection_tester::ConnectionTester;
use crate::domain::ports::pipeline_inspector::PipelineInspector;
use crate::domain::ports::pipeline_source::{PipelineInfo, PipelineSource, PipelineSourceError};
use crate::domain::ports::plugin_registry::PluginRegistry;
use crate::proto::rapidbyte::v1::agent_service_server::AgentServiceServer;
use crate::proto::rapidbyte::v1::pipeline_service_server::PipelineServiceServer;

// ---------------------------------------------------------------------------
// NoOpPipelineSource (private — only used by run() backwards-compat shim)
// ---------------------------------------------------------------------------

/// Placeholder `PipelineSource` used when no project directory is available.
struct NoOpPipelineSource;

#[async_trait::async_trait]
impl PipelineSource for NoOpPipelineSource {
    async fn list(&self) -> Result<Vec<PipelineInfo>, PipelineSourceError> {
        Ok(vec![])
    }

    async fn get(&self, name: &str) -> Result<String, PipelineSourceError> {
        Err(PipelineSourceError::NotFound(name.to_string()))
    }

    async fn connections_yaml(&self) -> Result<Option<String>, PipelineSourceError> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// ServeContext
// ---------------------------------------------------------------------------

/// Context provided by the caller for engine-backed operations.
///
/// Contains driven-port implementations and optionally a [`TaskExecutor`]
/// for the embedded agent.  The CLI supplies real implementations; the
/// standalone `run()` shim uses no-op stubs.
pub struct ServeContext {
    /// Guard that keeps the OpenTelemetry exporter alive for the server
    /// lifetime.
    pub otel_guard: Arc<rapidbyte_metrics::OtelGuard>,
    /// Secret provider backends (Vault, env-var, …).
    pub secrets: SecretProviders,
    /// Source of pipeline YAML definitions (e.g. filesystem or in-memory).
    pub pipeline_source: Arc<dyn PipelineSource>,
    /// Driver for testing connector reachability and schema discovery.
    pub connection_tester: Arc<dyn ConnectionTester>,
    /// Registry for listing, installing, and removing plugins.
    pub plugin_registry: Arc<dyn PluginRegistry>,
    /// Inspector for synchronous pipeline operations (check, diff).
    pub pipeline_inspector: Arc<dyn PipelineInspector>,
    /// When `Some`, an embedded agent is spawned alongside the server that
    /// executes tasks using this executor.
    pub task_executor: Option<Arc<dyn TaskExecutor>>,
}

// ---------------------------------------------------------------------------
// ServerComponents
// ---------------------------------------------------------------------------

/// Shared server components produced by [`setup`].
struct ServerComponents {
    ctx: Arc<AppContext>,
    config: ControllerConfig,
    started_at: chrono::DateTime<chrono::Utc>,
}

// ---------------------------------------------------------------------------
// setup
// ---------------------------------------------------------------------------

/// Perform shared setup: metrics, auth validation, DB pool + migrations, adapters,
/// event bus listener, `AppContext`, and background tasks.
///
/// # Errors
///
/// Returns an error if auth config is invalid, the database connection or
/// migration fails, or the event bus listener cannot be started.
#[allow(clippy::too_many_lines)]
async fn setup(config: ControllerConfig, ctx: ServeContext) -> Result<ServerComponents> {
    let started_at = chrono::Utc::now();

    // 0. Optionally bind Prometheus metrics endpoint.
    //    The otel_guard is kept alive for the server lifetime regardless.
    let _otel_keep = ctx.otel_guard.clone();
    if let Some(ref metrics_addr) = config.metrics_listen {
        tracing::info!(addr = %metrics_addr, "Prometheus metrics endpoint");
        let metrics_listener = rapidbyte_metrics::bind_prometheus(metrics_addr).await?;
        tokio::spawn(rapidbyte_metrics::serve_prometheus(
            ctx.otel_guard,
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
    let secrets_resolver = Arc::new(VaultSecretResolver::new(ctx.secrets));
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

    // 5. Compose AppContext — use the driven-port implementations from ServeContext
    let app_ctx = Arc::new(AppContext {
        runs,
        tasks,
        agents,
        store,
        event_bus,
        secrets: secrets_resolver,
        clock,
        cursor_store,
        log_store,
        connection_tester: ctx.connection_tester,
        plugin_registry: ctx.plugin_registry,
        pipeline_source: ctx.pipeline_source,
        pipeline_inspector: ctx.pipeline_inspector,
        config: app_config,
    });

    // 6. Spawn background tasks
    let ctx_sweep = app_ctx.clone();
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

    let ctx_reaper = app_ctx.clone();
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
        ctx: app_ctx,
        config,
        started_at,
    })
}

// ---------------------------------------------------------------------------
// build_grpc_server
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// run  (backwards-compatible shim)
// ---------------------------------------------------------------------------

/// Start the controller gRPC server.
///
/// This is a backwards-compatible shim that builds a [`ServeContext`] with
/// no-op driven-port stubs and delegates to [`serve`].
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
    let ctx = ServeContext {
        otel_guard,
        secrets,
        pipeline_source: Arc::new(NoOpPipelineSource),
        connection_tester: Arc::new(NoOpConnectionTester),
        plugin_registry: Arc::new(NoOpPluginRegistry),
        pipeline_inspector: Arc::new(NoOpPipelineInspector),
        task_executor: None,
    };
    serve(config, ctx).await
}

// ---------------------------------------------------------------------------
// serve
// ---------------------------------------------------------------------------

/// Start the controller gRPC server and REST API server.
///
/// This is the full composition root for `rapidbyte serve`: it wires all
/// adapters, spawns background tasks, and starts both the gRPC server and
/// the REST HTTP server in parallel.  If `config.rest_listen_addr` is `None`,
/// only the gRPC server is started (identical to [`run`]).
///
/// If `ctx.task_executor` is `Some`, an embedded agent is spawned in the
/// background and will begin polling for and executing tasks immediately.
///
/// # Errors
///
/// Returns an error if the database connection, migration, TLS setup, gRPC
/// server startup, or REST server startup fails.
#[allow(clippy::similar_names, clippy::too_many_lines)]
pub async fn serve(config: ControllerConfig, ctx: ServeContext) -> Result<()> {
    let task_executor = ctx.task_executor.clone();
    let components = setup(config, ctx).await?;
    let grpc_server = build_grpc_server(&components)?;

    // Spawn the embedded agent if a TaskExecutor was provided.
    if let Some(executor) = task_executor {
        let agent_ctx = components.ctx.clone();
        let agent_cancel = CancellationToken::new();
        tokio::spawn(async move {
            if let Err(e) = crate::application::embedded_agent::run_embedded_agent(
                agent_ctx,
                executor,
                EmbeddedAgentConfig::default(),
                agent_cancel,
            )
            .await
            {
                tracing::error!(error = %e, "embedded agent failed");
            }
        });
    }

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

        if let Some(ref tls) = components.config.tls {
            // TLS mode — use axum_server with rustls
            let rustls_config = axum_server::tls_rustls::RustlsConfig::from_pem(
                tls.cert_pem.clone(),
                tls.key_pem.clone(),
            )
            .await?;
            tracing::info!(addr = %rest_addr, tls = true, "controller REST listening");
            tokio::select! {
                result = grpc_server => result?,
                result = axum_server::bind_rustls(rest_addr, rustls_config)
                    .serve(rest_router.into_make_service()) => result?,
            }
        } else {
            // Plaintext mode
            let rest_listener = tokio::net::TcpListener::bind(rest_addr).await?;
            tracing::info!(addr = %rest_addr, tls = false, "controller REST listening");
            tokio::select! {
                result = grpc_server => result?,
                result = axum::serve(rest_listener, rest_router) => result?,
            }
        }
    } else {
        grpc_server.await?;
    }

    Ok(())
}
