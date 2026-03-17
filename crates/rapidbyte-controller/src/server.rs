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
use crate::adapter::postgres::event_bus::PgEventBus;
use crate::adapter::postgres::run::PgRunRepository;
use crate::adapter::postgres::store::PgPipelineStore;
use crate::adapter::postgres::task::PgTaskRepository;
use crate::adapter::secrets::VaultSecretResolver;
use crate::application::context::{AppConfig, AppContext, RegistryConfig};
use crate::config::ControllerConfig;
use crate::proto::rapidbyte::v1::agent_service_server::AgentServiceServer;
use crate::proto::rapidbyte::v1::pipeline_service_server::PipelineServiceServer;

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
    if !config.auth.allow_unauthenticated && config.auth.tokens.is_empty() {
        anyhow::bail!(
            "controller misconfigured: auth tokens are empty and allow_unauthenticated is false — all requests would be rejected"
        );
    }

    // 1. Connect to Postgres
    let database_url = config
        .metadata_database_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("metadata_database_url is required"))?;
    let pool = PgPool::connect(database_url).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;

    // 2. Build adapters
    let runs = Arc::new(PgRunRepository::new(pool.clone()));
    let tasks = Arc::new(PgTaskRepository::new(pool.clone()));
    let agents = Arc::new(PgAgentRepository::new(pool.clone()));
    let store = Arc::new(PgPipelineStore::new(pool.clone()));
    let event_bus = Arc::new(PgEventBus::new(pool.clone()));
    let secrets_resolver = Arc::new(VaultSecretResolver::new(secrets));
    let clock = Arc::new(SystemClock);

    // 3. Start PG LISTEN listener for real-time event dispatch
    event_bus.start_listener().await?;

    // 4. Build AppConfig from ControllerConfig
    let registry = config.registry.url.map(|url| RegistryConfig {
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

    // 7. Build gRPC services with auth interceptor
    let auth = BearerAuthInterceptor::new(
        config.auth.tokens.clone(),
        config.auth.allow_unauthenticated,
    );
    let pipeline_svc = PipelineServiceServer::with_interceptor(
        PipelineGrpcService::new(ctx.clone()),
        auth.clone(),
    );
    let agent_svc = AgentServiceServer::with_interceptor(AgentGrpcService::new(ctx.clone()), auth);

    // 8. Configure TLS if provided
    let mut builder = Server::builder();
    if let Some(tls) = config.tls {
        let identity = tonic::transport::Identity::from_pem(tls.cert_pem, tls.key_pem);
        builder =
            builder.tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?;
    }

    // 9. Start server
    tracing::info!(addr = %config.listen_addr, "controller listening");
    builder
        .add_service(pipeline_svc)
        .add_service(agent_svc)
        .serve(config.listen_addr)
        .await?;

    Ok(())
}
