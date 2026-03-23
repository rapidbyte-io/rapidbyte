pub mod error;
pub mod extract;
pub mod middleware;
pub mod routes;
pub mod sse;

use std::sync::Arc;

use axum::middleware as axum_middleware;
use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;

use rapidbyte_api::{ApiContext, DeploymentMode};

/// Build the full axum router with all routes, auth middleware, and CORS.
pub fn build_router(ctx: ApiContext, auth_config: middleware::AuthConfig) -> Router {
    let cors = if auth_config.required {
        CorsLayer::new()
    } else {
        CorsLayer::permissive()
    };

    let auth_config = Arc::new(auth_config);

    let api = Router::new()
        // Pipelines
        .route("/pipelines", get(routes::pipelines::list))
        .route("/pipelines/sync", post(routes::pipelines::sync_batch))
        .route("/pipelines/assert", post(routes::pipelines::assert_all))
        .route("/pipelines/{name}", get(routes::pipelines::get))
        .route("/pipelines/{name}/sync", post(routes::pipelines::sync))
        .route("/pipelines/{name}/check", post(routes::pipelines::check))
        .route(
            "/pipelines/{name}/check-apply",
            post(routes::pipelines::check_apply),
        )
        .route(
            "/pipelines/{name}/compiled",
            get(routes::pipelines::compile),
        )
        .route("/pipelines/{name}/diff", get(routes::pipelines::diff))
        .route(
            "/pipelines/{name}/assert",
            post(routes::pipelines::assert_one),
        )
        .route(
            "/pipelines/{name}/teardown",
            post(routes::pipelines::teardown),
        )
        .route("/pipelines/{name}/pause", post(routes::operations::pause))
        .route("/pipelines/{name}/resume", post(routes::operations::resume))
        .route("/pipelines/{name}/reset", post(routes::operations::reset))
        // Runs
        .route("/runs", get(routes::runs::list))
        .route("/runs/{id}", get(routes::runs::get))
        .route("/runs/{id}/events", get(routes::runs::events))
        .route("/runs/{id}/cancel", post(routes::runs::cancel))
        // Batches
        .route("/batches/{id}", get(routes::runs::get_batch))
        .route("/batches/{id}/events", get(routes::runs::batch_events))
        // Connections
        .route("/connections", get(routes::connections::list))
        .route("/connections/{name}", get(routes::connections::get))
        .route("/connections/{name}/test", post(routes::connections::test))
        .route(
            "/connections/{name}/discover",
            get(routes::connections::discover),
        )
        // Plugins
        .route("/plugins", get(routes::plugins::list))
        .route("/plugins/search", get(routes::plugins::search))
        .route("/plugins/install", post(routes::plugins::install))
        // Operations
        .route("/status", get(routes::operations::status))
        .route("/status/{name}", get(routes::operations::status_detail))
        .route("/freshness", get(routes::operations::freshness))
        .route("/logs", get(routes::operations::logs))
        .route("/logs/stream", get(routes::operations::logs_stream))
        // Server
        .route("/server/health", get(routes::server::health))
        .route("/server/version", get(routes::server::version))
        .route("/server/config", get(routes::server::config))
        .layer(axum_middleware::from_fn_with_state(
            Arc::clone(&auth_config),
            middleware::auth::auth_middleware,
        ))
        .layer(cors)
        .with_state(Arc::new(ctx));

    Router::new().nest("/api/v1", api)
}

/// Execute the serve command — starts the REST API server.
pub async fn execute(
    listen: &str,
    allow_unauthenticated: bool,
    auth_token: Option<&str>,
    registry_config: rapidbyte_registry::RegistryConfig,
    secrets: rapidbyte_secrets::SecretProviders,
) -> anyhow::Result<()> {
    let cwd = std::env::current_dir()?;
    let ctx =
        ApiContext::from_project(&cwd, DeploymentMode::Local, secrets, registry_config).await?;

    let tokens = auth_token
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    let auth_config = middleware::AuthConfig {
        required: !allow_unauthenticated,
        tokens,
    };

    let app = build_router(ctx, auth_config);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("Listening on http://{listen}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
    tracing::info!("Shutting down...");
}
