pub mod connections;
pub mod error;
pub mod extractors;
pub mod operations;
pub mod pagination;
pub mod pipelines;
pub mod plugins;
pub mod runs;
pub mod server;
pub mod sse;

use axum::routing::{get, post};
use axum::Router;
use tower_http::trace::TraceLayer;

use extractors::RestState;

/// Build the REST API router.
pub fn router(state: RestState) -> Router {
    let public = Router::new().route("/api/v1/server/health", get(server::health));

    let protected = Router::new()
        // Server
        .route("/api/v1/server/version", get(server::version))
        .route("/api/v1/server/config", get(server::config))
        // Pipelines — static routes BEFORE parameterized to avoid wildcard capture
        .route("/api/v1/pipelines", get(pipelines::list))
        .route("/api/v1/pipelines/sync", post(pipelines::sync_batch))
        .route("/api/v1/pipelines/assert", post(pipelines::assert_all))
        .route("/api/v1/pipelines/{name}", get(pipelines::get))
        .route("/api/v1/pipelines/{name}/sync", post(pipelines::sync))
        .route("/api/v1/pipelines/{name}/check", post(pipelines::check))
        .route(
            "/api/v1/pipelines/{name}/check-apply",
            post(pipelines::check_apply),
        )
        .route("/api/v1/pipelines/{name}/compiled", get(pipelines::compile))
        .route("/api/v1/pipelines/{name}/diff", get(pipelines::diff))
        .route(
            "/api/v1/pipelines/{name}/assert",
            post(pipelines::assert_one),
        )
        .route(
            "/api/v1/pipelines/{name}/teardown",
            post(pipelines::teardown),
        )
        // Operations — pause/resume/reset per pipeline
        .route("/api/v1/pipelines/{name}/pause", post(operations::pause))
        .route("/api/v1/pipelines/{name}/resume", post(operations::resume))
        .route("/api/v1/pipelines/{name}/reset", post(operations::reset))
        // Runs
        .route("/api/v1/runs", get(runs::list))
        .route("/api/v1/runs/{id}", get(runs::get))
        .route("/api/v1/runs/{id}/cancel", post(runs::cancel))
        .route("/api/v1/runs/{id}/events", get(runs::events))
        // Batches
        .route("/api/v1/batches/{id}", get(runs::get_batch))
        .route("/api/v1/batches/{id}/events", get(runs::batch_events))
        // Operations — global status, freshness, logs
        .route("/api/v1/status", get(operations::status))
        .route("/api/v1/status/{name}", get(operations::pipeline_status))
        .route("/api/v1/freshness", get(operations::freshness))
        .route("/api/v1/logs", get(operations::logs))
        .route("/api/v1/logs/stream", get(operations::logs_stream))
        // Connections
        .route("/api/v1/connections", get(connections::list))
        .route("/api/v1/connections/{name}", get(connections::get))
        .route("/api/v1/connections/{name}/test", post(connections::test))
        .route(
            "/api/v1/connections/{name}/discover",
            get(connections::discover),
        )
        // Plugins — static routes BEFORE wildcard to avoid capture conflicts
        .route("/api/v1/plugins", get(plugins::list))
        .route("/api/v1/plugins/search", get(plugins::search))
        .route("/api/v1/plugins/install", post(plugins::install))
        .route(
            "/api/v1/plugins/{*plugin_ref}",
            get(plugins::info).delete(plugins::remove),
        );

    // No CORS layer: browser cross-origin requests are blocked by default.
    // API clients (CLI, agents) don't need CORS. If a web dashboard is
    // added later, configure explicit allowed origins via ControllerConfig.
    public
        .merge(protected)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
