pub mod error;
pub mod extractors;
pub mod pagination;
pub mod runs;
pub mod server;
pub mod sse;

use axum::routing::{get, post};
use axum::Router;
use extractors::RestState;

/// Build the REST API router.
pub fn router(state: RestState) -> Router {
    let public = Router::new().route("/api/v1/server/health", get(server::health));

    let protected = Router::new()
        // Server
        .route("/api/v1/server/version", get(server::version))
        .route("/api/v1/server/config", get(server::config))
        // Runs
        .route("/api/v1/runs", get(runs::list))
        .route("/api/v1/runs/{id}", get(runs::get))
        .route("/api/v1/runs/{id}/cancel", post(runs::cancel))
        .route("/api/v1/runs/{id}/events", get(runs::events))
        // Batches
        .route("/api/v1/batches/{id}", get(runs::get_batch))
        .route("/api/v1/batches/{id}/events", get(runs::batch_events));

    public.merge(protected).with_state(state)
}
