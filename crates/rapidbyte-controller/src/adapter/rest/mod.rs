pub mod error;
pub mod extractors;
pub mod pagination;
pub mod server;
pub mod sse;

use axum::routing::get;
use axum::Router;
use extractors::RestState;

/// Build the REST API router.
pub fn router(state: RestState) -> Router {
    let public = Router::new().route("/api/v1/server/health", get(server::health));

    let protected = Router::new()
        .route("/api/v1/server/version", get(server::version))
        .route("/api/v1/server/config", get(server::config));

    public.merge(protected).with_state(state)
}
