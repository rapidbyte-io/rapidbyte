#![allow(clippy::missing_errors_doc)]

use axum::extract::State;
use axum::Json;

use super::error::RestError;
use super::extractors::{Auth, RestState};
use crate::traits::server::{HealthStatus, ServerConfigInfo, VersionInfo};
use crate::traits::ServerService;

/// GET /api/v1/server/health — no auth required
pub async fn health(State(state): State<RestState>) -> Result<Json<HealthStatus>, RestError> {
    let status = state.services.health().await?;
    Ok(Json(status))
}

/// GET /api/v1/server/version
pub async fn version(
    _auth: Auth,
    State(state): State<RestState>,
) -> Result<Json<VersionInfo>, RestError> {
    let info = state.services.version().await?;
    Ok(Json(info))
}

/// GET /api/v1/server/config
pub async fn config(
    _auth: Auth,
    State(state): State<RestState>,
) -> Result<Json<ServerConfigInfo>, RestError> {
    let info = state.services.config().await?;
    Ok(Json(info))
}
