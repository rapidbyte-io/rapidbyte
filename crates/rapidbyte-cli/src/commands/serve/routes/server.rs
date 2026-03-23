use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use rapidbyte_api::{types::HealthStatus, types::ServerConfig, types::VersionInfo, ApiContext};

use crate::commands::serve::error::ApiErrorResponse;

pub async fn health(
    State(ctx): State<Arc<ApiContext>>,
) -> Result<Json<HealthStatus>, ApiErrorResponse> {
    let status = ctx.server.health().await.map_err(ApiErrorResponse::from)?;
    Ok(Json(status))
}

pub async fn version(
    State(ctx): State<Arc<ApiContext>>,
) -> Result<Json<VersionInfo>, ApiErrorResponse> {
    let info = ctx.server.version().await.map_err(ApiErrorResponse::from)?;
    Ok(Json(info))
}

pub async fn config(
    State(ctx): State<Arc<ApiContext>>,
) -> Result<Json<ServerConfig>, ApiErrorResponse> {
    let cfg = ctx.server.config().await.map_err(ApiErrorResponse::from)?;
    Ok(Json(cfg))
}
