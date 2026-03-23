use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use rapidbyte_api::types::PluginSummary;
use rapidbyte_api::{ApiContext, ApiError};

use crate::commands::serve::error::ApiErrorResponse;

pub async fn list(
    State(ctx): State<Arc<ApiContext>>,
) -> Result<Json<Vec<PluginSummary>>, ApiErrorResponse> {
    let result = ctx.plugins.list().await.map_err(ApiErrorResponse::from)?;
    Ok(Json(result))
}

// Stubs for not-yet-implemented endpoints.

pub async fn search(State(_ctx): State<Arc<ApiContext>>) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("plugin search"))
}

pub async fn install(State(_ctx): State<Arc<ApiContext>>) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("plugin install"))
}
