use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use rapidbyte_api::types::{PipelineStatus, PipelineStatusDetail};
use rapidbyte_api::{ApiContext, ApiError};

use crate::commands::serve::error::ApiErrorResponse;

pub async fn status(
    State(ctx): State<Arc<ApiContext>>,
) -> Result<Json<Vec<PipelineStatus>>, ApiErrorResponse> {
    let result = ctx
        .operations
        .status()
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(result))
}

pub async fn status_detail(
    State(ctx): State<Arc<ApiContext>>,
    Path(name): Path<String>,
) -> Result<Json<PipelineStatusDetail>, ApiErrorResponse> {
    let result = ctx
        .operations
        .pipeline_status(&name)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(result))
}

// Stubs for not-yet-implemented endpoints.

pub async fn pause(
    State(_ctx): State<Arc<ApiContext>>,
    Path(_name): Path<String>,
) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("pause"))
}

pub async fn resume(
    State(_ctx): State<Arc<ApiContext>>,
    Path(_name): Path<String>,
) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("resume"))
}

pub async fn reset(
    State(_ctx): State<Arc<ApiContext>>,
    Path(_name): Path<String>,
) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("reset"))
}

pub async fn freshness(State(_ctx): State<Arc<ApiContext>>) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("freshness"))
}

pub async fn logs(State(_ctx): State<Arc<ApiContext>>) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("logs"))
}

pub async fn logs_stream(State(_ctx): State<Arc<ApiContext>>) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("logs stream"))
}
