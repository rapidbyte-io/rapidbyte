use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use rapidbyte_api::types::{PipelineFilter, SyncRequest};
use rapidbyte_api::{ApiContext, ApiError};

use crate::commands::serve::error::ApiErrorResponse;
use crate::commands::serve::extract::PaginationParams;

/// Deserializable query params for list.
#[derive(Debug, serde::Deserialize, Default)]
pub struct ListPipelinesParams {
    pub tag: Option<Vec<String>>,
    #[serde(flatten)]
    #[allow(dead_code)]
    pub pagination: PaginationParams,
}

pub async fn list(
    State(ctx): State<Arc<ApiContext>>,
    Query(params): Query<ListPipelinesParams>,
) -> Result<
    Json<rapidbyte_api::PaginatedList<rapidbyte_api::types::PipelineSummary>>,
    ApiErrorResponse,
> {
    let filter = PipelineFilter {
        tag: params.tag,
        dir: None,
    };
    let result = ctx
        .pipelines
        .list(filter)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(result))
}

pub async fn get(
    State(ctx): State<Arc<ApiContext>>,
    Path(name): Path<String>,
) -> Result<Json<rapidbyte_api::types::PipelineDetail>, ApiErrorResponse> {
    let detail = ctx
        .pipelines
        .get(&name)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(detail))
}

/// Body for sync request.
#[derive(Debug, serde::Deserialize, Default)]
pub struct SyncBody {
    pub stream: Option<String>,
    #[serde(default)]
    pub full_refresh: bool,
    pub cursor_start: Option<String>,
    pub cursor_end: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

pub async fn sync(
    State(ctx): State<Arc<ApiContext>>,
    Path(name): Path<String>,
    Json(body): Json<SyncBody>,
) -> Result<(StatusCode, Json<rapidbyte_api::types::RunHandle>), ApiErrorResponse> {
    let request = SyncRequest {
        pipeline: name,
        stream: body.stream,
        full_refresh: body.full_refresh,
        cursor_start: body.cursor_start,
        cursor_end: body.cursor_end,
        dry_run: body.dry_run,
    };
    let handle = ctx
        .pipelines
        .sync(request)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok((StatusCode::ACCEPTED, Json(handle)))
}

pub async fn check(
    State(ctx): State<Arc<ApiContext>>,
    Path(name): Path<String>,
) -> Result<Json<rapidbyte_api::types::CheckResult>, ApiErrorResponse> {
    let result = ctx
        .pipelines
        .check(&name)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(result))
}

pub async fn compile(
    State(ctx): State<Arc<ApiContext>>,
    Path(name): Path<String>,
) -> Result<Json<rapidbyte_api::types::ResolvedConfig>, ApiErrorResponse> {
    let config = ctx
        .pipelines
        .compile(&name)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(config))
}

// Stubs for not-yet-implemented endpoints.

pub async fn sync_batch(State(_ctx): State<Arc<ApiContext>>) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("batch sync"))
}

pub async fn check_apply(
    State(_ctx): State<Arc<ApiContext>>,
    Path(_name): Path<String>,
) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("check and apply"))
}

pub async fn diff(
    State(_ctx): State<Arc<ApiContext>>,
    Path(_name): Path<String>,
) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("diff"))
}

pub async fn assert_one(
    State(_ctx): State<Arc<ApiContext>>,
    Path(_name): Path<String>,
) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("assert"))
}

pub async fn assert_all(State(_ctx): State<Arc<ApiContext>>) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("assert all"))
}

pub async fn teardown(
    State(_ctx): State<Arc<ApiContext>>,
    Path(_name): Path<String>,
) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("teardown"))
}
