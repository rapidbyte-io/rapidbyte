use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;

use super::error::RestError;
use super::extractors::{Auth, RestState};
use crate::traits::pipeline::{
    AssertRequest, AssertResult, BatchRunHandle, CheckResult, DiffResult, PipelineDetail,
    PipelineFilter, PipelineService, PipelineSummary, ResolvedConfig, RunHandle, RunLinks,
    SyncBatchRequest, SyncRequest, TeardownRequest,
};
use crate::traits::PaginatedList;

/// Combined query parameters for listing pipelines.
/// Merges pagination and filter params to avoid axum dual-`Query<>` deserialization issues.
#[derive(Debug, Deserialize)]
pub struct PipelineListParams {
    pub tag: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl PipelineListParams {
    fn limit(&self) -> u32 {
        self.limit.unwrap_or(20).min(100).max(1)
    }
}

/// GET /api/v1/pipelines
pub async fn list(
    _auth: Auth,
    State(state): State<RestState>,
    Query(params): Query<PipelineListParams>,
) -> Result<Json<PaginatedList<PipelineSummary>>, RestError> {
    let limit = params.limit();
    let filter = PipelineFilter {
        tag: params.tag.map(|t| t.split(',').map(String::from).collect()),
        limit,
        cursor: params.cursor,
    };
    let page = state.services.list(filter).await?;
    Ok(Json(page))
}

/// GET /api/v1/pipelines/{name}
pub async fn get(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<Json<PipelineDetail>, RestError> {
    let detail = state.services.get(&name).await?;
    Ok(Json(detail))
}

/// POST /api/v1/pipelines/{name}/sync — returns 202 Accepted
pub async fn sync(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
    body: Option<Json<SyncRequestBody>>,
) -> Result<(StatusCode, Json<RunHandle>), RestError> {
    let body = body.map(|b| b.0).unwrap_or_default();
    let request = SyncRequest {
        pipeline: name.clone(),
        stream: body.stream,
        full_refresh: body.full_refresh.unwrap_or(false),
        cursor_start: body.cursor_start,
        cursor_end: body.cursor_end,
        dry_run: body.dry_run.unwrap_or(false),
    };
    let mut handle = state.services.sync(request).await?;
    handle.links = Some(RunLinks {
        self_url: format!("/api/v1/runs/{}", handle.run_id),
        events: format!("/api/v1/runs/{}/events", handle.run_id),
    });
    Ok((StatusCode::ACCEPTED, Json(handle)))
}

/// Request body for a single-pipeline sync.
#[derive(Debug, Deserialize, Default)]
pub struct SyncRequestBody {
    pub stream: Option<String>,
    pub full_refresh: Option<bool>,
    pub cursor_start: Option<String>,
    pub cursor_end: Option<String>,
    pub dry_run: Option<bool>,
}

/// POST /api/v1/pipelines/sync — batch sync, returns 202
pub async fn sync_batch(
    _auth: Auth,
    State(state): State<RestState>,
    Json(request): Json<SyncBatchRequest>,
) -> Result<(StatusCode, Json<BatchRunHandle>), RestError> {
    let handle = state.services.sync_batch(request).await?;
    Ok((StatusCode::ACCEPTED, Json(handle)))
}

/// POST /api/v1/pipelines/{name}/check
pub async fn check(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<Json<CheckResult>, RestError> {
    let result = state.services.check(&name).await?;
    Ok(Json(result))
}

/// POST /api/v1/pipelines/{name}/check-apply — returns 202
pub async fn check_apply(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<RunHandle>), RestError> {
    let handle = state.services.check_apply(&name).await?;
    Ok((StatusCode::ACCEPTED, Json(handle)))
}

/// GET /api/v1/pipelines/{name}/compiled
pub async fn compile(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<Json<ResolvedConfig>, RestError> {
    let config = state.services.compile(&name).await?;
    Ok(Json(config))
}

/// GET /api/v1/pipelines/{name}/diff
pub async fn diff(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<Json<DiffResult>, RestError> {
    let result = state.services.diff(&name).await?;
    Ok(Json(result))
}

/// POST /api/v1/pipelines/{name}/assert
pub async fn assert_one(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<Json<AssertResult>, RestError> {
    let request = AssertRequest {
        pipeline: Some(name),
        tag: None,
    };
    let result = state.services.assert(request).await?;
    Ok(Json(result))
}

/// POST /api/v1/pipelines/assert — batch assert
pub async fn assert_all(
    _auth: Auth,
    State(state): State<RestState>,
    Json(request): Json<AssertRequest>,
) -> Result<Json<AssertResult>, RestError> {
    let result = state.services.assert(request).await?;
    Ok(Json(result))
}

/// POST /api/v1/pipelines/{name}/teardown — returns 202
pub async fn teardown(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
    Json(body): Json<TeardownBody>,
) -> Result<(StatusCode, Json<RunHandle>), RestError> {
    let request = TeardownRequest {
        pipeline: name,
        reason: body.reason,
    };
    let handle = state.services.teardown(request).await?;
    Ok((StatusCode::ACCEPTED, Json(handle)))
}

/// Request body for pipeline teardown.
#[derive(Debug, Deserialize)]
pub struct TeardownBody {
    pub reason: String,
}
