use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;

use super::error::RestError;
use super::extractors::{Auth, RestState};
use super::sse::to_sse_response;
use crate::domain::ports::log_store::StoredLogEntry;
use crate::traits::operations::{
    FreshnessFilter, FreshnessStatus, LogsRequest, LogsResult, LogsStreamFilter, OperationsService,
    PipelineStateChange, PipelineStatus, PipelineStatusDetail, ResetRequest, ResetResult,
};

/// GET /api/v1/status
pub async fn status(
    _auth: Auth,
    State(state): State<RestState>,
) -> Result<Json<Vec<PipelineStatus>>, RestError> {
    let result = state.services.status().await?;
    Ok(Json(result))
}

/// GET /api/v1/status/{name}
pub async fn pipeline_status(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<Json<PipelineStatusDetail>, RestError> {
    let result = state.services.pipeline_status(&name).await?;
    Ok(Json(result))
}

/// POST /api/v1/pipelines/{name}/pause
pub async fn pause(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<Json<PipelineStateChange>, RestError> {
    let result = state.services.pause(&name).await?;
    Ok(Json(result))
}

/// POST /api/v1/pipelines/{name}/resume
pub async fn resume(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<Json<PipelineStateChange>, RestError> {
    let result = state.services.resume(&name).await?;
    Ok(Json(result))
}

/// Optional JSON body for reset — stream is the only field.
#[derive(Debug, Deserialize, Default)]
pub struct ResetBody {
    pub stream: Option<String>,
}

/// POST /api/v1/pipelines/{name}/reset
pub async fn reset(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
    body: Option<Json<ResetBody>>,
) -> Result<Json<ResetResult>, RestError> {
    let body = body.map(|b| b.0).unwrap_or_default();
    let request = ResetRequest {
        pipeline: name,
        stream: body.stream,
    };
    let result = state.services.reset(request).await?;
    Ok(Json(result))
}

/// Query parameters for the freshness endpoint.
#[derive(Debug, Deserialize)]
pub struct FreshnessParams {
    pub tag: Option<String>,
}

/// GET /api/v1/freshness
pub async fn freshness(
    _auth: Auth,
    State(state): State<RestState>,
    Query(params): Query<FreshnessParams>,
) -> Result<Json<Vec<FreshnessStatus>>, RestError> {
    let filter = FreshnessFilter {
        tag: params.tag.map(|t| t.split(',').map(String::from).collect()),
    };
    let result = state.services.freshness(filter).await?;
    Ok(Json(result))
}

/// Query parameters for the logs endpoint.
#[derive(Debug, Deserialize)]
pub struct LogsParams {
    pub pipeline: String,
    pub run_id: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

/// GET /api/v1/logs
pub async fn logs(
    _auth: Auth,
    State(state): State<RestState>,
    Query(params): Query<LogsParams>,
) -> Result<Json<LogsResult>, RestError> {
    let request = LogsRequest {
        pipeline: params.pipeline,
        run_id: params.run_id,
        limit: params.limit,
        cursor: params.cursor,
    };
    let result = state.services.logs(request).await?;
    Ok(Json(result))
}

/// Query parameters for the log stream endpoint.
#[derive(Debug, Deserialize)]
pub struct LogsStreamParams {
    pub pipeline: Option<String>,
}

/// GET /api/v1/logs/stream — SSE stream of log entries
pub async fn logs_stream(
    _auth: Auth,
    State(state): State<RestState>,
    Query(params): Query<LogsStreamParams>,
) -> Result<impl IntoResponse, RestError> {
    let filter = LogsStreamFilter {
        pipeline: params.pipeline,
    };
    let stream = state.services.logs_stream(filter).await?;
    Ok(to_sse_response(stream, |_: &StoredLogEntry| "log"))
}
