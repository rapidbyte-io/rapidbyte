use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;

use super::error::RestError;
use super::extractors::{Auth, RestState};
use super::sse::to_sse_response;
use crate::traits::run::{BatchDetail, ProgressEvent, RunDetail, RunFilter, RunSummary};
use crate::traits::{PaginatedList, RunService};

/// Combined query parameters for listing runs.
/// Merges pagination and filter params to avoid axum dual-`Query<>` deserialization issues.
#[derive(Debug, Deserialize)]
pub struct ListRunsParams {
    // Pagination
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    // Filters
    pub pipeline: Option<String>,
    pub status: Option<String>,
}

impl ListRunsParams {
    fn limit(&self) -> u32 {
        self.limit.unwrap_or(20).min(100).max(1)
    }
}

/// GET /api/v1/runs
pub async fn list(
    _auth: Auth,
    State(state): State<RestState>,
    Query(params): Query<ListRunsParams>,
) -> Result<Json<PaginatedList<RunSummary>>, RestError> {
    let limit = params.limit();
    let filter = RunFilter {
        pipeline: params.pipeline,
        status: params.status,
        limit,
        cursor: params.cursor,
    };
    let page = state.services.list(filter).await?;
    Ok(Json(page))
}

/// GET /api/v1/runs/{id}
pub async fn get(
    _auth: Auth,
    State(state): State<RestState>,
    Path(id): Path<String>,
) -> Result<Json<RunDetail>, RestError> {
    let run = state.services.get(&id).await?;
    Ok(Json(run))
}

/// POST /api/v1/runs/{id}/cancel
pub async fn cancel(
    _auth: Auth,
    State(state): State<RestState>,
    Path(id): Path<String>,
) -> Result<Json<RunDetail>, RestError> {
    let run = state.services.cancel(&id).await?;
    Ok(Json(run))
}

/// GET /api/v1/runs/{id}/events — SSE stream
pub async fn events(
    _auth: Auth,
    State(state): State<RestState>,
    Path(id): Path<String>,
) -> Result<impl axum::response::IntoResponse, RestError> {
    let stream = state.services.events(&id).await?;
    Ok(to_sse_response(stream, |e: &ProgressEvent| e.event_name()))
}

/// GET /api/v1/batches/{id}
pub async fn get_batch(
    _auth: Auth,
    State(state): State<RestState>,
    Path(id): Path<String>,
) -> Result<Json<BatchDetail>, RestError> {
    let batch = state.services.get_batch(&id).await?;
    Ok(Json(batch))
}

/// GET /api/v1/batches/{id}/events — SSE stream
pub async fn batch_events(
    _auth: Auth,
    State(state): State<RestState>,
    Path(id): Path<String>,
) -> Result<impl axum::response::IntoResponse, RestError> {
    let stream = state.services.batch_events(&id).await?;
    Ok(to_sse_response(stream, |e: &ProgressEvent| e.event_name()))
}
