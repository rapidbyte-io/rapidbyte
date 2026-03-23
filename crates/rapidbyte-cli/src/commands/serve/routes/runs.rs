use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, Sse};
use axum::Json;
use futures::StreamExt;
use rapidbyte_api::types::{RunDetail, RunFilter, RunSummary};
use rapidbyte_api::{ApiContext, ApiError, PaginatedList};

use crate::commands::serve::error::ApiErrorResponse;
use crate::commands::serve::extract::PaginationParams;

/// Deserializable query params for listing runs.
#[derive(Debug, serde::Deserialize, Default)]
pub struct ListRunsParams {
    pub pipeline: Option<String>,
    pub status: Option<rapidbyte_api::RunStatus>,
    #[serde(flatten)]
    pub pagination: PaginationParams,
}

pub async fn list(
    State(ctx): State<Arc<ApiContext>>,
    Query(params): Query<ListRunsParams>,
) -> Result<Json<PaginatedList<RunSummary>>, ApiErrorResponse> {
    let filter = RunFilter {
        pipeline: params.pipeline,
        status: params.status,
        limit: params.pagination.limit(),
        cursor: params.pagination.cursor,
    };
    let result = ctx
        .runs
        .list(filter)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(result))
}

pub async fn get(
    State(ctx): State<Arc<ApiContext>>,
    Path(id): Path<String>,
) -> Result<Json<RunDetail>, ApiErrorResponse> {
    let detail = ctx.runs.get(&id).await.map_err(ApiErrorResponse::from)?;
    Ok(Json(detail))
}

pub async fn events(
    State(ctx): State<Arc<ApiContext>>,
    Path(id): Path<String>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, Infallible>>>, ApiErrorResponse> {
    let stream = ctx.runs.events(&id).await.map_err(ApiErrorResponse::from)?;
    let sse_stream = stream.map(|event| {
        let sse_event = Event::default()
            .event(event.event_type())
            .json_data(&event)
            .unwrap_or_else(|_| Event::default().event("error").data("serialization failed"));
        Ok(sse_event)
    });
    Ok(Sse::new(sse_stream))
}

pub async fn cancel(
    State(ctx): State<Arc<ApiContext>>,
    Path(id): Path<String>,
) -> Result<Json<RunDetail>, ApiErrorResponse> {
    let detail = ctx.runs.cancel(&id).await.map_err(ApiErrorResponse::from)?;
    Ok(Json(detail))
}

// Stubs for not-yet-implemented endpoints.

pub async fn get_batch(
    State(_ctx): State<Arc<ApiContext>>,
    Path(_id): Path<String>,
) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("get batch"))
}

pub async fn batch_events(
    State(_ctx): State<Arc<ApiContext>>,
    Path(_id): Path<String>,
) -> ApiErrorResponse {
    ApiErrorResponse(ApiError::not_implemented("batch events"))
}
