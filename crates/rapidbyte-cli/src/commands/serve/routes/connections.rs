use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use rapidbyte_api::types::{
    ConnectionDetail, ConnectionSummary, ConnectionTestResult, DiscoverRequest, DiscoverResult,
};
use rapidbyte_api::ApiContext;

use crate::commands::serve::error::ApiErrorResponse;

pub async fn list(
    State(ctx): State<Arc<ApiContext>>,
) -> Result<Json<Vec<ConnectionSummary>>, ApiErrorResponse> {
    let result = ctx
        .connections
        .list()
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(result))
}

pub async fn get(
    State(ctx): State<Arc<ApiContext>>,
    Path(name): Path<String>,
) -> Result<Json<ConnectionDetail>, ApiErrorResponse> {
    let detail = ctx
        .connections
        .get(&name)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(detail))
}

pub async fn test(
    State(ctx): State<Arc<ApiContext>>,
    Path(name): Path<String>,
) -> Result<Json<ConnectionTestResult>, ApiErrorResponse> {
    let result = ctx
        .connections
        .test(&name)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(result))
}

pub async fn discover(
    State(ctx): State<Arc<ApiContext>>,
    Path(name): Path<String>,
) -> Result<Json<DiscoverResult>, ApiErrorResponse> {
    let request = DiscoverRequest {
        connection: name,
        table: None,
    };
    let result = ctx
        .connections
        .discover(request)
        .await
        .map_err(ApiErrorResponse::from)?;
    Ok(Json(result))
}
