use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;

use super::error::RestError;
use super::extractors::{Auth, RestState};
use crate::traits::connection::{
    ConnectionDetail, ConnectionDiscoverResponse, ConnectionService, ConnectionSummary,
    ConnectionTestResponse,
};
use crate::traits::PaginatedList;

/// GET /api/v1/connections
pub async fn list(
    _auth: Auth,
    State(state): State<RestState>,
) -> Result<Json<PaginatedList<ConnectionSummary>>, RestError> {
    let items = state.services.list().await?;
    Ok(Json(PaginatedList {
        items,
        next_cursor: None,
    }))
}

/// GET /api/v1/connections/{name}
pub async fn get(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<Json<ConnectionDetail>, RestError> {
    let detail = state.services.get(&name).await?;
    Ok(Json(detail))
}

/// POST /api/v1/connections/{name}/test
pub async fn test(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
) -> Result<Json<ConnectionTestResponse>, RestError> {
    let result = state.services.test(&name).await?;
    Ok(Json(result))
}

/// Query parameters for the discover endpoint.
#[derive(Debug, Deserialize)]
pub struct DiscoverParams {
    pub table: Option<String>,
}

/// GET /api/v1/connections/{name}/discover
pub async fn discover(
    _auth: Auth,
    State(state): State<RestState>,
    Path(name): Path<String>,
    Query(params): Query<DiscoverParams>,
) -> Result<Json<ConnectionDiscoverResponse>, RestError> {
    let result = state
        .services
        .discover(&name, params.table.as_deref())
        .await?;
    Ok(Json(result))
}
