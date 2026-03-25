use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;

use super::error::RestError;
use super::extractors::{Auth, RestState};
use crate::traits::plugin::{
    PluginDetail, PluginInstallResult, PluginSearchRequest, PluginSearchResult, PluginService,
    PluginSummary,
};
use crate::traits::PaginatedList;

/// GET /api/v1/plugins
pub async fn list(
    _auth: Auth,
    State(state): State<RestState>,
) -> Result<Json<PaginatedList<PluginSummary>>, RestError> {
    let items = state.services.list().await?;
    Ok(Json(PaginatedList {
        items,
        next_cursor: None,
    }))
}

/// Query parameters for the plugin search endpoint.
#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub q: Option<String>,
    #[serde(rename = "type")]
    pub plugin_type: Option<String>,
}

/// GET /api/v1/plugins/search?q=postgres&type=source
pub async fn search(
    _auth: Auth,
    State(state): State<RestState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<PaginatedList<PluginSearchResult>>, RestError> {
    let request = PluginSearchRequest {
        query: params.q.unwrap_or_default(),
        plugin_type: params.plugin_type,
    };
    let items = state.services.search(request).await?;
    Ok(Json(PaginatedList {
        items,
        next_cursor: None,
    }))
}

/// Request body for POST /api/v1/plugins/install.
#[derive(Debug, Deserialize)]
pub struct InstallRequest {
    pub plugin_ref: String,
}

/// POST /api/v1/plugins/install
pub async fn install(
    _auth: Auth,
    State(state): State<RestState>,
    Json(body): Json<InstallRequest>,
) -> Result<Json<PluginInstallResult>, RestError> {
    let result = state.services.install(&body.plugin_ref).await?;
    Ok(Json(result))
}

/// GET /api/v1/plugins/{*plugin_ref}
pub async fn info(
    _auth: Auth,
    State(state): State<RestState>,
    Path(plugin_ref): Path<String>,
) -> Result<Json<PluginDetail>, RestError> {
    let detail = state.services.info(&plugin_ref).await?;
    Ok(Json(detail))
}

/// DELETE /api/v1/plugins/{*plugin_ref}
pub async fn remove(
    _auth: Auth,
    State(state): State<RestState>,
    Path(plugin_ref): Path<String>,
) -> Result<Json<()>, RestError> {
    state.services.remove(&plugin_ref).await?;
    Ok(Json(()))
}
