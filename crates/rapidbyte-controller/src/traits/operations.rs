use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{EventStream, ServiceError};
use crate::domain::ports::log_store::StoredLogEntry;

#[async_trait]
pub trait OperationsService: Send + Sync {
    /// Get the status of all known pipelines.
    async fn status(&self) -> Result<Vec<PipelineStatus>, ServiceError>;

    /// Get detailed status for a single pipeline.
    async fn pipeline_status(&self, name: &str) -> Result<PipelineStatusDetail, ServiceError>;

    /// Pause a pipeline's scheduled execution.
    async fn pause(&self, name: &str) -> Result<PipelineStateChange, ServiceError>;

    /// Resume a paused pipeline.
    async fn resume(&self, name: &str) -> Result<PipelineStateChange, ServiceError>;

    /// Clear cursors and reset sync state for a pipeline.
    async fn reset(&self, request: ResetRequest) -> Result<ResetResult, ServiceError>;

    /// Get freshness status for pipelines matching the filter.
    async fn freshness(
        &self,
        filter: FreshnessFilter,
    ) -> Result<Vec<FreshnessStatus>, ServiceError>;

    /// Query stored log entries for a pipeline.
    async fn logs(&self, request: LogsRequest) -> Result<LogsResult, ServiceError>;

    /// Subscribe to a real-time log stream.
    async fn logs_stream(
        &self,
        filter: LogsStreamFilter,
    ) -> Result<EventStream<StoredLogEntry>, ServiceError>;
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineStatus {
    pub pipeline: String,
    pub state: String,
    pub health: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineStatusDetail {
    pub pipeline: String,
    pub state: String,
    pub health: String,
    pub streams: Vec<StreamStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamStatus {
    pub name: String,
    pub sync_mode: Option<String>,
    pub cursor_value: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineStateChange {
    pub pipeline: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResetRequest {
    #[serde(default)]
    pub pipeline: String,
    pub stream: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResetResult {
    pub pipeline: String,
    pub streams_reset: Vec<String>,
    pub cursors_cleared: u64,
    pub next_sync_mode: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FreshnessFilter {
    pub tag: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FreshnessStatus {
    pub pipeline: String,
    pub last_sync_age: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogsRequest {
    pub pipeline: String,
    pub run_id: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogsResult {
    pub items: Vec<StoredLogEntry>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogsStreamFilter {
    pub pipeline: Option<String>,
}
