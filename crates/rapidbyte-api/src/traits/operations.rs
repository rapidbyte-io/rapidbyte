use async_trait::async_trait;

use crate::error::ApiError;
use crate::types::{
    EventStream, FreshnessFilter, FreshnessStatus, LogsRequest, LogsResult, LogsStreamFilter,
    PipelineState, PipelineStatus, PipelineStatusDetail, ResetRequest, ResetResult, SseEvent,
};

type Result<T> = std::result::Result<T, ApiError>;

/// Driving port for operational tasks: status, pause/resume, reset, freshness, logs.
#[async_trait]
pub trait OperationsService: Send + Sync {
    async fn status(&self) -> Result<Vec<PipelineStatus>>;
    async fn pipeline_status(&self, name: &str) -> Result<PipelineStatusDetail>;
    async fn pause(&self, name: &str) -> Result<PipelineState>;
    async fn resume(&self, name: &str) -> Result<PipelineState>;
    async fn reset(&self, request: ResetRequest) -> Result<ResetResult>;
    async fn freshness(&self, filter: FreshnessFilter) -> Result<Vec<FreshnessStatus>>;
    async fn logs(&self, request: LogsRequest) -> Result<LogsResult>;
    async fn logs_stream(&self, filter: LogsStreamFilter) -> Result<EventStream<SseEvent>>;
}
