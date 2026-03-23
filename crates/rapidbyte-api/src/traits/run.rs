use async_trait::async_trait;

use crate::error::ApiError;
use crate::types::{
    BatchDetail, EventStream, PaginatedList, RunDetail, RunFilter, RunSummary, SseEvent,
};

type Result<T> = std::result::Result<T, ApiError>;

/// Driving port for run inspection, cancellation, and event streaming.
#[async_trait]
pub trait RunService: Send + Sync {
    async fn get(&self, run_id: &str) -> Result<RunDetail>;
    async fn list(&self, filter: RunFilter) -> Result<PaginatedList<RunSummary>>;
    async fn events(&self, run_id: &str) -> Result<EventStream<SseEvent>>;
    async fn cancel(&self, run_id: &str) -> Result<RunDetail>;
    async fn get_batch(&self, batch_id: &str) -> Result<BatchDetail>;
    async fn batch_events(&self, batch_id: &str) -> Result<EventStream<SseEvent>>;
}
