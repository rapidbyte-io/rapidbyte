use async_trait::async_trait;

use crate::error::ApiError;
use crate::types::{
    AssertRequest, AssertResult, BatchRunHandle, CheckResult, DiffResult, PaginatedList,
    PipelineDetail, PipelineFilter, PipelineSummary, ResolvedConfig, RunHandle, SyncBatchRequest,
    SyncRequest, TeardownRequest,
};

type Result<T> = std::result::Result<T, ApiError>;

/// Driving port for pipeline CRUD and sync operations.
#[async_trait]
pub trait PipelineService: Send + Sync {
    async fn list(&self, filter: PipelineFilter) -> Result<PaginatedList<PipelineSummary>>;
    async fn get(&self, name: &str) -> Result<PipelineDetail>;
    async fn sync(&self, request: SyncRequest) -> Result<RunHandle>;
    async fn sync_batch(&self, request: SyncBatchRequest) -> Result<BatchRunHandle>;
    async fn check(&self, name: &str) -> Result<CheckResult>;
    async fn check_apply(&self, name: &str) -> Result<RunHandle>;
    async fn compile(&self, name: &str) -> Result<ResolvedConfig>;
    async fn diff(&self, name: &str) -> Result<DiffResult>;
    async fn assert(&self, request: AssertRequest) -> Result<AssertResult>;
    async fn teardown(&self, request: TeardownRequest) -> Result<RunHandle>;
}
