use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{PaginatedList, ServiceError};

#[async_trait]
pub trait PipelineService: Send + Sync {
    /// List all discovered pipelines in the project.
    async fn list(
        &self,
        filter: PipelineFilter,
    ) -> Result<PaginatedList<PipelineSummary>, ServiceError>;

    /// Get a single pipeline's resolved configuration.
    async fn get(&self, name: &str) -> Result<PipelineDetail, ServiceError>;

    /// Trigger a pipeline sync. Returns a run ID for tracking.
    async fn sync(&self, request: SyncRequest) -> Result<RunHandle, ServiceError>;

    /// Trigger sync for multiple pipelines by tag/filter. Returns a batch handle.
    async fn sync_batch(&self, request: SyncBatchRequest) -> Result<BatchRunHandle, ServiceError>;

    /// Validate pipeline config and plugin connectivity.
    async fn check(&self, name: &str) -> Result<CheckResult, ServiceError>;

    /// Validate and provision resources. Async — returns run handle.
    async fn check_apply(&self, name: &str) -> Result<RunHandle, ServiceError>;

    /// Resolve all defaults and env vars, return effective config.
    async fn compile(&self, name: &str) -> Result<ResolvedConfig, ServiceError>;

    /// Detect schema drift since last sync.
    async fn diff(&self, name: &str) -> Result<DiffResult, ServiceError>;

    /// Run data quality assertions for a pipeline.
    async fn assert(&self, request: AssertRequest) -> Result<AssertResult, ServiceError>;

    /// Tear down resources provisioned by a pipeline.
    async fn teardown(&self, request: TeardownRequest) -> Result<RunHandle, ServiceError>;
}

// --- Request types ---

#[derive(Debug, Clone, Default)]
pub struct PipelineFilter {
    pub tag: Option<Vec<String>>,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncRequest {
    #[serde(default)]
    pub pipeline: String,
    pub stream: Option<String>,
    #[serde(default)]
    pub full_refresh: bool,
    pub cursor_start: Option<String>,
    pub cursor_end: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncBatchRequest {
    pub tag: Option<Vec<String>>,
    pub exclude: Option<Vec<String>>,
    #[serde(default)]
    pub full_refresh: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AssertRequest {
    pub pipeline: Option<String>,
    pub tag: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TeardownRequest {
    pub pipeline: String,
    pub reason: String,
}

// --- Response types ---

#[derive(Debug, Clone, Serialize)]
pub struct RunHandle {
    pub run_id: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<RunLinks>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunLinks {
    #[serde(rename = "self")]
    pub self_url: String,
    pub events: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchRunHandle {
    pub batch_id: String,
    pub runs: Vec<BatchRunRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub links: Option<BatchLinks>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchRunRef {
    pub pipeline: String,
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchLinks {
    #[serde(rename = "self")]
    pub self_url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineSummary {
    pub name: String,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub source: String,
    pub destination: String,
    pub schedule: Option<String>,
    pub streams: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineDetail {
    pub name: String,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub schedule: Option<String>,
    pub source: serde_json::Value,
    pub destination: serde_json::Value,
    pub state: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    pub passed: bool,
    pub checks: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedConfig {
    pub pipeline: String,
    pub resolved_config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffResult {
    pub streams: Vec<StreamDiff>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamDiff {
    pub stream_name: String,
    pub changes: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AssertResult {
    pub passed: bool,
    pub results: Vec<serde_json::Value>,
}
