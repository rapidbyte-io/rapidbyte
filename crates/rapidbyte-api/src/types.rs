//! Request/response DTOs and SSE event model.
//!
//! These types are distinct from engine domain types and are
//! serde-serializable for HTTP/JSON transport.

use std::path::PathBuf;
use std::pin::Pin;

use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Generic
// ---------------------------------------------------------------------------

/// Paginated list wrapper for any item type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedList<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// A streaming event source (not serde-serializable).
pub type EventStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

/// Status of a pipeline run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    /// Returns `true` if this status is a terminal state (no further transitions).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Active/paused state of a pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineState {
    Active,
    Paused,
}

// ---------------------------------------------------------------------------
// SSE Events (Section 6)
// ---------------------------------------------------------------------------

/// Server-sent event variants emitted during a pipeline run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum SseEvent {
    Started {
        run_id: String,
        pipeline: String,
        started_at: DateTime<Utc>,
    },
    Progress {
        phase: String,
        stream: String,
        records_read: Option<u64>,
        records_written: Option<u64>,
        bytes_read: Option<u64>,
    },
    Log {
        level: String,
        message: String,
    },
    Complete {
        run_id: String,
        status: RunStatus,
        duration_secs: f64,
        counts: Option<PipelineCounts>,
    },
    Failed {
        run_id: String,
        error: String,
    },
    Cancelled {
        run_id: String,
        reason: String,
    },
}

impl SseEvent {
    /// Returns the event type name as a static str.
    #[must_use]
    pub fn event_type(&self) -> &str {
        match self {
            Self::Started { .. } => "started",
            Self::Progress { .. } => "progress",
            Self::Log { .. } => "log",
            Self::Complete { .. } => "complete",
            Self::Failed { .. } => "failed",
            Self::Cancelled { .. } => "cancelled",
        }
    }

    /// Returns `true` if this event terminates the stream.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Complete { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }
}

/// Aggregate record/byte counts for a pipeline run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineCounts {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

// ---------------------------------------------------------------------------
// Filters / Requests
// ---------------------------------------------------------------------------

/// Filter for listing pipelines.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineFilter {
    pub tag: Option<Vec<String>>,
    pub dir: Option<PathBuf>,
}

/// Filter for listing runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunFilter {
    pub pipeline: Option<String>,
    pub status: Option<RunStatus>,
    pub limit: u32,
    pub cursor: Option<String>,
}

impl Default for RunFilter {
    fn default() -> Self {
        Self {
            pipeline: None,
            status: None,
            limit: 20,
            cursor: None,
        }
    }
}

/// Request to trigger a single pipeline sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRequest {
    pub pipeline: String,
    pub stream: Option<String>,
    pub full_refresh: bool,
    pub cursor_start: Option<String>,
    pub cursor_end: Option<String>,
    pub dry_run: bool,
}

/// Request to trigger a batch sync across multiple pipelines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatchRequest {
    pub tag: Option<Vec<String>>,
    pub exclude: Option<Vec<String>>,
    pub full_refresh: bool,
}

/// Request to run assertions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertRequest {
    pub pipeline: Option<String>,
    pub tag: Option<Vec<String>>,
}

/// Request to tear down a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeardownRequest {
    pub pipeline: String,
    pub reason: String,
}

/// Request to discover connection metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverRequest {
    pub connection: String,
    pub table: Option<String>,
}

/// Request to search the plugin registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginSearchRequest {
    pub query: String,
    pub plugin_type: Option<String>,
}

/// Request to reset pipeline state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetRequest {
    pub pipeline: String,
    pub stream: Option<String>,
}

/// Filter for freshness checks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FreshnessFilter {
    pub tag: Option<Vec<String>>,
}

/// Request for pipeline logs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogsRequest {
    pub pipeline: String,
    pub run_id: Option<String>,
    pub limit: u32,
}

impl Default for LogsRequest {
    fn default() -> Self {
        Self {
            pipeline: String::new(),
            run_id: None,
            limit: 100,
        }
    }
}

/// Filter for streaming logs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogsStreamFilter {
    pub pipeline: Option<String>,
}

// ---------------------------------------------------------------------------
// Responses — Pipeline
// ---------------------------------------------------------------------------

/// Summary view of a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSummary {
    pub name: String,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub source: String,
    pub destination: String,
    pub schedule: Option<String>,
    pub streams: u32,
    pub last_run: Option<LastRunInfo>,
}

/// Brief info about the most recent run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastRunInfo {
    pub run_id: String,
    pub status: RunStatus,
    pub finished_at: Option<DateTime<Utc>>,
}

/// Detailed pipeline configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDetail {
    pub name: String,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub schedule: Option<String>,
    pub source: serde_json::Value,
    pub destination: serde_json::Value,
    pub state: PipelineState,
}

/// Handle returned when a run is started.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunHandle {
    pub run_id: String,
    pub status: RunStatus,
    pub links: RunLinks,
}

/// HATEOAS links for a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunLinks {
    #[serde(rename = "self")]
    pub self_url: String,
    pub events: String,
}

/// Handle returned when a batch sync is started.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRunHandle {
    pub batch_id: String,
    pub runs: Vec<BatchRunRef>,
    pub links: BatchLinks,
}

/// Reference to a single run within a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRunRef {
    pub pipeline: String,
    pub run_id: String,
}

/// HATEOAS links for a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchLinks {
    #[serde(rename = "self")]
    pub self_url: String,
}

/// Result of a pipeline config check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub passed: bool,
    pub checks: serde_json::Value,
}

/// Fully-resolved pipeline configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedConfig {
    pub pipeline: String,
    pub resolved_config: serde_json::Value,
}

/// Diff of streams between local config and remote state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffResult {
    pub streams: Vec<serde_json::Value>,
}

/// Result of assertion checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertResult {
    pub passed: bool,
    pub results: Vec<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Responses — Run
// ---------------------------------------------------------------------------

/// Detailed view of a single run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunDetail {
    pub run_id: String,
    pub pipeline: String,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_secs: Option<f64>,
    pub trigger: String,
    pub counts: Option<PipelineCounts>,
    pub streams: Vec<serde_json::Value>,
    pub timing: Option<serde_json::Value>,
    pub retry_count: u32,
    pub parallelism: u32,
    pub error: Option<String>,
}

/// Summary view of a run (for list responses).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummary {
    pub run_id: String,
    pub pipeline: String,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub duration_secs: Option<f64>,
    pub records_written: Option<u64>,
}

/// Detailed view of a batch of runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchDetail {
    pub batch_id: String,
    pub status: RunStatus,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub runs: Vec<BatchRunEntry>,
}

/// Single run entry within a batch detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRunEntry {
    pub pipeline: String,
    pub run_id: String,
    pub status: RunStatus,
}

// ---------------------------------------------------------------------------
// Responses — Connection
// ---------------------------------------------------------------------------

/// Summary view of a connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionSummary {
    pub name: String,
    pub connector: String,
    pub used_by: Vec<String>,
}

/// Detailed view of a connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionDetail {
    pub name: String,
    pub connector: String,
    pub config: serde_json::Value,
    pub used_by: Vec<String>,
}

/// Result of testing a connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionTestResult {
    pub name: String,
    pub status: String,
    pub latency_ms: Option<u64>,
    pub details: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
}

/// Result of discovering connection metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverResult {
    pub connection: String,
    pub streams: Option<Vec<serde_json::Value>>,
    pub table: Option<String>,
    pub columns: Option<Vec<serde_json::Value>>,
    pub indexes: Option<Vec<serde_json::Value>>,
    pub suggested_config: Option<serde_json::Value>,
    pub estimated_rows: Option<u64>,
}

// ---------------------------------------------------------------------------
// Responses — Plugin
// ---------------------------------------------------------------------------

/// Summary view of an installed plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginSummary {
    pub name: String,
    pub version: String,
    pub plugin_type: String,
    pub size_bytes: Option<u64>,
    pub installed_at: Option<DateTime<Utc>>,
}

/// Detailed view of a plugin (installed or registry).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginDetail {
    pub name: String,
    pub version: String,
    pub plugin_type: String,
    pub description: Option<String>,
    pub size_bytes: Option<u64>,
    pub manifest: Option<serde_json::Value>,
    pub available_versions: Vec<String>,
    pub installed: bool,
}

/// Plugin search result from the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginSearchResult {
    pub name: String,
    pub description: Option<String>,
    pub latest_version: String,
    pub plugin_type: String,
    pub downloads: Option<u64>,
}

/// Result of installing a plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginInstallResult {
    pub name: String,
    pub version: String,
    pub size_bytes: u64,
    pub installed: bool,
}

// ---------------------------------------------------------------------------
// Responses — Operations
// ---------------------------------------------------------------------------

/// Status summary of a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStatus {
    pub pipeline: String,
    pub schedule: Option<String>,
    pub state: PipelineState,
    pub last_run: Option<LastRunInfo>,
    pub next_run_in: Option<String>,
    pub health: String,
}

/// Detailed status of a pipeline including recent activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStatusDetail {
    pub pipeline: String,
    pub schedule: Option<String>,
    pub state: PipelineState,
    pub health: String,
    pub recent_runs: Vec<serde_json::Value>,
    pub streams: Vec<serde_json::Value>,
    pub dlq_rows: u64,
    pub assertions: Option<serde_json::Value>,
}

/// Result of resetting pipeline state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetResult {
    pub pipeline: String,
    pub streams_reset: Vec<String>,
    pub cursors_cleared: u32,
    pub next_sync_mode: String,
}

/// Freshness status for a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessStatus {
    pub pipeline: String,
    pub last_sync_age: String,
    pub sla_warn: Option<String>,
    pub sla_error: Option<String>,
    pub status: String,
}

/// Paginated log response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogsResult {
    pub items: Vec<LogEntry>,
    pub next_cursor: Option<String>,
}

/// Single log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub pipeline: Option<String>,
    pub run_id: Option<String>,
    pub message: String,
    pub fields: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Responses — Server
// ---------------------------------------------------------------------------

/// Server health check response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub mode: String,
    pub uptime_secs: u64,
    pub state_backend: String,
    pub state_backend_healthy: bool,
    pub agents_connected: u32,
}

/// Server version information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    pub version: String,
    pub rustc: Option<String>,
    pub wasmtime: Option<String>,
    pub mode: String,
    pub plugins: Vec<serde_json::Value>,
}

/// Server configuration snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub mode: String,
    pub port: u16,
    pub metrics_port: Option<u16>,
    pub state_backend: String,
    pub registry_url: Option<String>,
    pub trust_policy: Option<String>,
    pub auth_required: bool,
    pub pipelines_discovered: u32,
    pub scheduler_active: bool,
}
