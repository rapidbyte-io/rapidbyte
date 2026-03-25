use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

use super::{EventStream, PaginatedList, ServiceError};

#[async_trait]
pub trait RunService: Send + Sync {
    async fn get(&self, run_id: &str) -> Result<RunDetail, ServiceError>;
    async fn list(&self, filter: RunFilter) -> Result<PaginatedList<RunSummary>, ServiceError>;
    async fn events(&self, run_id: &str) -> Result<EventStream<ProgressEvent>, ServiceError>;
    async fn cancel(&self, run_id: &str) -> Result<RunDetail, ServiceError>;
    async fn get_batch(&self, batch_id: &str) -> Result<BatchDetail, ServiceError>;
    async fn batch_events(
        &self,
        batch_id: &str,
    ) -> Result<EventStream<ProgressEvent>, ServiceError>;
}

#[derive(Debug, Clone, Serialize)]
pub struct RunDetail {
    pub run_id: String,
    pub pipeline: String,
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub duration_secs: Option<f64>,
    pub counts: Option<RunCounts>,
    pub timing: Option<RunTiming>,
    pub retry_count: u32,
    pub cancel_requested: bool,
    pub max_retries: u32,
    pub error: Option<RunErrorInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunCounts {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunTiming {
    pub source_duration_secs: Option<f64>,
    pub dest_duration_secs: Option<f64>,
    pub wasm_overhead_secs: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunErrorInfo {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub run_id: String,
    pub pipeline: String,
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub duration_secs: Option<f64>,
    pub records_written: Option<u64>,
    pub attempt: u32,
}

#[derive(Debug, Clone)]
pub struct RunFilter {
    pub pipeline: Option<String>,
    pub status: Option<String>,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchDetail {
    pub batch_id: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub runs: Vec<BatchRunEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchRunEntry {
    pub pipeline: String,
    pub run_id: String,
    pub status: String,
}

/// Progress event emitted during pipeline execution.
/// Used by both run SSE and batch SSE streams.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProgressEvent {
    Started {
        run_id: String,
        pipeline: String,
    },
    Progress {
        run_id: String,
        phase: String,
        stream: Option<String>,
        records_read: Option<u64>,
        records_written: Option<u64>,
    },
    Complete {
        run_id: String,
        status: String,
        duration_secs: Option<f64>,
    },
    Failed {
        run_id: String,
        error: RunErrorInfo,
    },
    Cancelled {
        run_id: String,
    },
}

impl ProgressEvent {
    #[must_use]
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::Started { .. } => "started",
            Self::Progress { .. } => "progress",
            Self::Complete { .. } => "complete",
            Self::Failed { .. } => "failed",
            Self::Cancelled { .. } => "cancelled",
        }
    }
}
