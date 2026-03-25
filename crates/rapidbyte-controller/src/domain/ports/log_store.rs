use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::traits::{EventStream, PaginatedList};

#[derive(Debug, Clone, Serialize)]
pub struct StoredLogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub pipeline: String,
    pub run_id: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct LogFilter {
    pub pipeline: String,
    pub run_id: Option<String>,
    pub limit: u32,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LogStreamFilter {
    pub pipeline: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("database error: {0}")]
    Database(String),
}

#[async_trait]
pub trait LogStore: Send + Sync {
    /// Query stored log entries with filtering and pagination.
    async fn query(&self, filter: LogFilter) -> Result<PaginatedList<StoredLogEntry>, LogError>;

    /// Subscribe to real-time log entries.
    async fn subscribe(
        &self,
        filter: LogStreamFilter,
    ) -> Result<EventStream<StoredLogEntry>, LogError>;
}
