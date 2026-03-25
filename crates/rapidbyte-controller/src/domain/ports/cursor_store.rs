use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct StreamCursor {
    pub stream: String,
    pub cursor_field: String,
    pub cursor_value: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SyncTimestamp {
    pub pipeline: String,
    pub last_sync_at: Option<DateTime<Utc>>,
}

#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    #[error("database error: {0}")]
    Database(String),
}

#[async_trait]
pub trait CursorStore: Send + Sync {
    /// Get cursor values for a pipeline's streams.
    async fn get_cursors(&self, pipeline: &str) -> Result<Vec<StreamCursor>, CursorError>;

    /// Clear cursors for a pipeline (optionally a single stream).
    /// Returns the number of cursors cleared.
    async fn clear(&self, pipeline: &str, stream: Option<&str>) -> Result<u64, CursorError>;

    /// Get last sync timestamps for freshness checks.
    async fn last_sync_times(
        &self,
        pipelines: &[String],
    ) -> Result<Vec<SyncTimestamp>, CursorError>;
}
