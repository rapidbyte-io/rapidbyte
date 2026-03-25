use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Operational state of a pipeline (active or paused).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineState {
    Active,
    Paused,
}

impl PipelineState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
        }
    }
}

impl std::str::FromStr for PipelineState {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "active" => Ok(Self::Active),
            "paused" => Ok(Self::Paused),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for PipelineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

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

    /// Get pipeline operational state (e.g. `"active"` or `"paused"`).
    async fn get_pipeline_state(
        &self,
        pipeline: &str,
    ) -> Result<Option<PipelineState>, CursorError>;

    /// Set pipeline operational state.
    async fn set_pipeline_state(
        &self,
        pipeline: &str,
        state: PipelineState,
    ) -> Result<(), CursorError>;
}
