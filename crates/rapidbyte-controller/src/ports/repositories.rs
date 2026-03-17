//! Persistence repository interfaces.

use async_trait::async_trait;

use crate::domain::run::{Run, RunId};

#[derive(Debug, Clone)]
pub struct StoredRun {
    pub run: Run,
    pub retryable: bool,
}

/// Repository contract for run aggregate persistence.
#[async_trait]
pub trait RunRepository: Send + Sync {
    /// Create a run record or return an existing run when idempotency key matches.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence fails.
    async fn create_or_get_by_idempotency(
        &self,
        run: Run,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<StoredRun>;

    /// Get a run by ID.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence fails.
    async fn get(&self, run_id: &RunId) -> anyhow::Result<Option<StoredRun>>;

    /// Queue a new attempt for an existing run.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence fails.
    async fn queue_retry(&self, run_id: &RunId) -> anyhow::Result<()>;

    /// List runs in stable repository order.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence fails.
    async fn list(&self, limit: usize) -> anyhow::Result<Vec<StoredRun>>;

    /// Mark a run as cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence fails.
    async fn mark_cancelled(&self, run_id: &RunId) -> anyhow::Result<()>;
}
