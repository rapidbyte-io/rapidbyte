//! Preview cleanup use-case contract.

use async_trait::async_trait;

#[async_trait]
pub trait PreviewCleanupExecutor: Send + Sync {
    async fn execute_preview_cleanup_tick(&self) -> anyhow::Result<()>;
}
