//! Timeout attempt use-case contract.

use async_trait::async_trait;

#[async_trait]
pub trait TimeoutAttemptExecutor: Send + Sync {
    async fn execute_timeout_tick(&self) -> anyhow::Result<()>;
}
