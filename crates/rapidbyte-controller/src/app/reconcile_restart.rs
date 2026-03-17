//! Reconcile restart use-case contract.

use async_trait::async_trait;

#[async_trait]
pub trait ReconcileRestartExecutor: Send + Sync {
    async fn execute_reconcile_tick(&self) -> anyhow::Result<()>;
}
