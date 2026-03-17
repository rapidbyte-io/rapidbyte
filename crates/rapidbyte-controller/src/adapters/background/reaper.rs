//! Background reconciliation/reaper adapter.

use crate::app::reconcile_restart::ReconcileRestartExecutor;

/// Run one reconciliation tick by invoking the reconciliation use-case executor.
///
/// # Errors
///
/// Returns an error when reconciliation execution fails.
pub async fn run_reconcile_tick(executor: &impl ReconcileRestartExecutor) -> anyhow::Result<()> {
    executor.execute_reconcile_tick().await
}
