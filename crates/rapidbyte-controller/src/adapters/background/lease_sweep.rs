//! Background lease/timeout sweeper adapter.

use crate::app::timeout_attempt::TimeoutAttemptExecutor;

/// Run one timeout sweep tick by invoking the timeout use-case executor.
///
/// # Errors
///
/// Returns an error when timeout execution fails.
pub async fn run_timeout_tick(executor: &impl TimeoutAttemptExecutor) -> anyhow::Result<()> {
    executor.execute_timeout_tick().await
}
