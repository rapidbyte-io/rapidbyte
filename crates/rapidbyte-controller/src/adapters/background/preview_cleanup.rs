//! Background preview cleanup adapter.

use crate::app::cleanup_preview::PreviewCleanupExecutor;

/// Run one preview cleanup tick by invoking the cleanup use-case executor.
///
/// # Errors
///
/// Returns an error when cleanup execution fails.
pub async fn run_preview_cleanup_tick(
    executor: &impl PreviewCleanupExecutor,
) -> anyhow::Result<()> {
    executor.execute_preview_cleanup_tick().await
}
