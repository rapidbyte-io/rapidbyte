//! `rapidbyte resume` — resume scheduled runs for a pipeline.

use anyhow::Result;

/// Resume a pipeline via the controller REST API.
///
/// # Errors
///
/// Returns `Err` if the controller URL cannot be resolved or the request fails.
pub async fn execute(ctrl: &crate::ControllerFlags, pipeline: &str) -> Result<()> {
    super::rest_client::pipeline_action(ctrl, pipeline, "resume").await
}
