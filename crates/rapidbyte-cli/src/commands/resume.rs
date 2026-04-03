//! `rapidbyte resume` — resume scheduled runs for a pipeline.

use anyhow::Result;

use super::rest_client::{resolve_controller_and_token, RestClient};

/// Resume a pipeline via the controller REST API.
///
/// # Errors
///
/// Returns `Err` if the controller URL cannot be resolved or the request fails.
pub async fn execute(ctrl: &crate::ControllerFlags, pipeline: &str) -> Result<()> {
    let (url, token) = resolve_controller_and_token(ctrl)?;
    let client = RestClient::new(&url, token.as_deref())?;
    let resp = client
        .post(&format!("/api/v1/pipelines/{pipeline}/resume"), None)
        .await?;
    let state = resp
        .get("state")
        .and_then(|v| v.as_str())
        .unwrap_or("resumed");
    eprintln!("Pipeline '{pipeline}' {state}");
    Ok(())
}
