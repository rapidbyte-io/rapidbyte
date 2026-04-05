//! `rapidbyte reset` — clear sync state (cursors) for a pipeline.

use anyhow::Result;

use super::rest_client::{resolve_controller_and_token, url_encode, RestClient};

/// Reset cursor state for a pipeline via the controller REST API.
///
/// # Errors
///
/// Returns `Err` if the controller URL cannot be resolved or the request fails.
pub async fn execute(
    ctrl: &crate::ControllerFlags,
    pipeline: &str,
    stream: Option<&str>,
) -> Result<()> {
    let (url, token) = resolve_controller_and_token(ctrl)?;
    let client = RestClient::new(&url, token.as_deref())?;
    let body = stream.map(|s| serde_json::json!({ "stream": s }));
    let encoded = url_encode(pipeline);
    let resp = client
        .post(&format!("/api/v1/pipelines/{encoded}/reset"), body.as_ref())
        .await?;
    let cleared = resp
        .get("cursors_cleared")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    eprintln!("Cleared {cleared} cursor(s) for '{pipeline}'. Next sync: full refresh.");
    Ok(())
}
