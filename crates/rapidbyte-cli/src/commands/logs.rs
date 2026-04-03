//! `rapidbyte logs` — view pipeline logs from the controller.

use std::fmt::Write as _;

use anyhow::Result;

use super::rest_client::{resolve_controller_and_token, url_encode, RestClient};

/// Fetch and display pipeline log entries from the controller REST API.
///
/// # Errors
///
/// Returns `Err` if the controller URL cannot be resolved or the request fails.
pub async fn execute(
    ctrl: &crate::ControllerFlags,
    pipeline: &str,
    run_id: Option<&str>,
    limit: u32,
) -> Result<()> {
    let (url, token) = resolve_controller_and_token(ctrl)?;
    let client = RestClient::new(&url, token.as_deref())?;
    let encoded_pipeline = url_encode(pipeline);
    let mut path = format!("/api/v1/logs?pipeline={encoded_pipeline}&limit={limit}");
    if let Some(id) = run_id {
        let encoded_id = url_encode(id);
        write!(path, "&run_id={encoded_id}").expect("writing to String is infallible");
    }
    let resp = client.get(&path).await?;
    let empty = vec![];
    let items = resp
        .get("items")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty);
    if items.is_empty() {
        eprintln!("No log entries found.");
        return Ok(());
    }
    for entry in items {
        let ts = entry
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let level = entry
            .get("level")
            .and_then(|v| v.as_str())
            .unwrap_or("info");
        let msg = entry.get("message").and_then(|v| v.as_str()).unwrap_or("");
        eprintln!("{ts}  [{level}]  {msg}");
    }
    Ok(())
}
