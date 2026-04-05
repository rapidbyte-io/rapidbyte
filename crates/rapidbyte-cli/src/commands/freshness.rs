//! `rapidbyte freshness` — check data freshness for pipelines.

use anyhow::Result;

use super::rest_client::{resolve_controller_and_token, url_encode, RestClient};

/// Query freshness data via the controller REST API.
///
/// # Errors
///
/// Returns `Err` if the controller URL cannot be resolved or the request fails.
pub async fn execute(ctrl: &crate::ControllerFlags, tag: Option<&str>) -> Result<()> {
    let (url, token) = resolve_controller_and_token(ctrl)?;
    let client = RestClient::new(&url, token.as_deref())?;
    let path = match tag {
        Some(t) => format!("/api/v1/freshness?tag={}", url_encode(t)),
        None => "/api/v1/freshness".to_string(),
    };
    let resp = client.get(&path).await?;
    let empty = vec![];
    let items = resp.as_array().unwrap_or(&empty);
    if items.is_empty() {
        eprintln!("No freshness data available.");
        return Ok(());
    }
    for item in items {
        let pipeline = item.get("pipeline").and_then(|v| v.as_str()).unwrap_or("?");
        let age = item
            .get("last_sync_age")
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        let status = item.get("status").and_then(|v| v.as_str()).unwrap_or("?");
        let symbol = if status == "fresh" {
            "\u{2713}"
        } else {
            "\u{2717}"
        };
        eprintln!("  {pipeline:<30} {age:<12} {symbol} {status}");
    }
    Ok(())
}
