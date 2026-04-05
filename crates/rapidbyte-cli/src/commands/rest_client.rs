//! Shared HTTP client for REST-based operational commands.

use anyhow::{bail, Context, Result};

/// Percent-encode characters that are unsafe in URL path segments or query
/// values.  Only the characters that are most likely to appear in user-supplied
/// pipeline names, run IDs, and tags are encoded; this avoids adding a new
/// dependency for a narrow use-case.
pub(crate) fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                use std::fmt::Write as _;
                write!(out, "%{b:02X}").expect("writing to String is infallible");
            }
        }
    }
    out
}

/// A thin wrapper around `reqwest::Client` with base URL and optional auth.
pub(crate) struct RestClient {
    base_url: String,
    client: reqwest::Client,
    auth_token: Option<String>,
}

impl RestClient {
    /// Create a new `RestClient`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the underlying HTTP client cannot be constructed.
    pub fn new(base_url: &str, auth_token: Option<&str>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("failed to create HTTP client")?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
            auth_token: auth_token.map(String::from),
        })
    }

    /// Perform a GET request and return the parsed JSON body.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the request fails, the response is not valid JSON, or
    /// the HTTP status indicates an error.
    pub async fn get(&self, path: &str) -> Result<serde_json::Value> {
        let url = format!("{}{path}", self.base_url);
        let mut req = self.client.get(&url);
        if let Some(ref token) = self.auth_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        let resp = req.send().await.context("request failed")?;
        self.handle_response(resp).await
    }

    /// Perform a POST request and return the parsed JSON body.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the request fails, the response is not valid JSON, or
    /// the HTTP status indicates an error.
    pub async fn post(
        &self,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let url = format!("{}{path}", self.base_url);
        let mut req = self.client.post(&url);
        if let Some(ref token) = self.auth_token {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await.context("request failed")?;
        self.handle_response(resp).await
    }

    async fn handle_response(&self, resp: reqwest::Response) -> Result<serde_json::Value> {
        let status = resp.status();
        if !status.is_success() {
            // Read as text first — error responses may not be valid JSON
            let text = match resp.text().await {
                Ok(t) => t,
                Err(e) => format!("failed to read response body: {e}"),
            };
            let msg = serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v.get("error")?.get("message")?.as_str().map(String::from))
                .unwrap_or_else(|| {
                    if text.is_empty() {
                        "unknown error".to_string()
                    } else {
                        text.chars().take(200).collect()
                    }
                });
            bail!("{msg} (HTTP {status})");
        }
        resp.json().await.context("failed to parse response JSON")
    }
}

/// Perform a simple pipeline state-change action (pause, resume, etc.).
///
/// # Errors
///
/// Returns `Err` if the controller URL cannot be resolved or the request fails.
pub async fn pipeline_action(
    ctrl: &crate::ControllerFlags,
    pipeline: &str,
    action: &str,
) -> anyhow::Result<()> {
    let (url, token) = resolve_controller_and_token(ctrl)?;
    let client = RestClient::new(&url, token.as_deref())?;
    let encoded = url_encode(pipeline);
    let resp = client
        .post(&format!("/api/v1/pipelines/{encoded}/{action}"), None)
        .await?;
    let state = resp.get("state").and_then(|v| v.as_str()).unwrap_or(action);
    eprintln!("Pipeline '{pipeline}' {state}");
    Ok(())
}

/// Resolve the controller URL and auth token for REST commands.
///
/// Priority order:
/// 1. CLI flag / env var (already resolved by clap into `ctrl`)
/// 2. `~/.rapidbyte/config.yaml`
///
/// # Errors
///
/// Returns `Err` if no controller URL can be determined from any source.
pub fn resolve_controller_and_token(
    ctrl: &crate::ControllerFlags,
) -> Result<(String, Option<String>)> {
    // REST commands read `rest_url` from config (written by `login`).
    // This is separate from `url` which is used by gRPC commands.
    let rest_url_from_config = || -> Option<String> {
        let cfg = super::config::read_config().ok()?;
        cfg.get("controller")?
            .get("rest_url")?
            .as_str()
            .map(String::from)
    };

    let url = ctrl
        .controller
        .clone()
        .or_else(rest_url_from_config)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "controller URL required: use --controller, RAPIDBYTE_CONTROLLER, or `rapidbyte login`"
            )
        })?;

    // Token resolution: CLI flag/env → config file.
    // Only reuse stored token if no explicit URL was given (URL came from config)
    // or if the explicit URL matches the stored rest_url.
    let token = ctrl.auth_token.clone().or_else(|| {
        let cfg = super::config::read_config().ok()?;
        let ctrl_section = cfg.get("controller")?;
        let stored_rest_url = ctrl_section.get("rest_url")?.as_str()?;
        let stored_token = ctrl_section.get("token")?.as_str()?.to_string();
        let normalize = |u: &str| u.trim_end_matches('/').to_owned();
        match &ctrl.controller {
            None => Some(stored_token),
            Some(explicit_url) if normalize(explicit_url) == normalize(stored_rest_url) => {
                Some(stored_token)
            }
            Some(_) => None,
        }
    });

    Ok((url, token))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_encode_preserves_safe_chars() {
        assert_eq!(
            url_encode("hello-world_123.test~ok"),
            "hello-world_123.test~ok"
        );
    }

    #[test]
    fn url_encode_encodes_space_and_slash() {
        let encoded = url_encode("my pipeline/test");
        assert!(encoded.contains("%20"), "space should be encoded as %20");
        assert!(encoded.contains("%2F"), "slash should be encoded as %2F");
        assert!(!encoded.contains(' '), "raw space must not appear");
        assert!(!encoded.contains('/'), "raw slash must not appear");
    }

    #[test]
    fn url_encode_encodes_query_chars() {
        let encoded = url_encode("tag=tier-1&extra");
        assert!(encoded.contains("%3D"), "= should be encoded as %3D");
        assert!(encoded.contains("%26"), "& should be encoded as %26");
    }

    #[test]
    fn url_encode_empty_string() {
        assert_eq!(url_encode(""), "");
    }
}
