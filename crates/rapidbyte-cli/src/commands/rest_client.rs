//! Shared HTTP client for REST-based operational commands.

use anyhow::{bail, Context, Result};

/// A thin wrapper around `reqwest::Client` with base URL and optional auth.
pub struct RestClient {
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
        let body: serde_json::Value = resp.json().await.context("failed to parse JSON")?;
        if !status.is_success() {
            let msg = body
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error");
            bail!("{msg} (HTTP {status})");
        }
        Ok(body)
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
    let resp = client
        .post(&format!("/api/v1/pipelines/{pipeline}/{action}"), None)
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
    let url = ctrl
        .controller
        .clone()
        .or_else(crate::controller_url_from_config)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "controller URL required: use --controller, RAPIDBYTE_CONTROLLER, or `rapidbyte login`"
            )
        })?;

    let token = ctrl.auth_token.clone().or_else(|| {
        super::config::read_config().ok().and_then(|v| {
            v.get("controller")?
                .get("token")?
                .as_str()
                .map(String::from)
        })
    });

    Ok((url, token))
}
