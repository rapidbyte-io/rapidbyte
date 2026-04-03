//! `rapidbyte login` — store an authentication token for a controller.

use anyhow::{bail, Context, Result};

use super::config;

/// Store authentication credentials for a controller in the user config file.
///
/// If `token` is `None` the user is prompted to enter it interactively.
///
/// # Errors
///
/// Returns `Err` if the token cannot be read from stdin, the config cannot be
/// read, or the updated config cannot be written.
pub fn execute(controller_url: &str, token: Option<&str>) -> Result<()> {
    let token = if let Some(t) = token {
        let t = t.trim().to_string();
        if t.is_empty() {
            bail!("token cannot be empty");
        }
        t
    } else {
        eprint!("Token: ");
        let t = rpassword::read_password()
            .context("failed to read token")?
            .trim()
            .to_string();
        if t.is_empty() {
            bail!("token cannot be empty");
        }
        t
    };

    let mut cfg = config::read_config()?;
    let map = cfg
        .as_mapping_mut()
        .ok_or_else(|| anyhow::anyhow!("config is not a mapping"))?;
    let controller = serde_yaml::Value::Mapping({
        let mut m = serde_yaml::Mapping::new();
        m.insert("url".into(), controller_url.into());
        m.insert("token".into(), serde_yaml::Value::String(token));
        m
    });
    map.insert("controller".into(), controller);
    config::write_config(&cfg)?;
    eprintln!("Logged in to {controller_url}");
    eprintln!("Note: this URL is used by REST commands (pause/resume/reset/freshness/logs).");
    eprintln!("      gRPC commands (status/watch/list-runs) may use a different port.");
    Ok(())
}
