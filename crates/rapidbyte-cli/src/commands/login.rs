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
    // Store under `rest_url` to distinguish from gRPC endpoint.
    // gRPC commands fall back to `url` in config (set manually if needed).
    // Token is shared by both REST and gRPC commands.
    let existing = map
        .get(serde_yaml::Value::String("controller".into()))
        .and_then(|v| v.as_mapping())
        .cloned()
        .unwrap_or_default();
    let mut ctrl_map = existing;
    ctrl_map.insert("rest_url".into(), controller_url.into());
    ctrl_map.insert("token".into(), serde_yaml::Value::String(token));
    map.insert("controller".into(), serde_yaml::Value::Mapping(ctrl_map));
    config::write_config(&cfg)?;
    eprintln!("Logged in to {controller_url}");
    Ok(())
}
