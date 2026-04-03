//! `rapidbyte login` — store an authentication token for a controller.

use std::io::BufRead as _;

use anyhow::{bail, Result};

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
        t.to_string()
    } else {
        eprint!("Token: ");
        let mut input = String::new();
        std::io::stdin().lock().read_line(&mut input)?;
        let t = input.trim().to_string();
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
    Ok(())
}
