//! Config file helpers for `~/.rapidbyte/config.yaml`.

use std::path::PathBuf;

use anyhow::{Context, Result};

/// Returns the path to the user config file.
#[must_use]
pub fn config_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".rapidbyte").join("config.yaml")
}

/// Read the config file, returning an empty mapping if it does not exist.
///
/// # Errors
///
/// Returns `Err` if the file exists but cannot be read or parsed.
pub fn read_config() -> Result<serde_yaml::Value> {
    let path = config_path();
    if !path.exists() {
        return Ok(serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    }
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_yaml::from_str(&contents).context("failed to parse config.yaml")
}

/// Write `value` to the config file, creating parent directories if needed.
///
/// # Errors
///
/// Returns `Err` if the directory cannot be created or the file cannot be written.
pub fn write_config(value: &serde_yaml::Value) -> Result<()> {
    let path = config_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let yaml = serde_yaml::to_string(value).context("failed to serialize config")?;
    std::fs::write(&path, yaml).with_context(|| format!("failed to write {}", path.display()))
}
