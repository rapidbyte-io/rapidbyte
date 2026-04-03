//! Config file helpers for `~/.rapidbyte/config.yaml`.

use std::path::PathBuf;

use anyhow::{Context, Result};

/// Returns the path to the user config file.
///
/// # Errors
///
/// Returns `Err` if `HOME` is not set in the environment.
pub fn config_path() -> Result<PathBuf> {
    let home = std::env::var("HOME")
        .context("HOME environment variable not set; cannot locate config directory")?;
    Ok(PathBuf::from(home).join(".rapidbyte").join("config.yaml"))
}

/// Read the config file, returning an empty mapping if it does not exist or
/// if `HOME` is unset (no config directory reachable).
///
/// # Errors
///
/// Returns `Err` if the file exists but cannot be read or parsed.
pub fn read_config() -> Result<serde_yaml::Value> {
    let Ok(path) = config_path() else {
        return Ok(serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    };
    if !path.exists() {
        return Ok(serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    }
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_yaml::from_str(&contents).context("failed to parse config.yaml")
}

/// Write `value` to the config file, creating parent directories if needed.
///
/// On Unix the config directory is created with mode `0o700` and the file
/// with mode `0o600` so that credentials are not world-readable.
///
/// # Errors
///
/// Returns `Err` if `HOME` is unset, the directory cannot be created, or
/// the file cannot be written.
pub fn write_config(value: &serde_yaml::Value) -> Result<()> {
    let path = config_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                .with_context(|| format!("failed to set permissions on {}", parent.display()))?;
        }
    }
    let yaml = serde_yaml::to_string(value).context("failed to serialize config")?;

    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        file.write_all(yaml.as_bytes())
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(&path, yaml)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    Ok(())
}
