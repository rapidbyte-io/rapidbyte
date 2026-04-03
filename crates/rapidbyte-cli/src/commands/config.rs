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

    // Atomic write: write to temp file, sync, then rename to avoid
    // corruption on interruption.
    let parent = path.parent().expect("config path has parent");
    let tmp_path = parent.join(".config.yaml.tmp");

    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp_path)
            .with_context(|| format!("failed to open {}", tmp_path.display()))?;
        file.write_all(yaml.as_bytes())
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to sync {}", tmp_path.display()))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(&tmp_path, &yaml)
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    }
    std::fs::rename(&tmp_path, &path).with_context(|| {
        format!(
            "failed to rename {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// All tests that mutate the `HOME` environment variable must hold this
    /// lock for their entire duration so they do not race with each other.
    static HOME_LOCK: Mutex<()> = Mutex::new(());

    /// Set `HOME` to `new_value`, returning the previous value so the caller
    /// can restore it later.
    fn set_home(new_value: &std::path::Path) -> Option<String> {
        let original = std::env::var("HOME").ok();
        // SAFETY: guarded by HOME_LOCK; no other thread may touch HOME while
        // the lock is held.
        unsafe { std::env::set_var("HOME", new_value) };
        original
    }

    /// Restore `HOME` to `original` (or remove it if it was unset).
    fn restore_home(original: Option<String>) {
        unsafe {
            match original {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
    }

    #[test]
    fn config_path_succeeds_when_home_is_set() {
        // HOME is always set in CI/dev environments, so config_path() must
        // return Ok and the tail of the path must be the expected file.
        let _guard = HOME_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = config_path();
        assert!(
            result.is_ok(),
            "config_path should succeed when HOME is set"
        );
        let path = result.unwrap();
        assert!(
            path.ends_with(".rapidbyte/config.yaml"),
            "expected path to end with .rapidbyte/config.yaml, got: {path:?}"
        );
    }

    #[test]
    fn read_config_returns_empty_mapping_when_no_file() {
        // Point HOME at a freshly-created empty tempdir so no config file
        // exists; read_config must return an empty mapping rather than Err.
        let dir = tempfile::tempdir().unwrap();
        let _guard = HOME_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let original = set_home(dir.path());
        let result = read_config();
        restore_home(original);

        let cfg = result.unwrap();
        assert!(
            cfg.as_mapping().is_some(),
            "expected empty mapping, got: {cfg:?}"
        );
    }

    #[test]
    fn write_and_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let _guard = HOME_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let original = set_home(dir.path());

        let mut mapping = serde_yaml::Mapping::new();
        mapping.insert("key".into(), "value".into());
        let value = serde_yaml::Value::Mapping(mapping);

        let write_result = write_config(&value);
        let read_result = read_config();
        restore_home(original);

        write_result.expect("write_config failed");
        let read_back = read_result.expect("read_config failed");
        assert_eq!(
            read_back.get("key").and_then(|v| v.as_str()),
            Some("value"),
            "round-trip value mismatch"
        );
    }
}
