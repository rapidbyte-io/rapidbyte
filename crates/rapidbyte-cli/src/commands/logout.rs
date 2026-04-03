//! `rapidbyte logout` — remove a stored authentication token.

use anyhow::Result;

use super::config;

fn yaml_key(s: &str) -> serde_yaml::Value {
    serde_yaml::Value::String(s.to_string())
}

/// Remove controller credentials from the user config file.
///
/// If `controller_url` is `Some`, only the token field is removed while keeping
/// the controller URL.  If `None`, the entire `controller` section is removed.
///
/// Prints `"Logged out"` only when credentials were actually present and
/// removed; otherwise prints `"No controller credentials found"`.
///
/// # Errors
///
/// Returns `Err` if the config cannot be read or written.
pub fn execute(controller_url: Option<&str>) -> Result<()> {
    let mut cfg = config::read_config()?;
    let Some(map) = cfg.as_mapping_mut() else {
        eprintln!("No controller credentials found");
        return Ok(());
    };

    let mut modified = false;

    if controller_url.is_some() {
        if let Some(ctrl) = map.get_mut(yaml_key("controller")) {
            if let Some(ctrl_map) = ctrl.as_mapping_mut() {
                if ctrl_map.remove(yaml_key("token")).is_some() {
                    modified = true;
                }
            }
        }
    } else if map.remove(yaml_key("controller")).is_some() {
        modified = true;
    }

    if modified {
        config::write_config(&cfg)?;
        eprintln!("Logged out");
    } else {
        eprintln!("No controller credentials found");
    }

    Ok(())
}
