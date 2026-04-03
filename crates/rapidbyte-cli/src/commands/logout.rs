//! `rapidbyte logout` — remove a stored authentication token.

use anyhow::Result;

use super::config;

/// Remove controller credentials from the user config file.
///
/// If `controller_url` is `Some`, only the token field is removed while keeping
/// the controller URL.  If `None`, the entire `controller` section is removed.
///
/// # Errors
///
/// Returns `Err` if the config cannot be read or written.
pub fn execute(controller_url: Option<&str>) -> Result<()> {
    let mut cfg = config::read_config()?;
    if let Some(map) = cfg.as_mapping_mut() {
        if controller_url.is_some() {
            if let Some(ctrl) = map.get_mut(serde_yaml::Value::String("controller".to_string())) {
                if let Some(ctrl_map) = ctrl.as_mapping_mut() {
                    ctrl_map.remove(serde_yaml::Value::String("token".to_string()));
                }
            }
        } else {
            map.remove(serde_yaml::Value::String("controller".to_string()));
        }
        config::write_config(&cfg)?;
    }
    eprintln!("Logged out");
    Ok(())
}
