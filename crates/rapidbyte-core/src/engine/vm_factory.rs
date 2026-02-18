use anyhow::Result;
use wasmedge_sdk::wasi::WasiModule;

use rapidbyte_sdk::manifest::Permissions;

/// Create a WasiModule with security boundaries derived from the connector manifest.
///
/// When `permissions` is `Some`, only the explicitly declared env vars and
/// filesystem preopens are forwarded to the guest.  When `None` (no manifest,
/// backwards-compat path), everything is denied.
pub(crate) fn create_secure_wasi_module(permissions: Option<&Permissions>) -> Result<WasiModule> {
    let (env_strings, preopen_strings) = match permissions {
        Some(perms) => {
            // Only pass through env vars that the manifest explicitly allows AND that exist
            // on the host.  Format: "KEY=VALUE".
            let allowed_envs: Vec<String> = perms
                .env
                .allowed_vars
                .iter()
                .filter_map(|var| {
                    std::env::var(var)
                        .ok()
                        .map(|val| format!("{}={}", var, val))
                })
                .collect();

            // Map preopens to the WasiModule format: "guest_dir:host_dir"
            let preopened: Vec<String> = perms
                .fs
                .preopens
                .iter()
                .map(|dir| format!("{}:{}", dir, dir))
                .collect();

            (allowed_envs, preopened)
        }
        None => {
            // No manifest -> deny all (backwards compat)
            (vec![], vec![])
        }
    };

    // WasiModule::create expects Option<Vec<&str>>, so borrow from owned Strings
    let envs: Vec<&str> = env_strings.iter().map(|s| s.as_str()).collect();
    let preopens: Vec<&str> = preopen_strings.iter().map(|s| s.as_str()).collect();

    WasiModule::create(
        Some(vec![]), // args: empty (no CLI args)
        Some(envs),
        if preopens.is_empty() {
            None
        } else {
            Some(preopens)
        },
    )
    .map_err(|e| anyhow::anyhow!("Failed to create WASI module: {:?}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::manifest::{EnvPermissions, FsPermissions, NetworkPermissions, Permissions};

    #[test]
    fn test_create_secure_wasi_module_no_permissions() {
        let result = create_secure_wasi_module(None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_secure_wasi_module_with_permissions() {
        let perms = Permissions {
            network: NetworkPermissions::default(),
            env: EnvPermissions {
                allowed_vars: vec!["PATH".to_string()],
            },
            fs: FsPermissions::default(),
        };
        let result = create_secure_wasi_module(Some(&perms));
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_secure_wasi_module_default_permissions() {
        // Default permissions should behave like deny-all (empty env, no preopens)
        let perms = Permissions::default();
        let result = create_secure_wasi_module(Some(&perms));
        assert!(result.is_ok());
    }
}
