use anyhow::Result;
use wasmedge_sdk::wasi::WasiModule;

/// Create a WasiModule with deny-by-default security:
/// - No CLI args passed to guest
/// - No environment variables leaked to guest
/// - No filesystem preopens (connectors receive config via rb_open JSON)
pub(crate) fn create_secure_wasi_module() -> Result<WasiModule> {
    WasiModule::create(
        Some(vec![]), // args: empty (no CLI args)
        Some(vec![]), // envs: empty (deny all env vars)
        None,         // preopens: no filesystem access
    )
    .map_err(|e| anyhow::anyhow!("Failed to create WASI module: {:?}", e))
}
