use rapidbyte_runtime::{resolve_min_limit, SandboxOverrides};

use crate::config::types::parse_byte_size;

/// Build `SandboxOverrides` from pipeline permissions/limits and manifest resource limits.
/// Returns `None` if no overrides are specified from either side.
#[must_use]
pub fn build_sandbox_overrides(
    pipeline_perms: Option<&crate::config::types::PipelinePermissions>,
    pipeline_limits: Option<&crate::config::types::PipelineLimits>,
    manifest_limits: &rapidbyte_types::manifest::ResourceLimits,
) -> Option<SandboxOverrides> {
    let manifest_mem = manifest_limits
        .max_memory
        .as_ref()
        .and_then(|s| parse_byte_size(s).ok());
    let pipeline_mem = pipeline_limits
        .and_then(|l| l.max_memory.as_ref())
        .and_then(|s| parse_byte_size(s).ok());

    let manifest_timeout = manifest_limits.timeout_seconds;
    let pipeline_timeout = pipeline_limits.and_then(|l| l.timeout_seconds);

    let has_overrides = pipeline_perms.is_some()
        || pipeline_limits.is_some()
        || manifest_limits.max_memory.is_some()
        || manifest_limits.timeout_seconds.is_some();

    if has_overrides {
        Some(SandboxOverrides {
            allowed_hosts: pipeline_perms.and_then(|p| p.network.allowed_hosts.clone()),
            allowed_vars: pipeline_perms.and_then(|p| p.env.allowed_vars.clone()),
            allowed_preopens: pipeline_perms.and_then(|p| p.fs.allowed_preopens.clone()),
            max_memory_bytes: resolve_min_limit(manifest_mem, pipeline_mem),
            timeout_seconds: resolve_min_limit(manifest_timeout, pipeline_timeout),
        })
    } else {
        None
    }
}
