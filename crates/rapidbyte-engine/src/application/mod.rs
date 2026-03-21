//! Application layer: DI context, use-case orchestration, and testing fakes.
//!
//! | Module     | Responsibility |
//! |------------|----------------|
//! | `check`    | [`check_pipeline`] — resolve and validate all pipeline plugins |
//! | `context`  | [`EngineContext`] DI container and [`EngineConfig`] |
//! | `discover` | [`discover_plugin`] — resolve a source plugin and discover streams |
//! | `run`      | [`run_pipeline`] — core pipeline orchestration with retry loop |
//! | `testing`  | In-memory fakes and [`TestContext`] factory (test / `test-support` only) |

pub mod check;
pub mod context;
pub mod discover;
pub mod run;

#[cfg(any(test, feature = "test-support"))]
pub mod testing;

/// Split a plugin reference like `"rapidbyte/source-postgres:1.0"` into
/// `(id, version)`. Falls back to `("plugin_ref", "0.0.0")` for bare names.
///
/// Only treats `:` as a version separator after the last `/`, so OCI
/// references like `registry:5000/org/plugin:1.0` are handled correctly.
pub fn parse_plugin_id(plugin_ref: &str) -> (String, String) {
    // Only consider ':' as version separator after the last '/'
    let after_slash = plugin_ref.rfind('/').map_or(0, |i| i + 1);
    let suffix = &plugin_ref[after_slash..];
    if let Some(colon_pos) = suffix.rfind(':') {
        let split_at = after_slash + colon_pos;
        (
            plugin_ref[..split_at].to_string(),
            plugin_ref[split_at + 1..].to_string(),
        )
    } else {
        (plugin_ref.to_string(), "0.0.0".to_string())
    }
}

/// Extract the WASI sandbox permissions from a resolved plugin's manifest.
///
/// Returns `None` when the plugin has no embedded manifest, or `Some(permissions)`
/// when one is present.
pub fn extract_permissions(
    resolved: &crate::domain::ports::resolver::ResolvedPlugin,
) -> Option<rapidbyte_types::manifest::Permissions> {
    resolved.manifest.as_ref().map(|m| m.permissions.clone())
}

/// Build [`SandboxOverrides`](rapidbyte_runtime::SandboxOverrides) from
/// pipeline YAML permission and limit overrides.
///
/// Returns `None` when neither permissions nor limits are specified in the
/// pipeline config. When set, the overrides are intersected with manifest
/// permissions by the runtime's `build_wasi_ctx`.
///
/// Uses `min(manifest, pipeline)` for resource limits — pipeline config
/// can only narrow plugin-declared limits, never widen them (per PROTOCOL.md).
/// If only one side specifies a limit, that value is used directly.
///
/// Returns an error if a memory limit string is present but malformed, to
/// prevent silently degrading into no memory cap.
pub fn build_sandbox_overrides(
    yaml_permissions: Option<&rapidbyte_pipeline_config::PipelinePermissions>,
    yaml_limits: Option<&rapidbyte_pipeline_config::PipelineLimits>,
    manifest: Option<&rapidbyte_types::manifest::PluginManifest>,
) -> Result<Option<rapidbyte_runtime::SandboxOverrides>, crate::domain::error::PipelineError> {
    use rapidbyte_pipeline_config::parse_byte_size;
    use rapidbyte_runtime::SandboxOverrides;

    use crate::domain::error::PipelineError;

    let manifest_limits = manifest.map(|m| &m.limits);
    let has_yaml = yaml_permissions.is_some() || yaml_limits.is_some();
    let has_manifest =
        manifest_limits.is_some_and(|l| l.max_memory.is_some() || l.timeout_seconds.is_some());

    if !has_yaml && !has_manifest {
        return Ok(None);
    }

    let yaml_memory = match yaml_limits.and_then(|l| l.max_memory.as_deref()) {
        Some(s) => Some(parse_byte_size(s).map_err(|e| {
            PipelineError::infra(format!("invalid pipeline max_memory '{s}': {e}"))
        })?),
        None => None,
    };
    let manifest_memory = match manifest_limits.and_then(|l| l.max_memory.as_deref()) {
        Some(s) => Some(parse_byte_size(s).map_err(|e| {
            PipelineError::infra(format!("invalid manifest max_memory '{s}': {e}"))
        })?),
        None => None,
    };

    // min(manifest, pipeline) — pipeline can only narrow, never widen
    let max_memory_bytes = match (yaml_memory, manifest_memory) {
        (Some(y), Some(m)) => Some(y.min(m)),
        (Some(y), None) => Some(y),
        (None, Some(m)) => Some(m),
        (None, None) => None,
    };

    let yaml_timeout = yaml_limits.and_then(|l| l.timeout_seconds);
    let manifest_timeout = manifest_limits.and_then(|l| l.timeout_seconds);

    let timeout_seconds = match (yaml_timeout, manifest_timeout) {
        (Some(y), Some(m)) => Some(y.min(m)),
        (Some(y), None) => Some(y),
        (None, Some(m)) => Some(m),
        (None, None) => None,
    };

    Ok(Some(SandboxOverrides {
        allowed_hosts: yaml_permissions.and_then(|p| p.network.allowed_hosts.clone()),
        allowed_vars: yaml_permissions.and_then(|p| p.env.allowed_vars.clone()),
        allowed_preopens: yaml_permissions.and_then(|p| p.fs.allowed_preopens.clone()),
        max_memory_bytes,
        timeout_seconds,
    }))
}
