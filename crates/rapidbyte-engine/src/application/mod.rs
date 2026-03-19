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
pub(crate) fn parse_plugin_id(plugin_ref: &str) -> (String, String) {
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
pub(crate) fn extract_permissions(
    resolved: &crate::domain::ports::resolver::ResolvedPlugin,
) -> Option<rapidbyte_types::manifest::Permissions> {
    resolved.manifest.as_ref().map(|m| m.permissions.clone())
}
