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
pub(crate) fn parse_plugin_id(plugin_ref: &str) -> (String, String) {
    if let Some((id, ver)) = plugin_ref.rsplit_once(':') {
        (id.to_string(), ver.to_string())
    } else {
        (plugin_ref.to_string(), "0.0.0".to_string())
    }
}
