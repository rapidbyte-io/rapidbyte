//! Verify that plugin module items are publicly accessible.

use rapidbyte_engine::plugin::resolver::{resolve_plugins, ResolvedPlugins};
use rapidbyte_engine::plugin::sandbox::build_sandbox_overrides;

#[test]
fn resolve_types_are_public() {
    // Compile-time check: these types are importable from outside the crate.
    // `resolve_plugins` is async so we verify it's callable and returns the right type.
    let _ = resolve_plugins;
    let _ = build_sandbox_overrides;

    // Verify ResolvedPlugins fields are accessible.
    fn _assert_resolved_plugins_fields(r: &ResolvedPlugins) {
        let _ = &r.source_wasm;
        let _ = &r.dest_wasm;
        let _ = &r.source_manifest;
        let _ = &r.dest_manifest;
    }
}
