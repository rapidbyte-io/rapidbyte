//! Rapidbyte Connector SDK.
//!
//! Provides traits, protocol types, and host-import wrappers for building
//! WASI-based data pipeline connectors.

#[cfg(feature = "runtime")]
pub mod arrow;
#[cfg(feature = "build")]
pub mod build;
#[cfg(feature = "runtime")]
pub mod connector;
#[cfg(feature = "runtime")]
pub mod host_ffi;
#[cfg(feature = "runtime")]
pub mod host_tcp;
#[cfg(feature = "runtime")]
pub mod prelude;

// Type re-exports — always available (no feature gate)
pub use rapidbyte_types::errors;
pub use rapidbyte_types::manifest;
pub use rapidbyte_types::protocol;

#[cfg(feature = "runtime")]
pub use wit_bindgen;

/// Embed the manifest JSON as a `rapidbyte_manifest_v1` custom section.
///
/// Call this once at crate root (e.g., `src/main.rs`) after `connector_main!`.
/// Requires a `build.rs` that calls `ManifestBuilder::emit()`.
#[macro_export]
macro_rules! embed_manifest {
    () => {
        include!(concat!(env!("OUT_DIR"), "/rapidbyte_manifest_embed.rs"));
    };
}

/// Trait for config types that provide a JSON Schema at compile time.
///
/// Derived via `#[derive(ConfigSchema)]`. Do not implement manually.
pub trait ConfigSchema {
    /// JSON Schema (Draft 7) as a compile-time string.
    const SCHEMA_JSON: &'static str;
}

/// Re-export the derive macro so users write `use rapidbyte_sdk::ConfigSchema`.
#[cfg(feature = "runtime")]
pub use rapidbyte_sdk_macros::ConfigSchema;

/// Embed the config schema as a `rapidbyte_config_schema_v1` Wasm custom section.
///
/// Place at crate root after `connector_main!` and `embed_manifest!`:
///
/// ```ignore
/// rapidbyte_sdk::connector_main!(source, MySource);
/// rapidbyte_sdk::embed_manifest!();
/// rapidbyte_sdk::embed_config_schema!(config::Config);
/// ```
#[macro_export]
macro_rules! embed_config_schema {
    ($config_type:ty) => {
        const __RB_SCHEMA_BYTES: &[u8] =
            <$config_type as $crate::ConfigSchema>::SCHEMA_JSON.as_bytes();

        #[link_section = "rapidbyte_config_schema_v1"]
        #[used]
        static __RAPIDBYTE_CONFIG_SCHEMA: [u8; __RB_SCHEMA_BYTES.len()] = {
            let src = __RB_SCHEMA_BYTES;
            let mut dst = [0u8; __RB_SCHEMA_BYTES.len()];
            let mut i = 0;
            while i < src.len() {
                dst[i] = src[i];
                i += 1;
            }
            dst
        };
    };
}
