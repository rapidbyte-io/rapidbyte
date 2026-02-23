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
/// Requires a `build.rs` that calls `ManifestEmitter::emit()`.
#[macro_export]
macro_rules! embed_manifest {
    () => {
        include!(concat!(env!("OUT_DIR"), "/rapidbyte_manifest_embed.rs"));
    };
}
