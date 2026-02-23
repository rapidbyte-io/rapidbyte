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
pub mod context;
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

/// Re-export the `#[connector]` attribute macro.
#[cfg(feature = "runtime")]
pub use rapidbyte_sdk_macros::connector;

