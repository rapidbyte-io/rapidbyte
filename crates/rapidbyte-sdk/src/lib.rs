//! Rapidbyte Connector SDK.
//!
//! Provides traits, protocol types, and host-import wrappers for building
//! WASI-based data pipeline connectors.

pub mod connector;
pub mod host_ffi;
pub mod host_tcp;
pub mod prelude;

pub use rapidbyte_types::errors;
pub use rapidbyte_types::manifest;
pub use rapidbyte_types::protocol;

pub use wit_bindgen;
