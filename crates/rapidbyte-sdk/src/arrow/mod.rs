//! Arrow integration for Rapidbyte connectors.
//!
//! This module is the only way connectors interact with Arrow. It provides:
//! - IPC encoding/decoding (`ipc`)
//! - Schema construction from protocol types (`schema`)
//! - Type conversion between protocol and Arrow types (`types`)

pub mod ipc;
pub mod schema;
pub mod types;

pub use ipc::{decode_ipc, encode_ipc};
pub use schema::build_arrow_schema;
pub use types::arrow_data_type;

/// Re-export the arrow crate for connectors that need array builder types.
///
/// Connectors should use this rather than depending on `arrow` directly.
/// This ensures all connectors use the same arrow version.
pub use arrow as arrow_reexport;
