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
