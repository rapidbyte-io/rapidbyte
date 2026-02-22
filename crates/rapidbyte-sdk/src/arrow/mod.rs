//! Arrow integration for Rapidbyte connectors.
//!
//! This module is the only way connectors interact with Arrow. It provides:
//! - IPC encoding/decoding (`ipc`)
//! - Schema construction from protocol types (`schema`)
//! - Type conversion between protocol and Arrow types (`types`)

pub mod ipc;

pub use ipc::{decode_ipc, encode_ipc};
