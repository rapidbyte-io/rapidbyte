//! Wasmtime component runtime for Rapidbyte connectors.
//!
//! Manages the WASI component model runtime, host import implementations,
//! connector resolution, and network/sandbox policies.

#![warn(clippy::pedantic)]

pub mod acl;
pub mod compression;
pub mod connector;
pub mod engine;
pub mod error;
#[allow(dead_code, clippy::needless_pass_by_value)]
pub mod host_state;
pub mod sandbox;
pub mod socket;
