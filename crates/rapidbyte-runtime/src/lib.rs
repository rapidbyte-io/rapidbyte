//! Wasmtime component runtime for Rapidbyte connectors.
//!
//! Manages the WASI component model runtime, host import implementations,
//! connector resolution, and network/sandbox policies.

#![warn(clippy::pedantic)]

pub mod error;
