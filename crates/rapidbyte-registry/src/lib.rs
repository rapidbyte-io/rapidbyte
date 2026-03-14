//! OCI-based plugin registry client, cache, and verification.
//!
//! | Module      | Responsibility |
//! |-------------|----------------|
//! | `cache`     | Local disk cache for plugin WASM artifacts |
//! | `client`    | OCI registry client wrapper (pull/push/list) |
//! | `reference` | Plugin reference parsing (`registry/repo:tag`) |
//! | `verify`    | SHA-256 digest computation and verification |

#![warn(clippy::pedantic)]

pub mod cache;
pub mod client;
pub mod reference;
pub mod verify;

pub use cache::{CacheEntry, PluginCache};
pub use client::{RegistryClient, RegistryConfig};
pub use reference::PluginRef;
