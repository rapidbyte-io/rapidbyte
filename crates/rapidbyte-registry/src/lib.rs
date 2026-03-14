//! OCI-based plugin registry client, cache, and verification.
//!
//! | Module      | Responsibility |
//! |-------------|----------------|
//! | `cache`     | Local disk cache for plugin WASM artifacts |
//! | `client`    | OCI registry client wrapper (pull/push/list) |
//! | `reference` | Plugin reference parsing (`registry/repo:tag`) |
//! | `verify`    | SHA-256 digest computation and verification |

#![warn(clippy::pedantic)]

pub mod artifact;
pub mod cache;
pub mod client;
pub mod reference;
pub mod verify;

pub use artifact::{
    pack_artifact, unpack_artifact, PackedArtifact, PluginArtifactConfig, MEDIA_TYPE_CONFIG,
    MEDIA_TYPE_MANIFEST_LAYER, MEDIA_TYPE_WASM_LAYER,
};
pub use cache::{CacheEntry, PluginCache};
pub use client::{RegistryClient, RegistryConfig};
pub use reference::PluginRef;
