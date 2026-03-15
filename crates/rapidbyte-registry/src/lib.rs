//! OCI-based plugin registry client, cache, and verification.
//!
//! | Module      | Responsibility |
//! |-------------|----------------|
//! | `artifact`  | OCI artifact pack/unpack helpers |
//! | `cache`     | Local disk cache for plugin WASM artifacts |
//! | `client`    | OCI registry client wrapper (pull/push/list) |
//! | `index`     | Searchable plugin index data types |
//! | `reference` | Plugin reference parsing (`registry/repo:tag`) |
//! | `signing`   | Ed25519 signing and signature verification |
//! | `trust`     | Plugin signature trust policy |
//! | `verify`    | SHA-256 digest computation and verification |

#![warn(clippy::pedantic)]

pub mod artifact;
pub mod cache;
pub mod client;
pub mod index;
pub mod reference;
pub mod signing;
pub mod trust;
pub mod verify;

pub use artifact::{
    pack_artifact, pack_artifact_signed, unpack_artifact, PackedArtifact, PluginArtifactConfig,
    UnpackedArtifact, MEDIA_TYPE_CONFIG, MEDIA_TYPE_MANIFEST_LAYER, MEDIA_TYPE_WASM_LAYER,
};
pub use cache::{CacheEntry, PluginCache};
pub use client::{RegistryClient, RegistryConfig};
pub use index::{PluginIndex, PluginIndexEntry, INDEX_REPOSITORY, INDEX_TAG};
pub use reference::{normalize_registry_url, normalize_registry_url_option, PluginRef};
pub use trust::{parse_trusted_keys, verify_artifact_trust, TrustPolicy};
