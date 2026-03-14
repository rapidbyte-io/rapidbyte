//! OCI-based plugin registry client, cache, and verification.
//!
//! | Module      | Responsibility |
//! |-------------|----------------|
//! | `reference` | Plugin reference parsing (`registry/repo:tag`) |
//! | `verify`    | SHA-256 digest computation and verification |

#![warn(clippy::pedantic)]

pub mod reference;
pub mod verify;

pub use reference::PluginRef;
