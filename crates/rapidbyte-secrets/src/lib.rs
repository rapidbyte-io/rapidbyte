#![warn(clippy::pedantic)]

//! Secret management for pipeline configs.
//!
//! Provides a [`SecretProvider`] trait and a registry of providers
//! ([`SecretProviders`]) used by the engine's config parser to resolve
//! `${prefix:path#key}` references in pipeline YAML.
//!
//! Currently supported providers:
//! - `vault` — `HashiCorp` Vault KV v2 via [`VaultProvider`]

mod vault;

use std::collections::HashMap;
use std::sync::Arc;

pub use vault::{VaultAuth, VaultConfig, VaultProvider};

/// Typed error for secret resolution failures.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("no secret provider registered for prefix '{prefix}'")]
    UnknownProvider { prefix: String },

    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    AuthFailed(String),

    #[error("{0}")]
    InvalidPath(String),

    #[error("{0}")]
    Unavailable(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl SecretError {
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}

/// A provider that can resolve secret references.
///
/// Each provider handles a specific prefix (e.g. `vault`, `aws`, `gcp`)
/// and knows how to interpret the path format for its backend.
#[async_trait::async_trait]
pub trait SecretProvider: Send + Sync {
    /// Read a secret value.
    ///
    /// `path` is the provider-specific path (e.g. `secret/postgres` for Vault).
    /// `key` is the field name within the secret (e.g. `password`).
    async fn read_secret(&self, path: &str, key: &str) -> Result<String, SecretError>;
}

/// Registry of named secret providers.
///
/// Maps prefixes (e.g. `"vault"`) to provider implementations. Used by
/// the parser to route `${prefix:path#key}` references to the correct
/// backend.
pub struct SecretProviders {
    providers: HashMap<String, Arc<dyn SecretProvider>>,
}

impl SecretProviders {
    /// Create an empty registry with no providers.
    #[must_use]
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
        }
    }

    /// Register a provider under a given prefix.
    pub fn register(&mut self, prefix: &str, provider: Arc<dyn SecretProvider>) {
        self.providers.insert(prefix.to_owned(), provider);
    }

    /// Returns `true` if no providers are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// Resolve a single secret reference.
    ///
    /// `prefix` identifies the provider, `path` and `key` are
    /// provider-specific.
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix has no registered provider or
    /// the provider fails to read the secret.
    pub async fn resolve(
        &self,
        prefix: &str,
        path: &str,
        key: &str,
    ) -> Result<String, SecretError> {
        let provider = self
            .providers
            .get(prefix)
            .ok_or_else(|| SecretError::UnknownProvider {
                prefix: prefix.to_owned(),
            })?;
        provider.read_secret(path, key).await
    }
}

impl Default for SecretProviders {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_transient_only_for_unavailable() {
        assert!(SecretError::Unavailable("timeout".into()).is_transient());

        assert!(!SecretError::UnknownProvider {
            prefix: "vault".into()
        }
        .is_transient());
        assert!(!SecretError::NotFound("missing".into()).is_transient());
        assert!(!SecretError::AuthFailed("denied".into()).is_transient());
        assert!(!SecretError::InvalidPath("bad".into()).is_transient());
        assert!(!SecretError::Other(anyhow::anyhow!("other")).is_transient());
    }

    #[test]
    fn display_formatting() {
        assert_eq!(
            SecretError::UnknownProvider {
                prefix: "aws".into()
            }
            .to_string(),
            "no secret provider registered for prefix 'aws'"
        );
        assert_eq!(
            SecretError::NotFound("key not found".into()).to_string(),
            "key not found"
        );
        assert_eq!(
            SecretError::AuthFailed("permission denied".into()).to_string(),
            "permission denied"
        );
        assert_eq!(
            SecretError::InvalidPath("bad path".into()).to_string(),
            "bad path"
        );
        assert_eq!(
            SecretError::Unavailable("connection refused".into()).to_string(),
            "connection refused"
        );
    }
}
