//! Secret management for pipeline configs.
//!
//! Provides a [`SecretProvider`] trait and a registry of providers
//! ([`SecretProviders`]) used by the engine's config parser to resolve
//! `${prefix:path#key}` references in pipeline YAML.
//!
//! Currently supported providers:
//! - `vault` — HashiCorp Vault KV v2 via [`VaultProvider`]

mod vault;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};

pub use vault::{VaultAuth, VaultConfig, VaultProvider};

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
    async fn read_secret(&self, path: &str, key: &str) -> Result<String>;
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

    /// Look up a provider by prefix.
    #[must_use]
    pub fn get(&self, prefix: &str) -> Option<&dyn SecretProvider> {
        self.providers.get(prefix).map(AsRef::as_ref)
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
    pub async fn resolve(&self, prefix: &str, path: &str, key: &str) -> Result<String> {
        let provider = self
            .providers
            .get(prefix)
            .with_context(|| format!("no secret provider registered for prefix '{prefix}'"))?;
        provider.read_secret(path, key).await
    }
}

impl Default for SecretProviders {
    fn default() -> Self {
        Self::new()
    }
}
