//! Plugin resolver port trait.
//!
//! Abstracts plugin resolution (OCI registry, local filesystem) so the
//! orchestrator does not depend on specific resolution strategies.

use std::path::PathBuf;

use async_trait::async_trait;
use rapidbyte_types::manifest::PluginManifest;
use rapidbyte_types::wire::PluginKind;

use crate::domain::error::PipelineError;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A fully-resolved plugin ready for loading.
#[derive(Debug, Clone)]
pub struct ResolvedPlugin {
    /// Absolute path to the compiled WASM module.
    pub wasm_path: PathBuf,
    /// Plugin manifest extracted from the WASM binary (if present).
    pub manifest: Option<PluginManifest>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Port for resolving plugin references to loadable WASM modules.
///
/// Implementations handle OCI registry pulls, local cache lookups,
/// and filesystem fallback resolution.
#[async_trait]
pub trait PluginResolver: Send + Sync {
    /// Resolve a plugin reference to a loadable module.
    ///
    /// `plugin_ref` may be an OCI reference (e.g.
    /// `registry.example.com/source/postgres:1.2.0`) or a bare name
    /// resolved via filesystem search.
    ///
    /// `config_json` is the plugin config, passed for schema validation
    /// during resolution when a manifest is available.
    async fn resolve(
        &self,
        plugin_ref: &str,
        expected_kind: PluginKind,
        config_json: Option<&serde_json::Value>,
    ) -> Result<ResolvedPlugin, PipelineError>;
}
