//! CLI subcommand implementations.

use std::path::Path;

use anyhow::{Context, Result};

use rapidbyte_pipeline_config::types::PipelineConfig;
use rapidbyte_pipeline_config::{parser, validator};
use rapidbyte_secrets::SecretProviders;

pub mod agent;
pub mod check;
pub mod controller;
pub mod dev;
pub mod discover;
pub mod list_runs;
pub mod plugin;
pub mod scaffold;
pub mod status;
pub mod sync;
pub mod teardown;
pub mod transport;
pub mod watch;

/// Parse and validate a pipeline YAML file.
///
/// # Errors
///
/// Returns `Err` if parsing or validation fails.
pub async fn load_pipeline(path: &Path, secrets: &SecretProviders) -> Result<PipelineConfig> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read pipeline file: {}", path.display()))?;
    let config = parser::parse_pipeline(&content, secrets)
        .await
        .with_context(|| format!("Failed to parse pipeline: {}", path.display()))?;
    validator::validate_pipeline(&config)?;
    Ok(config)
}
