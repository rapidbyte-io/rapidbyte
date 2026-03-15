//! CLI subcommand implementations.

use std::path::Path;

use anyhow::{Context, Result};

use rapidbyte_engine::config::types::PipelineConfig;
use rapidbyte_engine::config::{parser, validator};
use rapidbyte_secrets::SecretProviders;

pub mod agent;
pub mod check;
pub mod controller;
pub mod dev;
pub mod discover;
pub mod distributed_run;
pub mod list_runs;
pub mod plugin;
pub mod run;
pub mod scaffold;
pub mod status;
pub mod transport;
pub mod watch;

/// Parse and validate a pipeline YAML file.
///
/// # Errors
///
/// Returns `Err` if parsing or validation fails.
pub async fn load_pipeline(path: &Path, secrets: &SecretProviders) -> Result<PipelineConfig> {
    let config = parser::parse_pipeline(path, secrets)
        .await
        .with_context(|| format!("Failed to parse pipeline: {}", path.display()))?;
    validator::validate_pipeline(&config)?;
    Ok(config)
}
