//! Pipeline teardown subcommand.

use std::path::Path;

use anyhow::Result;
use console::style;

use crate::Verbosity;

/// Execute the `teardown` command: clean up resources provisioned by a pipeline.
///
/// Resolves source and destination plugins and invokes their teardown
/// lifecycle method to drop replication slots, staging tables, and other
/// resources created during prior runs.
///
/// # Errors
///
/// Returns `Err` if pipeline parsing, plugin resolution, or teardown fails.
pub async fn execute(
    pipeline_path: &Path,
    reason: &str,
    verbosity: Verbosity,
    registry_config: &rapidbyte_registry::RegistryConfig,
    secrets: &rapidbyte_secrets::SecretProviders,
) -> Result<()> {
    let config = super::load_pipeline(pipeline_path, secrets).await?;

    let ctx = rapidbyte_engine::build_lightweight_context(registry_config, &config).await?;
    let report = rapidbyte_engine::teardown_pipeline(&ctx, &config, reason)
        .await
        .map_err(|e| anyhow::anyhow!("teardown failed: {e}"))?;

    if verbosity != Verbosity::Quiet {
        if report.actions.is_empty() {
            eprintln!(
                "{} no teardown actions needed",
                style("\u{2713}").green().bold()
            );
        } else {
            for action in &report.actions {
                eprintln!("{} {}", style("\u{2713}").green().bold(), action);
            }
            eprintln!(
                "\n{} teardown complete ({} action(s))",
                style("\u{2713}").green().bold(),
                report.actions.len()
            );
        }
    }

    Ok(())
}
