//! Pipeline validation subcommand (check).

use std::path::Path;

use anyhow::Result;
use console::style;
use rapidbyte_types::error::{ValidationResult, ValidationStatus};

use rapidbyte_engine::orchestrator;

use crate::Verbosity;

/// Execute the `check` command: validate pipeline config and connector connectivity.
///
/// # Errors
///
/// Returns `Err` if pipeline parsing, validation, or connectivity check fails.
pub async fn execute(pipeline_path: &Path, verbosity: Verbosity) -> Result<()> {
    let config = super::load_pipeline(pipeline_path)?;

    let result = orchestrator::check_pipeline(&config).await?;

    if verbosity != Verbosity::Quiet {
        print_validation(&config.source.use_ref, &result.source_validation);
        print_validation(&config.destination.use_ref, &result.destination_validation);

        for (i, tv) in result.transform_validations.iter().enumerate() {
            let label = config
                .transforms
                .get(i)
                .map_or_else(|| format!("transform[{i}]"), |t| t.use_ref.clone());
            print_validation(&label, tv);
        }

        if result.state_ok {
            eprintln!("{} {:<20} valid", style("\u{2713}").green().bold(), "state");
        } else {
            eprintln!("{} {:<20} failed", style("\u{2717}").red().bold(), "state");
        }
    }

    // Return error if anything failed
    let source_ok = result.source_validation.status == ValidationStatus::Success;
    let dest_ok = result.destination_validation.status == ValidationStatus::Success;
    let transforms_ok = result
        .transform_validations
        .iter()
        .all(|v| v.status == ValidationStatus::Success);

    if source_ok && dest_ok && transforms_ok && result.state_ok {
        Ok(())
    } else {
        // Validation details already printed above — return a silent error.
        std::process::exit(1);
    }
}

fn print_validation(label: &str, result: &ValidationResult) {
    match result.status {
        ValidationStatus::Success => {
            eprintln!("{} {:<20} valid", style("\u{2713}").green().bold(), label);
        }
        ValidationStatus::Failed => {
            eprintln!("{} {:<20} failed", style("\u{2717}").red().bold(), label);
            if !result.message.is_empty() {
                eprintln!("  {}", result.message);
            }
        }
        ValidationStatus::Warning => {
            eprintln!("{} {:<20} warning", style("!").yellow().bold(), label);
            if !result.message.is_empty() {
                eprintln!("  {}", result.message);
            }
        }
    }
}
