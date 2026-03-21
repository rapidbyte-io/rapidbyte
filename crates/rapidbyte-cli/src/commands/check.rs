//! Pipeline validation subcommand (check).

use std::path::Path;

use anyhow::Result;
use console::style;
use rapidbyte_engine::CheckStatus;
use rapidbyte_types::validation::{PrerequisitesReport, ValidationReport, ValidationStatus};

use crate::Verbosity;

/// Execute the `check` command: validate pipeline config and plugin connectivity.
///
/// # Errors
///
/// Returns `Err` if pipeline parsing, validation, or connectivity check fails.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    pipeline_path: &Path,
    verbosity: Verbosity,
    registry_config: &rapidbyte_registry::RegistryConfig,
    secrets: &rapidbyte_secrets::SecretProviders,
) -> Result<()> {
    let config = super::load_pipeline(pipeline_path, secrets).await?;

    let ctx = rapidbyte_engine::build_lightweight_context(registry_config, &config).await?;
    let result = rapidbyte_engine::check_pipeline(&ctx, &config).await?;

    if verbosity != Verbosity::Quiet {
        if let Some(outcome) = &result.source_manifest {
            print_check_item("source manifest", outcome);
        }
        if let Some(outcome) = &result.destination_manifest {
            print_check_item("dest manifest", outcome);
        }
        if let Some(outcome) = &result.source_config {
            print_check_item("source config", outcome);
        }
        if let Some(outcome) = &result.destination_config {
            print_check_item("dest config", outcome);
        }
        for (i, outcome) in result.transform_configs.iter().enumerate() {
            let label = config.transforms.get(i).map_or_else(
                || format!("transform config[{i}]"),
                |t| format!("transform config ({})", t.use_ref),
            );
            print_check_item(&label, outcome);
        }
        print_validation(&config.source.use_ref, &result.source_validation);
        print_validation(&config.destination.use_ref, &result.destination_validation);

        for (i, tv) in result.transform_validations.iter().enumerate() {
            let label = config
                .transforms
                .get(i)
                .map_or_else(|| format!("transform[{i}]"), |t| t.use_ref.clone());
            print_validation(&label, tv);
        }

        print_check_item("state", &result.state);

        // Prerequisites
        if let Some(ref prereqs) = result.source_prerequisites {
            print_prerequisites("source prerequisites", prereqs);
        }
        if let Some(ref prereqs) = result.destination_prerequisites {
            print_prerequisites("dest prerequisites", prereqs);
        }

        // Schema negotiation
        for neg in &result.schema_negotiation {
            if neg.passed && neg.warnings.is_empty() {
                eprintln!(
                    "{} schema [{}]     valid",
                    style("\u{2713}").green().bold(),
                    neg.stream_name
                );
            } else if neg.passed {
                eprintln!(
                    "{} schema [{}]     warning",
                    style("!").yellow().bold(),
                    neg.stream_name
                );
                for w in &neg.warnings {
                    eprintln!("  warning: {w}");
                }
            } else {
                eprintln!(
                    "{} schema [{}]     failed",
                    style("\u{2717}").red().bold(),
                    neg.stream_name
                );
                for e in &neg.errors {
                    eprintln!("  {e}");
                }
                for w in &neg.warnings {
                    eprintln!("  warning: {w}");
                }
            }
        }
    }

    // Return error if anything failed
    let source_ok = check_item_passes(result.source_manifest.as_ref())
        && check_item_passes(result.source_config.as_ref())
        && validation_passes(&result.source_validation);
    let dest_ok = check_item_passes(result.destination_manifest.as_ref())
        && check_item_passes(result.destination_config.as_ref())
        && validation_passes(&result.destination_validation);
    let transform_configs_ok = result.transform_configs.iter().all(|item| item.ok);
    let transforms_ok = result.transform_validations.iter().all(validation_passes);
    let prereqs_ok = result
        .source_prerequisites
        .as_ref()
        .is_none_or(|p| p.passed)
        && result
            .destination_prerequisites
            .as_ref()
            .is_none_or(|p| p.passed);
    let schema_ok = result.schema_negotiation.iter().all(|n| n.passed);

    if source_ok
        && dest_ok
        && transform_configs_ok
        && transforms_ok
        && result.state.ok
        && prereqs_ok
        && schema_ok
    {
        Ok(())
    } else {
        Err(anyhow::anyhow!("pipeline validation failed"))
    }
}

fn print_check_item(label: &str, result: &CheckStatus) {
    if result.ok {
        eprintln!("{} {:<20} valid", style("\u{2713}").green().bold(), label);
    } else {
        eprintln!("{} {:<20} failed", style("\u{2717}").red().bold(), label);
        if !result.message.is_empty() {
            eprintln!("  {}", result.message);
        }
    }
}

fn print_validation(label: &str, result: &ValidationReport) {
    match result.status {
        ValidationStatus::Success => {
            eprintln!("{} {:<20} valid", style("\u{2713}").green().bold(), label);
        }
        ValidationStatus::Failed => {
            eprintln!("{} {:<20} failed", style("\u{2717}").red().bold(), label);
            print_validation_details(result);
        }
        ValidationStatus::Warning => {
            eprintln!("{} {:<20} warning", style("!").yellow().bold(), label);
            print_validation_details(result);
        }
    }
}

fn validation_passes(result: &ValidationReport) -> bool {
    matches!(
        result.status,
        ValidationStatus::Success | ValidationStatus::Warning
    )
}

fn check_item_passes(result: Option<&CheckStatus>) -> bool {
    result.is_none_or(|item| item.ok)
}

fn print_validation_details(result: &ValidationReport) {
    if !result.message.is_empty() {
        eprintln!("  {}", result.message);
    }
    for warning in &result.warnings {
        eprintln!("  warning: {warning}");
    }
}

fn print_prerequisites(label: &str, report: &PrerequisitesReport) {
    if report.passed {
        eprintln!("{} {:<20} passed", style("\u{2713}").green().bold(), label);
    } else {
        eprintln!("{} {:<20} failed", style("\u{2717}").red().bold(), label);
    }
    for check in &report.checks {
        if check.passed {
            eprintln!("  {} {}", style("\u{2713}").green(), check.message);
        } else {
            eprintln!("  {} {}", style("\u{2717}").red(), check.message);
            if let Some(hint) = &check.fix_hint {
                eprintln!("    fix: {hint}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warnings_count_as_non_fatal_validation() {
        let mut result = ValidationReport::success("Validation not implemented");
        result.status = ValidationStatus::Warning;
        result.warnings = vec!["live connectivity check missing".to_string()];

        assert!(validation_passes(&result));
    }

    #[test]
    fn failures_remain_fatal_validation() {
        let mut result = ValidationReport::failed("connection refused");
        result.warnings = vec!["retry disabled".to_string()];

        assert!(!validation_passes(&result));
    }

    #[test]
    fn failed_check_item_is_fatal() {
        let result = CheckStatus {
            ok: false,
            message: "invalid schema".to_string(),
        };

        assert!(!check_item_passes(Some(&result)));
    }
}
