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
    apply: bool,
    dry_run: bool,
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

    let all_ok = source_ok
        && dest_ok
        && transform_configs_ok
        && transforms_ok
        && result.state.ok
        && prereqs_ok
        && schema_ok;

    if !all_ok {
        return Err(anyhow::anyhow!("pipeline validation failed"));
    }

    // --- Apply phase (optional) ---
    if apply {
        run_apply_phase(&ctx, &config, verbosity, dry_run).await?;
    }

    Ok(())
}

/// Run the apply phase for source and destination plugins.
///
/// Called when `--apply` is passed to `check` and all validations pass.
async fn run_apply_phase(
    ctx: &rapidbyte_engine::EngineContext,
    config: &rapidbyte_pipeline_config::PipelineConfig,
    verbosity: Verbosity,
    dry_run: bool,
) -> Result<()> {
    use rapidbyte_engine::domain::ports::runner::ApplyParams;
    use rapidbyte_types::wire::PluginKind;

    let mode = if dry_run { " (dry-run)" } else { "" };
    if verbosity != Verbosity::Quiet {
        eprintln!("\n{} apply phase{}", style("=>").cyan().bold(), mode);
    }

    let source_resolved = ctx
        .resolver
        .resolve(
            &config.source.use_ref,
            PluginKind::Source,
            Some(&config.source.config),
        )
        .await
        .map_err(|e| anyhow::anyhow!("source resolution failed: {e}"))?;

    let dest_resolved = ctx
        .resolver
        .resolve(
            &config.destination.use_ref,
            PluginKind::Destination,
            Some(&config.destination.config),
        )
        .await
        .map_err(|e| anyhow::anyhow!("destination resolution failed: {e}"))?;

    let (src_id, src_ver) = rapidbyte_engine::application::parse_plugin_id(&config.source.use_ref);
    let (dst_id, dst_ver) =
        rapidbyte_engine::application::parse_plugin_id(&config.destination.use_ref);

    let src_permissions = rapidbyte_engine::application::extract_permissions(&source_resolved);
    let dst_permissions = rapidbyte_engine::application::extract_permissions(&dest_resolved);

    let src_overrides = rapidbyte_engine::application::build_sandbox_overrides(
        config.source.permissions.as_ref(),
        config.source.limits.as_ref(),
        source_resolved.manifest.as_ref(),
    )
    .map_err(|e| anyhow::anyhow!("source sandbox overrides: {e}"))?;
    let dst_overrides = rapidbyte_engine::application::build_sandbox_overrides(
        config.destination.permissions.as_ref(),
        config.destination.limits.as_ref(),
        dest_resolved.manifest.as_ref(),
    )
    .map_err(|e| anyhow::anyhow!("destination sandbox overrides: {e}"))?;

    // Source apply
    let src_report = ctx
        .runner
        .apply(&ApplyParams {
            wasm_path: source_resolved.wasm_path.clone(),
            kind: PluginKind::Source,
            plugin_id: src_id,
            plugin_version: src_ver,
            config: config.source.config.clone(),
            streams: Vec::new(),
            dry_run,
            permissions: src_permissions,
            sandbox_overrides: src_overrides,
        })
        .await
        .map_err(|e| anyhow::anyhow!("source apply failed: {e}"))?;

    if verbosity != Verbosity::Quiet {
        print_apply_report("source", &src_report);
    }

    // Destination apply
    let dst_report = ctx
        .runner
        .apply(&ApplyParams {
            wasm_path: dest_resolved.wasm_path.clone(),
            kind: PluginKind::Destination,
            plugin_id: dst_id,
            plugin_version: dst_ver,
            config: config.destination.config.clone(),
            streams: Vec::new(),
            dry_run,
            permissions: dst_permissions,
            sandbox_overrides: dst_overrides,
        })
        .await
        .map_err(|e| anyhow::anyhow!("destination apply failed: {e}"))?;

    if verbosity != Verbosity::Quiet {
        print_apply_report("destination", &dst_report);
    }

    Ok(())
}

/// Print the actions from an apply report.
fn print_apply_report(role: &str, report: &rapidbyte_types::lifecycle::ApplyReport) {
    if report.actions.is_empty() {
        eprintln!(
            "  {} {:<20} no actions",
            style("\u{2713}").green().bold(),
            role
        );
    } else {
        for action in &report.actions {
            eprintln!(
                "  {} {} [{}]: {}",
                style("\u{2713}").green().bold(),
                role,
                action.stream_name,
                action.description
            );
            if let Some(ref ddl) = action.ddl_executed {
                eprintln!("    DDL: {ddl}");
            }
        }
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
