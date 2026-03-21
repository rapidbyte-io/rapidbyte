//! Source schema discovery subcommand (discover).

use std::path::Path;

use anyhow::Result;
use console::style;
use rapidbyte_types::discovery::DiscoveredStream;
use rapidbyte_types::wire::SyncMode;

use crate::Verbosity;

/// Execute the `discover` command: discover available streams from a source plugin.
///
/// # Errors
///
/// Returns `Err` if pipeline parsing, validation, or schema discovery fails.
#[allow(clippy::too_many_lines)]
pub async fn execute(
    pipeline_path: &Path,
    verbosity: Verbosity,
    registry_config: &rapidbyte_registry::RegistryConfig,
    secrets: &rapidbyte_secrets::SecretProviders,
) -> Result<()> {
    let config = super::load_pipeline(pipeline_path, secrets).await?;

    // Discover typed streams from the source plugin
    let ctx = rapidbyte_engine::build_discover_context(registry_config).await?;
    let discovered_streams = rapidbyte_engine::discover_plugin(
        &ctx,
        &config.source.use_ref,
        Some(&config.source.config),
    )
    .await
    .map_err(anyhow::Error::from)?;

    // Human-readable output to stderr (skip in quiet mode)
    if verbosity != Verbosity::Quiet {
        let count = discovered_streams.len();
        eprintln!(
            "{} Discovered {} stream{}",
            style("\u{2713}").green().bold(),
            count,
            if count == 1 { "" } else { "s" },
        );
        eprintln!();

        // Compute column widths for the table
        let name_width = discovered_streams
            .iter()
            .map(|s| s.name.len())
            .max()
            .unwrap_or(6)
            .max(6);
        let sync_width = discovered_streams
            .iter()
            .map(|s| sync_label(s.supported_sync_modes.as_slice()).len())
            .max()
            .unwrap_or(4)
            .max(4);
        let cursor_width = discovered_streams
            .iter()
            .map(|s| cursor_label(s).len())
            .max()
            .unwrap_or(6)
            .max(6);

        // Header
        eprintln!(
            "  {:<name_w$}   {:<sync_w$}   {:<cursor_w$}   Columns",
            style("Stream").bold(),
            style("Sync").bold(),
            style("Cursor").bold(),
            name_w = name_width,
            sync_w = sync_width,
            cursor_w = cursor_width,
        );
        let rule_len = name_width + sync_width + cursor_width + 18;
        eprintln!("  {}", "\u{2500}".repeat(rule_len));

        for stream in &discovered_streams {
            let sync = sync_label(&stream.supported_sync_modes);
            let cursor = cursor_label(stream);
            let cols = stream.schema.fields.len();

            eprintln!(
                "  {:<name_w$}   {:<sync_w$}   {:<cursor_w$}   {}",
                stream.name,
                sync,
                cursor,
                cols,
                name_w = name_width,
                sync_w = sync_width,
                cursor_w = cursor_width,
            );

            // Verbose mode: show full schema per stream
            if verbosity == Verbosity::Verbose || verbosity == Verbosity::Diagnostic {
                for col in &stream.schema.fields {
                    let nullable = if col.nullable {
                        style("NULL").dim()
                    } else {
                        style("NOT NULL").dim()
                    };
                    eprintln!(
                        "  {pad:>name_w$}     {:<24} {:<12} {}",
                        style(&col.name).cyan(),
                        col.arrow_type,
                        nullable,
                        pad = "",
                        name_w = name_width,
                    );
                }
            }
        }

        eprintln!();
    }

    // Machine-readable JSON on stdout (always emitted)
    let json = serde_json::to_string(&discovered_streams)?;
    println!("@@DISCOVERY_JSON@@{json}");

    Ok(())
}

/// Return a human-friendly label for the "best" sync mode supported.
fn sync_label(modes: &[SyncMode]) -> &'static str {
    if modes.contains(&SyncMode::Cdc) {
        "cdc"
    } else if modes.contains(&SyncMode::Incremental) {
        "incremental"
    } else {
        "full"
    }
}

fn cursor_label(stream: &DiscoveredStream) -> &str {
    stream
        .default_cursor_field
        .as_deref()
        .or(stream.schema.source_defined_cursor.as_deref())
        .unwrap_or("\u{2014}")
}
