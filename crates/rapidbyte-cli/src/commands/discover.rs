//! Source schema discovery subcommand (discover).

use std::path::Path;

use anyhow::Result;
use console::style;
use rapidbyte_engine::orchestrator;
use rapidbyte_types::wire::SyncMode;

use crate::Verbosity;

/// Execute the `discover` command: discover available streams from a source plugin.
///
/// # Errors
///
/// Returns `Err` if pipeline parsing, validation, or schema discovery fails.
pub async fn execute(pipeline_path: &Path, verbosity: Verbosity) -> Result<()> {
    let config = super::load_pipeline(pipeline_path)?;

    // Discover catalog from source plugin
    let catalog =
        orchestrator::discover_plugin(&config.source.use_ref, &config.source.config).await?;

    // Human-readable output to stderr (skip in quiet mode)
    if verbosity != Verbosity::Quiet {
        let count = catalog.streams.len();
        eprintln!(
            "{} Discovered {} stream{}",
            style("\u{2713}").green().bold(),
            count,
            if count == 1 { "" } else { "s" },
        );
        eprintln!();

        // Compute column widths for the table
        let name_width = catalog
            .streams
            .iter()
            .map(|s| s.name.len())
            .max()
            .unwrap_or(6)
            .max(6);
        let sync_width = catalog
            .streams
            .iter()
            .map(|s| sync_label(s.supported_sync_modes.as_slice()).len())
            .max()
            .unwrap_or(4)
            .max(4);
        let cursor_width = catalog
            .streams
            .iter()
            .map(|s| {
                s.source_defined_cursor
                    .as_deref()
                    .unwrap_or("\u{2014}")
                    .len()
            })
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

        for stream in &catalog.streams {
            let sync = sync_label(&stream.supported_sync_modes);
            let cursor = stream
                .source_defined_cursor
                .as_deref()
                .unwrap_or("\u{2014}");
            let cols = stream.schema.len();

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
            if (verbosity == Verbosity::Verbose || verbosity == Verbosity::Diagnostic)
                && !stream.schema.is_empty()
            {
                for col in &stream.schema {
                    let nullable = if col.nullable {
                        style("NULL").dim()
                    } else {
                        style("NOT NULL").dim()
                    };
                    eprintln!(
                        "  {pad:>name_w$}     {:<24} {:<12} {}",
                        style(&col.name).cyan(),
                        format!("{}", col.data_type),
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
    let json = serde_json::to_string(&catalog)?;
    println!("@@CATALOG_JSON@@{json}");

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
