use std::path::Path;

use anyhow::{Context, Result};

use rapidbyte_engine::orchestrator;
use rapidbyte_engine::config::parser;
use rapidbyte_engine::config::validator;

/// Execute the `discover` command: discover available streams from a source connector.
pub async fn execute(pipeline_path: &Path) -> Result<()> {
    // 1. Parse pipeline YAML
    let config = parser::parse_pipeline(pipeline_path)
        .with_context(|| format!("Failed to parse pipeline: {}", pipeline_path.display()))?;

    // 2. Validate pipeline structure
    validator::validate_pipeline(&config)?;

    // 3. Discover catalog from source connector
    let catalog =
        orchestrator::discover_connector(&config.source.use_ref, &config.source.config).await?;

    // 4. Print human-readable catalog
    println!("Discovered {} stream(s):\n", catalog.streams.len());

    for stream in &catalog.streams {
        println!("Stream: {}", stream.name);

        let modes: Vec<String> = stream
            .supported_sync_modes
            .iter()
            .map(|m| format!("{:?}", m))
            .collect();
        println!("  Sync modes:  {}", modes.join(", "));

        if let Some(ref cursor) = stream.source_defined_cursor {
            println!("  Cursor:      {}", cursor);
        }

        if let Some(ref pk) = stream.source_defined_primary_key {
            if !pk.is_empty() {
                println!("  Primary key: {}", pk.join(", "));
            }
        }

        if !stream.schema.is_empty() {
            println!("  Columns:");
            for col in &stream.schema {
                let nullable = if col.nullable { "NULL" } else { "NOT NULL" };
                println!("    - {} ({}, {})", col.name, col.data_type, nullable);
            }
        }

        println!();
    }

    // 5. Print machine-readable JSON
    let json = serde_json::to_string(&catalog)?;
    println!("@@CATALOG_JSON@@{}", json);

    Ok(())
}
