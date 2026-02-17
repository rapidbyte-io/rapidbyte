use std::path::Path;

use anyhow::{Context, Result};

use rapidbyte_core::engine::orchestrator;
use rapidbyte_core::pipeline::parser;
use rapidbyte_core::pipeline::validator;

/// Execute the `run` command: parse, validate, and run a pipeline.
pub async fn execute(pipeline_path: &Path) -> Result<()> {
    // 1. Parse pipeline YAML
    let config = parser::parse_pipeline(pipeline_path)
        .with_context(|| format!("Failed to parse pipeline: {}", pipeline_path.display()))?;

    // 2. Validate
    validator::validate_pipeline(&config)?;

    tracing::info!(
        pipeline = config.pipeline,
        source = config.source.use_ref,
        destination = config.destination.use_ref,
        streams = config.source.streams.len(),
        "Pipeline validated"
    );

    // 3. Run
    let result = orchestrator::run_pipeline(&config).await?;

    println!("Pipeline '{}' completed successfully.", config.pipeline);
    println!("  Records read:    {}", result.records_read);
    println!("  Records written: {}", result.records_written);
    println!("  Bytes read:      {}", format_bytes(result.bytes_read));
    println!("  Duration:        {:.2}s", result.duration_secs);
    println!("  Source duration:  {:.2}s", result.source_duration_secs);
    println!("  Dest duration:   {:.2}s", result.dest_duration_secs);
    println!("  Source load:     {}ms", result.source_module_load_ms);
    println!("  Dest load:       {}ms", result.dest_module_load_ms);

    // Machine-readable JSON for benchmarking tools
    let json = serde_json::json!({
        "records_read": result.records_read,
        "records_written": result.records_written,
        "bytes_read": result.bytes_read,
        "duration_secs": result.duration_secs,
        "source_duration_secs": result.source_duration_secs,
        "dest_duration_secs": result.dest_duration_secs,
        "source_module_load_ms": result.source_module_load_ms,
        "dest_module_load_ms": result.dest_module_load_ms,
    });
    println!("@@BENCH_JSON@@{}", json);

    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.2} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}
