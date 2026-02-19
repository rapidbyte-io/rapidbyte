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
    println!("  Bytes written:   {}", format_bytes(result.bytes_written));
    if result.records_read > 0 {
        let avg_row_bytes = result.bytes_read / result.records_read;
        println!("  Avg row size:    {} B", avg_row_bytes);
    }
    println!("  Duration:        {:.2}s", result.duration_secs);
    println!(
        "  Throughput:      {:.0} rows/sec, {:.2} MB/s",
        result.records_read as f64 / result.duration_secs,
        result.bytes_read as f64 / result.duration_secs / 1_048_576.0,
    );
    println!("  Source duration:  {:.2}s", result.source_duration_secs);
    println!("    Connect:       {:.3}s", result.source_connect_secs);
    println!("    Query:         {:.3}s", result.source_query_secs);
    println!("    Fetch:         {:.3}s", result.source_fetch_secs);
    println!("    Arrow encode:  {:.3}s", result.source_arrow_encode_secs);
    println!("  Dest duration:   {:.2}s", result.dest_duration_secs);
    println!("    VM setup:      {:.3}s", result.dest_vm_setup_secs);
    println!("    Recv loop:     {:.3}s", result.dest_recv_secs);
    println!("    Connect:       {:.3}s", result.dest_connect_secs);
    println!("    Flush:         {:.3}s", result.dest_flush_secs);
    println!("    Arrow decode:  {:.3}s", result.dest_arrow_decode_secs);
    println!("    Commit:        {:.3}s", result.dest_commit_secs);
    println!("    WASM overhead: {:.3}s", result.wasm_overhead_secs);
    println!(
        "  Host emit_batch: {:.3}s ({} calls)",
        result.source_emit_nanos as f64 / 1e9,
        result.source_emit_count
    );
    if result.source_compress_nanos > 0 {
        println!(
            "    Compression:   {:.3}s",
            result.source_compress_nanos as f64 / 1e9
        );
    }
    println!(
        "  Host next_batch: {:.3}s ({} calls)",
        result.dest_recv_nanos as f64 / 1e9,
        result.dest_recv_count
    );
    if result.dest_decompress_nanos > 0 {
        println!(
            "    Decompression: {:.3}s",
            result.dest_decompress_nanos as f64 / 1e9
        );
    }
    if result.transform_count > 0 {
        println!(
            "  Transforms:      {} stage(s), {:.2}s total",
            result.transform_count, result.transform_duration_secs,
        );
        for (i, ms) in result.transform_module_load_ms.iter().enumerate() {
            println!("    Transform[{}] load: {}ms", i, ms);
        }
    }
    println!("  Source load:     {}ms", result.source_module_load_ms);
    println!("  Dest load:       {}ms", result.dest_module_load_ms);
    if result.retry_count > 0 {
        println!("  Retries:         {}", result.retry_count);
    }

    // Machine-readable JSON for benchmarking tools
    let json = serde_json::json!({
        "records_read": result.records_read,
        "records_written": result.records_written,
        "bytes_read": result.bytes_read,
        "bytes_written": result.bytes_written,
        "duration_secs": result.duration_secs,
        "source_duration_secs": result.source_duration_secs,
        "dest_duration_secs": result.dest_duration_secs,
        "dest_connect_secs": result.dest_connect_secs,
        "dest_flush_secs": result.dest_flush_secs,
        "dest_commit_secs": result.dest_commit_secs,
        "dest_vm_setup_secs": result.dest_vm_setup_secs,
        "dest_recv_secs": result.dest_recv_secs,
        "wasm_overhead_secs": result.wasm_overhead_secs,
        "source_connect_secs": result.source_connect_secs,
        "source_query_secs": result.source_query_secs,
        "source_fetch_secs": result.source_fetch_secs,
        "source_arrow_encode_secs": result.source_arrow_encode_secs,
        "dest_arrow_decode_secs": result.dest_arrow_decode_secs,
        "source_module_load_ms": result.source_module_load_ms,
        "dest_module_load_ms": result.dest_module_load_ms,
        "source_emit_nanos": result.source_emit_nanos,
        "source_compress_nanos": result.source_compress_nanos,
        "source_emit_count": result.source_emit_count,
        "dest_recv_nanos": result.dest_recv_nanos,
        "dest_decompress_nanos": result.dest_decompress_nanos,
        "dest_recv_count": result.dest_recv_count,
        "transform_count": result.transform_count,
        "transform_duration_secs": result.transform_duration_secs,
        "transform_module_load_ms": result.transform_module_load_ms,
        "retry_count": result.retry_count,
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
