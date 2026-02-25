use std::path::Path;

use anyhow::{Context, Result};

use rapidbyte_engine::orchestrator;
use rapidbyte_engine::config::parser;
use rapidbyte_engine::config::validator;
use rapidbyte_engine::execution::ExecutionOptions;

/// Execute the `run` command: parse, validate, and run a pipeline.
pub async fn execute(pipeline_path: &Path, dry_run: bool, limit: Option<u64>) -> Result<()> {
    // 1. Parse pipeline YAML
    let config = parser::parse_pipeline(pipeline_path)
        .with_context(|| format!("Failed to parse pipeline: {}", pipeline_path.display()))?;

    // 2. Validate
    validator::validate_pipeline(&config)?;

    // Build execution options (--limit implies --dry-run)
    let dry_run = dry_run || limit.is_some();
    let _options = ExecutionOptions { dry_run, limit };

    tracing::info!(
        pipeline = config.pipeline,
        source = config.source.use_ref,
        destination = config.destination.use_ref,
        streams = config.source.streams.len(),
        "Pipeline validated"
    );

    // 3. Run
    let result = orchestrator::run_pipeline(&config).await?;
    let counts = &result.counts;
    let source = &result.source;
    let dest = &result.dest;

    println!("Pipeline '{}' completed successfully.", config.pipeline);
    println!("  Records read:    {}", counts.records_read);
    println!("  Records written: {}", counts.records_written);
    println!("  Bytes read:      {}", format_bytes(counts.bytes_read));
    println!("  Bytes written:   {}", format_bytes(counts.bytes_written));
    if counts.records_read > 0 {
        let avg_row_bytes = counts.bytes_read / counts.records_read;
        println!("  Avg row size:    {} B", avg_row_bytes);
    }
    println!("  Duration:        {:.2}s", result.duration_secs);
    println!(
        "  Throughput:      {:.0} rows/sec, {:.2} MB/s",
        counts.records_read as f64 / result.duration_secs,
        counts.bytes_read as f64 / result.duration_secs / 1_048_576.0,
    );
    println!("  Source duration:  {:.2}s", source.duration_secs);
    println!("    Connect:       {:.3}s", source.connect_secs);
    println!("    Query:         {:.3}s", source.query_secs);
    println!("    Fetch:         {:.3}s", source.fetch_secs);
    println!("    Arrow encode:  {:.3}s", source.arrow_encode_secs);
    println!("  Dest duration:   {:.2}s", dest.duration_secs);
    println!("    VM setup:      {:.3}s", dest.vm_setup_secs);
    println!("    Recv loop:     {:.3}s", dest.recv_secs);
    println!("    Connect:       {:.3}s", dest.connect_secs);
    println!("    Flush:         {:.3}s", dest.flush_secs);
    println!("    Arrow decode:  {:.3}s", dest.arrow_decode_secs);
    println!("    Commit:        {:.3}s", dest.commit_secs);
    println!("    WASM overhead: {:.3}s", result.wasm_overhead_secs);
    println!(
        "  Host emit_batch: {:.3}s ({} calls)",
        source.emit_nanos as f64 / 1e9,
        source.emit_count
    );
    if source.compress_nanos > 0 {
        println!(
            "    Compression:   {:.3}s",
            source.compress_nanos as f64 / 1e9
        );
    }
    println!(
        "  Host next_batch: {:.3}s ({} calls)",
        dest.recv_nanos as f64 / 1e9,
        dest.recv_count
    );
    if dest.decompress_nanos > 0 {
        println!(
            "    Decompression: {:.3}s",
            dest.decompress_nanos as f64 / 1e9
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
    println!("  Source load:     {}ms", source.module_load_ms);
    println!("  Dest load:       {}ms", dest.module_load_ms);
    if result.retry_count > 0 {
        println!("  Retries:         {}", result.retry_count);
    }

    // Machine-readable JSON for benchmarking tools
    let json = serde_json::json!({
        "records_read": counts.records_read,
        "records_written": counts.records_written,
        "bytes_read": counts.bytes_read,
        "bytes_written": counts.bytes_written,
        "duration_secs": result.duration_secs,
        "source_duration_secs": source.duration_secs,
        "dest_duration_secs": dest.duration_secs,
        "dest_connect_secs": dest.connect_secs,
        "dest_flush_secs": dest.flush_secs,
        "dest_commit_secs": dest.commit_secs,
        "dest_vm_setup_secs": dest.vm_setup_secs,
        "dest_recv_secs": dest.recv_secs,
        "wasm_overhead_secs": result.wasm_overhead_secs,
        "source_connect_secs": source.connect_secs,
        "source_query_secs": source.query_secs,
        "source_fetch_secs": source.fetch_secs,
        "source_arrow_encode_secs": source.arrow_encode_secs,
        "dest_arrow_decode_secs": dest.arrow_decode_secs,
        "source_module_load_ms": source.module_load_ms,
        "dest_module_load_ms": dest.module_load_ms,
        "source_emit_nanos": source.emit_nanos,
        "source_compress_nanos": source.compress_nanos,
        "source_emit_count": source.emit_count,
        "dest_recv_nanos": dest.recv_nanos,
        "dest_decompress_nanos": dest.decompress_nanos,
        "dest_recv_count": dest.recv_count,
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
