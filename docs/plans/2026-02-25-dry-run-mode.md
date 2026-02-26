# Dry Run Mode (`--dry-run --limit N`) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `--dry-run --limit N` to `rapidbyte run` so users can preview pipeline output (source → transforms → stdout) without writing to the destination.

**Architecture:** CLI passes `ExecutionOptions` to the orchestrator. When dry-run is active, the orchestrator skips destination WASM loading and spawns a lightweight frame collector instead of the destination runner. The collector decodes IPC frames into `RecordBatch`es, enforces the row limit, and returns them. The CLI pretty-prints the results as an ASCII table. The source connector also respects `max_records` in `StreamLimits` to avoid fetching unnecessary data.

**Tech Stack:** clap (CLI), arrow `RecordBatch` + `pretty::pretty_format_batches` (display), tokio mpsc (frame collection), existing Arrow IPC decode and compression utilities.

---

## Task 1: Add `max_records` to `StreamLimits`

**Files:**
- Modify: `crates/rapidbyte-types/src/stream.rs:106-151`

**Step 1: Write the failing test**

Add to the existing `mod tests` block at the bottom of `stream.rs`:

```rust
#[test]
fn stream_limits_max_records_serde_roundtrip() {
    let limits = StreamLimits {
        max_records: Some(500),
        ..StreamLimits::default()
    };
    let json = serde_json::to_string(&limits).unwrap();
    let back: StreamLimits = serde_json::from_str(&json).unwrap();
    assert_eq!(back.max_records, Some(500));
}

#[test]
fn stream_limits_max_records_default_is_none() {
    let limits = StreamLimits::default();
    assert_eq!(limits.max_records, None);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-types stream_limits_max_records`
Expected: FAIL — `max_records` field doesn't exist yet.

**Step 3: Add `max_records` field to `StreamLimits`**

Add the field to the struct (after `checkpoint_interval_seconds`):

```rust
/// Maximum number of records to read (None = unlimited).
/// Used by dry-run mode to cap source reads.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub max_records: Option<u64>,
```

Update the `Default` impl to include:

```rust
max_records: None,
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-types`
Expected: ALL PASS. Existing `stream_context_roundtrip` test still passes because `max_records` defaults to `None` and is skipped during serialization.

**Step 5: Commit**

```
feat(types): add max_records to StreamLimits for dry-run row limiting
```

---

## Task 2: Add `ExecutionOptions` and `DryRunResult` types

**Files:**
- Create: `crates/rapidbyte-engine/src/execution.rs`
- Modify: `crates/rapidbyte-engine/src/lib.rs:22-38`

**Step 1: Write the failing test**

Create `crates/rapidbyte-engine/src/execution.rs`:

```rust
//! Execution mode types for pipeline runs.

use arrow::record_batch::RecordBatch;

use crate::result::{PipelineResult, SourceTiming};

/// Runtime execution options (not part of pipeline YAML config).
#[derive(Debug, Clone, Default)]
pub struct ExecutionOptions {
    /// Skip destination, print output to stdout.
    pub dry_run: bool,
    /// Maximum rows to read per stream (only used with dry_run).
    pub limit: Option<u64>,
}

/// Result of a single stream in dry-run mode.
#[derive(Debug)]
pub struct DryRunStreamResult {
    pub stream_name: String,
    pub batches: Vec<RecordBatch>,
    pub total_rows: u64,
    pub total_bytes: u64,
}

/// Result of a dry-run pipeline execution.
#[derive(Debug)]
pub struct DryRunResult {
    pub streams: Vec<DryRunStreamResult>,
    pub source: SourceTiming,
    pub transform_count: usize,
    pub transform_duration_secs: f64,
    pub duration_secs: f64,
}

/// Either a normal pipeline result or a dry-run result.
#[derive(Debug)]
pub enum PipelineOutcome {
    Run(PipelineResult),
    DryRun(DryRunResult),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_options_default_is_normal_mode() {
        let opts = ExecutionOptions::default();
        assert!(!opts.dry_run);
        assert!(opts.limit.is_none());
    }

    #[test]
    fn dry_run_stream_result_holds_batches() {
        let result = DryRunStreamResult {
            stream_name: "public.users".into(),
            batches: vec![],
            total_rows: 0,
            total_bytes: 0,
        };
        assert_eq!(result.stream_name, "public.users");
    }
}
```

**Step 2: Register the module and re-exports in `lib.rs`**

Add to the module declarations:

```rust
pub mod execution;
```

Add to the re-exports:

```rust
pub use execution::{DryRunResult, DryRunStreamResult, ExecutionOptions, PipelineOutcome};
```

**Step 3: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-engine execution`
Expected: PASS.

**Step 4: Commit**

```
feat(engine): add ExecutionOptions and DryRunResult types
```

---

## Task 3: Wire `--dry-run` and `--limit` CLI flags

**Files:**
- Modify: `crates/rapidbyte-cli/src/main.rs:24-29`
- Modify: `crates/rapidbyte-cli/src/commands/run.rs:10-27`

**Step 1: Add CLI flags to the `Run` variant**

In `main.rs`, update the `Run` variant:

```rust
Run {
    /// Path to pipeline YAML file
    pipeline: PathBuf,
    /// Preview mode: skip destination, print output to stdout
    #[arg(long)]
    dry_run: bool,
    /// Maximum rows to read per stream (implies --dry-run)
    #[arg(long)]
    limit: Option<u64>,
},
```

Update the match arm in `main()`:

```rust
Commands::Run { pipeline, dry_run, limit } => {
    commands::run::execute(&pipeline, dry_run, limit).await
}
```

**Step 2: Update `run::execute` signature**

In `commands/run.rs`, update the signature:

```rust
pub async fn execute(pipeline_path: &Path, dry_run: bool, limit: Option<u64>) -> Result<()> {
```

Construct `ExecutionOptions` after validation (before `run_pipeline`):

```rust
use rapidbyte_engine::execution::ExecutionOptions;

let dry_run = dry_run || limit.is_some(); // --limit implies --dry-run
let options = ExecutionOptions { dry_run, limit };
```

For now, still call `run_pipeline(&config)` unchanged — the threading happens in Task 4. This step just gets the CLI compiling with the new flags.

**Step 3: Verify it compiles**

Run: `cargo build -p rapidbyte-cli`
Expected: PASS (warning about unused `options` variable is fine).

**Step 4: Commit**

```
feat(cli): add --dry-run and --limit flags to run command
```

---

## Task 4: Thread `ExecutionOptions` through the orchestrator

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs:116-211`
- Modify: `crates/rapidbyte-engine/src/lib.rs:37` (update re-export)
- Modify: `crates/rapidbyte-cli/src/commands/run.rs:27`

**Step 1: Update `run_pipeline` signature**

In `orchestrator.rs`, change:

```rust
pub async fn run_pipeline(
    config: &PipelineConfig,
    options: &ExecutionOptions,
) -> Result<PipelineOutcome, PipelineError> {
```

Update the body: pass `options` to `execute_pipeline_once`:

```rust
let result = execute_pipeline_once(config, options, attempt).await;
```

And wrap the return:

```rust
Ok(pipeline_result) => return Ok(PipelineOutcome::Run(pipeline_result)),
```

**Step 2: Update `execute_pipeline_once` signature**

```rust
async fn execute_pipeline_once(
    config: &PipelineConfig,
    options: &ExecutionOptions,
    attempt: u32,
) -> Result<PipelineOutcome, PipelineError> {
```

Key changes inside `execute_pipeline_once`:

1. When `options.dry_run`, skip loading destination WASM module. Modify `load_modules` or handle after:

```rust
let modules = if options.dry_run {
    load_modules_source_only(config, &connectors).await?
} else {
    load_modules(config, &connectors).await?
};
```

Alternatively, keep `load_modules` loading both but skip the dest module usage later. The simpler approach: still load both (manifest validation is valuable even in dry-run), but skip running the destination. Choose whichever is simpler.

2. Set `max_records` on stream limits when limit is provided:

In `build_stream_contexts`, after building the `limits` struct, apply the limit:

```rust
fn build_stream_contexts(
    config: &PipelineConfig,
    state: &dyn StateBackend,
    max_records: Option<u64>,  // NEW parameter
) -> Result<StreamBuild, PipelineError> {
    // ... existing code building `limits` ...

    let limits = StreamLimits {
        max_records,  // pass through
        // ... rest of fields unchanged ...
    };
```

3. When `options.dry_run`, pass the option to `execute_streams` and return `PipelineOutcome::DryRun` instead of calling `finalize_run`.

**Step 3: Update the CLI caller**

In `commands/run.rs`:

```rust
use rapidbyte_engine::execution::{ExecutionOptions, PipelineOutcome};

let outcome = orchestrator::run_pipeline(&config, &options).await?;
match outcome {
    PipelineOutcome::Run(result) => {
        // existing display code
    }
    PipelineOutcome::DryRun(result) => {
        // placeholder: println!("Dry run complete: {} streams", result.streams.len());
    }
}
```

**Step 4: Update the lib.rs re-export**

```rust
pub use orchestrator::run_pipeline;  // already exported, signature changed
```

**Step 5: Verify it compiles**

Run: `cargo build -p rapidbyte-cli`
Expected: PASS. Dry-run path returns placeholder result.

**Step 6: Commit**

```
feat(engine): thread ExecutionOptions through orchestrator
```

---

## Task 5: Implement dry-run frame collector

This is the core logic: replace destination runner with a lightweight collector.

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs` (inside `execute_streams`)

**Step 1: Write the unit test**

Add at the bottom of `orchestrator.rs` (or in a new `tests/` file):

```rust
#[cfg(test)]
mod dry_run_tests {
    use super::*;
    use crate::arrow::record_batch_to_ipc;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ids: Vec<i64> = (0..n as i64).collect();
        let names: Vec<String> = (0..n).map(|i| format!("row_{i}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn collect_dry_run_frames_basic() {
        let (tx, rx) = mpsc::channel::<Frame>(16);
        let batch = make_test_batch(5);
        let ipc = record_batch_to_ipc(&batch).unwrap();
        tx.send(Frame::Data(bytes::Bytes::from(ipc))).await.unwrap();
        tx.send(Frame::EndStream).await.unwrap();
        drop(tx);

        let result = collect_dry_run_frames(rx, None, None).await.unwrap();
        assert_eq!(result.total_rows, 5);
        assert_eq!(result.batches.len(), 1);
    }

    #[tokio::test]
    async fn collect_dry_run_frames_with_limit() {
        let (tx, rx) = mpsc::channel::<Frame>(16);
        let batch = make_test_batch(100);
        let ipc = record_batch_to_ipc(&batch).unwrap();
        tx.send(Frame::Data(bytes::Bytes::from(ipc))).await.unwrap();
        tx.send(Frame::EndStream).await.unwrap();
        drop(tx);

        let result = collect_dry_run_frames(rx, Some(10), None).await.unwrap();
        assert_eq!(result.total_rows, 10);
        // Batch should be sliced to exactly 10 rows
        let total: usize = result.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
    }

    #[tokio::test]
    async fn collect_dry_run_frames_multiple_batches() {
        let (tx, rx) = mpsc::channel::<Frame>(16);
        for _ in 0..3 {
            let batch = make_test_batch(5);
            let ipc = record_batch_to_ipc(&batch).unwrap();
            tx.send(Frame::Data(bytes::Bytes::from(ipc))).await.unwrap();
        }
        tx.send(Frame::EndStream).await.unwrap();
        drop(tx);

        let result = collect_dry_run_frames(rx, Some(12), None).await.unwrap();
        assert_eq!(result.total_rows, 12);
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-engine dry_run`
Expected: FAIL — `collect_dry_run_frames` doesn't exist.

**Step 3: Implement `collect_dry_run_frames`**

Add to `orchestrator.rs`:

```rust
use crate::arrow::ipc_to_record_batches;

/// Collect frames from a channel, decode IPC, enforce row limit.
/// Used in dry-run mode instead of the destination runner.
async fn collect_dry_run_frames(
    mut receiver: mpsc::Receiver<Frame>,
    limit: Option<u64>,
    compression: Option<rapidbyte_runtime::CompressionCodec>,
) -> Result<DryRunStreamResult, PipelineError> {
    let mut batches = Vec::new();
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;

    loop {
        match receiver.recv().await {
            Some(Frame::Data(data)) => {
                let ipc_bytes = match compression {
                    Some(codec) => {
                        let decompressed =
                            rapidbyte_runtime::compression::decompress(codec, &data)
                                .map_err(|e| {
                                    PipelineError::Infrastructure(anyhow::anyhow!(
                                        "Dry-run decompression failed: {e}"
                                    ))
                                })?;
                        decompressed
                    }
                    None => data.to_vec(),
                };

                let decoded = ipc_to_record_batches(&ipc_bytes)
                    .map_err(PipelineError::Infrastructure)?;

                for batch in decoded {
                    let rows = batch.num_rows() as u64;
                    total_bytes += batch.get_array_memory_size() as u64;

                    if let Some(max) = limit {
                        let remaining = max.saturating_sub(total_rows);
                        if remaining == 0 {
                            return Ok(DryRunStreamResult {
                                stream_name: String::new(),
                                batches,
                                total_rows,
                                total_bytes,
                            });
                        }
                        if rows > remaining {
                            #[allow(clippy::cast_possible_truncation)]
                            let sliced = batch.slice(0, remaining as usize);
                            total_rows += remaining;
                            batches.push(sliced);
                            return Ok(DryRunStreamResult {
                                stream_name: String::new(),
                                batches,
                                total_rows,
                                total_bytes,
                            });
                        }
                    }

                    total_rows += rows;
                    batches.push(batch);
                }
            }
            Some(Frame::EndStream) | None => break,
        }
    }

    Ok(DryRunStreamResult {
        stream_name: String::new(),
        batches,
        total_rows,
        total_bytes,
    })
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-engine dry_run`
Expected: ALL PASS.

**Step 5: Commit**

```
feat(engine): implement dry-run frame collector with row limiting
```

---

## Task 6: Wire dry-run collector into `execute_streams`

**Files:**
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs:408-770`

**Step 1: Add dry_run parameter to `execute_streams`**

Update signature:

```rust
async fn execute_streams(
    config: &PipelineConfig,
    connectors: &ResolvedConnectors,
    modules: &LoadedModules,
    stream_build: &StreamBuild,
    state: Arc<dyn StateBackend>,
    options: &ExecutionOptions,          // NEW
) -> Result<AggregatedStreamResults, PipelineError> {
```

**Step 2: Add dry-run batches to `StreamResult` and `AggregatedStreamResults`**

```rust
struct StreamResult {
    // ... existing fields ...
    dry_run_batches: Option<Vec<RecordBatch>>,  // NEW
}

struct AggregatedStreamResults {
    // ... existing fields ...
    dry_run_streams: Vec<DryRunStreamResult>,   // NEW
}
```

**Step 3: Replace destination spawn with collector when dry_run**

Inside the per-stream spawn (around line 567), replace the destination task conditionally:

```rust
// After source and transform spawns...

let dry_run = options.dry_run;
let dry_run_limit = options.limit;
let stream_compression = params.compression;

let (dst_result, dry_batches) = if dry_run {
    // Dry-run: collect frames instead of writing to destination
    let collector = tokio::spawn(async move {
        collect_dry_run_frames(dest_rx, dry_run_limit, stream_compression).await
    });

    // Wait for source first
    let src_result = src_handle.await.map_err(/* ... */)?;

    // Wait for transforms
    // ... existing transform await code ...

    // Wait for collector
    let collected = collector.await.map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!("Dry-run collector panicked: {e}"))
    })??;

    // Build a stub DestRunResult (no actual writes)
    (src_result, Some(collected.batches))
} else {
    // Normal: spawn destination runner
    let dst_handle = tokio::task::spawn_blocking(move || {
        run_destination_stream(/* ... existing args ... */)
    });
    // ... existing await + result handling ...
    (src_result, None)
};
```

When aggregating results, for dry-run streams build `DryRunStreamResult`:

```rust
if let Some(batches) = dry_batches {
    dry_run_streams.push(DryRunStreamResult {
        stream_name: stream_ctx.stream_name.clone(),
        batches,
        total_rows: sr.read_summary.records_read,
        total_bytes: sr.read_summary.bytes_read,
    });
}
```

**Step 4: Return dry-run results from `execute_pipeline_once`**

In `execute_pipeline_once`, when `options.dry_run`:

```rust
if options.dry_run {
    let duration_secs = start.elapsed().as_secs_f64();
    return Ok(PipelineOutcome::DryRun(DryRunResult {
        streams: aggregated.dry_run_streams,
        source: SourceTiming {
            duration_secs: aggregated.max_src_duration,
            module_load_ms: modules.source_module_load_ms,
            emit_nanos: aggregated.src_timings.emit_batch_nanos,
            compress_nanos: aggregated.src_timings.compress_nanos,
            emit_count: aggregated.src_timings.emit_batch_count,
            ..SourceTiming::default()
        },
        transform_count: config.transforms.len(),
        transform_duration_secs: aggregated.transform_durations.iter().sum(),
        duration_secs,
    }));
}
// ... existing finalize_run code ...
```

**Step 5: Verify it compiles and existing tests pass**

Run: `cargo test -p rapidbyte-engine`
Expected: ALL PASS.

**Step 6: Commit**

```
feat(engine): wire dry-run collector into stream execution pipeline
```

---

## Task 7: Display dry-run output in CLI

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/run.rs`

**Step 1: Implement dry-run display**

Replace the `PipelineOutcome::DryRun` placeholder with:

```rust
PipelineOutcome::DryRun(result) => {
    use arrow::util::pretty::pretty_format_batches;

    println!(
        "Dry run: '{}' ({} stream{})",
        config.pipeline,
        result.streams.len(),
        if result.streams.len() == 1 { "" } else { "s" },
    );
    println!();

    for stream in &result.streams {
        println!("Stream: {}", stream.stream_name);

        if stream.batches.is_empty() {
            println!("  (no data)");
        } else {
            // Print schema
            let schema = stream.batches[0].schema();
            println!("  Columns:");
            for field in schema.fields() {
                println!("    {}: {:?}{}", field.name(), field.data_type(),
                    if field.is_nullable() { " (nullable)" } else { "" });
            }
            println!();

            // Print table
            match pretty_format_batches(&stream.batches) {
                Ok(table) => println!("{table}"),
                Err(e) => println!("  (display error: {e})"),
            }
        }

        println!(
            "{} rows ({}, {} batch{})",
            stream.total_rows,
            format_bytes(stream.total_bytes),
            stream.batches.len(),
            if stream.batches.len() == 1 { "" } else { "es" },
        );
        println!();
    }

    println!("Duration: {:.2}s", result.duration_secs);
    if result.transform_count > 0 {
        println!(
            "Transforms: {} applied ({:.2}s)",
            result.transform_count, result.transform_duration_secs,
        );
    }
}
```

**Step 2: Add `arrow` dependency to CLI crate if needed**

In `crates/rapidbyte-cli/Cargo.toml`, ensure `arrow` is a dependency (it may already be transitive through `rapidbyte-engine`). If the `pretty` module is behind a feature flag, add:

```toml
arrow = { version = "...", features = ["prettyprint"] }
```

Use the same version as `rapidbyte-engine`.

**Step 3: Verify it compiles**

Run: `cargo build -p rapidbyte-cli`
Expected: PASS.

**Step 4: Commit**

```
feat(cli): display dry-run output as formatted Arrow table
```

---

## Task 8: Source connector — respect `max_records`

**Files:**
- Modify: `connectors/source-postgres/src/reader.rs:216-351`

**Step 1: Add limit check to the fetch loop**

After the `exhausted` check (around line 348), add a max_records check:

```rust
if exhausted {
    break;
}

// Stop early if max_records limit reached
if let Some(max) = stream.limits.max_records {
    if state.total_records >= max {
        ctx.log(
            LogLevel::Info,
            &format!(
                "Reached max_records limit ({}) for stream '{}'",
                max, stream.stream_name
            ),
        );
        break;
    }
}
```

This goes right before the closing `}` of the `loop` block (before line 351).

**Step 2: Optimize SQL with LIMIT for full_refresh mode**

In `connectors/source-postgres/src/query.rs`, when building the base query for `FullRefresh` mode, append `LIMIT` if `max_records` is set. Find where `FullRefresh` query is built and add:

```rust
if let Some(max) = stream.limits.max_records {
    sql.push_str(&format!(" LIMIT {max}"));
}
```

This is an optimization — the fetch-loop check in reader.rs is the safety net.

**Step 3: Build the connector**

Run: `cd connectors/source-postgres && cargo build`
Expected: PASS (builds for `wasm32-wasip2`).

**Step 4: Commit**

```
feat(source-postgres): respect max_records limit in read loop
```

---

## Task 9: End-to-end manual test

**Step 1: Build everything**

```bash
just build
just build-connectors
```

**Step 2: Test with a fixture pipeline**

```bash
./target/debug/rapidbyte run tests/fixtures/pipelines/pg_to_pg.yaml --dry-run --limit 10
```

Expected output (approximate):

```
Dry run: 'pg_to_pg' (1 stream)

Stream: public.users
  Columns:
    id: Int64
    name: Utf8
    email: Utf8

+----+-------+-------------------+
| id | name  | email             |
+----+-------+-------------------+
|  1 | Alice | alice@example.com |
| .. | ...   | ...               |
+----+-------+-------------------+
10 rows (4.20 KB, 1 batch)

Duration: 0.85s
```

**Step 3: Test --limit without --dry-run (should imply dry-run)**

```bash
./target/debug/rapidbyte run tests/fixtures/pipelines/pg_to_pg.yaml --limit 5
```

Expected: Same dry-run behavior.

**Step 4: Test --dry-run without --limit (reads all rows)**

```bash
./target/debug/rapidbyte run tests/fixtures/pipelines/pg_to_pg.yaml --dry-run
```

Expected: Reads all source rows, prints all, no destination writes.

**Step 5: Commit**

```
feat: dry-run mode complete (--dry-run --limit N)
```

---

## Summary of touched files

| File | Action |
|------|--------|
| `crates/rapidbyte-types/src/stream.rs` | Add `max_records` to `StreamLimits` |
| `crates/rapidbyte-engine/src/execution.rs` | NEW — `ExecutionOptions`, `DryRunResult`, `PipelineOutcome` |
| `crates/rapidbyte-engine/src/lib.rs` | Register module, add re-exports |
| `crates/rapidbyte-engine/src/orchestrator.rs` | Thread options, frame collector, skip dest in dry-run |
| `crates/rapidbyte-cli/src/main.rs` | `--dry-run` and `--limit` flags |
| `crates/rapidbyte-cli/src/commands/run.rs` | Pass options, display dry-run output |
| `connectors/source-postgres/src/reader.rs` | Respect `max_records` limit |
| `connectors/source-postgres/src/query.rs` | SQL LIMIT optimization |
