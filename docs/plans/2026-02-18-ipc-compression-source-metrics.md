# IPC Compression & Source Per-Stage Metrics Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add configurable LZ4/Zstd compression on the host-level IPC channel between source and destination, and add per-stage timing (ReadPerf) to the source connector matching the dest connector's WritePerf pattern.

**Architecture:** Compression is applied at the host transport layer (inside `host_emit_batch` and `host_next_batch`), making it completely transparent to connectors. A new `compression` field in `ResourceConfig` (pipeline YAML) controls the codec. Source metrics follow the same pattern as WritePerf — the source connector times connect/query/fetch phases and returns them in ReadSummary, which the orchestrator wires into PipelineResult.

**Tech Stack:** `lz4_flex` (pure Rust, no C deps), `zstd` (via `zstd` crate), Arrow IPC (unchanged), WasmEdge host functions.

---

### Task 1: Add CompressionCodec type and ReadPerf to SDK protocol

**Files:**
- Modify: `crates/rapidbyte-sdk/src/protocol.rs`

**Step 1: Write the failing test**

Add to the `mod tests` block at the bottom of `protocol.rs`:

```rust
#[test]
fn test_read_perf_roundtrip() {
    let perf = ReadPerf {
        connect_secs: 0.05,
        query_secs: 0.8,
        fetch_secs: 2.3,
    };
    let json = serde_json::to_string(&perf).unwrap();
    let back: ReadPerf = serde_json::from_str(&json).unwrap();
    assert_eq!(perf, back);
}

#[test]
fn test_read_summary_with_perf() {
    let s = ReadSummary {
        records_read: 1000,
        bytes_read: 65536,
        batches_emitted: 10,
        checkpoint_count: 2,
        records_skipped: 0,
        perf: Some(ReadPerf {
            connect_secs: 0.1,
            query_secs: 1.0,
            fetch_secs: 3.0,
        }),
    };
    let json = serde_json::to_string(&s).unwrap();
    let back: ReadSummary = serde_json::from_str(&json).unwrap();
    assert_eq!(s, back);
}

#[test]
fn test_read_summary_perf_backwards_compat() {
    // Old JSON without perf should deserialize with None
    let json = r#"{"records_read":100,"bytes_read":4096,"batches_emitted":1,"checkpoint_count":1,"records_skipped":0}"#;
    let s: ReadSummary = serde_json::from_str(json).unwrap();
    assert!(s.perf.is_none());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --workspace -p rapidbyte-sdk -- test_read_perf`
Expected: FAIL — `ReadPerf` type doesn't exist yet.

**Step 3: Write minimal implementation**

Add `ReadPerf` struct next to `WritePerf` (around line 290):

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadPerf {
    pub connect_secs: f64,
    pub query_secs: f64,
    pub fetch_secs: f64,
}
```

Add `perf` field to `ReadSummary` (around line 297):

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReadSummary {
    pub records_read: u64,
    pub bytes_read: u64,
    pub batches_emitted: u64,
    pub checkpoint_count: u64,
    #[serde(default)]
    pub records_skipped: u64,
    #[serde(default)]
    pub perf: Option<ReadPerf>,
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --workspace -p rapidbyte-sdk -- test_read_perf`
Expected: PASS

Also run: `cargo test --workspace -p rapidbyte-sdk -- test_read_summary`
Expected: All existing ReadSummary tests should still pass (backwards compat via `#[serde(default)]`).

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/protocol.rs
git commit -m "feat(sdk): add ReadPerf type and perf field to ReadSummary"
```

---

### Task 2: Add compression setting to pipeline config

**Files:**
- Modify: `crates/rapidbyte-core/src/pipeline/types.rs`

**Step 1: Write the failing test**

Add to the `mod tests` block in `types.rs`:

```rust
#[test]
fn test_deserialize_compression_lz4() {
    let yaml = r#"
version: "1"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config: {}
  write_mode: append
resources:
  compression: lz4
"#;
    let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.resources.compression, Some("lz4".to_string()));
}

#[test]
fn test_compression_defaults_to_none() {
    let config = ResourceConfig::default();
    assert_eq!(config.compression, None);
}

#[test]
fn test_deserialize_compression_zstd() {
    let yaml = r#"
version: "1"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config: {}
  write_mode: append
resources:
  compression: zstd
"#;
    let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.resources.compression, Some("zstd".to_string()));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --workspace -p rapidbyte-core -- test_deserialize_compression`
Expected: FAIL — `compression` field doesn't exist.

**Step 3: Write minimal implementation**

Add `compression` field to `ResourceConfig` in `types.rs`:

```rust
/// IPC compression codec for data transferred between source and dest.
/// Valid values: "lz4", "zstd". None = no compression (default).
#[serde(default)]
pub compression: Option<String>,
```

Update the `Default` impl for `ResourceConfig`:

```rust
compression: None,
```

**Step 4: Run test to verify it passes**

Run: `cargo test --workspace -p rapidbyte-core -- test_deserialize_compression`
Expected: PASS

Run: `cargo test --workspace -p rapidbyte-core -- test_compression_defaults`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/rapidbyte-core/src/pipeline/types.rs
git commit -m "feat(core): add compression setting to ResourceConfig"
```

---

### Task 3: Add lz4_flex and zstd dependencies, implement compress/decompress helpers

**Files:**
- Modify: `crates/rapidbyte-core/Cargo.toml`
- Create: `crates/rapidbyte-core/src/runtime/compression.rs`
- Modify: `crates/rapidbyte-core/src/runtime/mod.rs`

**Step 1: Add dependencies**

Add to `[dependencies]` in `crates/rapidbyte-core/Cargo.toml`:

```toml
lz4_flex = "0.11"
zstd = "0.13"
```

**Step 2: Write the failing test**

Create `crates/rapidbyte-core/src/runtime/compression.rs` with tests first:

```rust
/// IPC channel compression codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    Lz4,
    Zstd,
}

impl CompressionCodec {
    /// Parse from pipeline config string. Returns None for unknown/empty values.
    pub fn from_str_opt(s: Option<&str>) -> Option<Self> {
        match s {
            Some("lz4") => Some(Self::Lz4),
            Some("zstd") => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// Compress bytes using the given codec.
pub fn compress(codec: CompressionCodec, data: &[u8]) -> Vec<u8> {
    match codec {
        CompressionCodec::Lz4 => lz4_flex::compress_prepend_size(data),
        CompressionCodec::Zstd => zstd::encode_all(data, 1).expect("zstd compress failed"),
    }
}

/// Decompress bytes using the given codec.
pub fn decompress(codec: CompressionCodec, data: &[u8]) -> Vec<u8> {
    match codec {
        CompressionCodec::Lz4 => {
            lz4_flex::decompress_size_prepended(data).expect("lz4 decompress failed")
        }
        CompressionCodec::Zstd => zstd::decode_all(data).expect("zstd decompress failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"hello world repeated hello world repeated hello world repeated";
        let compressed = compress(CompressionCodec::Lz4, data);
        let decompressed = decompress(CompressionCodec::Lz4, &compressed);
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_zstd_roundtrip() {
        let data = b"hello world repeated hello world repeated hello world repeated";
        let compressed = compress(CompressionCodec::Zstd, data);
        let decompressed = decompress(CompressionCodec::Zstd, &compressed);
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_lz4_compresses_repetitive_data() {
        let data = vec![42u8; 10_000];
        let compressed = compress(CompressionCodec::Lz4, &data);
        assert!(compressed.len() < data.len() / 2);
    }

    #[test]
    fn test_zstd_compresses_repetitive_data() {
        let data = vec![42u8; 10_000];
        let compressed = compress(CompressionCodec::Zstd, &data);
        assert!(compressed.len() < data.len() / 2);
    }

    #[test]
    fn test_codec_from_str() {
        assert_eq!(CompressionCodec::from_str_opt(Some("lz4")), Some(CompressionCodec::Lz4));
        assert_eq!(CompressionCodec::from_str_opt(Some("zstd")), Some(CompressionCodec::Zstd));
        assert_eq!(CompressionCodec::from_str_opt(Some("none")), None);
        assert_eq!(CompressionCodec::from_str_opt(None), None);
    }

    #[test]
    fn test_empty_data_roundtrip() {
        let data = b"";
        let compressed = compress(CompressionCodec::Lz4, data);
        let decompressed = decompress(CompressionCodec::Lz4, &compressed);
        assert_eq!(data.as_slice(), decompressed.as_slice());

        let compressed = compress(CompressionCodec::Zstd, data);
        let decompressed = decompress(CompressionCodec::Zstd, &compressed);
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }
}
```

**Step 3: Register the module**

Add `pub mod compression;` to `crates/rapidbyte-core/src/runtime/mod.rs`.

**Step 4: Run tests to verify they pass**

Run: `cargo test --workspace -p rapidbyte-core -- compression`
Expected: PASS (all 6 tests)

**Step 5: Commit**

```bash
git add crates/rapidbyte-core/Cargo.toml crates/rapidbyte-core/src/runtime/compression.rs crates/rapidbyte-core/src/runtime/mod.rs
git commit -m "feat(core): add lz4/zstd compression helpers"
```

---

### Task 4: Wire compression into HostState and host_emit_batch / host_next_batch

**Files:**
- Modify: `crates/rapidbyte-core/src/runtime/host_functions.rs`

**Step 1: Add compression field to HostState**

Add to `HostState` struct (after the `last_error` field):

```rust
// --- Compression ---
/// Optional compression codec for IPC batches on the channel.
pub compression: Option<super::compression::CompressionCodec>,
```

**Step 2: Compress in host_emit_batch**

In `host_emit_batch`, right before `sender.send(Frame::Data(batch_bytes))` (line 130), compress if configured:

```rust
let batch_bytes = if let Some(codec) = data.compression {
    super::compression::compress(codec, &batch_bytes)
} else {
    batch_bytes
};
```

Note: `batch_bytes` needs to be mutable or re-bound. Since the original is a `Vec<u8>` from `read_from_guest`, rebind it with `let batch_bytes = ...`.

**Step 3: Decompress in host_next_batch**

In `host_next_batch`, after receiving `Frame::Data(batch)` from the channel (line 172) and before using the batch, decompress if configured. After the `let batch = ...` block (line 176), add:

```rust
let batch = if let Some(codec) = data.compression {
    super::compression::decompress(codec, &batch)
} else {
    batch
};
```

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: PASS (compression is None by default, no behavior change)

**Step 5: Commit**

```bash
git add crates/rapidbyte-core/src/runtime/host_functions.rs
git commit -m "feat(core): wire compression into host_emit_batch and host_next_batch"
```

---

### Task 5: Wire compression from pipeline config through runner and orchestrator

**Files:**
- Modify: `crates/rapidbyte-core/src/engine/runner.rs`
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs`

**Step 1: Update runner functions to accept compression**

Add `compression: Option<CompressionCodec>` parameter to `run_source`, `run_destination`, and `run_transform` function signatures in `runner.rs`.

Set `compression` in each `HostState` construction:

```rust
compression: compression,
```

Also update `validate_connector` to set `compression: None` in its HostState.

**Step 2: Update orchestrator to parse and pass compression**

In `orchestrator.rs`'s `execute_pipeline_once`:

After the `limits` construction (around line 150), parse the compression codec:

```rust
use crate::runtime::compression::CompressionCodec;
let compression = CompressionCodec::from_str_opt(config.resources.compression.as_deref());
```

Pass `compression` to the `run_source`, `run_destination`, and `run_transform` calls in the spawn_blocking closures.

**Step 3: Run tests**

Run: `cargo test --workspace`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/rapidbyte-core/src/engine/runner.rs crates/rapidbyte-core/src/engine/orchestrator.rs
git commit -m "feat(core): wire compression from pipeline config to host functions"
```

---

### Task 6: Validate compression setting in pipeline validator

**Files:**
- Modify: `crates/rapidbyte-core/src/pipeline/validator.rs`

**Step 1: Find the validator**

Read `crates/rapidbyte-core/src/pipeline/validator.rs` to understand the existing validation pattern.

**Step 2: Add validation**

Add a check for `config.resources.compression` to reject invalid values:

```rust
if let Some(ref c) = config.resources.compression {
    if c != "lz4" && c != "zstd" {
        return Err(anyhow::anyhow!(
            "Invalid compression codec '{}'. Must be 'lz4' or 'zstd'",
            c
        ));
    }
}
```

**Step 3: Run tests**

Run: `cargo test --workspace`
Expected: PASS

**Step 4: Commit**

```bash
git add crates/rapidbyte-core/src/pipeline/validator.rs
git commit -m "feat(core): validate compression setting in pipeline validator"
```

---

### Task 7: Instrument source-postgres with ReadPerf timing

**Files:**
- Modify: `connectors/source-postgres/src/main.rs`
- Modify: `connectors/source-postgres/src/source.rs`

**Step 1: Update read_stream to return ReadPerf**

In `source.rs`, add timing to `read_stream`:

1. Time the `connect` call (done in `main.rs`'s `rb_run_read`, so pass connect_secs in).
2. Time the cursor DECLARE (query phase).
3. Time the fetch loop (fetch phase).

Update `read_stream` signature to return `ReadSummary` with `perf` populated.

Add timing in `read_stream`:

```rust
use std::time::Instant;

// At the start of the function (after connect, which happens in main.rs):
// ... schema discovery and DECLARE CURSOR ...

let query_start = Instant::now();
// DECLARE CURSOR is part of the query phase
client.execute(&declare, &[]).await.map_err(...)?;
let query_secs = query_start.elapsed().as_secs_f64();

let fetch_start = Instant::now();
// ... existing fetch loop ...
let fetch_secs = fetch_start.elapsed().as_secs_f64();
```

Actually, the cleaner approach: pass `connect_secs` into `read_stream` as a parameter, then time query (schema discovery + DECLARE) and fetch (the loop) inside the function. Return the perf in ReadSummary.

Update the `read_stream` signature:

```rust
pub async fn read_stream(
    client: &Client,
    ctx: &StreamContext,
    connect_secs: f64,
) -> Result<ReadSummary, String>
```

Wrap schema discovery + DECLARE CURSOR in `query_secs` timing.
Wrap the fetch loop in `fetch_secs` timing.

Set `perf` on the returned `ReadSummary`:

```rust
Ok(ReadSummary {
    records_read: total_records,
    bytes_read: total_bytes,
    batches_emitted,
    checkpoint_count,
    records_skipped,
    perf: Some(rapidbyte_sdk::protocol::ReadPerf {
        connect_secs,
        query_secs,
        fetch_secs,
    }),
})
```

**Step 2: Update main.rs to time connect and pass to read_stream**

In `rb_run_read` in `main.rs`, time the `connect(config).await` call and pass it:

```rust
let connect_start = Instant::now();
let client = match connect(config).await {
    Ok(c) => c,
    Err(e) => return make_err_response(ConnectorError::transient_network("CONNECTION_FAILED", e)),
};
let connect_secs = connect_start.elapsed().as_secs_f64();

match source::read_stream(&client, &stream_ctx, connect_secs).await {
    // ...
}
```

Add `use std::time::Instant;` if not already imported.

**Step 3: Build the connector**

Run: `cd connectors/source-postgres && cargo build`
Expected: BUILD SUCCESS (wasm32-wasip1)

**Step 4: Commit**

```bash
git add connectors/source-postgres/src/main.rs connectors/source-postgres/src/source.rs
git commit -m "feat(source-postgres): add ReadPerf with connect/query/fetch timing"
```

---

### Task 8: Wire ReadPerf through runner, orchestrator, PipelineResult, and CLI

**Files:**
- Modify: `crates/rapidbyte-core/src/engine/runner.rs`
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs`
- Modify: `crates/rapidbyte-cli/src/commands/run.rs`

**Step 1: Add source timing fields to PipelineResult**

In `runner.rs`, add to `PipelineResult`:

```rust
// Source sub-phase timing (from connector)
pub source_connect_secs: f64,
pub source_query_secs: f64,
pub source_fetch_secs: f64,
```

**Step 2: Wire ReadSummary.perf through orchestrator**

In `orchestrator.rs`, in the `Ok(...)` match arm where `PipelineResult` is constructed (around line 414):

```rust
let src_perf = read_summary.perf.as_ref();
// ...
source_connect_secs: src_perf.map(|p| p.connect_secs).unwrap_or(0.0),
source_query_secs: src_perf.map(|p| p.query_secs).unwrap_or(0.0),
source_fetch_secs: src_perf.map(|p| p.fetch_secs).unwrap_or(0.0),
```

**Step 3: Update CLI output**

In `run.rs`, add source timing display after the existing source duration line:

```rust
println!("  Source duration:  {:.2}s", result.source_duration_secs);
println!("    Connect:       {:.3}s", result.source_connect_secs);
println!("    Query:         {:.3}s", result.source_query_secs);
println!("    Fetch:         {:.3}s", result.source_fetch_secs);
```

Update the `@@BENCH_JSON@@` JSON to include source timing:

```rust
"source_connect_secs": result.source_connect_secs,
"source_query_secs": result.source_query_secs,
"source_fetch_secs": result.source_fetch_secs,
```

**Step 4: Update bench.sh report**

In `tests/bench.sh`, add source timing rows to the comparison table (around line 230, after the `'Source duration'` entry):

```python
('  Connect', 'source_connect_secs', 's'),
('  Query', 'source_query_secs', 's'),
('  Fetch', 'source_fetch_secs', 's'),
```

**Step 5: Run tests**

Run: `cargo test --workspace`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/rapidbyte-core/src/engine/runner.rs crates/rapidbyte-core/src/engine/orchestrator.rs crates/rapidbyte-cli/src/commands/run.rs tests/bench.sh
git commit -m "feat(core/cli): wire ReadPerf through PipelineResult and CLI output"
```

---

### Task 9: Verification — build, test, lint, benchmark

**Step 1: Run workspace tests**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 2: Run clippy on workspace**

Run: `cargo clippy --workspace -- -D warnings`
Expected: No warnings

**Step 3: Build both connectors**

Run: `cd connectors/source-postgres && cargo build --release`
Run: `cd connectors/dest-postgres && cargo build --release`
Expected: Both build successfully

**Step 4: Run clippy on both connectors**

Run: `cd connectors/source-postgres && cargo clippy -- -D warnings`
Run: `cd connectors/dest-postgres && cargo clippy -- -D warnings`
Expected: No warnings

**Step 5: Run benchmark (optional, if Docker available)**

Run: `just bench-connector-postgres`
Expected: Benchmark completes, source timing shows in report. Verify:
- `Source Connect` is non-zero
- `Source Query` is non-zero
- `Source Fetch` is non-zero
- No regression in throughput

**Step 6: Test with compression enabled (optional, if Docker available)**

Create a test pipeline YAML with `compression: lz4` in resources.
Run and verify it completes without errors.
