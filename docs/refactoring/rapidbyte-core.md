# Refactoring Guide: `rapidbyte-core`

> Organized around the same principles used in [`rapidbyte-sdk.md`](rapidbyte-sdk.md).
> Each item is tagged with a **priority tier** and **effort estimate**.

## Legend

| Tag | Meaning |
|-----|---------|
| **P0** | Do first — blocks other refactors or has outsized safety/quality impact |
| **P1** | Important — clear value, moderate risk |
| **P2** | Polish — nice-to-have, low risk, do when touching nearby code |
| **S** | Small — < 1 hour |
| **M** | Medium — 1-4 hours |
| **L** | Large — half day or more |

---

## File Inventory

| File | Lines | Domain | Tests? |
|------|------:|--------|--------|
| `engine/orchestrator.rs` | 1,096 | Pipeline execution, retry loop, stream orchestration | No |
| `engine/runner.rs` | 708 | Source/dest/transform WASM runners, validation, discover | No |
| `pipeline/types.rs` | 627 | Pipeline config structs, serde defaults, `parse_byte_size` | Yes (398 lines) |
| `runtime/component_runtime.rs` | 684 | `ComponentHostState`, host imports, WASI context | No |
| `state/sqlite.rs` | 502 | SQLite state backend implementation | Yes (313 lines) |
| `pipeline/validator.rs` | 411 | Pipeline YAML validation | Yes (318 lines) |
| `runtime/wasm_runtime.rs` | 346 | Wasmtime engine, AOT cache, module loading | No |
| `runtime/wit_bindings.rs` | 331 | WIT Host trait impls, error converters (macros) | No |
| `engine/checkpoint.rs` | 277 | Cursor correlation and persistence | Yes (212 lines) |
| `engine/errors.rs` | 214 | `PipelineError`, `compute_backoff` | Yes (134 lines) |
| `arrow_utils.rs` | 185 | Arrow IPC serialize/deserialize helpers | Yes (120 lines) |
| `runtime/network_acl.rs` | 181 | Network ACL derivation and matching | Yes (18 lines) |
| `runtime/host_socket.rs` | 172 | TCP socket entries, poll, resolve | Yes (62 lines) |
| `pipeline/parser.rs` | 142 | YAML parsing, env var substitution | Yes (93 lines) |
| `runtime/connector_resolve.rs` | 123 | Connector path resolution, manifest loading | No |
| `engine/compression.rs` | 96 | LZ4/Zstd compress/decompress | Yes (58 lines) |
| `state/backend.rs` | 58 | `StateBackend` trait, `CursorState`, `RunStats` | No |
| `engine/dlq.rs` | 39 | DLQ record persistence helper | No |
| `state/schema.rs` | 37 | SQLite DDL (`CREATE_TABLES`) | No |
| `engine/mod.rs` | 11 | Module declarations | No |
| `runtime/mod.rs` | 6 | Module declarations | No |
| `lib.rs` | 5 | Module declarations | No |
| `state/mod.rs` | 3 | Module declarations | No |
| `pipeline/mod.rs` | 3 | Module declarations | No |
| **Total** | **~5,257** | | |

---

## 1. Structural & Type-Level Principles

> Make invalid states unrepresentable.

### 1.1 Type pipeline config fields as enums &mdash; P0 / M

**File:** `pipeline/types.rs:28` (`sync_mode: String`), `types.rs:45` (`write_mode: String`), `types.rs:49` (`on_data_error: String`), `types.rs:61-67` (schema evolution `String` fields), `types.rs:86` (`backend: String`), `types.rs:126` (`compression: Option<String>`)

**Problem:** Seven config fields are `String` where the SDK already defines matching enums (`SyncMode`, `WriteMode`, `DataErrorPolicy`, etc.). This forces the orchestrator to perform 7 `match .as_str()` blocks to convert them:

```rust
// orchestrator.rs:262-266
let on_data_error = match config.destination.on_data_error.as_str() {
    "skip" => DataErrorPolicy::Skip,
    "dlq" => DataErrorPolicy::Dlq,
    _ => DataErrorPolicy::Fail,
};

// orchestrator.rs:298-302
let sync_mode = match s.sync_mode.as_str() {
    "incremental" => SyncMode::Incremental,
    "cdc" => SyncMode::Cdc,
    _ => SyncMode::FullRefresh,
};

// orchestrator.rs:341-347
let write_mode = match config.destination.write_mode.as_str() {
    "replace" => WriteMode::Replace,
    "upsert" => WriteMode::Upsert { ... },
    _ => WriteMode::Append,
};
```

A typo like `sync_mode: "incremnetal"` silently falls through to the `_` arm as `FullRefresh`. The validator (`validator.rs:33-41`) repeats the same string matching.

**After:**

```rust
// types.rs
use rapidbyte_types::protocol::{SyncMode, WriteMode, DataErrorPolicy};
use crate::engine::compression::CompressionCodec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: String,
    pub sync_mode: SyncMode,       // was: String
    pub cursor_field: Option<String>,
    pub columns: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationConfig {
    #[serde(rename = "use")]
    pub use_ref: String,
    pub config: serde_json::Value,
    pub write_mode: WriteMode,      // was: String
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub on_data_error: DataErrorPolicy,  // was: String
    // ...
}
```

Then delete all 7 `match .as_str()` blocks in orchestrator.rs and validator.rs — serde handles validation at parse time. Invalid values now produce a deserialization error with the exact field name rather than silently defaulting.

**Why P0:** This is the single highest-leverage type safety win. It eliminates ~85 lines of stringly-typed conversion code, moves validation from runtime to parse time, and prevents an entire class of silent-default bugs. The SDK enums already exist in `rapidbyte-types`.

**Migration strategy:**
1. Add `#[serde(rename_all = "snake_case")]` to the SDK enums if not already present.
2. Change `String` → enum in `types.rs` fields one at a time.
3. Delete corresponding `match .as_str()` block in orchestrator.rs after each field.
4. Update validator.rs to remove now-redundant string checks.
5. Update YAML fixtures in `tests/fixtures/` to confirm deserialization still works.

---

### 1.2 Flatten `PipelineResult` into nested timing structs &mdash; P1 / S

**File:** `engine/runner.rs:28-64`

**Problem:** `PipelineResult` has 26 flat fields spanning 5 domains. The struct is hard to read and easy to misassign:

```rust
pub struct PipelineResult {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub duration_secs: f64,
    pub source_duration_secs: f64,
    pub dest_duration_secs: f64,
    pub source_module_load_ms: u64,
    pub dest_module_load_ms: u64,
    pub dest_connect_secs: f64,
    pub dest_flush_secs: f64,
    pub dest_commit_secs: f64,
    pub source_connect_secs: f64,
    pub source_query_secs: f64,
    pub source_fetch_secs: f64,
    pub source_arrow_encode_secs: f64,
    pub dest_arrow_decode_secs: f64,
    pub dest_vm_setup_secs: f64,
    pub dest_recv_secs: f64,
    pub wasm_overhead_secs: f64,
    pub source_emit_nanos: u64,
    pub source_compress_nanos: u64,
    pub source_emit_count: u64,
    pub dest_recv_nanos: u64,
    pub dest_decompress_nanos: u64,
    pub dest_recv_count: u64,
    pub transform_count: usize,
    pub transform_duration_secs: f64,
    pub transform_module_load_ms: Vec<u64>,
    pub retry_count: u32,
}
```

**After:**

```rust
pub struct PipelineCounts {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

pub struct SourceTiming {
    pub duration_secs: f64,
    pub module_load_ms: u64,
    pub connect_secs: f64,
    pub query_secs: f64,
    pub fetch_secs: f64,
    pub arrow_encode_secs: f64,
    pub emit_nanos: u64,
    pub compress_nanos: u64,
    pub emit_count: u64,
}

pub struct DestTiming {
    pub duration_secs: f64,
    pub module_load_ms: u64,
    pub connect_secs: f64,
    pub flush_secs: f64,
    pub commit_secs: f64,
    pub arrow_decode_secs: f64,
    pub vm_setup_secs: f64,
    pub recv_secs: f64,
    pub recv_nanos: u64,
    pub decompress_nanos: u64,
    pub recv_count: u64,
}

pub struct PipelineResult {
    pub counts: PipelineCounts,
    pub source: SourceTiming,
    pub dest: DestTiming,
    pub transform_count: usize,
    pub transform_duration_secs: f64,
    pub transform_module_load_ms: Vec<u64>,
    pub duration_secs: f64,
    pub wasm_overhead_secs: f64,
    pub retry_count: u32,
}
```

**Why P1:** The flat struct makes it easy to accidentally swap `source_connect_secs` and `dest_connect_secs` when building the result (orchestrator.rs:793-824). Nested structs provide namespace grouping. The CLI already accesses fields with a `result.source_*` / `result.dest_*` prefix, so the migration is mechanical.

---

### 1.3 Newtype wrappers for `PipelineId` / `StreamName` &mdash; P2 / S

**File:** `state/backend.rs` (trait methods all take `pipeline: &str, stream: &str`), `state/sqlite.rs`, `engine/orchestrator.rs`

**Problem:** Every `StateBackend` method takes `pipeline: &str` and `stream: &str` as bare string arguments. Nothing prevents transposition:

```rust
// Easy to accidentally swap:
backend.get_cursor("users", "my_pipeline")  // wrong order, compiles fine
```

**After:**

```rust
pub struct PipelineId(pub String);
pub struct StreamName(pub String);

pub trait StateBackend: Send + Sync {
    fn get_cursor(&self, pipeline: &PipelineId, stream: &StreamName) -> Result<Option<CursorState>>;
    // ...
}
```

**Why P2:** Low risk of this bug today (the code is consistent), but as more callers are added the risk grows. Newtypes are cheap insurance.

---

## 2. God Function & Module Decomposition

> Avoid god-modules; keep functions focused.

### 2.1 Decompose `execute_pipeline_once()` &mdash; P0 / L

**File:** `engine/orchestrator.rs:128-825`

**Problem:** `execute_pipeline_once()` is 697 lines — the single largest function in the codebase. It handles 5 distinct phases sequentially:

1. **Resolve connectors** (lines 137-163): path resolution, manifest loading, schema validation
2. **Load modules** (lines 170-241): WASM module loading for source, dest, transforms
3. **Build stream contexts** (lines 244-363): parse config strings into typed SDK structs, load cursors
4. **Execute streams** (lines 382-712): spawn per-stream source/transform/dest tasks, aggregate results
5. **Finalize run** (lines 714-825): persist DLQ, correlate checkpoints, build `PipelineResult`

Each phase is independent and testable in isolation, but they're welded together with local variables threaded through.

**After:** Extract into 5 helper functions:

```rust
async fn execute_pipeline_once(config: &PipelineConfig, attempt: u32) -> Result<PipelineResult, PipelineError> {
    let start = Instant::now();

    let connectors = resolve_connectors(config)?;
    let modules = load_modules(config, &connectors).await?;
    let (stream_ctxs, compression) = build_stream_contexts(config, &connectors.state)?;
    let stream_results = execute_streams(config, &modules, &stream_ctxs, compression).await?;
    finalize_run(config, &connectors.state, stream_results, start, attempt)
}
```

**Why P0:** This function is where most orchestrator bugs surface. Extracting phases makes each independently testable and reviewable. Every other orchestrator change in this guide becomes easier once this decomposition lands.

**Migration strategy:**
1. Extract `resolve_connectors()` first — it has the fewest dependencies (returns paths + manifests + permissions).
2. Extract `load_modules()` next — takes paths, returns `LoadedComponent` instances.
3. Extract `build_stream_contexts()` — this is where item 1.1 (typed config enums) pays off, as the match blocks disappear.
4. Extract `execute_streams()` — the largest chunk, but self-contained once the inputs are defined.
5. Extract `finalize_run()` — aggregation and persistence.
6. Run `just test` and `just e2e` between each step.

---

### 2.2 Split `ComponentHostState` responsibilities &mdash; P1 / L

**File:** `runtime/component_runtime.rs:128-152`

**Problem:** `ComponentHostState` holds 4 distinct responsibilities in a single struct:

```rust
pub struct ComponentHostState {
    // 1. Connector identity
    pub(crate) pipeline_name: String,
    pub(crate) connector_id: String,
    pub(crate) current_stream: String,
    pub(crate) state_backend: Arc<dyn StateBackend>,

    // 2. Batch routing
    pub(crate) batch_sender: Option<mpsc::Sender<Frame>>,
    pub(crate) next_batch_id: u64,
    pub(crate) batch_receiver: Option<mpsc::Receiver<Frame>>,
    pub(crate) compression: Option<CompressionCodec>,

    // 3. Checkpoint collection
    pub(crate) source_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
    pub(crate) dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>>,
    pub(crate) dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    pub(crate) timings: Arc<Mutex<HostTimings>>,

    // 4. Socket management
    network_acl: NetworkAcl,
    sockets: HashMap<u64, SocketEntry>,
    next_socket_handle: u64,

    // WASI context
    ctx: WasiCtx,
    table: ResourceTable,
}
```

**After:** Extract inner structs:

```rust
pub(crate) struct ConnectorIdentity {
    pub pipeline_name: String,
    pub connector_id: String,
    pub current_stream: String,
    pub state_backend: Arc<dyn StateBackend>,
}

pub(crate) struct BatchRouter {
    pub sender: Option<mpsc::Sender<Frame>>,
    pub receiver: Option<mpsc::Receiver<Frame>>,
    pub next_batch_id: u64,
    pub compression: Option<CompressionCodec>,
}

pub(crate) struct CheckpointCollector {
    pub source: Arc<Mutex<Vec<Checkpoint>>>,
    pub dest: Arc<Mutex<Vec<Checkpoint>>>,
    pub dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    pub timings: Arc<Mutex<HostTimings>>,
}

pub(crate) struct SocketManager {
    acl: NetworkAcl,
    sockets: HashMap<u64, SocketEntry>,
    next_handle: u64,
}
```

**Why P1:** The current struct has 18 fields. Methods like `emit_batch_impl` only touch `BatchRouter` fields, but have access to everything. Inner structs clarify which state each method actually uses.

---

### 2.3 Deduplicate linker creation functions &mdash; P1 / S

**File:** `engine/runner.rs:75-109` (`create_source_linker`, `create_dest_linker`, `create_transform_linker`), `runtime/wasm_runtime.rs:137-171` (`source_linker`, `dest_linker`, `transform_linker`)

**Problem:** There are 6 linker creation functions (3 in runner.rs, 3 in wasm_runtime.rs) that are structurally identical — only the binding type differs:

```rust
// runner.rs:75-85
fn create_source_linker(engine: &wasmtime::Engine) -> Result<Linker<ComponentHostState>> {
    let mut linker = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
    source_bindings::RapidbyteSource::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
        &mut linker, |state| state,
    )?;
    Ok(linker)
}

// wasm_runtime.rs:137-147 — identical pattern
pub fn source_linker(&self) -> Result<Linker<ComponentHostState>> {
    let mut linker = Linker::new(&self.engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
    source_bindings::RapidbyteSource::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
        &mut linker, |state| state,
    )?;
    Ok(linker)
}
```

**After:** One generic function with a closure or trait:

```rust
fn create_linker<F>(engine: &wasmtime::Engine, add_bindings: F) -> Result<Linker<ComponentHostState>>
where
    F: FnOnce(&mut Linker<ComponentHostState>) -> Result<()>,
{
    let mut linker = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
    add_bindings(&mut linker)?;
    Ok(linker)
}

// Usage:
let linker = create_linker(&engine, |l| {
    source_bindings::RapidbyteSource::add_to_linker::<_, wasmtime::component::HasSelf<_>>(l, |s| s)?;
    Ok(())
})?;
```

Remove the 3 runner.rs functions and the 3 wasm_runtime.rs methods, replacing with calls to the single helper. This also means runner.rs no longer needs to import the binding modules directly.

**Why P1:** 6 copies of the same pattern means adding a new linker feature (e.g., epoch interruption) requires editing 6 places.

---

### 2.4 Factory for `ComponentHostState` construction &mdash; P1 / S

**File:** `engine/runner.rs:133-148, 279-294, 435-450, 545-559, 651-665`

**Problem:** `ComponentHostState::new()` is called 5 times in runner.rs, each with 13 positional arguments. Three of those calls construct dummy `Arc<Mutex<Vec::new()>>` placeholders:

```rust
// runner.rs:545-559 — validate_connector
let host_state = ComponentHostState::new(
    "check".to_string(),          // pipeline_name
    connector_id.to_string(),     // connector_id
    "check".to_string(),          // current_stream
    state,                        // state_backend
    None,                         // batch_sender
    None,                         // batch_receiver
    None,                         // compression
    Arc::new(Mutex::new(Vec::new())),  // source_checkpoints (dummy)
    Arc::new(Mutex::new(Vec::new())),  // dest_checkpoints (dummy)
    Arc::new(Mutex::new(Vec::new())),  // dlq_records (dummy)
    Arc::new(Mutex::new(HostTimings::default())),  // timings (dummy)
    permissions,                  // permissions
    config,                       // config
)?;
```

**After (depends on 2.2 params struct):**

```rust
let host_state = ComponentHostState::for_validation(
    connector_id, state, permissions, config,
)?;

// Or use a builder:
let host_state = ComponentHostState::builder()
    .pipeline("check")
    .connector(connector_id)
    .stream("check")
    .state_backend(state)
    .permissions(permissions)
    .config(config)
    .build()?;
```

**Why P1:** 13 positional arguments is a readability and correctness hazard. A builder or named constructors per use case (`for_source`, `for_destination`, `for_validation`) makes the intent clear and prevents argument swaps.

---

## 3. Narrative Flow & Readability

> Make the code easy to read top-to-bottom.

### 3.1 Return `Result` from compression functions &mdash; P0 / S

**File:** `engine/compression.rs:20-35`

**Problem:** Both `compress()` and `decompress()` panic on failure via `.expect()`:

```rust
pub fn compress(codec: CompressionCodec, data: &[u8]) -> Vec<u8> {
    match codec {
        CompressionCodec::Lz4 => lz4_flex::compress_prepend_size(data),
        CompressionCodec::Zstd => zstd::bulk::compress(data, 1).expect("zstd compress failed"),
    }
}

pub fn decompress(codec: CompressionCodec, data: &[u8]) -> Vec<u8> {
    match codec {
        CompressionCodec::Lz4 => {
            lz4_flex::decompress_size_prepended(data).expect("lz4 decompress failed")
        }
        CompressionCodec::Zstd => zstd::decode_all(data).expect("zstd decompress failed"),
    }
}
```

Corrupt IPC data arriving over the channel will crash the entire pipeline host process. These functions are called inside `ComponentHostState::emit_batch_impl()` and `next_batch_impl()` (component_runtime.rs:208, 256) which already return `Result<_, ConnectorError>`.

**After:**

```rust
pub fn compress(codec: CompressionCodec, data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    match codec {
        CompressionCodec::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
        CompressionCodec::Zstd => zstd::bulk::compress(data, 1),
    }
}

pub fn decompress(codec: CompressionCodec, data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    match codec {
        CompressionCodec::Lz4 => lz4_flex::decompress_size_prepended(data)
            .map_err(|e| CompressionError::Lz4(e.to_string())),
        CompressionCodec::Zstd => zstd::decode_all(std::io::Cursor::new(data))
            .map_err(CompressionError::Io),
    }
}
```

Callers in `component_runtime.rs` convert to `ConnectorError::internal("COMPRESS_FAILED", ...)`.

**Why P0:** `expect()` in library code called on every batch is a latent crash. This is the simplest safety fix in the crate — two function signatures change, two call sites add `?`.

---

### 3.2 Propagate Mutex lock errors &mdash; P1 / M

**File:** `component_runtime.rs:229, 266, 387, 394, 658`, `runner.rs:207, 245, 364, 399`, `sqlite.rs:46, 76, 101, 127, 136, 163`, `orchestrator.rs:714, 716`

**Problem:** There are 17 bare `.lock().unwrap()` calls on `Mutex` across non-test code. While Mutex poisoning is rare (it requires a panic while the lock is held), the crash message would be the generic "called `Result::unwrap()` on an `Err` value: PoisonError" with no context:

```rust
let mut t = self.timings.lock().unwrap();
let conn = self.conn.lock().unwrap();
let mut s = stats.lock().unwrap();
```

**After (minimum):** Replace with `.expect("context")`:

```rust
let mut t = self.timings.lock().expect("timings mutex poisoned");
let conn = self.conn.lock().expect("sqlite connection mutex poisoned");
```

**After (better, for `component_runtime.rs` methods that return `Result`):**

```rust
let mut t = self.timings.lock().map_err(|_| {
    ConnectorError::internal("MUTEX_POISONED", "timings lock poisoned")
})?;
```

For `sqlite.rs`, the `StateBackend` trait methods already return `Result`, so propagation is straightforward.

**Why P1:** These are all in hot paths (per-batch timing, per-query state access). A poisoned mutex means a prior panic — crashing again with no context makes debugging harder.

---

### 3.3 Return `Result` from `parse_byte_size` &mdash; P1 / S

**File:** `pipeline/types.rs:168-180`

**Problem:** `parse_byte_size` silently returns 0 for invalid input:

```rust
pub fn parse_byte_size(s: &str) -> u64 {
    let s = s.trim().to_lowercase();
    if let Some(n) = s.strip_suffix("gb") {
        n.trim().parse::<u64>().unwrap_or(0) * 1024 * 1024 * 1024
    } else if let Some(n) = s.strip_suffix("mb") {
        n.trim().parse::<u64>().unwrap_or(0) * 1024 * 1024
    } else {
        s.parse::<u64>().unwrap_or(0)
    }
}
```

A config like `max_batch_bytes: "sixty-four mb"` silently becomes 0 bytes, which the orchestrator then replaces with the default (orchestrator.rs:248-252). This masks config errors.

**After:**

```rust
pub fn parse_byte_size(s: &str) -> Result<u64, ParseByteSizeError> {
    let s = s.trim().to_lowercase();
    if let Some(n) = s.strip_suffix("gb") {
        Ok(n.trim().parse::<u64>()? * 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("mb") {
        Ok(n.trim().parse::<u64>()? * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("kb") {
        Ok(n.trim().parse::<u64>()? * 1024)
    } else {
        Ok(s.parse::<u64>()?)
    }
}
```

**Why P1:** Silent-zero is a correctness risk. The caller can decide to fall back to a default, but it should be an explicit choice, not an implicit swallow.

---

### 3.4 Log datetime parse fallback &mdash; P2 / S

**File:** `state/sqlite.rs:60-63`

**Problem:** If the stored `updated_at` string doesn't match `"%Y-%m-%d %H:%M:%S"`, the code silently falls back to `Utc::now()`:

```rust
let updated_at =
    NaiveDateTime::parse_from_str(&updated_at_str, "%Y-%m-%d %H:%M:%S")
        .map(|ndt| DateTime::from_naive_utc_and_offset(ndt, Utc))
        .unwrap_or_else(|_| Utc::now());
```

This hides data corruption — a bad timestamp in the database would silently appear as "just now".

**After:**

```rust
let updated_at = NaiveDateTime::parse_from_str(&updated_at_str, SQLITE_DATETIME_FMT)
    .map(|ndt| DateTime::from_naive_utc_and_offset(ndt, Utc))
    .unwrap_or_else(|e| {
        tracing::warn!(
            pipeline, stream,
            raw = updated_at_str,
            error = %e,
            "Failed to parse cursor updated_at; defaulting to now"
        );
        Utc::now()
    });
```

**Why P2:** The fallback behavior is acceptable, but the silence isn't. A single `tracing::warn!` makes corruption detectable.

---

## 4. Architecture & Tooling

> Module structure, documentation, encapsulation.

### 4.1 Module-level rustdoc &mdash; P1 / S

**Problem:** Only 1 of 24 files has `//!` module documentation (`wit_bindings.rs`). Missing from all others.

**Action:** Add `//!` doc comments to every module. Examples:

```rust
// engine/orchestrator.rs
//! Pipeline orchestrator: resolves connectors, loads WASM modules,
//! builds stream contexts, executes source→transform→dest stages,
//! and coordinates checkpoints with the state backend.

// runtime/component_runtime.rs
//! Host-side shared state for WASM component instances.
//! `ComponentHostState` implements the `Host` trait for all three WIT worlds
//! (source, destination, transform) via macro-generated impls in `wit_bindings`.

// engine/compression.rs
//! IPC channel compression: LZ4 and Zstd codecs for Arrow batch data
//! flowing between pipeline stages.
```

**Why P1:** Module docs are the first thing a new contributor reads. The crate has 24 files across 4 directories — without `//!` headers, understanding the module hierarchy requires reading code.

---

### 4.2 Test coverage &mdash; P1 / L

**Problem:** 5 major files have zero tests:

| File | Lines | Coverage |
|------|------:|----------|
| `engine/orchestrator.rs` | 1,096 | 0 tests |
| `engine/runner.rs` | 708 | 0 tests |
| `runtime/component_runtime.rs` | 684 | 0 tests |
| `runtime/wasm_runtime.rs` | 346 | 0 tests |
| `runtime/connector_resolve.rs` | 123 | 0 tests |

These are the most critical files — orchestrator.rs alone is 20% of the crate. They're hard to test because they require WASM connectors, but several pure functions can be unit-tested:

- `parse_connector_ref()` (connector_resolve.rs:19-31) — pure string parsing
- `compute_backoff()` (errors.rs) — already tested, but `PipelineResult` construction is not
- `correlate_and_persist_cursors()` — already tested, good model for orchestrator helpers
- `build_wasi_ctx()` (component_runtime.rs:92-125) — could test env/preopen configuration
- `create_state_backend()` (orchestrator.rs:992-1004) — pure config → backend

**Action after 2.1:** Once `execute_pipeline_once` is decomposed, each helper function becomes independently testable without WASM.

**Why P1:** The untested code contains the most complex logic. Item 2.1 unblocks this.

---

### 4.3 Cache compiled regex &mdash; P2 / S

**File:** `pipeline/parser.rs:10`

**Problem:** `Regex::new()` is called inside `substitute_env_vars()`, which runs on every pipeline parse:

```rust
pub fn substitute_env_vars(input: &str) -> Result<String> {
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").unwrap();
    // ...
}
```

**After:**

```rust
use std::sync::LazyLock;

static ENV_VAR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").expect("invalid regex")
});

pub fn substitute_env_vars(input: &str) -> Result<String> {
    let re = &*ENV_VAR_RE;
    // ...
}
```

**Why P2:** Minor performance win — regex compilation is ~microseconds, but it's a free improvement. More importantly, it's idiomatic Rust.

---

### 4.4 Encapsulate sqlite connection &mdash; P2 / S

**File:** `state/sqlite.rs:13-15`, test code at lines 319, 350, 454

**Problem:** `SqliteStateBackend` exposes `conn` as `pub` via struct visibility, and tests access it directly:

```rust
pub struct SqliteStateBackend {
    conn: Mutex<Connection>,  // pub(crate) by module visibility
}

// tests:
let conn = backend.conn.lock().unwrap();
let (status, records_read, finished): ... = conn.query_row(...);
```

This couples tests to the internal schema and prevents changing the storage representation.

**After:** Add a `pub(crate) fn raw_connection(&self) -> ...` method or better, add query methods to the `StateBackend` trait for what the tests actually need (e.g., `get_run_status(run_id) -> RunStatus`).

**Why P2:** The `conn` field isn't technically `pub`, but module-level access in tests creates implicit coupling. Adding trait methods for test queries improves encapsulation.

---

## 5. Performance

> Make it correct first, then fast. Measure.

No performance items identified. The AOT caching in `wasm_runtime.rs` is well-implemented with compatibility hash, atomic writes, and graceful fallback. The `RAPIDBYTE_WASMTIME_AOT` env flag parsing is already cached via the `WasmRuntime::new()` constructor (called once per pipeline run). The `socket_poll_timeout_ms()` in `host_socket.rs:66-74` correctly uses `OnceLock` for env var caching.

---

## 6. Polish

> Naming, layout, robustness, documentation.

### 6.1 Extract magic numbers as constants &mdash; P1 / S

**File:** `engine/errors.rs:69-77`, `engine/compression.rs:23`, `runtime/component_runtime.rs:513` (`64 * 1024`), `engine/dlq.rs:7`, `state/schema.rs:1-37`

**Problem:** Magic numbers scattered across multiple files:

```rust
// errors.rs:69-77 — backoff base values
let base_ms: u64 = match err.backoff_class {
    BackoffClass::Fast => 100,
    BackoffClass::Normal => 1000,
    BackoffClass::Slow => 5000,
};
let max_ms: u64 = 60_000;

// compression.rs:23
zstd::bulk::compress(data, 1)  // zstd level = 1

// component_runtime.rs:513
let read_len = len.clamp(1, 64 * 1024) as usize;  // socket buffer

// checkpoint kinds in component_runtime.rs:382-408
match kind {
    0 => { /* source */ }
    1 => { /* dest */ }
    2 => { /* transform */ }
    _ => { /* error */ }
}
```

**After:**

```rust
// errors.rs
const BACKOFF_FAST_BASE_MS: u64 = 100;
const BACKOFF_NORMAL_BASE_MS: u64 = 1_000;
const BACKOFF_SLOW_BASE_MS: u64 = 5_000;
const BACKOFF_MAX_MS: u64 = 60_000;

// compression.rs
const ZSTD_COMPRESSION_LEVEL: i32 = 1;

// component_runtime.rs
const MAX_SOCKET_READ_BYTES: u64 = 64 * 1024;
```

For checkpoint kinds, use `CheckpointKind` enum values instead of raw `u32`:

```rust
let kind = CheckpointKind::try_from(kind).map_err(|_| ...)?;
match kind {
    CheckpointKind::Source => { ... }
    CheckpointKind::Dest => { ... }
    CheckpointKind::Transform => { ... }
}
```

**Why P1:** Magic numbers make code harder to audit. The backoff values especially should be documented constants since they affect retry behavior.

---

### 6.2 Consolidate serde defaults &mdash; P2 / S

**File:** `pipeline/types.rs:54-81, 90-166`

**Problem:** 8 named default functions plus 2 `Default` impl blocks specify the same values in two places:

```rust
fn default_backend() -> String { "sqlite".to_string() }

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            backend: default_backend(),   // calls the function
            connection: None,
        }
    }
}
```

And `ResourceConfig` has 6 named default functions (`default_max_memory`, `default_max_batch_bytes`, etc.) plus a `Default` impl that calls all of them.

**After:** Use constants as the single source of truth:

```rust
const DEFAULT_MAX_MEMORY: &str = "256mb";
const DEFAULT_MAX_BATCH_BYTES: &str = "64mb";
const DEFAULT_CHECKPOINT_INTERVAL: &str = "64mb";
const DEFAULT_PARALLELISM: u32 = 1;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_MAX_INFLIGHT_BATCHES: u32 = 16;

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            max_memory: DEFAULT_MAX_MEMORY.to_string(),
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES.to_string(),
            // ...
        }
    }
}
```

Then use `#[serde(default)]` at the struct level instead of per-field `#[serde(default = "fn_name")]`, letting the `Default` impl handle everything.

**Why P2:** Reduces the number of free functions from 8 to 0, and eliminates the risk of the serde default and `Default` impl diverging.

---

### 6.3 Datetime format constant &mdash; P2 / S

**File:** `state/sqlite.rs:61, 77, 102`

**Problem:** The datetime format string `"%Y-%m-%d %H:%M:%S"` appears in 3 places:

```rust
// sqlite.rs:61
NaiveDateTime::parse_from_str(&updated_at_str, "%Y-%m-%d %H:%M:%S")

// sqlite.rs:77
let updated_at = cursor.updated_at.format("%Y-%m-%d %H:%M:%S").to_string();

// sqlite.rs:102
let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
```

**After:**

```rust
/// SQLite datetime format (UTC, no timezone suffix).
const SQLITE_DATETIME_FMT: &str = "%Y-%m-%d %H:%M:%S";
```

**Why P2:** Three occurrences of the same format string. If one changes (e.g., to include timezone), the others must follow.

---

### 6.4 FK constraint documentation &mdash; P2 / S

**File:** `state/schema.rs:24-34`

**Problem:** `dlq_records.run_id` has no foreign key constraint to `sync_runs.id`:

```sql
CREATE TABLE IF NOT EXISTS dlq_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline TEXT NOT NULL,
    run_id INTEGER NOT NULL,   -- no FK to sync_runs
    ...
);
```

**After:** Either add the FK constraint:

```sql
run_id INTEGER NOT NULL REFERENCES sync_runs(id),
```

Or add a comment explaining why it's intentionally omitted (e.g., DLQ records may outlive run records if runs are pruned).

**Why P2:** Missing FKs are either a design choice or an oversight. Either way, document it.

---

### 6.5 Runner lifecycle DRY &mdash; P2 / M

**File:** `engine/runner.rs:222-238, 376-392, 509-525`

**Problem:** The close handler is copy-pasted 3 times across `run_source_stream`, `run_destination_stream`, and `run_transform_stream`:

```rust
match iface.call_close(&mut store) {
    Ok(Ok(())) => {}
    Ok(Err(err)) => {
        tracing::warn!(
            stream = stream_ctx.stream_name,
            "Source close failed: {}",
            source_error_to_sdk(err)
        );
    }
    Err(err) => {
        tracing::warn!(
            stream = stream_ctx.stream_name,
            "Source close trap: {}",
            err
        );
    }
}
```

The only differences are the log message prefix ("Source"/"Destination"/"Transform") and the error converter function.

**After:** Extract a helper:

```rust
fn handle_close_result<E: std::fmt::Display>(
    result: Result<Result<(), E>, wasmtime::Error>,
    role: &str,
    stream_name: &str,
    convert: impl Fn(E) -> String,
) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::warn!(stream = stream_name, "{} close failed: {}", role, convert(err)),
        Err(err) => tracing::warn!(stream = stream_name, "{} close trap: {}", role, err),
    }
}
```

**Why P2:** 3 copies of the same 15-line block. Any change to close error handling requires editing all three.

---

## Execution Order

### Phase 1: Safety & Structural Foundation (P0 items)

```
1.1  Type config fields as enums    ─── Do first (eliminates 7 match blocks)
     │
     ▼
3.1  Return Result from compression ─── Quick safety fix (independent)
     │
     ▼
2.1  Decompose execute_pipeline_once ── Largest change, benefits from 1.1
```

Do **1.1** first — it simplifies orchestrator.rs by ~85 lines, making the subsequent decomposition in 2.1 cleaner. **3.1** is independent and can be done anytime.

### Phase 2: Quality & DRY (P1 items)

```
1.2  Flatten PipelineResult         (independent)
2.2  Split ComponentHostState       (independent)
2.3  Deduplicate linker functions   (independent)
2.4  Factory for ComponentHostState (depends on 2.2)
3.2  Propagate Mutex lock errors    (independent — do alongside any file you touch)
3.3  Return Result from parse_byte_size (independent)
4.1  Module-level rustdoc           (independent — ongoing)
4.2  Test coverage                  (depends on 2.1 for orchestrator testability)
6.1  Extract magic numbers          (independent)
```

### Phase 3: Polish (P2 items)

```
1.3  Newtype PipelineId/StreamName
3.4  Log datetime parse fallback
4.3  Cache compiled regex
4.4  Encapsulate sqlite connection
6.2  Consolidate serde defaults
6.3  Datetime format constant
6.4  FK constraint documentation
6.5  Runner lifecycle DRY
```

Phase 3 items can be done opportunistically whenever touching nearby code.

---

## Summary Table

| # | Item | Principle | Priority | Effort | Primary File(s) |
|---|------|-----------|----------|--------|-----------------|
| 1.1 | Type config fields as enums | Types | **P0** | M | `types.rs`, `orchestrator.rs`, `validator.rs` |
| 1.2 | Flatten `PipelineResult` | Types | P1 | S | `runner.rs`, `orchestrator.rs` |
| 1.3 | Newtype `PipelineId`/`StreamName` | Types | P2 | S | `backend.rs`, `sqlite.rs` |
| 2.1 | Decompose `execute_pipeline_once` | Architecture | **P0** | L | `orchestrator.rs` |
| 2.2 | Split `ComponentHostState` | Architecture | P1 | L | `component_runtime.rs` |
| 2.3 | Deduplicate linker functions | DRY | P1 | S | `runner.rs`, `wasm_runtime.rs` |
| 2.4 | Factory for `ComponentHostState` | DRY | P1 | S | `runner.rs`, `component_runtime.rs` |
| 3.1 | Return `Result` from compression | Safety | **P0** | S | `compression.rs`, `component_runtime.rs` |
| 3.2 | Propagate Mutex lock errors | Safety | P1 | M | `component_runtime.rs`, `runner.rs`, `sqlite.rs` |
| 3.3 | Return `Result` from `parse_byte_size` | Safety | P1 | S | `types.rs`, `orchestrator.rs` |
| 3.4 | Log datetime parse fallback | Readability | P2 | S | `sqlite.rs` |
| 4.1 | Module-level rustdoc | Documentation | P1 | S | All files |
| 4.2 | Test coverage | Tooling | P1 | L | `orchestrator.rs`, `runner.rs`, `component_runtime.rs`, `wasm_runtime.rs`, `connector_resolve.rs` |
| 4.3 | Cache compiled regex | Performance | P2 | S | `parser.rs` |
| 4.4 | Encapsulate sqlite connection | Encapsulation | P2 | S | `sqlite.rs` |
| 6.1 | Extract magic numbers | Polish | P1 | S | `errors.rs`, `compression.rs`, `component_runtime.rs` |
| 6.2 | Consolidate serde defaults | Polish | P2 | S | `types.rs` |
| 6.3 | Datetime format constant | DRY | P2 | S | `sqlite.rs` |
| 6.4 | FK constraint documentation | Documentation | P2 | S | `schema.rs` |
| 6.5 | Runner lifecycle DRY | DRY | P2 | M | `runner.rs` |

**Totals:** 3 P0, 9 P1, 8 P2 &mdash; 20 items.
