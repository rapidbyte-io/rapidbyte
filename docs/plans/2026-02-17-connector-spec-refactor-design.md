# Connector Protocol v1 Refactor Design

Refactor both connectors (source-postgres, dest-postgres), the SDK, host runtime, and orchestrator to conform to `docs/CONNECTOR-spec.md`.

## Scope

**In scope:**
- New lifecycle: `open` / `run_read` / `run_write` / `close`
- Pull-based destination (dest calls `host_next_batch` in a loop)
- Typed error enum with categories, scopes, retry hints
- Explicit checkpoint protocol with ordering-based correlation
- Scoped state (`pipeline` / `stream` / `connector_instance`)
- `host_metric()` API
- `host_last_error()` for structured error retrieval
- Protocol version gate (reject old-protocol connectors)

**Deferred to later:**
- `compare_and_set` (CAS) for state
- Exactly-once delivery semantics
- Schema evolution enforcement
- Per-record error handling (skip/fail/DLQ)
- MessagePack/protobuf encoding (keep JSON)
- Pull-mode source (keep push-only for v1)
- Multi-stream parallelism (sequential per-stream in v1)

## Approach

Bottom-up: SDK types -> host functions -> orchestrator -> connectors -> cleanup.

Each layer is independently testable before the next depends on it.

---

## 1. SDK Protocol Types

### Config encoding (future-proofed)

```rust
pub enum ConfigBlob {
    Json(serde_json::Value),
    // future: MsgPack(Vec<u8>), Proto(Vec<u8>)
}
```

Use `ConfigBlob` in `OpenContext`, `validate`, etc. Internally only JSON for now. Avoids a type refactor when encoding negotiation is added.

### Lifecycle types

```rust
pub struct OpenContext {
    pub config: ConfigBlob,
    pub connector_id: String,
    pub connector_version: String,
}

pub struct OpenInfo {
    pub protocol_version: String,       // "1"
    pub features: Vec<Feature>,
    pub default_max_batch_bytes: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Feature {
    ExactlyOnce,
    Cdc,
    SchemaAutoMigrate,
    BulkLoadCopy,
    BulkLoadCopyBinary,
}
```

### Stream context

Passed into `rb_run_read` / `rb_run_write`. Replaces old `ReadRequest` / `WriteRequest`.

```rust
pub struct StreamContext {
    pub stream_name: String,
    pub schema: SchemaHint,
    pub sync_mode: SyncMode,
    pub cursor_info: Option<CursorInfo>,
    pub limits: StreamLimits,
    pub policies: StreamPolicies,
    pub write_mode: Option<WriteMode>,   // dest only
}

pub enum SchemaHint {
    Columns(Vec<ColumnSchema>),
    ArrowIpcSchema(Vec<u8>),
    // ArrowIpcSchema: schema-only IPC message (no RecordBatch payload).
    // Arrow IPC includes version in the flatbuffer header.
}

pub struct StreamLimits {
    pub max_batch_bytes: u64,
    pub max_record_bytes: u64,
    pub max_inflight_batches: u32,
    pub max_parallel_requests: u32,
}

pub struct StreamPolicies {
    pub on_data_error: DataErrorPolicy,
    pub schema_evolution: SchemaEvolutionPolicy,
}

pub enum DataErrorPolicy { Skip, Fail, Dlq }

pub struct SchemaEvolutionPolicy {
    pub new_column: ColumnPolicy,
    pub removed_column: ColumnPolicy,
    pub type_change: TypeChangePolicy,
    pub nullability_change: NullabilityPolicy,
}

pub enum ColumnPolicy { Add, Ignore, Fail }
pub enum TypeChangePolicy { Coerce, Fail, Null }
pub enum NullabilityPolicy { Allow, Fail }
```

### Cursors (strongly typed, all variants matched)

```rust
pub enum CursorType {
    Int64, Utf8, TimestampMillis, TimestampMicros, Decimal, Json,
}

pub enum CursorValue {
    Null,
    Int64(i64),
    Utf8(String),
    TimestampMillis(i64),
    TimestampMicros(i64),
    Decimal { value: String, scale: i32 },
    Json(serde_json::Value),
}

pub struct CursorInfo {
    pub cursor_field: String,
    pub cursor_type: CursorType,
    pub last_value: Option<CursorValue>,
}
```

### Errors (typed, scoped, with retry hints)

```rust
pub enum ErrorCategory {
    Config,              // never retry
    Auth,                // never retry until config changes
    Permission,          // never retry
    RateLimit,           // retry with backoff
    TransientNetwork,    // retry
    TransientDb,         // retry
    Data,                // per-row policy (skip/fail/dlq)
    Schema,              // coerce/fail depending on policy
    Internal,            // fail fast
}

pub enum ErrorScope { Stream, Batch, Record }
pub enum BackoffClass { Fast, Normal, Slow }
pub enum CommitState { BeforeCommit, AfterCommitUnknown, AfterCommitConfirmed }

pub struct ConnectorError {
    pub category: ErrorCategory,
    pub scope: ErrorScope,
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
    pub backoff_class: BackoffClass,
    pub safe_to_retry: bool,
    pub commit_state: Option<CommitState>,
    pub details: Option<serde_json::Value>,
}
```

Constructors enforce category -> retryable invariants:

- `Config` / `Auth` / `Permission`: `retryable = false`
- `RateLimit` / `TransientNetwork` / `TransientDb`: `retryable = true`
- `Data` / `Schema`: depends on policy
- `Internal`: `retryable = false`

```rust
impl ConnectorError {
    pub fn config(code: &str, msg: &str) -> Self { /* retryable: false, scope: Stream */ }
    pub fn auth(code: &str, msg: &str) -> Self { /* retryable: false */ }
    pub fn transient_network(code: &str, msg: &str) -> Self { /* retryable: true, backoff: Normal */ }
    pub fn transient_db(code: &str, msg: &str) -> Self { /* retryable: true, backoff: Normal */ }
    pub fn rate_limit(code: &str, msg: &str, retry_after: Option<u64>) -> Self { /* retryable: true, backoff: Slow */ }
    pub fn data(code: &str, msg: &str) -> Self { /* scope: Record */ }
    pub fn internal(code: &str, msg: &str) -> Self { /* retryable: false */ }
}
```

### Checkpoints

```rust
pub enum CheckpointKind { Source, Dest }

pub struct Checkpoint {
    pub id: u64,
    pub kind: CheckpointKind,
    pub stream: String,
    pub cursor_field: Option<String>,
    pub cursor_value: Option<CursorValue>,
    pub records_processed: u64,
    pub bytes_processed: u64,
}
```

See "Checkpoint correlation" section below for semantics.

### State

```rust
pub enum StateScope { Pipeline, Stream, ConnectorInstance }
```

### Summaries

```rust
pub struct ReadSummary {
    pub records_read: u64,
    pub bytes_read: u64,
    pub batches_emitted: u64,
    pub checkpoint_count: u64,
}

pub struct WritePerf {
    pub connect_secs: f64,
    pub flush_secs: f64,
    pub commit_secs: f64,
}

pub struct WriteSummary {
    pub records_written: u64,
    pub bytes_written: u64,
    pub batches_written: u64,
    pub checkpoint_count: u64,
    pub perf: Option<WritePerf>,
}
```

### Metrics

```rust
pub enum MetricValue { Counter(u64), Gauge(f64), Histogram(f64) }

pub struct Metric {
    pub name: String,
    pub value: MetricValue,
    pub labels: Vec<(String, String)>,
}
```

### Removed types

- `ReadRequest` (replaced by `StreamContext`)
- `WriteRequest` (replaced by `StreamContext`)
- Old `ConnectorResult<T>` wrapper
- Old `ConnectorError` (code+message only)

---

## 2. Host Function Interface

### Error code convention (all host functions)

```
 >0  = success (byte count written)
  0  = EOF / not-found (context-dependent)
 -1  = error (call rb_host_last_error for details)
 -N  = buffer too small, need N bytes (where N > 1)
```

### ABI: u32 for pointers, i32 for return codes

All pointer/length/capacity parameters are `u32` at the ABI boundary (wasm32 addresses are u32). Return codes are `i32`.

### Guest-side FFI declarations

```rust
extern "C" {
    // Data exchange
    fn rb_host_emit_batch(ptr: u32, len: u32) -> i32;
    fn rb_host_next_batch(out_ptr: u32, out_cap: u32) -> i32;

    // Error retrieval
    fn rb_host_last_error(out_ptr: u32, out_cap: u32) -> i32;

    // State (scoped)
    fn rb_host_state_get(scope: i32, key_ptr: u32, key_len: u32,
                         out_ptr: u32, out_cap: u32) -> i32;
    fn rb_host_state_put(scope: i32, key_ptr: u32, key_len: u32,
                         val_ptr: u32, val_len: u32) -> i32;

    // Checkpointing
    fn rb_host_checkpoint(kind: i32, payload_ptr: u32, payload_len: u32) -> i32;

    // Observability
    fn rb_host_log(level: i32, msg_ptr: u32, msg_len: u32) -> i32;
    fn rb_host_metric(payload_ptr: u32, payload_len: u32) -> i32;
    fn rb_host_report_progress(records: i64, bytes: i64) -> i32;
}
```

### emit_batch semantics

- Blocks until channel has capacity. Backpressure is the host's job.
- Returns 0 on success, -1 on error.
- Stream name is NOT passed per-batch; it's established by the `StreamContext` passed to `rb_run_read`.
- Host must enforce `len <= max_batch_bytes + overhead`. Reject oversized batches with error.

### next_batch semantics

- Dest calls in a loop inside `rb_run_write`.
- Returns `>0` = bytes written, `0` = EOF (source done), `-N` = need N bytes, `-1` = error.
- Host checks `pending_batch` first (stashed from previous too-small-buffer call), then reads from channel receiver.
- On "buffer too small": host stashes the batch in `HostState.pending_batch`, returns `-N`. Next call returns the stashed batch without re-reading from channel.
- On memory write error or bounds failure: host stashes the batch (don't lose it), sets last_error, returns -1.

### last_error semantics

- Read+clear: `rb_host_last_error` returns the last error and clears it.
- Every successful host call clears `last_error`.
- Every failed host call sets `last_error`.
- Returns JSON-serialized `ConnectorError`.

### state_get / state_put

- `scope` parameter: 0=Pipeline, 1=Stream, 2=ConnectorInstance.
- **Host anchors scope=Stream to `HostState.current_stream`**. A connector cannot read/write another stream's state.
- `state_get` uses the `-N` retry pattern for buffer sizing. Guest wrapper caps at `max_batch_bytes` to prevent OOM.
- Keys must be limited in length (e.g. 1024 bytes). Host rejects oversized keys with error.

### checkpoint / metric payloads

JSON-serialized with envelope:

```json
{
    "protocol_version": "1",
    "connector_id": "source-postgres",
    "stream_name": "users",
    "checkpoint": { "id": 42, "kind": "source", ... }
}
```

Same envelope for metrics. Typed Rust structs in SDK, serialized once at FFI boundary.

### Safe wrappers

`emit_batch(ipc_bytes)` -- straightforward, check rc.

`next_batch(buf, max_bytes)` -- safe Vec handling:
1. Ensure buf has allocated backing memory.
2. `unsafe { buf.set_len(buf.capacity()) }` so host can write into the full allocation.
3. Call host function with `(buf.as_mut_ptr() as u32, buf.len() as u32)`.
4. `unsafe { buf.set_len(0) }` immediately (before any early return).
5. On success `n > 0`: `unsafe { buf.set_len(n as usize) }`.
6. On `-N`: `reserve_exact(needed)`, retry once. Cap at `max_bytes` to prevent OOM.

`state_get(scope, key)` -- same resize pattern, capped at 16MB.

`fetch_last_error()` -- call `rb_host_last_error`, parse JSON, fallback to `Internal` if parse fails.

### HostState additions

```rust
pub struct HostState {
    // existing
    pub pipeline_name: String,
    pub current_stream: String,
    pub state_backend: Arc<dyn StateBackend>,
    pub stats: Arc<Mutex<RunStats>>,

    // source only
    pub batch_sender: Option<std::sync::mpsc::SyncSender<Vec<u8>>>,
    pub next_batch_id: u64,

    // dest only
    pub batch_receiver: Option<std::sync::mpsc::Receiver<Vec<u8>>>,
    pub pending_batch: Option<Vec<u8>>,

    // error retrieval
    pub last_error: Option<ConnectorError>,

    // checkpoint tracking
    pub source_checkpoints: Vec<Checkpoint>,
    pub dest_checkpoint_count: u64,
}
```

### Removed host functions

- `rb_host_emit_record_batch` (replaced by `rb_host_emit_batch`)
- `rb_host_get_state` (old 4-arg, replaced by scoped 5-arg)
- `rb_host_set_state` (old 4-arg, replaced by scoped 5-arg)

---

## 3. Orchestrator Lifecycle

### Current flow

```
Source:  rb_init(config) -> rb_read(ReadRequest)
Dest:   rb_init(config) -> rb_write_batch(stream, batch) x N -> rb_write_finalize()
```

### New flow

```
Source:  rb_open(OpenContext) -> rb_run_read(StreamContext) -> rb_close()
Dest:   rb_open(OpenContext) -> rb_run_write(StreamContext) -> rb_close()
```

### Per-stream channels (sequential v1)

One stream at a time. Each stream gets its own dedicated `sync_channel<Vec<u8>>`:

```rust
for stream in &config.streams {
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(
        stream.limits.max_inflight_batches as usize
    );

    // Spawn source with batch_sender = Some(tx)
    // Spawn dest with batch_receiver = Some(rx)
    // Source and dest run concurrently for THIS stream
    // Join both, collect results
    // Next stream
}
```

This preserves the invariant: stream identity is fixed in StreamContext, channel carries raw `Vec<u8>` only, no stream ID needed.

### Threading model

Source and dest both run on `tokio::task::spawn_blocking`. This is the existing pattern. `SyncSender::send()` blocks when channel full (backpressure). `Receiver::recv()` blocks until batch available (pull). No async runtime threads blocked.

### check_pipeline changes

```
Current: validate_connector(wasm, config) -> rb_validate
New:     rb_open(ctx) -> rb_validate(config) -> rb_close()
```

Validate happens after open so the connector has config/connections established.

### ConnectorHandle

```rust
impl ConnectorHandle {
    pub fn open(&mut self, ctx: OpenContext) -> Result<OpenInfo>;
    pub fn run_read(&mut self, ctx: StreamContext) -> Result<ReadSummary>;
    pub fn run_write(&mut self, ctx: StreamContext) -> Result<WriteSummary>;
    pub fn close(&mut self) -> Result<()>;
    pub fn validate(&mut self, config: ConfigBlob) -> Result<ValidationResult>;
    pub fn discover(&mut self) -> Result<Catalog>;

    // REMOVED: init(), read(), write_batch(), write_finalize()
}
```

### Protocol version gate

On module load, the orchestrator checks that the Wasm module exports `rb_open`, plus `rb_run_read` or `rb_run_write`, plus `rb_close`. If old exports are detected (`rb_init`, `rb_read`), fail with: "Connector uses deprecated protocol v0. Rebuild with rapidbyte-sdk >= 0.5."

---

## 4. Checkpoint Correlation

### v1: Ordering-based (no framing)

Since v1 runs one stream per channel and batches are delivered in-order:

- **Batch IDs are implicit per stream run and start at 1.**
- Source emits checkpoints with monotonically increasing `id` (batch sequence number).
- Destination emits checkpoints with monotonically increasing `id` (batch sequence number).
- The host correlates by `id` equality.

**A source checkpoint with `id = k`** means: batches 1..k have been emitted and the cursor is valid up to that point.

**A destination checkpoint with `id = k`** means: batches 1..k are durably committed.

**The host advances the authoritative stream cursor** only when it holds both a source checkpoint and a destination checkpoint for the same `id`.

### Required invariant

For a given stream run, the Nth batch emitted by the source is the Nth batch delivered to the destination, without skipping or reordering.

This requires:
- `host_emit_batch` never drops batches (blocking send on bounded channel).
- `host_next_batch` never skips ahead (reads sequentially from receiver).
- "Buffer too small" does not consume a new batch (`pending_batch` stash).

### v2 (deferred): Host framing for multi-stream parallelism

When multi-stream parallel channels are added, prepend 8-byte LE batch_id to each payload. Not needed for v1.

---

## 5. Connector Rewrites

### Exported Wasm functions (both connectors)

```
rb_allocate(size: i32) -> i32           (from SDK, unchanged)
rb_deallocate(ptr: i32, cap: i32)       (from SDK, unchanged)
rb_open(ptr: u32, len: u32) -> i64
rb_validate(ptr: u32, len: u32) -> i64
rb_close() -> i32
```

Source adds: `rb_discover(ptr: u32, len: u32) -> i64`, `rb_run_read(ptr: u32, len: u32) -> i64`

Dest adds: `rb_run_write(ptr: u32, len: u32) -> i64`

Removed: `rb_init`, `rb_read`, `rb_write_batch`, `rb_write_finalize`

### Source-postgres: rb_run_read

Owns the full read loop:
1. Connect to PG.
2. Validate stream name with `validate_pg_identifier()`.
3. BEGIN transaction, DECLARE CURSOR.
4. FETCH in chunks, accumulate rows, emit batches via `host_ffi::emit_batch()`.
5. Checkpoint using threshold accumulator (not modulo):
   ```
   since_checkpoint_bytes += ipc_bytes.len();
   if since_checkpoint_bytes >= checkpoint_interval_bytes {
       emit checkpoint; since_checkpoint_bytes = 0;
   }
   ```
6. CLOSE CURSOR, COMMIT.
7. Final checkpoint.
8. Return `ReadSummary`.

### Dest-postgres: rb_run_write

Owns the full write loop (pull-based):
1. Connect to PG.
2. Validate identifiers.
3. BEGIN transaction (both INSERT and COPY modes).
4. Pull loop:
   ```
   loop {
       match host_ffi::next_batch(&mut buf, ctx.limits.max_batch_bytes)? {
           None => break,  // EOF
           Some(len) => {
               write batch (INSERT or COPY)
               checkpoint
               report progress
           }
       }
   }
   ```
5. COMMIT.
6. Return `WriteSummary`.

Both INSERT and COPY modes wrap in a transaction. On error, transaction rolls back (implicit on client drop or explicit ROLLBACK).

### Error usage

```rust
// Before: make_err_response("CONNECTION_FAILED", &e.to_string())
// After:  Err(ConnectorError::transient_db("CONNECTION_FAILED", &format!("PG connect: {}", e)))
```

Typed constructors enforce correct category/retryable invariants.

### SQL injection prevention

All stream names, schema names, and column names pass through `validate_pg_identifier()` before any SQL interpolation. Schema queries use parameterized queries where possible (`$1` for `table_name` in `information_schema.columns`).

---

## 6. Cleanup Phase

After everything works on the new protocol:

**Remove from SDK:** old `ReadRequest`, `WriteRequest`, old `ConnectorResult<T>`, old `ConnectorError`, old host FFI bindings (`rb_host_emit_record_batch`, old `rb_host_get_state`/`rb_host_set_state`).

**Remove from host runtime:** old host function registrations, old `ConnectorHandle` methods (`init`, `read`, `write_batch`, `write_finalize`).

**Remove from connectors:** old exports (`rb_init`, `rb_read`, `rb_write_batch`, `rb_write_finalize`).

**Protocol gate:** orchestrator rejects modules without `rb_open`/`rb_close` with a clear error message.

---

## Layer-by-layer implementation order

1. SDK protocol types (new types alongside old)
2. SDK host FFI bindings (new functions alongside old)
3. Host runtime: register new host functions
4. Host runtime: add `host_next_batch`, `host_last_error`, `host_checkpoint`, `host_metric`, scoped `host_state_get/put`
5. Orchestrator: new lifecycle (`open` -> `run_read/write` -> `close`), per-stream channels
6. Source-postgres: rewrite to new protocol
7. Dest-postgres: rewrite to new protocol
8. Cleanup: remove old types, functions, exports, add protocol gate
9. E2E + benchmark verification
