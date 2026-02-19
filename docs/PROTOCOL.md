# Rapidbyte Connector Protocol v1

## 1. Overview & Design Philosophy

Rapidbyte is a data pipeline engine that executes connectors inside **WASI sandboxes** (WasmEdge runtime). Connectors exchange data as **Arrow IPC** record batches through a host-guest **FFI** boundary, with the host managing state, backpressure, and observability.

The protocol defines the contract between the host (Rapidbyte engine) and guest (connector WASM module): which functions each side exports, what data formats cross the boundary, and how state and errors are handled.

### Design Goals

| Goal | Mechanism |
|------|-----------|
| Streaming-first | Bounded `sync_channel` with configurable capacity; no unbounded buffering |
| Batch-bytes, not rows | Limits expressed in bytes (`max_batch_bytes`), stable memory regardless of row width |
| At-least-once semantics | Dual-checkpoint correlation: cursor advances only when source AND dest confirm |
| Typed errors with retry hints | 9 error categories, backoff classes, commit state tracking |
| Minimal host surface area | 9 host imports total: data (2), state (3), checkpoint (1), metric (1), log (1), error (1) |
| Sandbox isolation | WASI deny-by-default; network/fs/env controlled by manifest permissions |
| Performance | Bulk IO encouraged, buffer reuse, optional IPC compression (lz4/zstd), AOT compilation |

### Comparison with Other Systems

- **Airbyte:** JSON messages over stdin/stdout in Docker containers. Rapidbyte uses binary Arrow IPC over FFI in WASI sandboxes for lower overhead.
- **Estuary Flow:** gRPC between Docker containers with transactional materialization. Rapidbyte achieves similar checkpoint atomicity via dual-checkpoint correlation without gRPC overhead.
- **Sling:** Go in-process streaming with step-based YAML. Rapidbyte provides similar streaming semantics with sandbox isolation via WASM.

---

## 2. Terminology

The key words "MUST", "MUST NOT", "SHOULD", "SHOULD NOT", and "MAY" in this document are to be interpreted as described in RFC 2119.

| Term | Definition |
|------|------------|
| **Host** | The Rapidbyte engine process that loads WASM modules, manages channels, and persists state |
| **Guest** | A connector WASM module running inside a WasmEdge VM instance |
| **Pipeline** | A configured data flow: source → [transforms] → destination, defined in YAML |
| **Stream** | A named data source (e.g., a database table) within a pipeline |
| **Batch** | An Arrow IPC-encoded `RecordBatch` — the unit of data exchange |
| **Frame** | A channel message: either `Data(Vec<u8>)` or `EndStream` sentinel |
| **Checkpoint** | A progress marker emitted by a connector (source or dest) recording cursor position |
| **Manifest** | A JSON file declaring connector identity, roles, capabilities, permissions, and config schema |
| **Cursor** | A position marker for incremental sync (e.g., `updated_at = '2024-01-15T10:00:00'`) |
| **Envelope** | `PayloadEnvelope<T>` — wraps protocol messages with `protocol_version`, `connector_id`, `stream_name` |

---

## 3. Connector Roles & Manifest

### 3.1 Roles

A connector declares one or more roles via its manifest. Each role requires specific guest exports:

| Role | Required Exports | Optional Exports | Description |
|------|-----------------|------------------|-------------|
| **Source** | `rb_open`, `rb_run_read`, `rb_close`, `rb_allocate`, `rb_deallocate` | `rb_discover`, `rb_validate` | Reads data from external systems |
| **Destination** | `rb_open`, `rb_run_write`, `rb_close`, `rb_allocate`, `rb_deallocate` | `rb_validate` | Writes data to external systems |
| **Transform** | `rb_open`, `rb_run_read` (as pass-through), `rb_close`, `rb_allocate`, `rb_deallocate` | `rb_validate` | Transforms batches in-flight |
| **Utility** | `rb_open`, `rb_validate`, `rb_close`, `rb_allocate`, `rb_deallocate` | — | Validation-only (e.g., schema registry) |

### 3.2 ConnectorManifest

Stored as `<connector_name>.manifest.json` alongside the `.wasm` binary.

**Source:** `crates/rapidbyte-sdk/src/manifest.rs:157`

```json
{
  "manifest_version": "1.0",
  "id": "rapidbyte/source-postgres",
  "name": "PostgreSQL Source",
  "version": "0.1.0",
  "description": "Reads from PostgreSQL via streaming queries",
  "author": "Rapidbyte Inc.",
  "license": "Apache-2.0",
  "protocol_version": "1",
  "artifact": {
    "entry_point": "source_postgres.wasm",
    "checksum": "sha256:abcd1234...",
    "min_memory_mb": 128
  },
  "permissions": {
    "network": {
      "allowed_domains": null,
      "allow_runtime_config_domains": true,
      "tls": "optional"
    },
    "fs": {
      "preopens": []
    },
    "env": {
      "allowed_vars": ["PGSSLROOTCERT"]
    }
  },
  "roles": {
    "source": {
      "supported_sync_modes": ["full_refresh", "incremental"],
      "features": []
    }
  },
  "config_schema": {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["host", "port", "user", "database"],
    "properties": {
      "host": { "type": "string" },
      "port": { "type": "integer", "default": 5432 },
      "user": { "type": "string" },
      "password": { "type": "string" },
      "database": { "type": "string" }
    }
  }
}
```

**Optional fields:** `author`, `license`, `description`, `checksum`, `min_memory_mb`, and `config_schema` are all optional and omitted when absent. The `features` arrays in role capabilities are omitted when empty (`skip_serializing_if`).

### 3.3 Feature Flags

Connectors declare capabilities via feature enums.

**Source features** (`SourceFeature`):

| Feature | Description |
|---------|-------------|
| `cdc` | Supports Change Data Capture via logical replication |
| `stateful` | Maintains internal state across runs |

**Destination features** (`DestinationFeature`):

| Feature | Description |
|---------|-------------|
| `exactly_once` | Supports transactional commit tokens (not yet implemented) |
| `schema_auto_migrate` | Can automatically apply DDL changes |
| `bulk_load_copy` | Supports PostgreSQL `COPY` protocol for bulk loading |
| `bulk_load_copy_binary` | Supports binary-format `COPY` protocol |

**Protocol-level features** (`Feature` enum, returned from `rb_open`):

| Feature | Description |
|---------|-------------|
| `exactly_once` | Connector supports exactly-once delivery |
| `cdc` | Connector supports CDC sync mode |
| `schema_auto_migrate` | Connector handles schema changes automatically |
| `bulk_load_copy` | Supports COPY-based bulk loading |
| `bulk_load_copy_binary` | Supports binary COPY |

### 3.4 Manifest Validation

On pipeline startup, the host:

1. Loads the manifest from `<stem>.manifest.json` adjacent to the WASM file
2. Verifies the connector supports the required role (`Source` or `Destination`)
3. Warns on protocol version mismatch (current host: `"1"`)
4. Validates WASM binary checksum if `artifact.checksum` is present (`sha256:<hex>`)
5. Validates connector config against `config_schema` (JSON Schema Draft 7) before starting the VM

If no manifest file exists, the host proceeds without pre-flight validation (backwards compatibility).

---

## 4. Lifecycle

### 4.1 Sequence

```
Host                                Guest
 │                                    │
 │── rb_open(OpenContext) ──────────>│  ← MUST be first call
 │<── OpenInfo ─────────────────────│
 │                                    │
 │── rb_discover({}) ───────────────>│  ← Optional (source only)
 │<── Catalog ──────────────────────│
 │                                    │
 │── rb_validate({}) ───────────────>│  ← Optional (check pipeline)
 │<── ValidationResult ─────────────│
 │                                    │
 │── rb_run_read(StreamContext) ────>│  ← Source: emits batches via host_emit_batch
 │     [guest calls host_emit_batch]  │
 │     [guest calls host_checkpoint]  │
 │<── ReadSummary ──────────────────│
 │                                    │
 │── rb_run_write(StreamContext) ───>│  ← Dest: pulls batches via host_next_batch
 │     [guest calls host_next_batch]  │
 │     [guest calls host_checkpoint]  │
 │<── WriteSummary ─────────────────│
 │                                    │
 │── rb_close() ────────────────────>│  ← MUST be called, even on error
 │<── 0 (success) or -1 (error) ───│
```

### 4.2 Normative Rules

1. `rb_open` MUST be the first call to any connector instance.
2. `rb_close` MUST be called even if earlier calls returned errors. `rb_close` MUST be idempotent.
3. After `rb_open`, the host MAY call `rb_discover`, `rb_validate`, or `rb_run_read`/`rb_run_write`.
4. **v1 constraint:** One stream per `rb_run_*` call. Streams are processed sequentially.
5. Source and destination run on separate blocking threads; the host channel connects them.

### 4.3 Check Pipeline Flow

`rapidbyte check` validates a pipeline without running it:

1. Resolve connector paths
2. Load and validate manifests
3. Validate configs against manifest schemas
4. Check state backend connectivity
5. Call `rb_open` + `rb_validate` + `rb_close` on each connector
6. Report results

### 4.4 Discover Flow

`rapidbyte discover` queries a source for available streams:

1. Resolve connector path and load manifest
2. Call `rb_open` with connector config
3. Call `rb_discover` (no input needed)
4. Returns `Catalog` with stream definitions

---

## 5. FFI ABI — Guest Exports

### 5.1 Exported Functions

All functions use the C ABI. Input is passed as `(ptr: i32, len: i32)` pointing to JSON bytes in guest linear memory. Output is returned as a packed `i64`.

**Source:** `crates/rapidbyte-sdk/src/connector.rs`

| Export | Signature | Input | Output |
|--------|-----------|-------|--------|
| `rb_open` | `(ptr: i32, len: i32) -> i64` | `OpenContext` JSON | `ConnectorResult<OpenInfo>` |
| `rb_discover` | `(ptr: i32, len: i32) -> i64` | `{}` (unused) | `ConnectorResult<Catalog>` |
| `rb_validate` | `(ptr: i32, len: i32) -> i64` | `{}` (unused) | `ConnectorResult<ValidationResult>` |
| `rb_run_read` | `(ptr: i32, len: i32) -> i64` | `StreamContext` JSON | `ConnectorResult<ReadSummary>` |
| `rb_run_write` | `(ptr: i32, len: i32) -> i64` | `StreamContext` JSON | `ConnectorResult<WriteSummary>` |
| `rb_close` | `() -> i32` | — | `0` = success, `-1` = error |
| `rb_allocate` | `(size: i32) -> i32` | Byte count | Pointer to allocated memory |
| `rb_deallocate` | `(ptr: i32, capacity: i32)` | Pointer + capacity | — |

> **Note on `rb_validate`:** Although the ABI passes `(ptr, len)` input bytes (currently `OpenContext` JSON) to `rb_validate`, the SDK-generated FFI macro **ignores the input**. The connector's `validate()` method takes no parameters and relies on state set by `rb_open()`. Connectors that need to validate against specific configuration should read it from their stored state, not from the `rb_validate` input.

### 5.2 Return Value Packing

Functions returning `i64` pack a `(ptr, len)` pair:

**Source:** `crates/rapidbyte-sdk/src/memory.rs:22`

```
i64 = (ptr as i64) << 32 | (len as u32 as i64)
```

The host unpacks this to read `len` bytes of JSON from guest memory at `ptr`, then calls `rb_deallocate(ptr, len)` to free the allocation.

### 5.3 ConnectorResult Envelope

All `i64`-returning exports write JSON conforming to `ConnectorResult<T>`:

**Success:**
```json
{"status": "ok", "data": <T>}
```

**Error:**
```json
{"status": "err", "error": <ConnectorError>}
```

The `status` field is used as a discriminant (`#[serde(tag = "status")]`).

### 5.4 SDK Macros

The SDK provides macros that generate all FFI glue:

```rust
use rapidbyte_sdk::source_connector_main;

struct MySource { /* ... */ }
impl Default for MySource { /* ... */ }
impl SourceConnector for MySource { /* ... */ }

source_connector_main!(MySource);
```

Similarly, `dest_connector_main!(MyDest)` for destinations.

The macros generate:
- All `rb_*` FFI exports with JSON serialization/deserialization
- A `static CONNECTOR: OnceLock<SyncRefCell<T>>` for the singleton instance
- Re-exports of `rb_allocate` and `rb_deallocate`
- A no-op `main()` function (WASM entry point; not used directly)

---

## 6. Host Imports — Guest-Callable Functions

All host functions are registered under the `"rapidbyte"` WASM import module.

**Source:** `crates/rapidbyte-core/src/runtime/wasm_runtime.rs:46` (registration), `crates/rapidbyte-sdk/src/host_ffi.rs:14` (guest declarations)

### 6.1 Data Exchange

#### `rb_host_emit_batch(ptr: u32, len: u32) -> i32`

Source pushes an Arrow IPC batch to the host channel.

- `ptr`/`len`: Guest memory region containing IPC bytes
- **Returns:** `0` = success, `-1` = error (call `rb_host_last_error`)
- **Behavior:**
  - Blocks when the channel is full (backpressure)
  - Rejects zero-length batches as a protocol violation (`EMPTY_BATCH` error)
  - If compression is configured, the host compresses before sending to the channel
  - Increments internal `next_batch_id` on success

#### `rb_host_next_batch(out_ptr: u32, out_cap: u32) -> i32`

Destination pulls the next batch from the host channel.

- `out_ptr`/`out_cap`: Guest buffer for the host to write into
- **Returns:**
  - `> 0`: Number of bytes written to guest buffer
  - `0`: End-of-stream (EOF) — `EndStream` sentinel received or channel closed
  - `-1`: Error (call `rb_host_last_error`)
  - `< -1` (i.e., `-N`): Buffer too small; guest MUST resize to at least `N` bytes and retry
- **Behavior:**
  - Checks `pending_batch` first (stashed from previous too-small-buffer call)
  - Blocks on channel `recv()` if no batch is pending
  - If compression is configured, decompresses before writing to guest memory
  - Batches exceeding `i32::MAX` bytes trigger an error

### 6.2 State Management

#### `rb_host_state_get(scope: i32, key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32`

Read state value by scoped key.

- `scope`: `0` = Pipeline, `1` = Stream, `2` = ConnectorInstance
- **Returns:**
  - `> 0`: Bytes written to `out_ptr`
  - `0`: Key not found
  - `-1`: Error
  - `< -1` (i.e., `-N`): Buffer too small, need `N` bytes
- **Behavior:** Key is stored as `"{current_stream}:{key}"` regardless of scope (see Section 10.2). Maximum key length: 1024 bytes.

#### `rb_host_state_put(scope: i32, key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32`

Write state value by scoped key.

- **Returns:** `0` = success, `-1` = error
- **Behavior:** Stores a `CursorState` with the current timestamp. Key stored as `"{current_stream}:{key}"`.

#### `rb_host_state_cas(scope: i32, key_ptr: u32, key_len: u32, expected_ptr: u32, expected_len: u32, new_ptr: u32, new_len: u32) -> i32`

Compare-and-set: atomically update value only if current value matches `expected`.

- `expected_ptr=0, expected_len=0`: Insert-if-absent (succeeds only if key doesn't exist)
- **Returns:** `1` = CAS succeeded, `0` = CAS failed (value mismatch), `-1` = error

### 6.3 Checkpointing

#### `rb_host_checkpoint(kind: i32, payload_ptr: u32, payload_len: u32) -> i32`

Emit a checkpoint to the host.

- `kind`: `0` = Source, `1` = Dest, `2` = Transform
- `payload`: JSON-encoded `PayloadEnvelope<Checkpoint>`
- **Returns:** `0` = success, `-1` = error
- **Behavior:**
  - Source checkpoints (kind=0) are stored in `source_checkpoints` vec
  - Dest checkpoints (kind=1) are stored in `dest_checkpoints` vec
  - Transform checkpoints (kind=2) are logged but have no dedicated storage yet
  - After both source and dest complete, the orchestrator correlates checkpoints

### 6.4 Observability

#### `rb_host_metric(payload_ptr: u32, payload_len: u32) -> i32`

Emit a metric observation.

- `payload`: JSON-encoded `PayloadEnvelope<Metric>`
- **Returns:** `0` = success, `-1` = error
- **Behavior:** Currently logged via `tracing::debug`; no metrics backend yet.

#### `rb_host_log(level: i32, msg_ptr: i32, msg_len: i32) -> i32`

Log a message from the connector.

- `level`: `0` = error, `1` = warn, `2` = info, `3` = debug, `4+` = trace
- **Returns:** `0` always
- **Behavior:** Routed through `tracing` with pipeline and stream context fields.

### 6.5 Error Retrieval

#### `rb_host_last_error(out_ptr: u32, out_cap: u32) -> i32`

Retrieve and clear the last error from any host function.

- **Returns:**
  - `> 0`: Bytes written (JSON-encoded `ConnectorError`)
  - `0`: No error available
  - `< 0` (i.e., `-N`): Buffer too small, need `N` bytes (error is preserved for retry)
- **Semantics:** Read-and-clear. The error is consumed on successful read.
- **Note:** Unlike `next_batch` and `state_get`, `last_error` has no `-1` error sentinel — all negative values are size hints.

### 6.6 Summary Table

| Function | ABI Signature | Module |
|----------|---------------|--------|
| `rb_host_log` | `(i32, i32, i32) -> i32` | `rapidbyte` |
| `rb_host_emit_batch` | `(i32, i32) -> i32` | `rapidbyte` |
| `rb_host_next_batch` | `(i32, i32) -> i32` | `rapidbyte` |
| `rb_host_last_error` | `(i32, i32) -> i32` | `rapidbyte` |
| `rb_host_state_get` | `(i32, i32, i32, i32, i32) -> i32` | `rapidbyte` |
| `rb_host_state_put` | `(i32, i32, i32, i32, i32) -> i32` | `rapidbyte` |
| `rb_host_state_cas` | `(i32, i32, i32, i32, i32, i32, i32) -> i32` | `rapidbyte` |
| `rb_host_checkpoint` | `(i32, i32, i32) -> i32` | `rapidbyte` |
| `rb_host_metric` | `(i32, i32) -> i32` | `rapidbyte` |

Additionally, the host registers a `wasi_snapshot_preview1` module (via `WasiModule::create`) for WASI system calls (sockets, clocks, random, etc.).

---

## 7. Memory Protocol

### 7.1 Host-to-Guest Writes

When the host needs to pass data to the guest (e.g., `OpenContext` for `rb_open`):

1. Host calls `rb_allocate(size)` → gets pointer `ptr_in` in guest linear memory
2. Host writes bytes to guest memory via `memory.set_data(bytes, ptr_in)`
3. Host calls the guest function with `(ptr_in, len)`
4. Guest reads from `(ptr_in, len)`, processes, writes result via `write_guest_bytes`
5. Guest returns packed `i64` with result pointer `ptr_out` and length
6. Host reads result from guest memory, then calls `rb_deallocate(ptr_out, capacity)`

> **Note:** The host does **not** free the input allocation (`ptr_in` from step 1) after the guest function returns. This is a minor memory leak per call. In practice it is negligible because inputs are small JSON payloads (config, stream context) and each WASM VM instance is short-lived. Future protocol versions may require the guest to free input buffers explicitly.

### 7.2 Guest-to-Host Writes

When the guest needs to send data to the host (e.g., `emit_batch`):

1. Guest has bytes in its own memory (e.g., IPC-encoded batch)
2. Guest calls `rb_host_emit_batch(ptr, len)` where `ptr`/`len` reference guest memory
3. Host reads bytes from guest via `frame.memory_ref(0).get_data(offset, len)`
4. Host processes the bytes (e.g., sends to channel)

### 7.3 Buffer-Resize Protocol

For `rb_host_next_batch`, `rb_host_state_get`, and `rb_host_last_error`, the guest provides an output buffer. If the buffer is too small:

1. Host returns `-N` where `N` is the required buffer size in bytes
2. Guest resizes its buffer to at least `N` bytes
3. Guest retries the call with the larger buffer
4. Host writes data and returns the actual byte count

The SDK safe wrappers (`host_ffi.rs`) implement this resize loop automatically. For `next_batch`, a cap (`max_bytes`) prevents unbounded buffer growth. For `state_get`, the cap is 16MB.

> **Edge case:** When the required buffer size is exactly 1 byte, the host returns `-1`, which collides with the error sentinel used by `host_next_batch` and `host_state_get`. The SDK interprets `-1` as an error, calls `rb_host_last_error()`, finds no error, and the caller receives a generic `UNKNOWN_ERROR`. In practice 1-byte payloads are extremely rare (a single-byte IPC batch or state value is effectively useless), but this is a protocol ambiguity that may be resolved in a future version by reserving a distinct error sentinel (e.g., `i32::MIN`).

### 7.4 Allocation Helpers

**Source:** `crates/rapidbyte-sdk/src/memory.rs`

```rust
// Allocate in guest linear memory (called by host)
pub extern "C" fn rb_allocate(size: i32) -> i32

// Free guest memory (called by host after reading results)
pub extern "C" fn rb_deallocate(ptr: i32, capacity: i32)

// Pack (ptr, len) into i64 for return values
pub fn pack_ptr_len(ptr: i32, len: i32) -> i64

// Unpack i64 into (ptr, len)
pub fn unpack_ptr_len(packed: i64) -> (i32, i32)
```

> **Note:** These functions use `i32` pointers designed for `wasm32` linear memory. They will SIGSEGV on native 64-bit targets. Connector code MUST be tested on the `wasm32-wasip1` target.

> **Capacity invariant:** `write_guest_bytes` (used by SDK macros to return results) internally calls `data.to_vec()`, producing a `Vec` where `capacity == len`. The host uses `rb_deallocate(ptr, len)` to free the result — this is correct **only** because of this invariant. If a future SDK change produces a `Vec` with `capacity > len` (e.g., from `Vec::with_capacity` followed by a partial write), the host would deallocate with the wrong capacity, causing heap corruption. SDK implementors MUST preserve this invariant.

---

## 8. Data Exchange Format

### 8.1 Arrow IPC Stream Encoding

All data between source and destination is encoded as **Arrow IPC Stream** format. Each batch includes a schema message so the host can validate without external metadata.

The SDK does not prescribe a specific Arrow library — connectors use whichever Arrow implementation is available for their target (e.g., `arrow` crate for Rust).

### 8.2 Schema Hints

The `StreamContext` includes a `SchemaHint` to inform the destination about the expected schema:

```rust
enum SchemaHint {
    Columns(Vec<ColumnSchema>),      // Typed column list
    ArrowIpcSchema(Vec<u8>),         // Raw Arrow IPC schema bytes
}

struct ColumnSchema {
    name: String,
    data_type: String,    // Arrow type name (e.g., "Int64", "Utf8", "Timestamp")
    nullable: bool,
}
```

### 8.3 Limits

| Limit | Default | Description |
|-------|---------|-------------|
| `max_batch_bytes` | 64 MB | Maximum size of a single IPC batch |
| `max_record_bytes` | 16 MB | Maximum size of a single record within a batch |

Zero-length batches are a **protocol violation**. The host rejects them with an `EMPTY_BATCH` error.

**Zero-row sources:** If a source query returns 0 rows, the connector MUST NOT call `emit_batch`. Instead, it SHOULD emit a final checkpoint (if applicable for incremental mode, to record the cursor position) and return normally from `rb_run_read`. The host automatically sends an `EndStream` frame to the destination, which will receive EOF on its first `host_next_batch` call and return from `rb_run_write` with zero records written.

### 8.4 IPC Compression

Compression is transparent to connectors — applied by host functions:

- **`host_emit_batch`**: Compresses IPC bytes before sending to the channel
- **`host_next_batch`**: Decompresses bytes after receiving from the channel

Supported codecs:

| Codec | Library | Notes |
|-------|---------|-------|
| `lz4` | `lz4_flex` | Fast compression, moderate ratio |
| `zstd` | `zstd` (level 1) | Better ratio, slightly slower |

Configured via `resources.compression` in pipeline YAML. Default: no compression.

---

## 9. Channel Architecture & Backpressure

### 9.1 Channel Structure

Stages are connected by `mpsc::sync_channel<Frame>` with bounded capacity:

```
Source ──[channel 0]──> Transform₁ ──[channel 1]──> ... ──[channel N]──> Destination
```

For `N` transforms, there are `N+1` channels. With no transforms, a single channel connects source to destination directly.

### 9.2 Frame Type

**Source:** `crates/rapidbyte-core/src/runtime/host_functions.rs:18`

```rust
pub enum Frame {
    Data(Vec<u8>),   // IPC-encoded Arrow RecordBatch (must be non-empty)
    EndStream,       // Signals end of current stream
}
```

### 9.3 Capacity & Backpressure

- Channel capacity: `resources.max_inflight_batches` (default: **16**)
- `emit_batch` blocks when the channel is full — this is the backpressure mechanism
- Source and destination run on separate `tokio::task::spawn_blocking` threads

### 9.4 End-of-Stream

EOF is signaled by either:

1. The `EndStream` frame sentinel — `host_next_batch` returns `0` (clean completion)
2. Channel closure (sender dropped) — `host_next_batch` returns `0` (source error or crash)

The destination's pull loop treats both as EOF and returns from `rb_run_write`.

> **Caveat:** The destination cannot distinguish between clean completion and source failure — both return `0` from `host_next_batch`. This is safe under at-least-once semantics: if the source errors, `correlate_and_persist_cursors` (Section 10.4) will not advance the cursor because no source checkpoint exists for the failed run, and the next run re-reads from the previous position. However, the destination may have already committed a partial batch to the target database before learning the source failed. This produces **duplicates on retry**, which is expected and acceptable under at-least-once delivery. Destinations requiring exactly-once semantics must implement idempotent writes (e.g., upsert with primary key).

---

## 10. State & Checkpointing

### 10.1 State Backend

**Source:** `crates/rapidbyte-core/src/state/backend.rs`

The `StateBackend` trait provides:

```rust
trait StateBackend: Send + Sync {
    fn get_cursor(&self, pipeline: &str, stream: &str) -> Result<Option<CursorState>>;
    fn set_cursor(&self, pipeline: &str, stream: &str, cursor: &CursorState) -> Result<()>;
    fn compare_and_set(&self, pipeline: &str, stream: &str,
                       expected: Option<&str>, new_value: &str) -> Result<bool>;
    fn start_run(&self, pipeline: &str, stream: &str) -> Result<i64>;
    fn complete_run(&self, run_id: i64, status: RunStatus, stats: &RunStats) -> Result<()>;
}
```

Default implementation: **SQLite** (`rusqlite` with bundled SQLite). Default path: `~/.rapidbyte/state.db`. Configurable via `state.connection` in pipeline YAML.

### 10.2 State Scope

| Scope | Integer | Description |
|-------|---------|-------------|
| `Pipeline` | `0` | Pipeline-wide state |
| `Stream` | `1` | Per-stream state |
| `ConnectorInstance` | `2` | Reserved for future use |

> **Implementation note:** In the current implementation, all scopes use the same key format: `"{current_stream}:{key}"`. The scope integer is validated but does not affect key routing. Future versions may differentiate Pipeline-scoped keys (stored without stream prefix) from Stream-scoped keys.

### 10.3 Compare-and-Set

The CAS operation enables concurrency-safe state updates:

- `compare_and_set(pipeline, stream_key, expected, new_value)` — where `stream_key` is the scoped key (see Section 10.2)
- When `expected = None`: insert-if-absent (succeeds only if key doesn't exist)
- Returns `true` if the update was applied, `false` if the current value didn't match

### 10.4 Dual-Checkpoint Correlation

**Source:** `crates/rapidbyte-core/src/engine/checkpoint.rs`

The host advances the authoritative stream cursor ONLY when **both** conditions are met:

1. A source checkpoint exists for the stream (with cursor field + value)
2. A destination checkpoint exists confirming the same stream's data

```
correlate_and_persist_cursors(state_backend, pipeline, source_checkpoints, dest_checkpoints)
```

This gives correct at-least-once semantics even if the process crashes mid-stream. If the source emits a checkpoint but the destination hasn't confirmed, the cursor is NOT advanced — the next run will re-read from the previous position.

**Checkpoint independence:** Source and destination emit checkpoints **independently** — there are no in-band checkpoint markers flowing through the channel:

- **Source** checkpoints after emitting batches, recording cursor position (e.g., `updated_at` value of the last row read)
- **Destination** checkpoints after committing data, triggered by configurable thresholds (bytes written, rows written, or elapsed seconds — see Section 10.6)
- Correlation happens **post-hoc** in the host after both sides complete, via `correlate_and_persist_cursors`

This means a source that emits data but crashes before emitting a checkpoint will still cause a re-read on retry (no source checkpoint → no cursor advance). Conversely, a destination that commits data but crashes before emitting its final checkpoint will cause the same data to be re-committed on retry (no dest checkpoint → no cursor advance → source re-reads).

### 10.5 Checkpoint Structure

**Source:** `crates/rapidbyte-sdk/src/protocol.rs:241`

```rust
struct Checkpoint {
    id: u64,                              // Monotonic checkpoint ID
    kind: CheckpointKind,                 // Source | Dest | Transform
    stream: String,                       // Stream name
    cursor_field: Option<String>,         // e.g., "updated_at"
    cursor_value: Option<CursorValue>,    // e.g., Utf8("2024-01-15T10:00:00")
    records_processed: u64,               // Records since last checkpoint
    bytes_processed: u64,                 // Bytes since last checkpoint
}
```

Checkpoints are wrapped in `PayloadEnvelope<Checkpoint>` before sending to the host. Due to `#[serde(flatten)]`, the envelope fields merge into the top-level JSON (no nested `"payload"` key).

### 10.6 Checkpoint Intervals

| Interval | Default | Pipeline YAML Key | Description |
|----------|---------|-------------------|-------------|
| Bytes | 64 MB | `resources.checkpoint_interval_bytes` | Checkpoint after this many bytes written |
| Rows | 0 (disabled) | `resources.checkpoint_interval_rows` | Checkpoint after this many rows |
| Seconds | 0 (disabled) | `resources.checkpoint_interval_seconds` | Checkpoint after this many seconds |

### 10.7 CursorValue Variants

| Variant | JSON Example | Notes |
|---------|-------------|-------|
| `Null` | `{"null": null}` | No cursor value |
| `Int64(i64)` | `{"int64": 42}` | Integer cursor |
| `Utf8(String)` | `{"utf8": "2024-01-15"}` | String/timestamp cursor |
| `TimestampMillis(i64)` | `{"timestamp_millis": 1700000000000}` | Millisecond timestamp |
| `TimestampMicros(i64)` | `{"timestamp_micros": 1700000000000000}` | Microsecond timestamp |
| `Decimal{value, scale}` | `{"decimal": {"value": "123.45", "scale": 2}}` | Decimal cursor |
| `Json(Value)` | `{"json": {...}}` | Arbitrary JSON |

> **Implementation note:** The orchestrator currently hardcodes `CursorType::Utf8` and uses a fallback chain (String → i64 → i32) for cursor extraction.

---

## 11. Error Model

### 11.1 ConnectorError Structure

**Source:** `crates/rapidbyte-sdk/src/errors.rs:91`

```rust
struct ConnectorError {
    category: ErrorCategory,
    scope: ErrorScope,
    code: String,               // Machine-readable error code (e.g., "CONN_RESET")
    message: String,            // Human-readable description
    retryable: bool,
    retry_after_ms: Option<u64>,
    backoff_class: BackoffClass,
    safe_to_retry: bool,
    commit_state: Option<CommitState>,
    details: Option<serde_json::Value>,
}
```

### 11.2 Error Categories

| Category | Retryable | Default Backoff | Default Scope | Use Case |
|----------|-----------|----------------|---------------|----------|
| `config` | No | Normal | Stream | Missing/invalid configuration |
| `auth` | No | Normal | Stream | Authentication failure |
| `permission` | No | Normal | Stream | Insufficient privileges |
| `rate_limit` | Yes | Slow (5s) | Stream | API/service rate limiting |
| `transient_network` | Yes | Normal (1s) | Stream | Connection reset, DNS failure |
| `transient_db` | Yes | Normal (1s) | Stream | Deadlock, lock timeout |
| `data` | No | Normal | Record | Invalid data (type mismatch, etc.) |
| `schema` | No | Normal | Stream | Schema incompatibility |
| `internal` | No | Normal | Stream | Bug or unexpected state |

### 11.3 Error Scope

| Scope | Description |
|-------|-------------|
| `stream` | Affects the entire stream; no further processing possible |
| `batch` | Affects the current batch; other batches may succeed |
| `record` | Affects a single record; governed by `DataErrorPolicy` |

### 11.4 Commit State

Destination errors SHOULD include commit state when relevant:

| State | Description | `safe_to_retry` |
|-------|-------------|-----------------|
| `before_commit` | Failure occurred before any data was committed | `true` |
| `after_commit_unknown` | Commit may or may not have succeeded | Forced to `false` |
| `after_commit_confirmed` | Data was committed; don't retry, just proceed | N/A |

When `CommitState::AfterCommitUnknown` is set, the SDK automatically sets `safe_to_retry = false` to prevent duplicate writes.

### 11.5 Backoff Computation

**Source:** `crates/rapidbyte-core/src/engine/errors.rs:62`

If `retry_after_ms` is set on the error, that value is used directly. Otherwise:

```
delay_ms = base_ms * 2^(attempt - 1)
```

| Backoff Class | Base | Example (attempts 1, 2, 3) |
|---------------|------|---------------------------|
| `fast` | 100ms | 100ms, 200ms, 400ms |
| `normal` | 1000ms | 1s, 2s, 4s |
| `slow` | 5000ms | 5s, 10s, 20s |

Maximum delay: **60 seconds**. Maximum retry attempts: `resources.max_retries` (default: **3**).

### 11.6 DataErrorPolicy

| Policy | Behavior |
|--------|----------|
| `fail` | Abort the pipeline on any data error (default) |
| `skip` | Skip the offending record and continue |
| `dlq` | Route to dead-letter queue (not yet implemented) |

### 11.7 ConnectorResult Envelope

```rust
#[serde(tag = "status", rename_all = "snake_case")]
enum ConnectorResult<T> {
    Ok { data: T },
    Err { error: ConnectorError },
}
```

### 11.8 ValidationResult

Returned by `rb_validate`:

```rust
struct ValidationResult {
    status: ValidationStatus,  // success | failed | warning
    message: String,
}
```

---

## 12. Schema Handling

### 12.1 Schema Flow

1. **Source** emits Arrow IPC batches with embedded schema per stream
2. **Host** can enforce schema evolution policies
3. **Destination** receives schema + batches and applies to target system

### 12.2 SchemaEvolutionPolicy

**Source:** `crates/rapidbyte-sdk/src/protocol.rs:177`

| Change | Policy Options | Default |
|--------|---------------|---------|
| New column at source | `add` / `ignore` / `fail` | `add` |
| Removed column at source | `add`* / `ignore` / `fail` | `ignore` |
| Type change | `coerce` / `fail` / `null` | `fail` |
| Nullability change | `allow` / `fail` | `allow` |

\* `add` is not meaningful for removed columns; use `ignore` or `fail`.

### 12.3 Discovery

Sources MAY implement `rb_discover` to return a `Catalog`:

```rust
struct Catalog {
    streams: Vec<Stream>,
}

struct Stream {
    name: String,
    schema: Vec<ColumnSchema>,
    supported_sync_modes: Vec<SyncMode>,
    source_defined_cursor: Option<String>,
    source_defined_primary_key: Option<Vec<String>>,
}
```

---

## 13. Security & Capabilities

### 13.1 WASI Sandbox

Connectors run in a **deny-by-default** WASI sandbox. The host controls what system resources the connector can access through the manifest's `permissions` section.

### 13.2 Network Permissions

```rust
struct NetworkPermissions {
    allowed_domains: Option<Vec<String>>,   // Static allowlist; ["*"] = all
    allow_runtime_config_domains: bool,     // Host extracts domains from validated config
    tls: TlsRequirement,                   // required | optional | forbidden
}
```

> **Known limitation:** Network ACLs are declarative only in the current implementation. WasmEdge v0.14 does not enforce network restrictions at the WASI layer. The manifest declares intent, but the host does not block unauthorized connections at runtime.

### 13.3 Filesystem Permissions

```rust
struct FsPermissions {
    preopens: Vec<String>,  // Directories mounted into the sandbox
}
```

Default: no filesystem access.

### 13.4 Environment Variable Permissions

```rust
struct EnvPermissions {
    allowed_vars: Vec<String>,  // Only listed vars forwarded to guest
}
```

Default: no environment variables.

### 13.5 Checksum Verification

Manifests MAY declare a checksum for the WASM binary:

```json
"artifact": {
  "checksum": "sha256:abcdef1234..."
}
```

The host verifies the SHA-256 hash of the WASM file before loading. Checksum mismatch aborts the pipeline with an error.

### 13.6 Secret Handling

- Secrets are passed as opaque values in `OpenContext.config`
- Connectors MUST NOT log secrets
- The host SHOULD redact sensitive fields in log output

---

## 14. Metrics & Logging

### 14.1 Metric Types

**Source:** `crates/rapidbyte-sdk/src/protocol.rs:276`

```rust
enum MetricValue {
    Counter(u64),      // Monotonically increasing count
    Gauge(f64),        // Point-in-time measurement
    Histogram(f64),    // Distribution observation
}

struct Metric {
    name: String,
    value: MetricValue,
    labels: Vec<(String, String)>,
}
```

### 14.2 Standard Metrics

Connectors SHOULD emit these metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `records_read` / `records_written` | Counter | Total records processed |
| `bytes_read` / `bytes_written` | Counter | Total bytes processed |
| `batches_emitted` / `batches_written` | Counter | Total batches |
| `checkpoint_count` | Counter | Checkpoints emitted |

### 14.3 Performance Summaries

Connectors report per-phase timing in their summaries:

**ReadPerf** (source):

| Field | Description |
|-------|-------------|
| `connect_secs` | Time to establish connection |
| `query_secs` | Time executing the query |
| `fetch_secs` | Time fetching result rows |
| `arrow_encode_secs` | Time encoding to Arrow IPC |

**WritePerf** (destination):

| Field | Description |
|-------|-------------|
| `connect_secs` | Time to establish connection |
| `flush_secs` | Time writing/flushing data |
| `commit_secs` | Time committing transaction |
| `arrow_decode_secs` | Time decoding from Arrow IPC |

### 14.4 Logging

- Level `0` = error, `1` = warn, `2` = info, `3` = debug, `4+` = trace
- Host routes through `tracing` with automatic `pipeline` and `stream` context fields
- Messages are prefixed with `[connector]` in host log output

---

## 15. Pipeline Configuration Reference

### 15.1 Full YAML Example

**Source:** `crates/rapidbyte-core/src/pipeline/types.rs`

```yaml
version: "1.0"
pipeline: pg_to_pg_incremental

source:
  use: rapidbyte/source-postgres@v0.1.0
  config:
    host: localhost
    port: 5432
    user: postgres
    password: secret
    database: source_db
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
    - name: orders
      sync_mode: full_refresh

transforms:
  - use: rapidbyte/transform-mask@v0.1.0
    config:
      columns:
        - email
        - phone

destination:
  use: rapidbyte/dest-postgres@v0.1.0
  config:
    host: localhost
    port: 5433
    user: postgres
    password: secret
    database: dest_db
    schema: raw
  write_mode: upsert
  primary_key:
    - id
  on_data_error: skip

state:
  backend: sqlite
  connection: /tmp/pipeline_state.db

resources:
  max_memory: 512mb
  max_batch_bytes: 128mb
  parallelism: 1
  checkpoint_interval_bytes: 32mb
  checkpoint_interval_rows: 5000
  checkpoint_interval_seconds: 30
  max_retries: 3
  compression: lz4
  max_inflight_batches: 32
```

### 15.2 Field Reference

#### Top-Level

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `version` | string | Yes | — | Config format version (e.g., `"1.0"`) |
| `pipeline` | string | Yes | — | Pipeline name (used as state key) |
| `source` | object | Yes | — | Source connector configuration |
| `transforms` | array | No | `[]` | Transform pipeline (applied in order) |
| `destination` | object | Yes | — | Destination connector configuration |
| `state` | object | No | sqlite defaults | State backend configuration |
| `resources` | object | No | defaults | Resource limits and tuning |

#### `source`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `use` | string | Yes | Connector reference (e.g., `"rapidbyte/source-postgres@v0.1.0"`) |
| `config` | object | Yes | Connector-specific configuration (passed as `OpenContext.config`) |
| `streams` | array | Yes | Stream definitions |

#### `source.streams[]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Stream name (e.g., table name) |
| `sync_mode` | string | Yes | `"full_refresh"`, `"incremental"`, or `"cdc"` |
| `cursor_field` | string | No | Column used for incremental cursor |

#### `transforms[]`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `use` | string | Yes | Transform connector reference |
| `config` | object | Yes | Transform-specific configuration |

#### `destination`

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `use` | string | Yes | — | Connector reference |
| `config` | object | Yes | — | Connector-specific configuration |
| `write_mode` | string | Yes | — | `"append"`, `"replace"`, or `"upsert"` |
| `primary_key` | array | No | `[]` | Primary key columns (required for `upsert`) |
| `on_data_error` | string | No | `"fail"` | `"fail"`, `"skip"`, or `"dlq"` |

#### `state`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `backend` | string | `"sqlite"` | State backend type |
| `connection` | string | `~/.rapidbyte/state.db` | Backend connection string/path |

#### `resources`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_memory` | string | `"256mb"` | Maximum memory for connector VM |
| `max_batch_bytes` | string | `"64mb"` | Maximum batch size |
| `parallelism` | u32 | `1` | Parallelism level |
| `checkpoint_interval_bytes` | string | `"64mb"` | Checkpoint every N bytes |
| `checkpoint_interval_rows` | u64 | `0` (disabled) | Checkpoint every N rows |
| `checkpoint_interval_seconds` | u64 | `0` (disabled) | Checkpoint every N seconds |
| `max_retries` | u32 | `3` | Max retry attempts for retryable errors |
| `compression` | string | none | IPC compression: `"lz4"` or `"zstd"` |
| `max_inflight_batches` | u32 | `16` | Channel capacity (backpressure) |

### 15.3 Connector Reference Format

Connector references follow this format:

```
[namespace/]connector-name[@vX.Y.Z]
```

Examples:
- `rapidbyte/source-postgres@v0.1.0` → id=`source-postgres`, version=`0.1.0`
- `source-postgres@v0.1.0` → id=`source-postgres`, version=`0.1.0`
- `source-postgres` → id=`source-postgres`, version=`unknown`

Resolution order:
1. `RAPIDBYTE_CONNECTOR_DIR` environment variable
2. `~/.rapidbyte/plugins/`

Connector name is mapped to filename by replacing hyphens with underscores: `source-postgres` → `source_postgres.wasm`.

### 15.4 Byte Size Parsing

Byte size strings are parsed case-insensitively:

| Suffix | Multiplier | Example |
|--------|-----------|---------|
| `gb` | 1024³ | `"1gb"` = 1,073,741,824 |
| `mb` | 1024² | `"64mb"` = 67,108,864 |
| `kb` | 1024 | `"512kb"` = 524,288 |
| (none) | 1 | `"1024"` = 1,024 |

---

## 16. Performance Requirements

### 16.1 MUST

- **No per-row host calls.** All data exchange is batch-based.
- **No unbounded buffering.** Connectors MUST NOT buffer all batches in memory before flushing.
- **Respect `max_batch_bytes`.** Batches exceeding the limit are protocol violations.
- **Reuse buffers.** The SDK's `next_batch` wrapper reuses a `Vec<u8>` across calls.
- **Prefer bulk IO.** Databases: use COPY/bulk insert, prepared statements. HTTP: use pagination with bounded concurrency, gzip, keep-alive.

### 16.2 SHOULD

- **Report per-phase timing.** Populate `ReadPerf`/`WritePerf` in summaries for observability.
- **Support AOT compilation.** WASM modules compiled with `wasmedge compile foo.wasm foo.so` load in ~4ms vs ~500ms for interpreted WASM.

### 16.3 AOT Compilation

WasmEdge supports ahead-of-time compilation:

```bash
wasmedge compile source_postgres.wasm source_postgres.so
```

The host loads `.so` files the same way as `.wasm` files. Module load time drops from ~500ms to ~4ms.

---

## 17. Known Limitations & Roadmap

| Item | Status | Notes |
|------|--------|-------|
| **CDC** | Protocol-ready | `SyncMode::Cdc` exists; no connector implements logical replication yet. Single-table CDC works with the current one-stream-at-a-time model. Multi-table CDC (e.g., a PostgreSQL logical replication slot streaming changes for multiple tables) requires multiplexed channels or a demuxing layer, which the current architecture does not support |
| **Exactly-once delivery** | Feature flag exists | `Feature::ExactlyOnce` declared; no commit token implementation |
| **Dead-letter queue** | Enum variant exists | `DataErrorPolicy::Dlq` parses but routes nowhere |
| **Network ACL enforcement** | Declarative only | Manifest declares `allowed_domains`; not enforced at WASI layer in WasmEdge v0.14 |
| **Multi-stream parallelism** | Sequential in v1 | One stream per `rb_run_*` call; future versions may parallelize |
| **OCI distribution** | Not implemented | Connectors loaded from local filesystem; no registry/pull support |
| **Pull-mode sources** | Not implemented | All sources use push mode (`emit_batch`); spec mentions pull but code doesn't support it |
| **Transform checkpoints** | Received but not stored | Transform checkpoints (kind=2) are logged but have no dedicated storage |
| **Metrics backend** | Logging only | `rb_host_metric` logs via `tracing`; no Prometheus/OTLP export |
| **Serialization format** | JSON only | Spec mentioned MessagePack/protobuf; implementation uses JSON everywhere |
| **ConnectorInstance scope** | Reserved | `StateScope::ConnectorInstance` (value 2) exists but is not used |
| **Crash vs clean EOF** | Indistinguishable | `host_next_batch` returns `0` for both `EndStream` (clean) and channel drop (source crash); destination cannot detect source failure. At-least-once semantics protect against data loss but duplicates are possible on retry (see §9.4) |
| **Buffer resize `-1` collision** | Edge case | When required buffer size is exactly 1 byte, host returns `-1` which collides with the error sentinel; SDK treats this as an error. Harmless in practice — 1-byte payloads are not meaningful (see §7.3) |
| **Input memory leak** | By design | Host allocates input via `rb_allocate` but never calls `rb_deallocate` on input buffers after the guest function returns. Negligible for small JSON inputs on short-lived VM instances (see §7.1) |

---

## Appendix A: Type Reference

### A.1 OpenContext

```json
{
  "config": {"json": {"host": "localhost", "port": 5432, "user": "postgres", "database": "mydb"}},
  "connector_id": "source-postgres",
  "connector_version": "0.1.0"
}
```

### A.2 OpenInfo

```json
{
  "protocol_version": "1",
  "features": ["bulk_load_copy"],
  "default_max_batch_bytes": 67108864
}
```

### A.3 StreamContext

```json
{
  "stream_name": "users",
  "schema": {"columns": [{"name": "id", "data_type": "Int64", "nullable": false}]},
  "sync_mode": "incremental",
  "cursor_info": {
    "cursor_field": "updated_at",
    "cursor_type": "timestamp_micros",
    "last_value": {"timestamp_micros": 1700000000000000}
  },
  "limits": {
    "max_batch_bytes": 67108864,
    "max_record_bytes": 16777216,
    "max_inflight_batches": 16,
    "max_parallel_requests": 1,
    "checkpoint_interval_bytes": 67108864,
    "checkpoint_interval_rows": 0,
    "checkpoint_interval_seconds": 0
  },
  "policies": {
    "on_data_error": "fail",
    "schema_evolution": {
      "new_column": "add",
      "removed_column": "ignore",
      "type_change": "fail",
      "nullability_change": "allow"
    }
  },
  "write_mode": {"append": null}
}
```

### A.4 ReadSummary

```json
{
  "records_read": 1000,
  "bytes_read": 65536,
  "batches_emitted": 10,
  "checkpoint_count": 2,
  "records_skipped": 0,
  "perf": {
    "connect_secs": 0.05,
    "query_secs": 0.8,
    "fetch_secs": 2.3,
    "arrow_encode_secs": 0.15
  }
}
```

### A.5 WriteSummary

```json
{
  "records_written": 1000,
  "bytes_written": 65536,
  "batches_written": 10,
  "checkpoint_count": 2,
  "records_failed": 0,
  "perf": {
    "connect_secs": 0.1,
    "flush_secs": 1.5,
    "commit_secs": 0.05,
    "arrow_decode_secs": 0.12
  }
}
```

### A.6 TransformSummary

```json
{
  "records_in": 1000,
  "records_out": 950,
  "bytes_in": 65536,
  "bytes_out": 60000,
  "batches_processed": 10
}
```

### A.7 Checkpoint (in PayloadEnvelope)

Due to `#[serde(flatten)]`, the envelope fields merge to top level:

```json
{
  "protocol_version": "1",
  "connector_id": "source-postgres",
  "stream_name": "users",
  "id": 1,
  "kind": "source",
  "stream": "users",
  "cursor_field": "id",
  "cursor_value": {"int64": 42},
  "records_processed": 100,
  "bytes_processed": 5000
}
```

### A.8 Metric (in PayloadEnvelope)

```json
{
  "protocol_version": "1",
  "connector_id": "source-postgres",
  "stream_name": "users",
  "name": "rows_read",
  "value": {"counter": 42},
  "labels": [["stream", "users"]]
}
```

### A.9 ConnectorError

```json
{
  "category": "transient_network",
  "scope": "stream",
  "code": "CONN_RESET",
  "message": "connection reset by peer",
  "retryable": true,
  "retry_after_ms": null,
  "backoff_class": "normal",
  "safe_to_retry": true,
  "commit_state": null,
  "details": null
}
```

### A.10 ValidationResult

```json
{
  "status": "success",
  "message": "Connection established and permissions verified"
}
```

### A.11 Catalog

```json
{
  "streams": [
    {
      "name": "users",
      "schema": [
        {"name": "id", "data_type": "Int64", "nullable": false},
        {"name": "email", "data_type": "Utf8", "nullable": true},
        {"name": "created_at", "data_type": "Timestamp", "nullable": true}
      ],
      "supported_sync_modes": ["full_refresh", "incremental"],
      "source_defined_cursor": "id",
      "source_defined_primary_key": ["id"]
    }
  ]
}
```

### A.12 ConnectorManifest

See Section 3.2 for a complete example.

---

## Appendix B: Mapping from CONNECTOR-spec.md

| Old Section (CONNECTOR-spec.md) | New Section (PROTOCOL.md) |
|--------------------------------|--------------------------|
| Goals | 1. Overview & Design Philosophy |
| Connector Types | 3. Connector Roles & Manifest |
| Runtime Model | 4. Lifecycle |
| Data Exchange Format | 8. Data Exchange Format |
| Standard Lifecycle | 4. Lifecycle, 5. FFI ABI |
| Execution Patterns | 6. Host Imports (emit_batch, next_batch) |
| State + Checkpointing | 10. State & Checkpointing |
| Delivery Semantics | 10.4 Dual-Checkpoint Correlation, 17. Known Limitations |
| Error Model (Unified) | 11. Error Model |
| Schema Handling | 12. Schema Handling |
| Performance Requirements | 16. Performance Requirements |
| Security + Capabilities | 13. Security & Capabilities |
| Standard Metrics + Logging | 14. Metrics & Logging |
| Standard FFI (Wasm ABI) | 5. FFI ABI, 6. Host Imports, 7. Memory Protocol |
| Standard Stream Context | 15.2 Field Reference (StreamContext) |
| Destination Write Modes | 15.2 Field Reference (destination.write_mode) |
