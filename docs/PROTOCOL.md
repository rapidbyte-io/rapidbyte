# Rapidbyte Connector Protocol (v4)

This document defines the connector protocol used by Rapidbyte v4 (Wasmtime component runtime).

- Runtime: **Wasmtime component model**
- Connector target: **`wasm32-wasip2`**
- WIT package: **`rapidbyte:connector@4.0.0`** (`wit/rapidbyte-connector.wit`)
- Protocol version string in manifests/ConnectorInfo: **`4`**

## 1. Architecture

Rapidbyte runs connectors as WebAssembly components and connects stages with Arrow IPC batches:

- Source component exports `source`
- Destination component exports `destination`
- Transform component exports `transform`
- Host imports are provided via WIT `interface host`

There is no pointer/length host ABI (`rb_*`) in v4. All calls are typed through canonical ABI generated from WIT.

## 2. WIT Worlds

Defined in `wit/rapidbyte-connector.wit`:

- `world rapidbyte-source`
  - imports: `host`
  - exports: `source`
- `world rapidbyte-destination`
  - imports: `host`
  - exports: `destination`
- `world rapidbyte-transform`
  - imports: `host`
  - exports: `transform`
- `world rapidbyte-host`
  - imports: `host` (guest-side SDK bindings)

## 3. Connector Lifecycle

### 3.1 Source

1. `open(config-json)`
2. optional `discover()`
3. optional `validate()`
4. `run(session, request)` (one stream at a time)
5. `close()` (always called best-effort)

### 3.2 Destination

1. `open(config-json)`
2. optional `validate()`
3. `run(session, request)` (one stream at a time)
4. `close()`

### 3.3 Transform

1. `open(config-json)`
2. optional `validate()`
3. `run(session, request)` (one stream at a time)
4. `close()`

`config-json` is connector config serialized as JSON.
`request.stream-context-json` is `StreamContext` JSON serialized by host.

## 4. Host Import API (`interface host`)

### 4.1 Batch transport

- `emit-batch(handle: u64) -> result<_, connector-error>`
  - Source/transform publishes a sealed host frame to the next stage.
- `next-batch() -> result<option<u64>, connector-error>`
  - Destination/transform pulls the next sealed host frame handle.
  - `none` signals end-of-stream.
- Frame lifecycle:
  - `frame-new(capacity)` allocates a writable frame handle.
  - `frame-write(handle, chunk)` appends bytes into the frame.
  - `frame-seal(handle)` marks the frame immutable and publishable.
  - `frame-len`, `frame-read`, and `frame-drop` support receive/decode and cleanup.

### 4.2 Logging and telemetry

- `log(level: u32, msg: string)`
- `checkpoint(kind: u32, payload-json: string) -> result<_, connector-error>`
- `metric(payload-json: string) -> result<_, connector-error>`

`kind` values:
- `0`: source checkpoint
- `1`: destination checkpoint
- `2`: transform checkpoint

### 4.3 State

- `state-get(scope, key) -> result<option<string>, connector-error>`
- `state-put(scope, key, val) -> result<_, connector-error>`
- `state-cas(scope, key, expected, new-val) -> result<bool, connector-error>`

Scopes:
- `0` pipeline
- `1` stream
- `2` connector-instance

### 4.4 Host-proxied TCP

- `connect-tcp(host, port) -> result<u64, connector-error>`
- `socket-read(handle, len) -> result<socket-read-result, connector-error>`
- `socket-write(handle, data) -> result<socket-write-result, connector-error>`
- `socket-close(handle)`

`socket-read-result`:
- `data(list<u8>)`
- `eof`
- `would-block`

`socket-write-result`:
- `written(u64)`
- `would-block`

## 5. Error Model

All fallible connector lifecycle and host functions use `connector-error` from WIT.

Fields:
- category (`config|auth|permission|rate-limit|transient-*|data|schema|internal`)
- scope (`per-stream|per-batch|per-record`)
- code/message
- retry metadata (`retryable`, `retry-after-ms`, `backoff-class`, `safe-to-retry`)
- optional commit state (`before-commit|after-commit-unknown|after-commit-confirmed`)
- optional JSON details

Host preserves connector retry metadata and maps connector failures to `PipelineError::Connector`.

## 6. Data Exchange

- Batch payloads are Arrow IPC stream fragments written into host-managed frames
- Stage handoff uses frame handles (`u64`) via `emit-batch`/`next-batch`
- Optional host-side channel compression (`lz4`/`zstd`) is transparent to connectors
- Stream execution is sequential per connector instance (`run-*` called once per stream)

### 6.1 Stream Runtime Overrides

`ctx-json` (`StreamContext`) may include host-resolved runtime override hints that
connectors can consume without changing data semantics:

- `effective_parallelism` (`u32?`): effective stream worker fan-out selected by host.
- `partition_strategy` (`mod|range?`): source full-refresh sharding strategy override.
- `copy_flush_bytes_override` (`u64?`): destination COPY flush threshold override.

Override precedence for each knob is:

1. Explicit user pin
2. Host autotune decision
3. Connector/default fallback

Connectors must treat these overrides as performance hints only and must not change
write semantics, checkpoint semantics, or cursor semantics.

## 7. Checkpoint Coordination

Host stores source and destination checkpoint envelopes and advances persisted cursors only after correlating source+destination confirmation for a stream.

This preserves exactly-once semantics for incremental workflows where destination commit acknowledgment is required before cursor advancement.

## 8. Security and Permissions

Permissions are read from connector manifests (`permissions`):

- `env.allowed_vars`: only listed env vars are passed into WASI context
- `fs.preopens`: only declared host directories are preopened
- `network.allowed_domains` and `allow_runtime_config_domains` drive host ACL for `connect-tcp`

Current enforcement behavior:
- Host **enforces ACL** on `connect-tcp`
- Host uses non-blocking sockets and returns `would-block` variants
- Direct WASI network usage is disabled in host WASI context; connectors are expected to use host-proxied TCP

## 9. Manifest Compatibility

Host validates connector manifest role compatibility before run/check/discover:

- Source pipelines require `roles.source`
- Destination pipelines require `roles.destination`
- Transform pipelines require `roles.transform`

Host expects `manifest.protocol_version == "4"`; mismatches emit warnings.

## 10. Building Connectors

### 10.1 Language-Agnostic Contract

Any language that compiles to `wasm32-wasip2` and implements the WIT interface can be a Rapidbyte connector. The WIT file (`wit/rapidbyte-connector.wit`) is the source of truth — not any particular SDK.

A valid connector is a WASI component that:
1. Exports one of `source`, `destination`, or `transform`
2. Accepts JSON config via `open(config-json: string)`
3. Exchanges Arrow IPC batches via host imports (`emit-batch`/`next-batch`)
4. Returns structured `connector-error` records on failure

### 10.2 Rust SDK (recommended for Rust connectors)

The `rapidbyte-sdk` crate provides ergonomic Rust traits and macros:

- `#[connector(source)]` — exports a source component
- `#[connector(destination)]` — exports a destination component
- `#[connector(transform)]` — exports a transform component

The SDK handles WIT binding generation, config JSON deserialization, Tokio runtime management, and error type conversion.

For TCP clients (e.g. `tokio-postgres`), use `rapidbyte_sdk::host_tcp::HostTcpStream` with `connect_raw` to route through host-proxied networking.

### 10.3 Other Languages

Connectors can be written in any language with WASI component support:
- **Go:** Use `wit-bindgen-go` to generate bindings from the WIT file
- **Python:** Use `componentize-py` to compile Python to a WASI component
- **C/C++:** Use `wit-bindgen-c` for C bindings

The connector must implement the same WIT exports and call the same WIT imports regardless of language.

## SQL Transform (`transform-sql`)

Executes a SQL query against each incoming Arrow batch using Apache DataFusion.
The incoming data is registered as a table named `input`.

**Config:**

```yaml
transforms:
  - use: transform-sql
    config:
      query: "SELECT id, UPPER(name) AS name, age + 1 AS next_age FROM input WHERE active = true"
```

**Supported operations:** column selection, filtering (WHERE), computed columns,
type casting (CAST), string functions (UPPER, LOWER, TRIM, CONCAT), math
expressions, CASE/WHEN, and all standard SQL scalar functions supported by
DataFusion.

**Limitations:** Batch-by-batch execution — cross-batch aggregations (GROUP BY,
DISTINCT, window functions) operate per-batch, not across the full stream.

**Table name:** Always `input`. The query must reference `FROM input`.

## 11. Migration Notes

Removed in prior protocols:
- `rb_open`, `rb_run_read`, `rb_run_write`, `rb_close` C-ABI exports
- `rb_allocate` / `rb_deallocate` memory protocol
- `rb_host_*` pointer-based host imports
- Legacy runtime integration

Replaced by:
- Wasmtime component worlds generated from WIT
- typed canonical ABI for all host/guest calls

### Pipeline-Level Permissions & Limits

Pipeline operators can restrict connector sandbox capabilities and resource usage
beyond what the connector manifest declares. Add `permissions` and/or `limits`
blocks to any connector in the pipeline:

    source:
      use: source-postgres
      config: { ... }
      permissions:
        network:
          allowed_hosts: [db.production.internal, "*.analytics.corp"]
        env:
          allowed_vars: [DATABASE_URL]
        fs:
          allowed_preopens: [/data/exports]
      limits:
        max_memory: 128mb
        timeout_seconds: 60

**Permissions** are capability-based (can/cannot access X). Effective permissions
use set intersection — a capability is granted only if both manifest and pipeline
allow it.

**Limits** are quantitative bounds (how much). Effective limit = min(manifest, pipeline).

| Category | Manifest field | Pipeline field | Merge rule |
|----------|---------------|----------------|------------|
| Network | `allowed_domains` | `allowed_hosts` | Set intersection |
| Env vars | `allowed_vars` | `allowed_vars` | Set intersection |
| Filesystem | `preopens` | `allowed_preopens` | Set intersection |
| Memory | `limits.max_memory` | `limits.max_memory` | min() |
| Timeout | `limits.timeout_seconds` | `limits.timeout_seconds` | min() |

- Pipeline can only narrow, never widen.
- Omitted fields leave the manifest values unchanged.
- Empty lists (`[]`) block all access for that category.
- Default limits when neither specifies: memory = unlimited, timeout = 300s.
