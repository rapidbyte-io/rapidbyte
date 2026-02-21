# Rapidbyte Connector Protocol (v2)

This document defines the connector protocol used by Rapidbyte v2 (Wasmtime component runtime).

- Runtime: **Wasmtime component model**
- Connector target: **`wasm32-wasip2`**
- WIT package: **`rapidbyte:connector@2.0.0`** (`wit/rapidbyte-connector.wit`)
- Protocol version string in manifests/OpenInfo: **`2`**

## 1. Architecture

Rapidbyte runs connectors as WebAssembly components and connects stages with Arrow IPC batches:

- Source component exports `source-connector`
- Destination component exports `dest-connector`
- Transform component exports `transform-connector`
- Host imports are provided via WIT `interface host`

There is no pointer/length host ABI (`rb_*`) in v2. All calls are typed through canonical ABI generated from WIT.

## 2. WIT Worlds

Defined in `wit/rapidbyte-connector.wit`:

- `world rapidbyte-source`
  - imports: `host`
  - exports: `source-connector`
- `world rapidbyte-destination`
  - imports: `host`
  - exports: `dest-connector`
- `world rapidbyte-transform`
  - imports: `host`
  - exports: `transform-connector`
- `world rapidbyte-host`
  - imports: `host` (guest-side SDK bindings)

## 3. Connector Lifecycle

### 3.1 Source

1. `open(config-json)`
2. optional `discover()`
3. optional `validate()`
4. `run-read(ctx-json)` (one stream at a time)
5. `close()` (always called best-effort)

### 3.2 Destination

1. `open(config-json)`
2. optional `validate()`
3. `run-write(ctx-json)` (one stream at a time)
4. `close()`

### 3.3 Transform

1. `open(config-json)`
2. optional `validate()`
3. `run-transform(ctx-json)` (one stream at a time)
4. `close()`

`config-json` is connector config serialized as JSON.
`ctx-json` is `StreamContext` JSON serialized by host.

## 4. Host Import API (`interface host`)

### 4.1 Batch transport

- `emit-batch(batch: list<u8>) -> result<_, connector-error>`
  - Source/transform pushes an Arrow IPC frame to the next stage.
  - Zero-length frames are rejected as protocol violations.
- `next-batch() -> result<option<list<u8>>, connector-error>`
  - Destination/transform pulls the next Arrow IPC frame.
  - `none` signals end-of-stream.

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

- Batch payloads are Arrow IPC stream fragments (`list<u8>`)
- Optional host-side channel compression (`lz4`/`zstd`) is transparent to connectors
- Stream execution is sequential per connector instance (`run-*` called once per stream)

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

Host expects `manifest.protocol_version == "2"`; mismatches emit warnings.

## 10. Building Connectors

### 10.1 Language-Agnostic Contract

Any language that compiles to `wasm32-wasip2` and implements the WIT interface can be a Rapidbyte connector. The WIT file (`wit/rapidbyte-connector.wit`) is the source of truth — not any particular SDK.

A valid connector is a WASI component that:
1. Exports one of `source-connector`, `dest-connector`, or `transform-connector`
2. Accepts JSON config via `open(config-json: string)`
3. Exchanges Arrow IPC batches via host imports (`emit-batch`/`next-batch`)
4. Returns structured `connector-error` records on failure

### 10.2 Rust SDK (recommended for Rust connectors)

The `rapidbyte-sdk` crate provides ergonomic Rust traits and macros:

- `source_connector_main!(Type)` — exports a source component
- `dest_connector_main!(Type)` — exports a destination component
- `transform_connector_main!(Type)` — exports a transform component

The SDK handles WIT binding generation, config JSON deserialization, Tokio runtime management, and error type conversion.

For TCP clients (e.g. `tokio-postgres`), use `rapidbyte_sdk::host_tcp::HostTcpStream` with `connect_raw` to route through host-proxied networking.

### 10.3 Other Languages

Connectors can be written in any language with WASI component support:
- **Go:** Use `wit-bindgen-go` to generate bindings from the WIT file
- **Python:** Use `componentize-py` to compile Python to a WASI component
- **C/C++:** Use `wit-bindgen-c` for C bindings

The connector must implement the same WIT exports and call the same WIT imports regardless of language.

## 11. Migration Notes from v1

Removed in v2:
- `rb_open`, `rb_run_read`, `rb_run_write`, `rb_close` C-ABI exports
- `rb_allocate` / `rb_deallocate` memory protocol
- `rb_host_*` pointer-based host imports
- Legacy runtime integration

Replaced by:
- Wasmtime component worlds generated from WIT
- typed canonical ABI for all host/guest calls
