# Rapidbyte Connector Protocol v1

## Goals

- **Streaming-first:** no unbounded buffering; bounded memory by design.
- **Batch-bytes, not rows:** stable memory independent of row width.
- **At-least-once** core semantics, with optional exactly-once where destinations support it.
- **Deterministic retries:** standardized error taxonomy + retry hints.
- **Minimal host surface area:** Arrow + state + observability + capabilities.
- **Performance:** zero/low-copy where possible, amortized crossings, bulk IO (COPY/load) encouraged.

## Connector Types

A connector module can implement any subset of these roles:

- **Source**
- **Destination**
- **Transform** (optional, DataFusion UDF or batch transform plugin)
- **Utility** (e.g., schema registry, validation-only)

Each connector declares its roles in a manifest (OCI artifact metadata).

## Runtime Model

### 1) Single long-lived instance per run

For each pipeline run and stream, the host creates one connector instance and calls:

1. `open()` once
2. `run_*()` (read/write/transform) once per stream
3. `close()` once

No "init per batch", no per-row callbacks. Everything is batch-streaming.

### 2) One stream at a time per instance (v1)

In v1, each instance handles exactly one stream per `run_*()` call. Parallelism is achieved by multiple instances.

This keeps correctness simple and avoids shared mutable state bugs.

## Data Exchange Format

**Canonical format:** Arrow RecordBatch

- Data between stages uses Arrow RecordBatch.
- Transport is Arrow IPC Stream encoding.
- Each batch must include a schema (or schema id + metadata) so the host can validate.

### Constraints

Batches must not exceed:

- `max_batch_bytes` (hard)
- `max_batch_rows` (optional soft guard)

If a source receives a "giant record" that would exceed `max_batch_bytes`, it must:

- either split the record into child structures if supported,
- or emit `DataError::RecordTooLarge` for that record (configurable "skip vs fail").

## Standard Lifecycle

### `open(ctx)`

Called once per instance. Must:

- Validate config minimally (structural)
- Create clients/pools (HTTP, DB)
- Apply capability grants (see [Security](#security--capabilities))
- Return `OpenInfo`: connector version, supported features, default limits

### `discover()`

Optional but recommended. Returns catalogs/streams and schema hints.

### `validate()`

Must do real connectivity/auth checks. Returns actionable failures with typed errors.

### `run_read(stream_ctx)`

Source reads and emits batches to host (push model) or host pulls (pull model). v1 supports both; pick one per connector for simplicity.

### `run_write(stream_ctx)`

Destination pulls batches from host and writes. Must implement standardized checkpointing (see [State](#state--checkpointing)).

### `close()`

Release resources. Must be idempotent.

## Execution Patterns (Standardized)

Source must implement one of:

### A) Push mode

- Source calls `host.emit_batch(batch)` repeatedly.
- Host applies backpressure by blocking the call if downstream is full.

### B) Pull mode

- Host calls `source.next_batch()` until EOF.

Pick one; don't mix.

### Destination is always pull mode

- Destination calls `host.next_batch()` until EOF.
- This avoids the host copying IPC into guest memory repeatedly if you design it right (see [FFI](#standard-ffi-wasm-abi)).

## State + Checkpointing

### State primitives

State is owned by the host (SQLite/Postgres). Connectors interact via:

- `state.get(scope, key) -> bytes?`
- `state.put(scope, key, bytes)`
- `state.compare_and_set(scope, key, expected, new)` (optional but recommended for concurrency-safe CDC)

Where `scope` is one of:

- `pipeline`
- `stream`
- `connector_instance` (rare)

### Standard checkpoint contract

Connectors must report progress using standardized checkpoints:

- **Source checkpoint:** "I have emitted data up to cursor X"
- **Dest checkpoint:** "I have durably committed data up to cursor X"

#### Rules

The host only advances the authoritative stream cursor when:

1. it has received a source checkpoint, **and**
2. it has received a destination checkpoint for the corresponding range.

This gives you correct at-least-once semantics even if the process crashes mid-stream.

### Checkpoint granularity

Connector must support:

- `checkpoint_interval_bytes` (default e.g. 16-64MB)
- `checkpoint_interval_rows` (optional)
- `checkpoint_interval_seconds` (optional)

Destination should checkpoint after durable commit.

## Delivery Semantics

### Default: At-least-once

Replays after crash are allowed.

Destinations must support either:

- idempotent writes (upsert/merge), or
- a dedupe strategy (primary key + watermark), or
- transactional replace (swap tables), or
- a staging+commit pattern.

### Optional: Exactly-once (Feature flag)

If a destination supports transactional semantics and can atomically apply batches:

1. Connector advertises `feature.exactly_once=true`
2. Host uses commit_token coordination:
   - stage with token
   - commit token once
   - retries safe

## Error Model (Unified)

All connector functions return either success or a typed `ConnectorError`.

### Error categories

| Category | Retry behavior |
|---|---|
| `ConfigError` | Never retry |
| `AuthError` | Never retry until config changes |
| `PermissionError` | Never retry |
| `RateLimitError` | Retry with backoff + `retry_after` if available |
| `TransientNetworkError` | Retry |
| `TransientDbError` | Retry |
| `DataError` | Per-row or per-batch; user policy decides skip/fail/DLQ |
| `SchemaError` | Coerce/fail depending on policy |
| `InternalError` | Bug/unsafe state; generally fail fast |

### Retry hints

Errors may include:

- `retryable: bool`
- `retry_after_ms: Option<u64>`
- `backoff_class: {fast, normal, slow}`
- `safe_to_retry: bool` (important for dest operations)

### Partial failure rules

A destination must clearly indicate if failure happened:

- `before_commit` -- safe to retry
- `after_commit_unknown` -- danger zone; host should query verification if supported
- `after_commit_confirmed` -- don't retry, just proceed

## Schema Handling

### Schema in v1

- Source emits Arrow schema per stream.
- Host can enforce contracts and evolution policy.
- Destination receives schema + batches.

### Standard evolution policies (host-configured)

| Change | Options |
|---|---|
| New column | `add` \| `ignore` \| `fail` |
| Removed column | `ignore` \| `fail` |
| Type change | `coerce` \| `fail` \| `null` |
| Nullability change | `allow` \| `fail` |

### Connector responsibilities

- **Source:** must include stable field names/types; nested types allowed.
- **Destination:** must either:
  - auto-migrate (if supported), or
  - request host to apply migrations (preferred), or
  - fail with actionable `SchemaError`.

## Performance Requirements (Normative)

### Musts

- No per-row host calls.
- No unbounded in-memory buffering (no "buffer all batches then flush").
- Must respect `max_batch_bytes`.
- Must avoid repeated allocations:
  - reuse buffers
  - pool encoders/decoders
- Prefer bulk IO:
  - DB: COPY/bulk insert, prepared statements
  - HTTP: pagination concurrency bounded, gzip, keep-alive

### Shoulds

- Support compression for IPC (lz4/zstd) as optional feature flag.
- Expose metrics per stage: fetch, decode, transform, encode, flush, commit.

## Security + Capabilities

### Manifest-declared capabilities

**Network:**

- `allowed_hosts` (static)
- `runtime_hosts` (resolved from pipeline config)
- `tls`: `required` | `optional` | `forbidden`

**Filesystem:**

- none by default
- explicit preopens if needed (rare)

**Env:**

- none by default
- allowlist vars if needed (prefer host-passed config)

### Enforced by host

- Host must apply deny-by-default.
- Connector cannot expand privileges at runtime.

### Secret handling

- Secrets are passed as opaque strings/bytes in config.
- Connector must not log secrets; host should also redact.

## Standard Metrics + Logging

Every connector must emit (via host functions):

- `records_read`, `records_written`
- `bytes_read`, `bytes_written`
- `batches_emitted`, `batches_written`
- `checkpoint_count`
- `retry_count`
- Latencies: `fetch_ms`, `decode_ms`, `flush_ms`, `commit_ms`

Logs must be structured with:

- `pipeline_id`, `stream`, `connector_id`/`version`, `run_id`

## Standard FFI (Wasm ABI)

To keep it fast and simple, use a single request/response ABI with opaque bytes.

### Core exported functions (Wasm -> Host calls these)

```
rb_open(ptr, len) -> Status
rb_discover(ptr, len) -> Status        (optional)
rb_validate(ptr, len) -> Status
rb_run_read(ptr, len) -> Status        (source)
rb_run_write(ptr, len) -> Status       (dest)
rb_close() -> Status
```

Where request bytes are encoded as:

- MessagePack or protobuf (pick one; protobuf is faster + typed)
- Avoid JSON in hot paths.

### Core host functions (Host -> Wasm imports)

```
host_next_batch() -> (ptr, len) | EOF      (dest pulls IPC bytes)
host_emit_batch(ptr, len)                  (source pushes IPC bytes)
host_state_get(scope, key) -> bytes?
host_state_put(scope, key, bytes)
host_log(level, ptr, len)
host_metric(name, value, labels)
host_checkpoint(kind, ptr, len)            where kind = {source, dest}
```

> **Key perf trick:** `host_next_batch()` should return a view into a host-managed shared buffer or a single copied buffer reused per call, not allocate a fresh `Vec` each time.

## Standard Stream Context

`StreamContext` (passed into run) includes:

- `stream_name`
- `schema` (Arrow)
- `sync_mode` (full / incremental / cdc)
- `cursor_info` (field / type / last_value)
- **limits:**
  - `max_batch_bytes`
  - `max_inflight_batches`
  - `max_parallel_requests` (for SaaS sources)
- **policies:**
  - schema evolution policy
  - `on_data_error` policy (skip / fail / dlq)
- **destination write mode:**
  - append / replace / upsert / merge

## Destination Write Modes (Standard)

Destination must implement at least:

- **append**
- **replace** (atomic if supported; else stage+swap)
- **upsert** (requires primary key)

Recommended features:

- **merge** (for warehouses)
- **dedupe_window** (for at-least-once CDC streams)
