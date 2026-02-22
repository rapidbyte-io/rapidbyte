# Rapidbyte

Single-binary data ingestion engine with Wasm-sandboxed connectors.

Rapidbyte replaces heavyweight managed ELT platforms (Fivetran, Airbyte) with a
single native binary that orchestrates data pipelines through sandboxed WebAssembly
connectors. Connectors run as Wasm components with host-proxied networking and
Arrow IPC batch exchange — no JVM, no Docker, no sidecar processes.

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│                rapidbyte-cli                     │
│   run · check · discover · connectors · scaffold│
├─────────────────────────────────────────────────┤
│                rapidbyte-core                    │
│  ┌───────────┐  ┌─────────┐  ┌───────────────┐ │
│  │Orchestrator│─▶│ Runner  │─▶│ComponentRuntime│ │
│  └───────────┘  └─────────┘  └───────────────┘ │
│       │              │              │            │
│  State (SQLite)  Channels     Wasmtime Engine   │
│       │         (mpsc+compress)     │            │
│  Checkpoints     Arrow IPC     WIT Imports      │
├─────────────────────────────────────────────────┤
│                rapidbyte-sdk                     │
│  Source · Destination · Transform traits         │
│  connector_main! macros · HostTcpStream          │
├─────────────────────────────────────────────────┤
│              Wasm Connectors                     │
│  source-postgres  ·  dest-postgres               │
└─────────────────────────────────────────────────┘
```

- **Runtime:** Wasmtime component model, `wasm32-wasip2` target.
- **Protocol:** Version 2. Interface contract: `wit/rapidbyte-connector.wit`.
- **Data format:** Arrow IPC record batches flow between stages via bounded `mpsc` channels.
- **State:** SQLite (`rusqlite` bundled) for run metadata, cursor/checkpoint state, and DLQ records.
- **Networking:** Connectors have no direct WASI socket access. All outbound TCP is mediated
  through host imports (`connect-tcp`, `socket-read`, `socket-write`, `socket-close`) with
  manifest-declared network ACLs.

## Wasm Engine & Sandbox

Wasmtime component model with WIT-typed imports and exports.

- **Component model:** Each connector is a Wasm component exporting one of `source-connector`,
  `dest-connector`, or `transform-connector` interfaces.
- **WIT interface** (`wit/rapidbyte-connector.wit`): defines types, host imports, and three
  connector worlds (`rapidbyte-source`, `rapidbyte-destination`, `rapidbyte-transform`).
- **Host imports:** `emit-batch`, `next-batch`, `checkpoint`, `metric`, `emit-dlq-record`,
  `state-get`, `state-put`, `state-cas`, `log`, plus TCP socket operations.
- **Network ACL:** Derived from connector manifest permissions. Supports domain allowlists,
  runtime config domain derivation, and TLS requirements (required/optional/forbidden).
- **AOT compilation cache:** Controlled via `RAPIDBYTE_WASMTIME_AOT` env var. Pre-compiles
  components to native code and caches them on disk for faster subsequent loads.
- **WASI P2:** `wasmtime_wasi::p2::add_to_linker_sync` provides standard WASI imports.

## SDK & Protocol

The `rapidbyte-sdk` crate provides everything needed to build a connector:

**Traits:**
- `Source` — `init`, `validate`, `discover`, `read`, `close`
- `Destination` — `init`, `validate`, `write`, `close`
- `Transform` — `init`, `validate`, `transform`, `close`

**Export macros:**
```rust
connector_main!(source, MySource);
connector_main!(destination, MyDest);
connector_main!(transform, MyTransform);
```

**Host FFI wrappers** (`host_ffi`): Typed functions for `emit_batch`, `next_batch`,
`checkpoint`, `metric`, `emit_dlq_record`, and key/value state operations.

**HostTcpStream** (`host_tcp`): Adapter implementing `AsyncRead + AsyncWrite` over host TCP
imports, enabling `tokio-postgres` `connect_raw` from inside the Wasm sandbox.

**Protocol v2 types:** `PayloadEnvelope` (serde flatten), `StreamContext`, `Checkpoint`,
`ReadSummary`, `WriteSummary`, `TransformSummary`, `ConnectorError` with structured
error categories, retry semantics, and commit state tracking.

## Connector Manifest

JSON manifest alongside each `.wasm` binary declaring identity, capabilities, and security:

```json
{
  "manifest_version": "1.0",
  "id": "rapidbyte/source-postgres",
  "name": "PostgreSQL Source",
  "version": "0.1.0",
  "protocol_version": "2",
  "artifact": { "entry_point": "source_postgres.wasm" },
  "permissions": {
    "network": { "tls": "optional", "allow_runtime_config_domains": true },
    "env": { "allowed_vars": ["PGSSLROOTCERT"] }
  },
  "roles": {
    "source": { "supported_sync_modes": ["full_refresh", "incremental"], "features": [] }
  },
  "config_schema": { "$schema": "http://json-schema.org/draft-07/schema#", ... }
}
```

**Fields:** id, name, version, description, author, license, protocol_version, artifact
(entry_point, checksum, min_memory_mb), permissions (network, env, fs), roles
(source/destination/transform/utility capabilities), config_schema (JSON Schema Draft 7).

The host validates config against the schema before instantiating the Wasm guest.

## CLI

```
rapidbyte run <pipeline.yaml>       Execute a data pipeline
rapidbyte check <pipeline.yaml>     Validate config, manifests, and connectivity
rapidbyte discover <pipeline.yaml>  Discover available streams from a source
rapidbyte connectors                List available connector plugins
rapidbyte scaffold <name>           Scaffold a new connector project
```

Global flag: `--log-level` (error/warn/info/debug/trace). Also respects `RUST_LOG`.

## Pipeline YAML

```yaml
version: "1.0"
pipeline: pg_to_pg

source:
  use: source-postgres
  config:
    host: localhost
    port: 5432
    user: app
    database: mydb
  streams:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
      columns: [id, name, email, updated_at]    # projection pushdown

transforms:                                       # optional, zero or more
  - use: transform-mask
    config: { fields: [email] }

destination:
  use: dest-postgres
  config:
    host: warehouse.internal
    port: 5432
    user: loader
    database: analytics
    schema: raw
  write_mode: upsert
  primary_key: [id]
  on_data_error: dlq                              # skip | fail | dlq
  schema_evolution:
    new_column: add                               # add | ignore | fail
    removed_column: ignore                        # ignore | fail
    type_change: coerce                           # coerce | fail | null
    nullability_change: allow                     # allow | fail

state:
  backend: sqlite
  connection: /var/lib/rapidbyte/state.db

resources:
  parallelism: 4
  max_batch_bytes: 64mb
  max_inflight_batches: 16
  checkpoint_interval_bytes: 64mb
  checkpoint_interval_rows: 0                     # 0 = disabled
  checkpoint_interval_seconds: 0                  # 0 = disabled
  max_retries: 3
  compression: lz4                                # lz4 | zstd | null
```

## Sync Modes

| Mode | Description |
|------|-------------|
| `full_refresh` | Read entire table, no cursor tracking |
| `incremental` | Cursor-based delta reads; resumes from last checkpoint value |
| `cdc` | PostgreSQL logical replication via `pg_logical_slot_get_changes()` |

## Write Modes

| Mode | Description |
|------|-------------|
| `append` | Insert all records |
| `replace` | Truncate target table, then insert |
| `upsert` | Insert or update by `primary_key` using `ON CONFLICT ... DO UPDATE` |

## CDC (Change Data Capture)

PostgreSQL logical replication using `test_decoding` output plugin:

- Calls `pg_logical_slot_get_changes()` to consume WAL changes.
- Parses INSERT, UPDATE, DELETE operations from `test_decoding` output.
- Adds `_rb_op` metadata column with operation type (`insert`/`update`/`delete`).
- Tracks WAL LSN as cursor for checkpoint recovery.
- Batches changes into Arrow record batches (10,000 rows per batch).
- Destructive slot consumption requires checkpoint safety for exactly-once delivery.

## Schema Evolution

Configurable per-pipeline policy with four dimensions:

| Dimension | Options | Default |
|-----------|---------|---------|
| `new_column` | `add`, `ignore`, `fail` | `add` |
| `removed_column` | `ignore`, `fail` | `ignore` |
| `type_change` | `coerce`, `fail`, `null` | `fail` |
| `nullability_change` | `allow`, `fail` | `allow` |

Policies are passed to connectors via `StreamPolicies` in `StreamContext`. The destination
connector applies them during DDL evolution and batch writes.

## Dead Letter Queue

Records that fail during writing are routed to a DLQ instead of failing the pipeline
(when `on_data_error: dlq` is configured):

- `DlqRecord`: stream_name, record_json, error_message, error_category, failed_at timestamp.
- Maximum 10,000 records held in memory per run (prevents unbounded growth).
- Persisted to the SQLite state backend at the end of each run.

## Projection Pushdown

The `columns` field on each stream in the pipeline YAML specifies which columns to read:

```yaml
streams:
  - name: users
    sync_mode: full_refresh
    columns: [id, name, email]
```

The source connector receives `selected_columns` in `StreamContext` and constructs queries
selecting only the specified columns. Column names are validated against PostgreSQL identifier
rules to prevent SQL injection.

## Pipeline Parallelism

Multiple streams execute concurrently within a single pipeline run:

- `resources.parallelism` controls the maximum number of concurrent streams (default: 1).
- Implemented via `tokio::sync::Semaphore` for concurrency control.
- Each stream gets its own source → [transform...] → destination channel pipeline.
- Channels are bounded by `max_inflight_batches` for backpressure.

## Compression

Arrow IPC batches transferred between pipeline stages can be compressed:

- **LZ4** (`lz4_flex`): Fast compression, lower ratio. Good default for most workloads.
- **Zstd** (level 1): Better ratio, slightly higher CPU. Good for large batches.
- Configured via `resources.compression` in pipeline YAML.
- Applied transparently by host imports on emit/receive.

## Error Handling & Retries

Connectors return structured `ConnectorError` with:
- **Category:** config, auth, permission, rate_limit, transient_network, transient_db, data, schema, internal.
- **Scope:** per-stream, per-batch, per-record.
- **Retry semantics:** retryable flag, retry_after_ms hint, backoff_class (fast/normal/slow),
  safe_to_retry flag, commit_state (before_commit/after_commit_unknown/after_commit_confirmed).

The orchestrator retries transient errors up to `max_retries` times with exponential backoff.

## Observability

- **Structured logging:** `tracing` + `tracing-subscriber` with `--log-level` CLI flag.
- **Connector metrics:** records_read/written, bytes_read/written (emitted via `metric` host import).
- **Host timing breakdown:** connect, query, fetch, encode, decode, flush, commit, vm_setup,
  emit_batch, next_batch, compress, decompress — all tracked per-run.
- **Run tracking:** Each pipeline run recorded in SQLite with start/end time, status,
  records read/written, and error messages.

## Connectors

### Implemented

| Connector | Roles | Notes |
|-----------|-------|-------|
| `source-postgres` | Source | Snapshot, incremental cursor, CDC. `tokio-postgres` over `HostTcpStream`. |
| `dest-postgres` | Destination | INSERT and COPY modes. Batch commits. DDL auto-creation. Schema evolution. |

### Roadmap

| Connector | Priority | Notes |
|-----------|----------|-------|
| `source-mysql` | P1 | MySQL binlog CDC |
| `dest-s3-parquet` | P1 | Parquet files on S3 |
| `dest-bigquery` | P1 | BigQuery Storage Write API |
| `dest-snowflake` | P2 | Snowflake PUT + COPY |
| `dest-duckdb` | P2 | Embedded analytics |
| `source-http` | P2 | REST/webhook source |
| `source-s3` | P2 | S3 file source (CSV, Parquet, JSON) |

## Roadmap

### Implemented (current)

- Wasmtime component model runtime with WIT interface
- Source, Destination, Transform connector lifecycle
- Connector manifests with config schema validation
- Pipeline YAML configuration
- Three sync modes: full_refresh, incremental, CDC
- Three write modes: append, replace, upsert
- Schema evolution policies (4 dimensions)
- Dead letter queue with SQLite persistence
- Projection pushdown (column selection)
- Pipeline parallelism (semaphore-based)
- LZ4 and Zstd channel compression
- AOT compilation cache
- Structured error handling with retry semantics
- CLI: run, check, discover, connectors, scaffold
- Host-proxied TCP networking with ACLs
- Connector metrics and host timing breakdown
- SQLite state backend for checkpoints and run history
- E2E test suite and benchmarking scripts

### Near-term (P1)

- TUI progress display during pipeline runs
- OCI registry for connector distribution (`rapidbyte pull`)
- Additional connectors: MySQL, S3/Parquet, BigQuery
- OpenTelemetry metrics and trace export
- Prometheus metrics endpoint
- `pgoutput` CDC plugin (in addition to `test_decoding`)

### Future (P2)

- DataFusion integration for in-pipeline SQL transforms
- Pipeline hooks / middleware (pre-batch, post-commit callbacks)
- Managed cloud service with scheduling and monitoring
- Connector marketplace
- Distributed tracing across pipeline stages
- PostgreSQL state backend option (in addition to SQLite)
- DLQ replay and routing to destination tables
