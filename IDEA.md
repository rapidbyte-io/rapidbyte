# Rapidbyte

**The single-binary data ingestion engine.**

*What dbt did for data transformation, Rapidbyte does for data ingestion. No JVM, no heavy Docker requirements, no Python dependency hell. Just pure performance.*

---

## Table of Contents

1. [The Problem](#the-problem)
2. [Competitive Landscape (2025-2026)](#competitive-landscape-2025-2026)
3. [Core Architecture](#core-architecture)
4. [Internal Data Format (Apache Arrow)](#internal-data-format-apache-arrow)
5. [The Extensibility Engine (Wasm)](#the-extensibility-engine-wasm)
6. [Connector SDK Protocol](#connector-sdk-protocol)
7. [Plugin Distribution (OCI Registry)](#plugin-distribution-oci-registry)
8. [CLI User Experience](#cli-user-experience)
9. [Pipeline YAML Specification](#pipeline-yaml-specification)
10. [Connector Strategy](#connector-strategy)
11. [CDC Support](#cdc-support)
12. [Schema Evolution & Data Contracts](#schema-evolution--data-contracts)
13. [Observability & Telemetry](#observability--telemetry)
14. [Error Handling](#error-handling)
15. [Local Analytics (DataFusion)](#local-analytics-datafusion)
16. [Development Roadmap](#development-roadmap)
17. [Commercialization](#commercialization)
18. [Community & Ecosystem](#community--ecosystem)

---

## The Problem

There is a massive void in the data engineering ecosystem for a blazing fast, lightweight, purely declarative, single-binary ingestion engine. The current landscape forces teams into uncomfortable trade-offs:

- **Airbyte / Fivetran (The Heavyweights):** Moving aggressively toward expensive, managed-cloud models. Airbyte's open-source offering requires a sprawling Docker Compose setup, a JVM, and gigabytes of RAM just to idle — a hostile funnel to their paid cloud. Fivetran restructured pricing in March 2025, increasing costs for multi-source teams.

- **dlt (Python-first):** Fantastic for Python-heavy developers (3M+ monthly downloads), but strictly imperative/code-first. Suffers from Python's dependency hell, dynamic typing flakiness at scale, and high memory consumption. Lacks the declarative, Infrastructure-as-Code feel that many data engineers prefer. Python-only — no polyglot ecosystem.

- **Meltano (Singer):** Pioneered the CLI-first approach, but is chained to the legacy Singer spec — passing JSON strings over stdout via Python scripts. Inherently slow, memory-intensive, and hard to maintain.

- **Sling:** Fast Go-based CLI for database-to-database and file transfers. Strong at what it does, but limited to structured sources — no SaaS API connectors, no plugin extensibility model.

- **CloudQuery:** Proves the Go single-binary model works (581 GB/hour throughput). However, Go's plugin model requires separate processes communicating over gRPC — heavier than Wasm in-process execution. Configuration is spread across multiple files.

- **Ingestr:** Python CLI wrapping dlt. Simple and effective for quick transfers, but inherits all of Python's runtime overhead and lacks a plugin ecosystem.

**84% of CIOs prioritize cost optimization** — the single-binary model directly addresses infrastructure costs. A data engineer can run a full ELT pipeline on a $5/month VM or a minimal CI runner with a 25MB footprint.

---

## Competitive Landscape (2025-2026)

| Feature | Rapidbyte | Airbyte | Fivetran | dlt | CloudQuery | Sling | Meltano |
|---|---|---|---|---|---|---|---|
| **Deployment** | Single binary | Docker Compose / K8s | Managed SaaS | pip install | Single binary | Single binary | pip install |
| **Language** | Rust | Java/Python | Proprietary | Python | Go | Go | Python |
| **Binary size** | ~25 MB | ~2 GB (Docker) | N/A | ~50 MB (venv) | ~50 MB | ~30 MB | ~100 MB (venv) |
| **Memory idle** | ~5 MB | ~2 GB | N/A | ~80 MB | ~20 MB | ~10 MB | ~200 MB |
| **Plugin model** | Wasm (in-process, self-contained) | Docker containers | Proprietary | Python modules | gRPC (out-of-process) | None | Singer (Python) |
| **Plugin cold start** | ~3 ms (with AOT) | ~5-30 s | N/A | ~2 s | ~500 ms | N/A | ~3 s |
| **Configuration** | Single YAML | Multiple YAML + UI | UI-only | Python code | Multiple HCL/YAML | CLI flags / YAML | YAML + Python |
| **SaaS connectors** | Yes (Wasm) | 400+ | 300+ | 100+ | 100+ | No | 300+ (Singer) |
| **CDC support** | Yes (planned) | Yes | Yes | Limited | Yes | Limited | No |
| **License** | Apache 2.0 | ELv2 / BSL | Proprietary | Apache 2.0 | Apache 2.0 | Apache 2.0 | MIT |
| **Local analytics** | DataFusion built-in | No | No | No | No | No | No |
| **Data format** | Apache Arrow | Custom / JSON | Proprietary | Python dicts | Apache Arrow | Native DB | JSON (Singer) |

**Key competitive advantages:**
1. **First Wasm-based plugin architecture** in the data ingestion space — no other major tool uses Wasm for connectors
2. **In-process plugin execution** — no IPC overhead, no container startup, no separate processes
3. **Self-contained connectors** — standard Rust crates (reqwest, tokio-postgres) compile to Wasm and run with direct network access via WASI sockets. No host function proxies, no IPC
4. **Single YAML pipeline definition** — one file defines source, destination, transforms, error handling, and state
5. **Built-in local analytics** — query synced data with SQL via DataFusion (already in the binary for transforms) without additional tooling

---

## Core Architecture

```
                          ┌─────────────────────────────────────┐
                          │         Rapidbyte CLI               │
                          │  (Single Rust Binary, ~25 MB)       │
                          └───────────┬─────────────────────────┘
                                      │
              ┌───────────────────────┼───────────────────────┐
              │                       │                       │
     ┌────────▼────────┐    ┌────────▼────────┐    ┌────────▼────────┐
     │  Pipeline YAML  │    │  Config (TOML)  │    │   State Mgmt    │
     │    Parser        │    │ ~/.rapidbyte/   │    │ (sqlite/postgres)│
     └────────┬────────┘    └─────────────────┘    └────────┬────────┘
              │                                             │
     ┌────────▼─────────────────────────────────────────────▼────────┐
     │                     Core Engine (Rust)                        │
     │                                                               │
     │  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐   │
     │  │  Scheduler    │  │  Arrow       │  │  DataFusion       │   │
     │  │  (tokio)      │  │  Record      │  │  Query Engine     │   │
     │  │               │  │  Batches     │  │  (transforms)     │   │
     │  └──────┬───────┘  └──────┬───────┘  └───────┬───────────┘   │
     │         │                 │                   │               │
     │  ┌──────▼─────────────────▼───────────────────▼───────────┐   │
     │  │     WasmEdge Runtime (WASI sockets enabled)            │   │
     │  │                                                        │   │
     │  │  ┌──────────────┐  ┌──────────────┐                   │   │
     │  │  │ Source Plugin │  │ Source Plugin │  Fat Connectors  │   │
     │  │  │ (.wasm)       │  │ (.wasm)       │  ─────────────── │   │
     │  │  │ [reqwest]     │  │ [tokio-pg]    │  Connectors      │   │
     │  │  │ [serde_json]  │  │ [arrow]       │  bundle their    │   │
     │  │  └──────┬───────┘  └──────┬───────┘  own networking   │   │
     │  │         │                 │           crates and do    │   │
     │  │  ┌──────┴───────┐  ┌──────┴───────┐  I/O directly     │   │
     │  │  │ Dest Plugin  │  │ Dest Plugin  │  via WASI sockets │   │
     │  │  │ (.wasm)       │  │ (.wasm)       │                   │   │
     │  │  │ [tokio-pg]    │  │ [reqwest]     │                   │   │
     │  │  │ [arrow]       │  │ [aws-sdk]     │                   │   │
     │  │  └──────┬───────┘  └──────┬───────┘                   │   │
     │  │         │                 │                            │   │
     │  │         │    WASI Sockets │                            │   │
     │  │         ▼                 ▼                            │   │
     │  │    ┌─────────────────────────────┐                    │   │
     │  │    │  Network / Database / APIs  │                    │   │
     │  │    │  (direct TCP via WASI)      │                    │   │
     │  │    └─────────────────────────────┘                    │   │
     │  │                                                        │   │
     │  │  Host Functions (minimal):                             │   │
     │  │  Arrow IPC │ State │ Logging │ Metrics │ Progress      │   │
     │  └────────────────────────────────────────────────────────┘   │
     │                                                               │
     │  ┌────────────────┐  ┌──────────────┐  ┌─────────────────┐   │
     │  │ Observability  │  │  Schema       │  │  Error          │   │
     │  │ (OTel)         │  │  Registry     │  │  Handler        │   │
     │  └────────────────┘  └──────────────┘  └─────────────────┘   │
     └───────────────────────────────────────────────────────────────┘
```

### The Core Engine (Rust)

Rust provides memory safety, zero-cost abstractions, and fearless concurrency (using `tokio` for async I/O and parallelized API pagination). It compiles to a single binary with no runtime dependencies (the WasmEdge runtime is statically linked at build time).

- **Async-first:** All I/O operations are non-blocking via `tokio`. Multiple streams within a pipeline sync concurrently.
- **Zero-copy data flow:** Apache Arrow record batches pass between source, transform, and destination stages without serialization overhead.
- **Backpressure:** Built-in channel-based backpressure prevents OOM when sources produce faster than destinations can consume.
- **Memory-bounded:** Byte-bounded batching (`max_batch_bytes`) and total memory limits ensure predictable resource usage regardless of row width.

### Configuration Model

- **YAML** for pipeline definitions — data engineers are deeply conditioned to YAML (via dbt, Kubernetes, GitHub Actions). Pipelines require complex nested configurations (schema mapping, credentials, stream selection). YAML handles hierarchies elegantly.
- **TOML** for global CLI settings (`~/.rapidbyte/config.toml`) — simple key-value pairs for defaults, registry URLs, telemetry preferences.

### State Management (Database-Backed)

Sync state — cursors, checkpoints, sync history, and metadata — is stored in a **database**, not flat files. This is a deliberate architectural choice:

**SQLite (default — local / dev / test):**
- Zero-configuration, embedded in the binary — no external dependencies
- State lives in `~/.rapidbyte/state.db` (global) or `.rapidbyte/state.db` (project-local)
- ACID transactions ensure state is never corrupted, even if a sync is interrupted mid-write
- WAL mode enables concurrent reads (e.g., `rapidbyte state show` while a sync is running)
- Single file — trivially backed up, copied between machines, or committed to version control for reproducibility

**PostgreSQL (production / shared environments):**
- Multiple Rapidbyte instances (CI runners, scheduled jobs, different team members) share a single source of truth for sync state
- Prevents conflicting concurrent syncs on the same pipeline — row-level locking ensures only one sync runs per stream at a time
- Queryable — ops teams can monitor sync health with standard SQL (`SELECT * FROM sync_runs WHERE status = 'failed' AND started_at > NOW() - INTERVAL '24 hours'`)
- Integrates with existing infrastructure — no new storage systems to manage, monitor, or back up

**Why not flat files (S3/GCS/local JSON)?**
- JSON files have no atomicity — a crash mid-write corrupts state, causing duplicate or missing data on the next sync
- No concurrency control — two syncs reading and writing the same JSON file race each other
- No query capability — understanding sync history requires parsing files manually
- S3/GCS adds latency (100-300ms per state read/write) and eventual consistency concerns

**State schema:**

```sql
-- Core state tables
CREATE TABLE sync_cursors (
    pipeline    TEXT NOT NULL,
    stream      TEXT NOT NULL,
    cursor_field TEXT,
    cursor_value TEXT,
    updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pipeline, stream)
);

CREATE TABLE sync_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline    TEXT NOT NULL,
    stream      TEXT NOT NULL,
    status      TEXT NOT NULL,  -- running | completed | failed
    started_at  TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    records_read    INTEGER DEFAULT 0,
    records_written INTEGER DEFAULT 0,
    bytes_read      INTEGER DEFAULT 0,
    error_message   TEXT
);

-- Prevents concurrent syncs on the same stream
CREATE UNIQUE INDEX idx_running_sync
    ON sync_runs (pipeline, stream)
    WHERE status = 'running';
```

---

## Internal Data Format (Apache Arrow)

All data flowing through Rapidbyte uses **Apache Arrow** as the internal columnar representation. This is a foundational architectural decision.

### Why Arrow

- **Zero-copy:** Data passes between pipeline stages (source → transform → destination) without serialization/deserialization. Arrow record batches are the universal in-memory format.
- **Columnar:** Enables vectorized operations for transforms, filtering, and type coercion. Column-oriented layout is optimal for analytical workloads and efficient compression.
- **Cross-language:** Arrow's C Data Interface allows Wasm plugins to exchange data with the host engine without format translation. Rust, Go, TypeScript, and Python all have mature Arrow libraries.
- **Streaming batches:** Data flows as a stream of Arrow `RecordBatch` objects, bounded by byte size (default: `max_batch_bytes: 64mb`), not row count. Row counts are a dangerous metric for memory — 65k rows of Stripe IDs is 2 MB, but 65k rows of Jira tickets with nested JSON could be 2 GB. Byte-bounded batching ensures predictable memory usage regardless of row width.
- **Ecosystem gravity:** Arrow is the backbone of modern data infrastructure — InfluxDB 3.0, Delta Lake, Apache Iceberg, Polars, and DataFusion all use Arrow natively. Rapidbyte data can flow into any of these without conversion.

### Arrow in the Pipeline

```
Source Plugin          Core Engine              Destination Plugin
┌───────────┐     ┌──────────────────┐      ┌───────────────┐
│ API / DB  │     │  RecordBatch     │      │  Arrow →      │
│ Response  │ ──► │  (Arrow IPC)     │ ──►  │  Parquet /    │
│ → Arrow   │     │  Transform /     │      │  DB Insert /  │
│ RecordBatch│     │  Filter /        │      │  API Call     │
└───────────┘     │  Schema Validate │      └───────────────┘
                  └──────────────────┘
```

### DataFusion for Transforms

The **Apache DataFusion** query engine powers Rapidbyte's transform layer. DataFusion operates directly on Arrow record batches, enabling SQL-based and expression-based transforms without data copies:

- Column renaming, type casting, and computed columns
- Row filtering with predicate pushdown
- Aggregations and windowing (for summary destinations)
- Custom UDFs registered as Wasm plugins

---

## The Extensibility Engine (Wasm)

Rapidbyte connectors are compiled to WebAssembly modules and executed in-process via **WasmEdge**, a high-performance Wasm runtime with WASI socket support. Connectors are "fat" modules — they bundle standard Rust networking crates (reqwest, tokio-postgres, mysql_async) and perform I/O directly via WASI sockets, without proxying through host functions.

### Why Wasm over Docker / Python

| Concern | Docker (Airbyte) | Python (Singer/dlt) | Wasm (Rapidbyte) |
|---|---|---|---|
| Cold start | 5-30 seconds | 2-5 seconds | ~3 ms (with AOT) |
| Package size | 100 MB - 2 GB | 50-200 MB (venv) | 5-15 MB (.wasm) |
| Isolation | Full OS-level | None (shared process) | Sandbox (WASI capabilities) |
| Memory overhead | 100+ MB per container | Shared, GC pressure | ~2-10 MB per plugin |
| Host access | Full (unless restricted) | Full | None (unless granted) |
| Networking | Full | Full | WASI sockets (capability-granted) |

### Why WasmEdge

WasmEdge is the right Wasm runtime for Rapidbyte's fat connector model:

- **WASI socket support:** Connectors can open TCP connections directly via the WASI socket API (TLS support in progress) — no host function proxies needed for HTTP or database I/O
- **Patched Rust crate ecosystem:** The WasmEdge project (via Second State) maintains forks of tokio, reqwest, tokio-postgres, mysql_async, and hyper (repos under the `second-state` GitHub org) that compile to `wasm32-wasip1` and use WASI sockets for networking
- **AOT compilation:** Ahead-of-time compilation via `wasmedge compile` delivers near-native performance (~15x faster than interpreter mode), with ~3 ms cold starts
- **`wasmedge-sdk` embedding:** The Rust `wasmedge-sdk` crate provides a clean API for embedding the runtime, registering host functions, and managing plugin lifecycles
- **Build dependency:** `wasmedge-sdk` requires the WasmEdge C library (`libwasmedge`) installed on the build machine. End users are unaffected — the library is statically linked into the Rapidbyte binary. WasmEdge provides install scripts for Linux/macOS (`curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash`)
- **Capability-based security:** WASI provides a deny-by-default sandbox. Rapidbyte builds a network enforcement layer on top — the manifest-declared `allowed_hosts` with per-host/port/protocol granularity is enforced by Rapidbyte's host engine, not a built-in WasmEdge feature
- **Configurable limits:** Memory caps, execution timeouts, and resource limits per plugin instance
- **TLS status:** TLS support in the WASI socket forks is evolving. REST connectors can use HTTPS via the reqwest fork for many APIs. Database connectors currently connect via plaintext TCP (as shown in WasmEdge's own examples). Initial Rapidbyte database connectors target same-VPC deployments; TLS is a follow-on milestone tracking upstream WASI socket progress

> **Ecosystem maturity note:** The WASI socket forks were primarily developed in 2022-2023 and their maintenance cadence is low. Rapidbyte should be prepared to: (a) contribute patches upstream, (b) maintain its own forks if needed, and (c) migrate to WASI Preview 2 (component model) networking when that ecosystem matures — which would eliminate patched forks entirely.

### Wasm Overhead

Performance benchmarks show Wasm overhead is minimal for I/O-bound data connectors:

- **Compute overhead:** ~1.55x native execution in interpreter mode (irrelevant for network-bound connectors)
- **AOT compilation:** Near-native performance with `wasmedge compile` — overhead drops to negligible levels
- **Rust → Wasm plugins:** Add only ~0.003s overhead per invocation

For data connectors that spend 95%+ of time waiting on network I/O, Wasm overhead is negligible. AOT-compiled connectors approach native speed even for compute-heavy transforms.

### How Database Connectors Work in Wasm

WasmEdge's WASI socket support enables standard Rust database drivers to run directly inside Wasm. Patched forks of `tokio-postgres`, `mysql_async`, and other async database crates compile to `wasm32-wasip1` and open TCP/TLS connections via the WASI socket API. The connector is a self-contained "fat" module — it bundles the database driver, manages its own connections and pools, and talks to the database directly:

```
Fat Wasm Connector (wasm32-wasip1)                  External
┌─────────────────────────────────────┐
│                                     │
│  Connector Logic                    │
│  (pagination, schema mapping, CDC) │
│                                     │
│  ┌───────────────────────────────┐  │
│  │ tokio-postgres (patched fork) │  │    WASI Sockets
│  │ Connection pooling            │──┼──────────────►  PostgreSQL
│  │ TCP (TLS in progress)         │  │    TCP
│  └───────────────────────────────┘  │
│                                     │
│  ┌───────────────────────────────┐  │
│  │ Arrow IPC (host function)     │──┼──► Host Pipeline
│  └───────────────────────────────┘  │
│                                     │
│  ┌───────────────────────────────┐  │
│  │ State (host function)         │──┼──► Host State DB
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

**The patched crate approach:**

Connectors depend on Second State-maintained forks of standard Rust crates. The `Cargo.toml` uses `[patch.crates-io]` to swap in WASI-compatible versions, and `.cargo/config.toml` sets the required RUSTFLAGS:

```toml
# .cargo/config.toml
[build]
target = "wasm32-wasip1"

[target.wasm32-wasip1]
rustflags = ["--cfg", "wasmedge", "--cfg", "tokio_unstable"]
```

```toml
# Cargo.toml
[dependencies]
tokio = { version = "1.36", features = ["full"] }
reqwest = { version = "0.11", features = ["json"] }
tokio-postgres = "0.7"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

[patch.crates-io]
tokio = { git = "https://github.com/second-state/wasi_tokio.git", branch = "v1.36.x" }
reqwest = { git = "https://github.com/second-state/wasi_reqwest.git", branch = "0.11.x" }
hyper = { git = "https://github.com/second-state/wasi_hyper.git", branch = "v0.14.x" }
socket2 = { git = "https://github.com/aspect-build/wasi_socket2.git", branch = "v0.5.x" }
tokio-postgres = { git = "https://github.com/aspect-build/rust-postgres.git" }
```

Connector code uses standard crate names (`tokio`, `reqwest`, `tokio_postgres`) — the `[patch.crates-io]` section transparently swaps in WASI-compatible forks at build time. An alternative approach uses `_wasi` suffix crates published to crates.io (e.g., `tokio_wasi`, `reqwest_wasi`); Rapidbyte uses the patch approach so connector code remains portable and identical to native Rust.

**This means:**
- Connector authors write standard async Rust — the same `tokio-postgres` API they'd use in a native application
- Each connector manages its own connection pools and authentication inside the Wasm sandbox
- The only host functions needed are for Arrow IPC (data exchange with the pipeline), state management (cursors/checkpoints), and observability (logging/metrics)
- WASI capability grants control which network hosts each connector can reach — a Postgres connector cannot connect to Snowflake or exfiltrate data to unauthorized endpoints

### Capability-Based Security (Rapidbyte Enforcement Layer)

WasmEdge's WASI controls filesystem preopens, environment variables, and basic socket access. **Rapidbyte adds a network enforcement layer on top** — the per-host/per-port/per-protocol capability model below is implemented by Rapidbyte's host engine. Each connector declares the network permissions it requires in its manifest. The host grants only what is declared, resolving runtime-specific hosts (like database endpoints) from the pipeline YAML config:

```yaml
# connector-manifest.yaml (REST connector example)
capabilities:
  network:
    allowed_hosts:
      - host: "api.stripe.com"
        port: 443
        protocol: tcp
        tls: required
      - host: "files.stripe.com"
        port: 443
        protocol: tcp
        tls: required
  host_functions:
    - arrow_ipc
    - state_read
    - state_write
    - logging
    - metrics
  # No other network access, no filesystem, no env vars
```

```yaml
# connector-manifest.yaml (Database connector example)
capabilities:
  network:
    runtime_hosts: true            # Host resolves from pipeline YAML config
    protocol: tcp
    tls: optional                  # Initial release uses plaintext within VPC; TLS planned
  host_functions:
    - arrow_ipc
    - state_read
    - state_write
    - logging
    - metrics
  # Network access limited to the database host specified in pipeline config
```

**Runtime host resolution:** Database connectors declare `runtime_hosts: true` because the database endpoint comes from the pipeline YAML (e.g., `host: ${PG_HOST}`). At runtime, the host engine reads the pipeline config, resolves the actual hostname, and grants WASI socket access only to that specific host and port. The connector never sees other network endpoints.

Users can review and approve capabilities before installing a community connector. The CLI warns if a connector requests unusual permissions (e.g., access to many hosts, wildcard ports).

---

## Connector SDK Protocol

Every Rapidbyte connector — source or destination — implements a standardized Wasm interface. The protocol defines five core functions that the host engine calls.

### Source Connector Interface

```rust
/// Called once when the plugin is loaded. Receives config from the pipeline YAML.
fn init(config: Json) -> Result<(), ConnectorError>;

/// Returns the catalog of available streams, their schemas, and supported sync modes.
fn discover() -> Result<Catalog, ConnectorError>;

/// Validates the provided configuration (credentials, endpoints, etc).
/// Returns a structured status with actionable error messages.
fn validate(config: Json) -> Result<ValidationResult, ConnectorError>;

/// Reads data from the source. Called with stream selection and optional state.
/// Yields Arrow RecordBatches to the host via a host function callback.
fn read(request: ReadRequest) -> Result<ReadSummary, ConnectorError>;
```

### Destination Connector Interface

```rust
/// Called once when the plugin is loaded.
fn init(config: Json) -> Result<(), ConnectorError>;

/// Validates the destination configuration and connectivity.
fn validate(config: Json) -> Result<ValidationResult, ConnectorError>;

/// Writes Arrow RecordBatches to the destination.
/// Receives batches from the host via a host function callback.
fn write(request: WriteRequest) -> Result<WriteSummary, ConnectorError>;
```

### Core Types

```rust
struct Catalog {
    streams: Vec<Stream>,
}

struct Stream {
    name: String,
    schema: ArrowSchema,          // Arrow schema definition
    supported_sync_modes: Vec<SyncMode>,
    source_defined_cursor: Option<String>,
    source_defined_primary_key: Option<Vec<String>>,
}

enum SyncMode {
    FullRefresh,
    Incremental,
    CDC,
}

struct ReadRequest {
    streams: Vec<StreamSelection>,
    state: Option<Json>,          // Previous sync state for incremental
}

struct StreamSelection {
    name: String,
    sync_mode: SyncMode,
    cursor_field: Option<String>,
    max_batch_bytes: u64,         // Byte limit per RecordBatch (default: 64 MB)
}

struct WriteRequest {
    stream: String,
    schema: ArrowSchema,
    write_mode: WriteMode,
}

enum WriteMode {
    Append,
    Replace,
    Upsert { primary_key: Vec<String> },
    Merge { merge_keys: Vec<String>, update_columns: Vec<String> },
}

struct ValidationResult {
    status: ValidationStatus,
    message: String,
    details: Option<Vec<ValidationDetail>>,
}

enum ValidationStatus {
    Success,
    Failed,
    Warning,
}
```

### Host Functions (Provided by Core Engine)

Because connectors handle their own networking via WASI sockets, the host function surface is minimal. The core engine only provides functions for data exchange with the pipeline, state management, and observability — things the connector cannot do itself because they require access to the host's internal state.

```rust
// ── Arrow I/O ─────────────────────────────────────────────────
/// Emit an Arrow RecordBatch to the host pipeline (source connectors).
/// The batch is serialized as Arrow IPC and passed to the pipeline's
/// transform/destination stages.
fn emit_record_batch(stream: &str, batch: ArrowIpcBytes);

/// Read the next Arrow RecordBatch from the host pipeline (destination connectors).
/// Returns None when the stream is exhausted.
fn next_record_batch() -> Option<(String, ArrowIpcBytes)>;

// ── State ─────────────────────────────────────────────────────
/// Read connector state (for incremental sync cursors/checkpoints).
/// State is stored in the host's SQLite/PostgreSQL state database —
/// connectors cannot access this database directly.
fn get_state(key: &str) -> Option<Json>;

/// Write connector state.
fn set_state(key: &str, value: Json);

// ── Observability ─────────────────────────────────────────────
/// Structured logging to the host's OTel pipeline.
/// Logs are enriched with pipeline/stream context by the host.
fn log(level: LogLevel, message: &str, fields: Option<Json>);

/// Emit a metric to the host's OTel pipeline.
fn emit_metric(name: &str, value: f64, labels: Option<Json>);

/// Report progress for circuit breaker and UI feedback.
fn report_progress(records: u64, bytes: u64);
```

**Why only 7 host functions?** Fat connectors handle HTTP, database, and storage I/O directly via WASI sockets using standard Rust crates. The host only needs to provide:
1. **Arrow IPC** — data exchange between the connector and the pipeline engine
2. **State** — cursor/checkpoint persistence in the host's state database (connectors can't access SQLite/PostgreSQL directly)
3. **Observability** — structured logging and metrics routed through the host's OTel pipeline for consistent formatting and export

---

## Plugin Distribution (OCI Registry)

Rapidbyte connectors are distributed as OCI (Open Container Initiative) artifacts — the same registry protocol used by Docker images, Helm charts, and Wasm modules in the CNCF ecosystem.

### Why OCI

- **Established infrastructure:** Docker Hub, GitHub Container Registry (GHCR), AWS ECR, and Google Artifact Registry all support OCI artifacts. No custom registry needed.
- **Content-addressable:** Every plugin version is identified by its SHA-256 digest. Immutable, verifiable, tamper-proof.
- **Versioned:** Semantic versioning via OCI tags (`rapidbyte/source-stripe:v1.2.0`).
- **Signable:** Cosign / Sigstore integration for provenance verification on official and community plugins.
- **Efficient:** Layer-based distribution with deduplication. Updates only download changed layers.

### Plugin Manifest

Each connector publishes a manifest alongside its Wasm binary:

```yaml
# rapidbyte-plugin.yaml
name: source-stripe
version: 1.2.0
description: "Stripe API source connector"
author: "Rapidbyte Team"
license: "Apache-2.0"
tier: official                    # official | community | partner

wasm:
  entrypoint: source_stripe.wasm
  runtime: wasmedge
  target: wasm32-wasip1
  min_engine_version: "0.5.0"

capabilities:
  network:
    allowed_hosts:
      - host: "api.stripe.com"
        port: 443
        protocol: tcp
        tls: required
      - host: "files.stripe.com"
        port: 443
        protocol: tcp
        tls: required
  host_functions:
    - arrow_ipc
    - state_read
    - state_write
    - logging
    - metrics

streams:
  - customers
  - invoices
  - charges
  - subscriptions
  - payment_intents
  - products
  - prices
  - refunds

sync_modes:
  - full_refresh
  - incremental

config_schema:
  required:
    - api_key
  optional:
    - start_date
    - account_id              # For Stripe Connect
```

### AOT Compilation

WasmEdge supports ahead-of-time compilation that converts `.wasm` modules into platform-native code, delivering near-native performance with ~3 ms cold starts. Rapidbyte uses a **deferred AOT** strategy:

1. **First use:** When a connector is first loaded, Rapidbyte runs it in interpreter mode and kicks off AOT compilation in the background via `wasmedge compile`
2. **Subsequent uses:** The AOT-compiled artifact is loaded directly, bypassing the interpreter entirely
3. **Cache invalidation:** AOT artifacts are keyed by the Wasm module's SHA-256 digest. Updating a connector automatically invalidates the AOT cache
4. **Optional pre-compiled artifacts:** Registry publishers can include pre-compiled AOT artifacts for common platforms (linux/amd64, linux/arm64, darwin/arm64) as additional OCI layers

```bash
# Manual AOT compilation (optional — happens automatically on first use)
$ rapidbyte connectors compile source-stripe
> AOT compiling source-stripe@v1.2.0 for darwin/arm64... Done in 2.1s
> Cached to ~/.rapidbyte/plugins/source-stripe/v1.2.0/source_stripe.dylib
```

### Local Cache

```
~/.rapidbyte/
├── config.toml               # Global settings
├── state.db                  # Global SQLite state database
├── plugins/
│   ├── registry-cache.json   # Cached registry metadata
│   ├── source-stripe/
│   │   ├── v1.2.0/
│   │   │   ├── source_stripe.wasm
│   │   │   ├── source_stripe.dylib  # AOT-compiled (.so on Linux, .dylib on macOS, .dll on Windows)
│   │   │   ├── rapidbyte-plugin.yaml
│   │   │   └── sha256.digest
│   │   └── v1.1.0/
│   │       └── ...
│   └── dest-postgres/
│       └── v1.0.5/
│           └── ...
```

### Registry Commands

```bash
# Default registry (configurable in config.toml)
$ rapidbyte registry list
> Official: ghcr.io/rapidbyte  (42 connectors)
> Community: ghcr.io/rapidbyte-community  (18 connectors)

# Add a private registry
$ rapidbyte registry add mycompany ghcr.io/mycompany/rapidbyte-plugins

# Verify plugin signature
$ rapidbyte connectors verify source-stripe@v1.2.0
> Signature: VALID (signed by rapidbyte-bot@rapidbyte.dev)
> Digest: sha256:a1b2c3d4...
```

---

## CLI User Experience

Rapidbyte should feel as snappy and intuitive as modern dev tools like `cargo`, `dbt`, or `npm`. The CLI is the primary interface — it acts as its own package manager, pipeline runner, schema inspector, and analytics engine.

### Workspace Management

```bash
# Initialize a new workspace
$ rapidbyte init my_data_project
> Created my_data_project/
>   ├── pipelines/           # Pipeline YAML definitions
>   ├── connectors/          # Local connector overrides
>   ├── contracts/           # Data contract definitions
>   └── rapidbyte.toml       # Project-level config

$ cd my_data_project
```

### Connector Management

```bash
# Search the registry
$ rapidbyte connectors search stripe
> rapidbyte/source-stripe     v1.2.0  Official   "Stripe API source"
> community/source-stripe-v2  v0.9.0  Community  "Stripe with expanded objects"

# Install plugins (downloads Wasm modules to local cache)
$ rapidbyte connectors add source-stripe dest-postgres
> Fetching source-stripe@v1.2.0 (6.2 MB)... Done in 0.8s
> Fetching dest-postgres@v1.0.5 (8.4 MB)... Done in 1.1s
> Verifying signatures... OK

# List installed connectors
$ rapidbyte connectors list
> source-stripe     v1.2.0  Official   6.2 MB
> dest-postgres     v1.0.5  Official   8.4 MB

# Update all connectors
$ rapidbyte connectors update --all
```

### Pipeline Execution

```bash
# Validate configuration and credentials
$ rapidbyte check pipelines/sync_stripe.yaml
> [OK] source-stripe: API key valid, 12 streams available
> [OK] dest-postgres: Connection successful, schema 'raw_stripe' exists
> [OK] State backend (sqlite): ~/.rapidbyte/state.db, last sync 2h ago

# Dry run (estimates row counts, shows what would sync)
$ rapidbyte run pipelines/sync_stripe.yaml --dry-run
> Stream 'invoices': ~15,230 records (incremental from 2025-01-15T08:00:00Z)
> Stream 'customers': ~8,400 records (full refresh)
> Estimated time: ~4s

# Run the pipeline
$ rapidbyte run pipelines/sync_stripe.yaml
> [08:56:01] INFO  Loaded state from sqlite://~/.rapidbyte/state.db
> [08:56:01] INFO  Syncing stream 'invoices' (incremental)
> [08:56:02] INFO  Syncing stream 'customers' (full refresh)
> [08:56:03] OK    invoices: 15,230 records in 1.8s (8,461 rows/s)
> [08:56:04] OK    customers: 8,400 records in 2.1s (4,000 rows/s)
> [08:56:04] INFO  State saved. Memory peak: 42 MB.

# Run all pipelines in a directory
$ rapidbyte run pipelines/ --parallel
```

### Schema Inspection

```bash
# Discover available streams from a source
$ rapidbyte schema discover source-stripe
> Available streams:
>   customers       (incremental, cursor: updated)
>   invoices        (incremental, cursor: created)
>   charges         (incremental, cursor: created)
>   subscriptions   (incremental, cursor: created)
>   products        (full_refresh)
>   prices          (full_refresh)

# Inspect schema of a specific stream
$ rapidbyte schema inspect source-stripe --stream customers
> customers:
>   id              Utf8        (primary key)
>   email           Utf8
>   name            Utf8
>   created         Timestamp   (cursor field)
>   updated         Timestamp
>   metadata        Json
>   ...
```

### State Management

```bash
# View sync state
$ rapidbyte state show pipelines/sync_stripe.yaml
> State backend: sqlite (~/.rapidbyte/state.db)
>
> Stream 'invoices':
>   cursor: created = 2025-01-15T08:00:00Z
>   last_sync: 2025-01-15T10:56:04Z
>   records_synced: 15,230
>
> Stream 'customers':
>   mode: full_refresh
>   last_sync: 2025-01-15T10:56:04Z
>   records_synced: 8,400

# Reset state for a specific stream (force full refresh)
$ rapidbyte state reset pipelines/sync_stripe.yaml --stream invoices
> State reset for 'invoices'. Next sync will be full refresh.

# Export/import state (for migration between environments)
$ rapidbyte state export pipelines/sync_stripe.yaml > state.json
$ rapidbyte state import pipelines/sync_stripe.yaml < state.json

# Migrate state from SQLite to PostgreSQL (e.g., moving to production)
$ rapidbyte state migrate --from sqlite --to postgres --target ${STATE_PG_URL}
```

### Connector Testing

```bash
# Test a connector with inline config
$ rapidbyte connectors test source-stripe --config api_key=$STRIPE_API_KEY
> Connection: OK
> Auth: Valid (API key ending in ...4x2B)
> Streams: 12 available
> Sample read (customers, 10 rows): OK in 0.3s

# Test a destination
$ rapidbyte connectors test dest-postgres --config host=localhost database=analytics
> Connection: OK
> Schema 'public': writable
> Max batch size: 10,000 rows
```

---

## Pipeline YAML Specification

A single YAML file defines the complete pipeline — source, destination, transforms, error handling, data contracts, and state management. It lives in Git, goes through PR reviews, and natively handles secrets.

### Full Pipeline Example

```yaml
version: "1.0"
pipeline: stripe_to_postgres

source:
  use: rapidbyte/source-stripe@v1.2.0
  config:
    api_key: ${STRIPE_API_KEY}          # Reads from ENV natively
    start_date: "{{ (now() - days(3)) | date('%Y-%m-%dT%H:%M:%SZ') }}"  # Dynamic lookback
    account_id: ${STRIPE_ACCOUNT_ID}    # Optional: Stripe Connect
  streams:
    - name: invoices
      sync_mode: incremental
      cursor_field: created
    - name: customers
      sync_mode: full_refresh
    - name: charges
      sync_mode: incremental
      cursor_field: created
      # Only sync charges above $100
      filter: "amount_cents > 10000"

destination:
  use: rapidbyte/dest-postgres@v1.0.5
  config:
    host: ${PG_HOST}
    port: 5432
    user: ${PG_USER}
    password: ${PG_PASSWORD}
    database: analytics
    schema: raw_stripe
  write_mode: upsert
  primary_key: [id]

# Optional: Transform data between source and destination
transforms:
  - stream: invoices
    operations:
      - rename_column: { from: "amount_due", to: "amount_due_cents" }
      - add_column:
          name: amount_due_dollars
          expression: "CAST(amount_due_cents AS DOUBLE) / 100.0"
      - cast_column: { column: "created", to: "Timestamp" }
  - stream: customers
    operations:
      - drop_columns: [metadata, discount]
      - rename_column: { from: "email", to: "customer_email" }

# Data contract: fail the pipeline if schema doesn't match expectations
contracts:
  - stream: invoices
    required_columns:
      - name: id
        type: Utf8
      - name: amount_due_cents
        type: Int64
      - name: created
        type: Timestamp
    on_violation: fail           # fail | warn | drop_row

# Error handling policy
errors:
  retry:
    max_attempts: 3
    backoff: exponential         # exponential | linear | fixed
    initial_delay: 1s
    max_delay: 30s
    retryable_errors:
      - connection_timeout
      - rate_limit
      - server_error
  on_error: skip_row             # skip_row | fail | dead_letter
  dead_letter:
    destination: rapidbyte/dest-s3@v1.0.0
    config:
      bucket: ${DLQ_BUCKET}
      prefix: "dead-letter/stripe/"

# State management (database-backed)
state:
  backend: sqlite                # sqlite (default) | postgres
  # SQLite uses ~/.rapidbyte/state.db by default (zero-config).
  # For production / shared environments, use PostgreSQL:
  # backend: postgres
  # connection: ${STATE_DATABASE_URL}  # postgres://user:pass@host/rapidbyte_state

# Resource limits
resources:
  max_memory: 256mb
  max_batch_bytes: 64mb          # Byte limit per Arrow RecordBatch (not row count)
  parallelism: 4                 # Concurrent streams

# Post-sync hooks
hooks:
  on_success:
    - exec: "dbt build --select raw_stripe+"
    - exec: "curl -X POST ${SLACK_WEBHOOK} -d '{\"text\": \"Stripe sync complete\"}'"
  on_failure:
    - exec: "curl -X POST ${PAGERDUTY_WEBHOOK} -d '{\"pipeline\": \"stripe_to_postgres\"}'"
```

### Dynamic Config Templating

Config values support a lightweight templating engine (Tera — Jinja2 syntax) for dynamic values. Data engineers rarely hardcode dates — they need dynamic lookbacks to handle late-arriving data, environment-aware configuration, and computed values:

```yaml
source:
  use: rapidbyte/source-stripe@v1.2.0
  config:
    # Dynamic lookback: re-sync the last 3 days to catch late-arriving data
    start_date: "{{ (now() - days(3)) | date('%Y-%m-%dT%H:%M:%SZ') }}"

    # Environment-aware config
    api_key: ${STRIPE_API_KEY}

destination:
  use: rapidbyte/dest-postgres@v1.0.5
  config:
    # Dynamic schema naming (e.g., for date-partitioned schemas)
    schema: "raw_stripe_{{ now() | date('%Y_%m') }}"
```

**Available template functions:**
- `now()` — current UTC timestamp
- `days(n)`, `hours(n)`, `minutes(n)` — duration for arithmetic
- `date(format)` — format a timestamp (strftime syntax)
- `env(name)` — read environment variable (alternative to `${VAR}` syntax)

### Byte-Bounded Batching

Row counts are a dangerous metric for memory. 65k rows of Stripe IDs is 2 MB of RAM. 65k rows of Jira tickets with massive nested JSON could be 2 GB, causing an OOM crash. Rapidbyte batches by **bytes, not rows**:

```yaml
resources:
  max_batch_bytes: 64mb          # Byte limit per Arrow RecordBatch (default)
  max_memory: 256mb              # Total memory ceiling for the pipeline
```

The core engine monitors the byte size of each `RecordBatch` as it's built. When a batch approaches `max_batch_bytes`, it flushes to the destination and starts a new batch — regardless of how many rows that is. This guarantees predictable memory usage whether syncing narrow ID columns or wide JSON blobs.

### Post-Sync Hooks

Ingestion is only step one. Users need to trigger downstream models, send notifications, or update orchestrators. The `hooks` system runs shell commands after pipeline completion:

```yaml
hooks:
  on_success:
    - exec: "dbt build --select raw_stripe+"          # Trigger dbt models
    - exec: "curl -X POST ${SLACK_WEBHOOK} -d '{\"text\": \"Stripe sync complete\"}'"
  on_failure:
    - exec: "curl -X POST ${PAGERDUTY_WEBHOOK} -d '{\"pipeline\": \"stripe_to_postgres\"}'"
  on_complete:                                         # Runs regardless of success/failure
    - exec: "echo 'Pipeline finished' >> /var/log/rapidbyte.log"
```

**Hook environment variables:** Hooks receive context about the completed run via environment variables:

| Variable | Example |
|---|---|
| `RAPIDBYTE_PIPELINE` | `stripe_to_postgres` |
| `RAPIDBYTE_STATUS` | `success` / `failure` |
| `RAPIDBYTE_RECORDS_READ` | `15230` |
| `RAPIDBYTE_RECORDS_WRITTEN` | `15230` |
| `RAPIDBYTE_DURATION_MS` | `2100` |
| `RAPIDBYTE_FAILED_STREAMS` | `charges` (comma-separated, on failure) |

This makes Rapidbyte composable with any orchestrator — Airflow, Dagster, Prefect, cron, or a simple Makefile.

### Low-Code REST Builder

For SaaS APIs without a dedicated connector, Rapidbyte includes a generic YAML-driven HTTP connector:

```yaml
version: "1.0"
pipeline: github_issues_to_postgres

source:
  use: rapidbyte/source-rest@v1.0.0
  config:
    base_url: "https://api.github.com"
    auth:
      type: bearer
      token: ${GITHUB_TOKEN}
    headers:
      Accept: "application/vnd.github+json"
      X-GitHub-Api-Version: "2022-11-28"
  streams:
    - name: issues
      path: "/repos/${GITHUB_OWNER}/${GITHUB_REPO}/issues"
      sync_mode: incremental
      cursor_field: updated_at
      cursor_param: since              # Query param for incremental
      pagination:
        type: link_header              # link_header | cursor | offset | page
      response_path: "$"               # JSONPath to records array
      schema:
        - { name: id, type: Int64 }
        - { name: title, type: Utf8 }
        - { name: state, type: Utf8 }
        - { name: body, type: Utf8 }
        - { name: created_at, type: Timestamp }
        - { name: updated_at, type: Timestamp }
        - { name: user, type: Json }

destination:
  use: rapidbyte/dest-postgres@v1.0.5
  config:
    host: ${PG_HOST}
    user: ${PG_USER}
    password: ${PG_PASSWORD}
    database: analytics
    schema: raw_github

state:
  backend: sqlite
```

---

## Connector Strategy

The graveyard of data tools is filled with platforms that couldn't build enough connectors. To compete, Rapidbyte needs a phased approach:

### Tier 1: Native Connectors (Built by Core Team)

Build the top 20 most-requested connectors natively in Rust → Wasm. Make them bulletproof, benchmark their speed, and use them as proof points:

**Databases:** PostgreSQL, MySQL, SQL Server, MongoDB, Snowflake, BigQuery, ClickHouse
**SaaS APIs:** Stripe, Salesforce, HubSpot, GitHub, Shopify, Google Analytics
**File/Object:** S3, GCS, Azure Blob, Local files (CSV, JSON, Parquet)

### Tier 2: Low-Code REST Builder

The generic `source-rest` connector (shown above) lets users define any REST API source in YAML. Supports:

- Multiple auth types: Bearer, OAuth2 (client_credentials, authorization_code), API key (header/query), Basic
- Pagination strategies: Link header, cursor-based, offset, page number
- Rate limiting: Automatic backoff on 429 responses
- Response extraction: JSONPath for nested response structures
- Schema definition: Explicit or auto-inferred from first response

### Tier 3: Community Wasm Connectors

Connectors are written in Rust — this is a strength, not a limitation. Rust provides the best Wasm performance, direct access to WasmEdge's patched crate ecosystem (reqwest, tokio-postgres, mysql_async), and a single consistent language for the entire connector ecosystem. Rust connectors compile to Wasm for sandboxed, portable execution.

The Connector SDK + OCI distribution makes it straightforward for the community to build and publish connectors. The `rapidbyte connector scaffold` command generates a complete Rust project with patched crate dependencies pre-configured:

```bash
$ rapidbyte connector scaffold --name source-jira
> Created source-jira/
>   ├── Cargo.toml              # Includes [patch.crates-io] for WasmEdge forks
>   ├── .cargo/
>   │   └── config.toml         # RUSTFLAGS for wasm32-wasip1
>   ├── src/
>   │   └── lib.rs              # Implements ConnectorSource trait
>   ├── rapidbyte-plugin.yaml   # Plugin manifest with WASI capabilities
>   ├── test/
>   │   └── fixtures/           # Test data
>   └── README.md

# Build to Wasm
$ cd source-jira && rapidbyte connector build
> Compiled to target/wasm32-wasip1/release/source_jira.wasm (7.2 MB)
> AOT compiling for darwin/arm64... Done in 1.8s

# Test locally
$ rapidbyte connector test --config api_token=$JIRA_TOKEN domain=mycompany

# Publish to registry
$ rapidbyte connector publish --registry ghcr.io/my-org
```

**Example: Database source connector using tokio-postgres directly**

```rust
use rapidbyte_sdk::prelude::*;
use tokio_postgres::{Client, NoTls};
use arrow::array::{StringArray, Int64Array};
use arrow::record_batch::RecordBatch;

struct PostgresSource {
    client: Option<Client>,
}

#[async_trait]
impl ConnectorSource for PostgresSource {
    async fn init(&mut self, config: Json) -> Result<(), ConnectorError> {
        // Connect directly via WASI sockets — no host function proxy
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            config["host"], config["port"], config["user"],
            config["password"], config["database"]
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
        tokio::spawn(connection);
        self.client = Some(client);
        Ok(())
    }

    async fn read(&mut self, request: ReadRequest) -> Result<ReadSummary, ConnectorError> {
        let client = self.client.as_ref().unwrap();
        for stream in &request.streams {
            let rows = client
                .query(&format!("SELECT * FROM {}", stream.name), &[])
                .await?;

            let batch = rows_to_arrow_batch(&rows, &stream.schema)?;
            emit_record_batch(&stream.name, &batch);
            report_progress(rows.len() as u64, batch.get_array_memory_size() as u64);
        }
        Ok(ReadSummary { /* ... */ })
    }
}
```

The connector uses standard `tokio_postgres` — the same API a native Rust application would use. The patched fork routes TCP/TLS through WASI sockets transparently. Only `emit_record_batch` and `report_progress` are host function calls.

### Tier 4: Airbyte Compatibility Layer (Escape Hatch)

For the long tail, build a wrapper that can execute Airbyte Docker connectors. Users can adopt Rapidbyte immediately for its UX and speed on core pipelines while falling back to Airbyte's connectors when necessary:

```yaml
source:
  use: rapidbyte/bridge-airbyte@v1.0.0
  config:
    image: airbyte/source-zendesk:0.3.0
    config:
      api_token: ${ZENDESK_TOKEN}
      subdomain: mycompany
```

This is explicitly a migration tool — the goal is to provide coverage while native/community Wasm connectors catch up.

---

## CDC Support

Change Data Capture is critical for database sources where full refreshes are impractical. Rapidbyte supports CDC as a first-class sync mode.

### Fat Connectors and CDC

The fat connector model simplifies CDC architecture significantly. Because connectors hold their own network connections via WASI sockets, they can maintain persistent connections to database replication slots directly — no complex host function needed for long-lived streaming. A CDC connector simply opens a replication connection using the patched `tokio-postgres` crate and streams WAL changes as Arrow RecordBatches to the host pipeline via `emit_record_batch`.

### WAL-Based CDC

For supported databases, Rapidbyte reads directly from the write-ahead log:

- **PostgreSQL:** Logical replication slots (pgoutput / wal2json)
- **MySQL:** Binlog streaming (GTID-based)
- **MongoDB:** Change streams (oplog)

```yaml
source:
  use: rapidbyte/source-postgres@v1.0.0
  config:
    host: ${PG_HOST}
    user: ${PG_REPLICATION_USER}
    password: ${PG_PASSWORD}
    database: production
    replication_slot: rapidbyte_slot
    publication: rapidbyte_pub
  streams:
    - name: orders
      sync_mode: cdc
      primary_key: [id]
    - name: order_items
      sync_mode: cdc
      primary_key: [id]
```

### Debezium Compatibility

For teams already using Debezium, Rapidbyte can consume Debezium-formatted events from Kafka:

```yaml
source:
  use: rapidbyte/source-kafka@v1.0.0
  config:
    brokers: ["kafka-1:9092", "kafka-2:9092"]
    group_id: rapidbyte-cdc
    topics: ["dbserver1.inventory.orders"]
    format: debezium_json
```

### CDC Event Types

Rapidbyte normalizes all CDC events into a standard envelope:

```
┌─────────────────────────────────────────┐
│ CDC Event                               │
│  op: INSERT | UPDATE | DELETE | SNAPSHOT │
│  ts: 2025-01-15T10:30:00Z              │
│  before: { ... }  (UPDATE/DELETE only)  │
│  after:  { ... }  (INSERT/UPDATE only)  │
│  source: { lsn, txid, ... }            │
└─────────────────────────────────────────┘
```

---

## Schema Evolution & Data Contracts

Production data pipelines break when schemas change. Rapidbyte handles schema evolution explicitly rather than silently dropping or corrupting data.

### Schema Evolution Policies

Configure per-stream how schema changes are handled:

```yaml
schema_evolution:
  on_new_column: add           # add | ignore | fail
  on_removed_column: ignore    # ignore | drop | fail
  on_type_change: coerce       # coerce | fail | ignore
  on_nullable_change: allow    # allow | fail
```

**Coercion rules:**
- `Int → BigInt`: Always safe, widen automatically
- `Int → Utf8`: Safe, convert to string representation
- `Utf8 → Int`: Attempt parse, fail or null on error (configurable)
- `Timestamp precision change`: Widen to higher precision

### Data Contracts

Data contracts let teams define expectations about incoming data. Contract violations can fail the pipeline, emit warnings, or drop offending rows:

```yaml
contracts:
  - stream: orders
    required_columns:
      - { name: id, type: Int64, nullable: false }
      - { name: customer_id, type: Int64, nullable: false }
      - { name: total_cents, type: Int64 }
      - { name: created_at, type: Timestamp }
    column_rules:
      - column: total_cents
        check: "value >= 0"
        on_violation: drop_row
      - column: email
        check: "value LIKE '%@%'"
        on_violation: warn
    on_violation: fail           # Default action for schema violations
    notify:
      slack_webhook: ${SLACK_WEBHOOK}
```

---

## Observability & Telemetry

Rapidbyte is built with observability as a first-class concern, not an afterthought.

### OpenTelemetry Integration

All telemetry uses the OpenTelemetry standard — traces, metrics, and logs export to any OTel-compatible backend (Grafana, Datadog, Honeycomb, Jaeger):

```toml
# ~/.rapidbyte/config.toml
[telemetry]
enabled = true
exporter = "otlp"
endpoint = "http://localhost:4317"

[telemetry.resource]
service.name = "rapidbyte"
deployment.environment = "production"
```

### Metrics

Rapidbyte emits the following metrics:

| Metric | Type | Description |
|---|---|---|
| `rapidbyte.records.read` | Counter | Records read from source |
| `rapidbyte.records.written` | Counter | Records written to destination |
| `rapidbyte.records.failed` | Counter | Records that failed processing |
| `rapidbyte.bytes.read` | Counter | Bytes read from source |
| `rapidbyte.bytes.written` | Counter | Bytes written to destination |
| `rapidbyte.sync.duration_ms` | Histogram | Sync duration per stream |
| `rapidbyte.sync.batch_duration_ms` | Histogram | Per-batch processing time |
| `rapidbyte.memory.peak_bytes` | Gauge | Peak memory usage |
| `rapidbyte.memory.current_bytes` | Gauge | Current memory usage |
| `rapidbyte.plugin.load_duration_ms` | Histogram | Wasm plugin cold start time |
| `rapidbyte.state.checkpoint_count` | Counter | State checkpoints written |
| `rapidbyte.errors.count` | Counter | Errors by type and stream |

### Structured Logging

All log output is structured JSON (for machines) or human-readable (for terminals), configurable via `RAPIDBYTE_LOG_FORMAT`:

```bash
# Human-readable (default for interactive terminals)
$ rapidbyte run sync_stripe.yaml
> [08:56:01] INFO  pipeline=stripe_to_postgres stream=invoices msg="Starting sync"
> [08:56:03] INFO  pipeline=stripe_to_postgres stream=invoices records=15230 duration=1.8s msg="Sync complete"

# JSON (for log aggregators)
$ RAPIDBYTE_LOG_FORMAT=json rapidbyte run sync_stripe.yaml
> {"ts":"2025-01-15T08:56:01Z","level":"info","pipeline":"stripe_to_postgres","stream":"invoices","msg":"Starting sync"}
```

### Distributed Tracing

Each pipeline run creates a trace with spans for:
- Pipeline initialization
- Plugin loading (per connector)
- Stream sync (per stream)
- Batch read / transform / write (per batch)
- State checkpoint

This enables pinpointing exactly where time is spent — is the source slow? Is the destination the bottleneck? Is a transform expensive?

---

## Error Handling

Rapidbyte classifies errors and applies configurable retry and recovery policies.

### Error Classification

| Category | Examples | Default Action |
|---|---|---|
| **Retryable** | Connection timeout, rate limit (429), server error (5xx) | Retry with backoff |
| **Configuration** | Invalid credentials, missing permissions, wrong endpoint | Fail immediately |
| **Data** | Schema mismatch, type coercion failure, constraint violation | Per-policy (skip/fail/DLQ) |
| **System** | OOM, disk full, plugin crash | Fail immediately |

### Retry Policies

```yaml
errors:
  retry:
    max_attempts: 3
    backoff: exponential
    initial_delay: 1s
    max_delay: 60s
    jitter: true                 # Prevent thundering herd
```

### Dead Letter Queue

Records that fail processing can be routed to a dead letter destination for later inspection and replay:

```yaml
errors:
  on_error: dead_letter
  dead_letter:
    destination: rapidbyte/dest-s3@v1.0.0
    config:
      bucket: ${DLQ_BUCKET}
      prefix: "dead-letter/${pipeline}/"
      format: parquet            # parquet | json
    include_error: true          # Include error message with each record
```

### Circuit Breaker

If error rates exceed a threshold, Rapidbyte halts the pipeline to prevent cascading damage:

```yaml
errors:
  circuit_breaker:
    error_threshold: 100         # Consecutive errors before tripping
    error_rate: 0.1              # 10% error rate threshold
    cooldown: 60s                # Wait before retry after trip
```

---

## Local Analytics (DataFusion)

DataFusion is already in the binary as the transform engine — Rapidbyte reuses it as a local SQL analytics engine via the `query` command. No additional dependency, no separate database, no extra binary size.

### Why Not DuckDB?

Adding DuckDB would mean bundling a separate C++ query engine alongside DataFusion, which already does the same job:

- **DataFusion operates on Arrow natively** — the same format Rapidbyte uses internally. Zero conversion overhead.
- **Already linked** — DataFusion is in the binary for transforms. Reusing it for analytics adds zero dependency weight.
- **Parquet support** — DataFusion reads Parquet files directly, same as DuckDB would.
- **Extensible** — custom UDFs registered for transforms are automatically available in `rapidbyte query`.
- **Single engine** — one SQL dialect to learn, one set of behaviors to reason about, one codebase to maintain.

### Use Cases

- **Pipeline debugging:** Inspect synced data without connecting to the warehouse
- **Quick analysis:** Answer ad-hoc questions without spinning up infrastructure
- **Data validation:** Verify data quality post-sync with SQL assertions
- **Local development:** Work with production-like data locally

### Usage

```bash
# Query synced data (reads from local Parquet cache via DataFusion)
$ rapidbyte query "SELECT customer_email, COUNT(*) as order_count
                    FROM raw_stripe.invoices
                    GROUP BY customer_email
                    ORDER BY order_count DESC
                    LIMIT 10"

# Export query results
$ rapidbyte query "SELECT * FROM raw_stripe.customers WHERE created > '2025-01-01'" \
    --format csv --output customers_q1.csv

# Interactive SQL shell
$ rapidbyte query --interactive
rapidbyte> SELECT COUNT(*) FROM raw_stripe.invoices;
> 15230
rapidbyte> \tables
> raw_stripe.customers
> raw_stripe.invoices
> raw_stripe.charges
```

### Local Cache

Rapidbyte stores synced data as Parquet files in the workspace, with state tracked in a database:

```
my_data_project/
├── .rapidbyte/
│   ├── state.db               # SQLite state database (cursors, sync history, metadata)
│   └── cache/
│       ├── raw_stripe/
│       │   ├── invoices.parquet
│       │   ├── customers.parquet
│       │   └── charges.parquet
│       └── raw_github/
│           └── issues.parquet
```

DataFusion reads the Parquet files directly — no import step, no data duplication. State lives in SQLite (or PostgreSQL in production), separate from the data cache.

---

## Development Roadmap

### v0.1 — Core Engine (Foundation)

- Rust CLI skeleton with `tokio` async runtime
- YAML pipeline parser and validator
- WasmEdge Wasm runtime integration (with WASI sockets)
- Arrow RecordBatch pipeline (source → destination)
- SQLite state backend (default)
- Basic structured logging
- 2 proof-of-concept connectors: `source-postgres` (read) and `dest-postgres` (write) as fat Wasm modules
- `rapidbyte run`, `rapidbyte check` commands

### v0.2 — Connector SDK & Distribution

- Connector SDK with `init`, `discover`, `validate`, `read`, `write` protocol and patched crate templates for Rust fat connectors
- `rapidbyte connector scaffold` command (generates Rust project with WasmEdge fork dependencies)
- `rapidbyte connector build` (compile to `wasm32-wasip1`)
- OCI registry push/pull (`rapidbyte connector publish`)
- Plugin manifest format and WASI capability-based security
- `rapidbyte connectors search/add/update/list` commands

### v0.2.5 — AOT Compilation Pipeline

- Deferred AOT compilation on first connector use via `wasmedge compile`
- Local AOT artifact caching keyed by Wasm SHA-256 digest
- `rapidbyte connectors compile` manual AOT command
- Optional pre-compiled AOT artifacts in OCI registry

### v0.3 — Core Connectors & Transforms

- Native Tier 1 connectors: PostgreSQL, MySQL, Snowflake, BigQuery, S3, Stripe, Salesforce
- `source-rest` low-code HTTP connector
- DataFusion-powered transform layer (rename, cast, filter, computed columns)
- PostgreSQL state backend (for production / shared environments)
- State migration command (`rapidbyte state migrate`)
- Incremental sync with cursor-based state management
- `rapidbyte schema discover/inspect` commands

### v0.4 — Data Quality & Reliability

- Data contracts (schema validation, column rules)
- Schema evolution policies (add/ignore/fail on changes)
- Error handling framework (retry, dead letter queue, circuit breaker)
- Parallel stream execution
- Byte-bounded batching and memory-bounded execution
- `rapidbyte state show/reset/export/import` commands

### v0.5 — CDC & Advanced Sources

- PostgreSQL CDC (logical replication)
- MySQL CDC (binlog)
- MongoDB CDC (change streams)
- Debezium-compatible Kafka consumer
- CDC event normalization
- Airbyte compatibility bridge

### v0.6 — Observability & Analytics

- OpenTelemetry integration (traces, metrics, logs)
- DataFusion-powered `rapidbyte query` command (reuses existing transform engine)
- Local Parquet cache for offline analytics
- Pipeline performance profiling
- Grafana dashboard templates

### v0.7 — Ecosystem & Community

- Connector bounty program
- Community connector showcase
- Plugin signing and verification (Sigstore/Cosign)
- `rapidbyte connector test` automated test harness
- Documentation site and tutorials
- Additional connectors: HubSpot, Shopify, GitHub, Google Analytics, ClickHouse, MongoDB (dest)

### v1.0 — Production Ready

- Stability guarantees on pipeline YAML format and Connector SDK
- Comprehensive test suite (unit, integration, end-to-end)
- Performance benchmarks published (vs Airbyte, dlt, CloudQuery, Sling)
- Security audit of Wasm sandbox and plugin isolation
- Long-running pipeline support (daemon mode for CDC)
- Migration guides from Airbyte, Meltano, and dlt

---

## Commercialization

The OSS core remains fully featured. Revenue comes from the control plane and ecosystem.

### Open Source (Apache 2.0)

Everything that runs locally is free forever:
- CLI binary
- Core engine (scheduler, Arrow pipeline, SQLite/PostgreSQL state management)
- All pipeline YAML features (transforms, contracts, error handling)
- Connector SDK and build tooling
- OCI registry integration
- DataFusion local analytics
- OpenTelemetry integration

### Rapidbyte Cloud (Control Plane SaaS)

The managed orchestration layer — customers run Rapidbyte on their own infrastructure, Rapidbyte Cloud provides the UI and scheduling. **Data never leaves the customer's VPC.**

**Free Tier:**
- Up to 5 pipelines
- Manual triggers only
- 7-day run history
- Community support

**Team ($49/month per workspace):**
- Unlimited pipelines
- Cron scheduling
- 90-day run history
- Slack/email alerting
- Git integration (auto-deploy on merge)
- Team RBAC (admin, editor, viewer)

**Enterprise (Custom pricing):**
- SSO (SAML/OIDC)
- Audit logs with export
- Custom RBAC policies
- SLA guarantees (99.9% control plane uptime)
- Dedicated support
- Custom connector development
- On-premises control plane option

### Connector Marketplace

Revenue share model for premium connectors:
- **Official connectors:** Free, maintained by Rapidbyte team
- **Partner connectors:** Built by SaaS vendors (e.g., Salesforce builds their own optimized connector), free or vendor-subsidized
- **Premium community connectors:** Community-built, optional paid tier with SLA guarantees and priority support
- **Enterprise connectors:** Custom connectors for legacy/proprietary systems, built by Rapidbyte professional services

### Revenue Projections (Rough Modeling)

The control plane model avoids the cost trap of metered pricing:
- No compute costs (runs on customer infrastructure)
- No data transfer costs (data stays in customer VPC)
- No SOC2 compliance burden for data handling
- Predictable, seat-based pricing

---

## Community & Ecosystem

### Contributor Philosophy

- **Low barrier to entry:** Writing a connector should take hours, not weeks. The SDK generates a complete Rust project scaffold with patched crate dependencies, test fixtures, and Wasm build configuration pre-configured. Contributors write standard async Rust.
- **Unified ecosystem:** Connectors are written in Rust — the same language as the core engine. This provides the best Wasm performance, direct access to WasmEdge's patched networking crates, and ecosystem consistency. One language means shared tooling, shared expertise, and shared libraries.
- **First-class testing:** `rapidbyte connector test` runs automated tests against connector implementations, including schema validation, incremental sync verification, and error handling checks.

### Connector Bounty Program

Fund community connector development for the long tail:
- Published bounty list for most-requested connectors
- Tiered rewards based on connector complexity ($500 - $5,000)
- Bounty connectors go through review and become official/community-tier
- Top contributors get recognition and early access to Rapidbyte Cloud

### Governance

- **Core engine:** Maintained by Rapidbyte team, accepting PRs with CLA
- **Official connectors:** Maintained by Rapidbyte team, community PRs welcome
- **Community connectors:** Maintained by authors, published to community registry namespace
- **RFC process:** Major changes to the pipeline YAML spec, Connector SDK, or core architecture go through an RFC process with community input
- **Semantic versioning:** Pipeline YAML format and Connector SDK follow strict semver — breaking changes only in major versions

### Documentation & Learning

- **Quickstart:** "Sync Postgres to Snowflake in 5 minutes"
- **Connector development guide:** Step-by-step tutorial for building a Wasm connector
- **Architecture deep-dive:** How Arrow, DataFusion, and WasmEdge work together
- **Migration guides:** "Moving from Airbyte/Meltano/dlt to Rapidbyte"
- **Example pipelines:** Curated repository of real-world pipeline configurations
