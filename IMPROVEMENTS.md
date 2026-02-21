# Rapidbyte — Strategic Improvement Roadmap

Prioritized improvements organized into three tiers, informed by competitive analysis of Airbyte, dlt, Fivetran, and Estuary Flow.

## Rapidbyte's Structural Advantages

Before diving into gaps, it's worth naming what Rapidbyte already does better than the field:

- **WASM-sandboxed connectors**: Container-like isolation at near-zero overhead. Airbyte spawns two Docker containers per sync; Rapidbyte instantiates WASM modules in microseconds.
- **Arrow IPC data format**: Zero-copy columnar transfer between stages. Airbyte is still migrating away from JSON-lines serialization.
- **Language-agnostic connectors**: Anything that compiles to WASM works — Rust, Go, C, Zig, AssemblyScript.
- **Structured error model**: 7 error categories, 3 scopes, backoff classes, commit-state tracking. Richer retry semantics than any competitor.
- **Exactly-once checkpoint correlation**: Source + destination checkpoint correlation before persisting cursors. Comparable to Estuary Flow's cooperative transactions but simpler.
- **Single-binary deployment**: No Kubernetes, no Docker, no Python runtime. Just one binary and .wasm files.

The roadmap below builds on these advantages while closing critical gaps.

---

## P0 — Foundation (Production-Viable Engine)

Items the engine needs before anyone can run it in production with confidence.

### Pipeline Parallelism

Currently hardcoded to sequential execution (parallelism=1). The orchestrator should support concurrent stream processing — run multiple source streams in parallel feeding into the destination. This is table-stakes: Airbyte's concurrent CDK and dlt's threaded extract both do this. For Rapidbyte, each stream can get its own WASM instance since they're cheap to instantiate — a structural advantage over Airbyte's container model.

### CDC (Change Data Capture)

The protocol defines CDC mode but no connector implements it. Postgres logical replication (pgoutput) is the obvious first target. Estuary Flow built their entire platform around streaming CDC. Rapidbyte doesn't need to go that far, but supporting CDC for database sources is expected. The host import `connect-tcp` + streaming socket reads already support the needed I/O pattern.

### Schema Evolution Enforcement

The protocol defines schema evolution policies (new_column, type_change, nullability_change) but the engine ignores them. dlt's schema contracts are the gold standard here — support `evolve`, `freeze`, and `fail` modes at minimum. Fivetran and Airbyte both auto-detect and propagate schema changes; Rapidbyte should at least enforce the policies it already defines.

### Dead Letter Queue (DLQ)

`on_data_error: dlq` is parsed but not implemented. When a record fails (bad type, constraint violation), it should be routed to a separate stream/table with error metadata rather than failing the entire batch. This is standard in every production pipeline tool.

### Projection Pushdown

Source connectors currently read all columns. The pipeline config should allow specifying which columns to sync per stream, and push that down to the source query. Fivetran and Airbyte both support column selection. Reduces I/O, memory, and destination storage.

---

## P1 — Competitive Parity (Close the Biggest Gaps)

Features that move Rapidbyte from "works" to "competitive" — things users expect when comparing against established tools.

### Observability & Metrics Pipeline

Connectors already emit metrics via host imports, but they're logged and discarded. Persist metrics to the state backend and expose them in a structured format (Prometheus/OpenTelemetry compatible). Estuary Flow exposes OpenMetrics; Airbyte exports to Prometheus. At minimum: records read/written, bytes, duration, error counts per stream per run — queryable after the fact.

### Schema Contracts

Inspired by dlt's best-in-class implementation. Let users declare schema expectations per stream — column types, nullability, required fields — and choose what happens on violation (fail, coerce, discard row, discard column). This is distinct from schema evolution (P0 handles structural changes at the destination); contracts validate data shape at the source boundary before it enters the pipeline.

### Connector Developer Experience

The SDK works but building a connector requires deep WASM knowledge. Improvements: better error messages when compilation fails, a local test harness that runs connectors against fixture data without Docker or a real database, and documentation with a step-by-step tutorial. dlt's "pure Python decorators" and Airbyte's no-code builder set the bar — Rapidbyte won't match that simplicity, but the Rust SDK should be as ergonomic as possible.

### Retry & Recovery Improvements

The error model is already richer than competitors (7 categories, 3 scopes, backoff classes). Build on it: support partial batch retry (skip failed records, retry the rest), automatic re-sync detection when state is corrupted (Fivetran added this in 2025), and resumable runs that pick up from the last successful checkpoint after a crash.

### Transform Checkpoint Persistence

Transform checkpoints are currently ignored with a log message. For stateful transforms (aggregations, deduplication windows), checkpoints need to survive restarts. Persist them alongside source/dest checkpoints in the state backend.

---

## P2 — Differentiation (Leverage WASM/Arrow Advantages)

Features that lean into what makes Rapidbyte structurally different — things competitors can't easily replicate.

### Edge & Embedded Deployment

Rapidbyte's single-binary + WASM model means it can run anywhere — IoT gateways, edge servers, embedded in other applications. No competitor offers this. Package the engine as a library crate (`rapidbyte-core` is already close) so it can be embedded in Rust applications. Support ARM targets. This positions Rapidbyte for use cases Airbyte/Fivetran physically cannot serve.

### Connector Marketplace & Registry

WASM binaries are portable, versioned, and sandboxed — ideal for distribution. Build a registry where connectors can be published, discovered, and pulled by version (`rapidbyte connector pull source-postgres@v0.2.0`). Airbyte uses Docker Hub; Rapidbyte can use something far lighter (OCI registry, or a simple HTTP registry). This is the foundation for ecosystem growth.

### Declarative Connector Builder

Most API sources follow the same pattern: paginated REST, OAuth/API key auth, cursor-based incremental. Define a YAML/JSON spec that describes the API, and compile it to a WASM connector automatically. Airbyte's no-code builder proved this covers 80% of API connectors. Rapidbyte's version compiles to the same sandboxed WASM binary — no runtime interpreter needed.

### Hash-Based Change Detection

Inspired by Fivetran's Teleport Sync. For databases where CDC/binlog access is impractical (permissions, managed databases), compute row hashes and compare across syncs. Store hashes in the state backend. This provides near-incremental performance with only read-only SQL access. Unique among open-source tools.

### Managed Cloud Offering

The long-term play. A hosted service that runs Rapidbyte pipelines with scheduling, monitoring, alerting, and a web UI. The WASM sandboxing model makes multi-tenant execution safe by design — each connector runs in its own sandbox with explicit permissions. This is a structural advantage over Airbyte's container model for multi-tenancy (lower overhead, faster startup, stronger isolation guarantees).

---

## Competitive Context

| Dimension | Airbyte | dlt | Fivetran | Estuary Flow | Rapidbyte (today) | Rapidbyte (target) |
|---|---|---|---|---|---|---|
| Connector isolation | Docker containers | None (in-process) | Proprietary | gRPC sidecar | WASM sandbox | WASM sandbox |
| Data format | JSON → Protobuf | Python dicts/Arrow | Proprietary | JSON/Protobuf | Arrow IPC | Arrow IPC |
| Connector count | 600+ | ~200 | 700+ | 200+ | 2 | 20+ (with declarative builder) |
| Delivery guarantee | At-least-once | At-least-once | Idempotent merge | Exactly-once | Exactly-once | Exactly-once |
| Parallelism | Per-stream concurrent | Threaded extract | Managed internally | Streaming | None (sequential) | Per-stream concurrent |
| CDC | Yes | No | Yes (Teleport + HVR) | Yes (core feature) | Protocol only | Postgres logical replication |
| Schema evolution | Auto-detect + soft-delete | Contracts + variants | Auto-detect + soft-delete | JSON Schema + inference | Defined, not enforced | Enforced with contracts |
| Deployment | Kubernetes / Cloud | Library (anywhere) | SaaS / Hybrid | Cloud only | Single binary | Single binary + Cloud |
| Self-hosting complexity | High (K8s) | None (library) | Enterprise only | Not supported | Low (one binary) | Low (one binary) |
