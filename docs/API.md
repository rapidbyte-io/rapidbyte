# RapidByte — API Reference

> Hexagonal architecture: the controller owns the entire external API
> surface — REST (axum) for CLI and web clients, gRPC (tonic) for
> internal agent communication. Both are integral to the controller.
>
> **Status:** The REST endpoints in this document are the design
> specification. The controller currently serves gRPC only; the REST
> adapter is planned.

---

## Table of Contents

1. [Overview](#1-overview)
   - [Hexagonal architecture](#hexagonal-architecture)
   - [AppContext: the DI container](#appcontext-the-di-container)
   - [Crate structure](#crate-structure)
2. [Authentication](#2-authentication)
3. [Conventions](#3-conventions)
   - [Error format](#error-format)
   - [Pagination](#pagination)
   - [Async operations](#async-operations)
   - [Server-Sent Events (SSE)](#server-sent-events-sse)
4. [Pipelines](#4-pipelines)
   - [Driving port: PipelineService](#driving-port-pipelineservice)
   - [List pipelines](#list-pipelines)
   - [Get pipeline](#get-pipeline)
   - [Sync pipeline](#sync-pipeline)
   - [Sync all / by tag](#sync-all--by-tag)
   - [Check pipeline](#check-pipeline)
   - [Check and apply](#check-and-apply)
   - [Compile pipeline](#compile-pipeline)
   - [Diff pipeline](#diff-pipeline)
   - [Assert pipeline](#assert-pipeline)
   - [Teardown pipeline](#teardown-pipeline)
5. [Runs](#5-runs)
   - [Driving port: RunService](#driving-port-runservice)
   - [Get run](#get-run)
   - [List runs](#list-runs)
   - [Run events (SSE)](#run-events-sse)
   - [Cancel run](#cancel-run)
   - [Get batch](#get-batch)
   - [Batch events (SSE)](#batch-events-sse)
6. [Connections](#6-connections)
   - [Driving port: ConnectionService](#driving-port-connectionservice)
   - [List connections](#list-connections)
   - [Get connection](#get-connection)
   - [Test connection](#test-connection)
   - [Discover streams](#discover-streams)
7. [Plugins](#7-plugins)
   - [Driving port: PluginService](#driving-port-pluginservice)
   - [List installed plugins](#list-installed-plugins)
   - [Search registry](#search-registry)
   - [Get plugin info](#get-plugin-info)
   - [Install plugin](#install-plugin)
   - [Remove plugin](#remove-plugin)
8. [Operations](#8-operations)
   - [Driving port: OperationsService](#driving-port-operationsservice)
   - [Status dashboard](#status-dashboard)
   - [Pipeline status detail](#pipeline-status-detail)
   - [Pause pipeline](#pause-pipeline)
   - [Resume pipeline](#resume-pipeline)
   - [Reset pipeline state](#reset-pipeline-state)
   - [Freshness](#freshness)
   - [Logs](#logs)
   - [Log stream (SSE)](#log-stream-sse)
9. [Server](#9-server)
   - [Driving port: ServerService](#driving-port-serverservice)
   - [Health check](#health-check)
   - [Version](#version)
   - [Server config](#server-config)

---

## 1. Overview

### Hexagonal architecture

RapidByte follows a hexagonal (ports & adapters) architecture with two
hexagonal boundaries — `rapidbyte-engine` and `rapidbyte-controller` —
each with its own domain, ports, adapters, and application core.

The controller owns ALL external surfaces: REST (planned) + gRPC
(implemented). The engine is a pure execution library with no network
surface of its own.

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │                      DRIVING ADAPTERS                              │
 │  ┌──────────┐  ┌──────────────┐  ┌──────────────────┐             │
 │  │ CLI      │  │ axum (REST)  │  │ scheduler (cron) │             │
 │  │ (clap)   │  │ (planned)    │  │ (planned)        │             │
 │  └────┬─────┘  └──────┬───────┘  └────────┬─────────┘             │
 │       │               │                   │                       │
 ├───────┴───────────────┴───────────────────┴───────────────────────┤
 │                    DRIVING PORTS (traits)                          │
 │  PipelineService · RunService · ConnectionService                 │
 │  PluginService · OperationsService · ServerService                │
 ├────────────────────────────────────────────────────────────────────┤
 │                                                                    │
 │                    rapidbyte-controller                            │
 │                                                                    │
 │  REST API + gRPC + scheduling                                      │
 │  AppContext { repos, event_bus, secrets, clock, config }            │
 │                                                                    │
 │  Orchestrates: task distribution, run lifecycle,                    │
 │  agent coordination (gRPC), and REST API (planned).                │
 │                                                                    │
 ├─────────────────────────┬──────────────────────────────────────────┤
 │  DRIVEN: engine         │  DRIVEN: distributed workers            │
 │  (local / standalone)   │  (controller + agents via gRPC)         │
 │                         │                                          │
 │  PluginRunner           │  RunRepository · TaskRepository         │
 │  PluginResolver         │  AgentRepository · EventBus             │
 │  CursorRepository       │  PipelineStore                          │
 │  RunRecordRepository    │  Clock · SecretResolver                 │
 │  DlqRepository          │                                          │
 │  ProgressReporter       ├──────────────────────────────────────────┤
 │  MetricsSnapshot        │  DRIVEN ADAPTERS (distributed)          │
 │                         │  ┌──────────┐  ┌──────────────────────┐ │
 ├─────────────────────────┤  │ Postgres  │  │ gRPC (agent ↔ ctrl) │ │
 │  DRIVEN ADAPTERS (eng)  │  └──────────┘  └──────────────────────┘ │
 │  ┌──────────┐ ┌───────┐│                                          │
 │  │ Wasmtime │ │ OCI   ││                                          │
 │  │ runner   │ │ reg   ││                                          │
 │  └──────────┘ └───────┘│                                          │
 │  ┌──────────┐ ┌───────┐│                                          │
 │  │ Pg/SQLite│ │ OTel  ││                                          │
 │  └──────────┘ └───────┘│                                          │
 └─────────────────────────┴──────────────────────────────────────────┘
```

The driving port traits (service traits) are defined inside
`rapidbyte-controller`. Every consumer — CLI, REST, scheduler — depends
only on these traits. They never reach past the controller into engine
internals.

The driven side has **two execution modes**, selected at startup:

- **Engine** (`rapidbyte-engine`) — used in local (`sync`) and standalone
  (`serve`) modes. The controller embeds the engine and delegates
  directly to its use-cases via `EngineContext`. Driven adapters:
  Wasmtime, OCI registry, Postgres/SQLite, OTel.

- **Distributed** — used in `rapidbyte serve --metadata-database-url` mode.
  The controller manages task distribution across remote agents via gRPC.
  Each agent runs `rapidbyte-engine` locally. Driven adapters: Postgres, gRPC.

### Two hexagons

Each hexagon follows the same internal layout:

```
rapidbyte-controller                     rapidbyte-engine
────────────────────                     ────────────────
domain/ports/                            domain/ports/
  RunRepository                            PluginRunner
  TaskRepository                           PluginResolver
  AgentRepository                          CursorRepository
  PipelineStore                            RunRecordRepository
  EventBus                                 DlqRepository
  Clock                                    ProgressReporter
  SecretResolver                           MetricsSnapshot

application/                             application/
  submit, poll, complete                   check, run, discover
  cancel, heartbeat                        teardown
  register, query, timeout
  background (lease_sweep,               context.rs
    agent_reaper)                           EngineContext
                                           Arc<dyn Trait>…
context.rs
  AppContext                             adapter/
  Arc<dyn Trait>…                          wasm_runner
                                           registry_resolver
adapter/                                   postgres/
  postgres/                                progress, metrics
  grpc/            (implemented)
  rest/            (planned)
  clock, secrets
```

Both crates share the same conventions:
- **Domain/ports** define trait interfaces (driven ports)
- **Application** contains use-case functions and a DI context
- **Adapters** implement traits with concrete infrastructure
- Tests use in-memory fakes via a `testing` module

### Deployment-mode routing

Routing depends on how RapidByte is invoked:

- **CLI local** — `rapidbyte sync` routes directly to
  `rapidbyte-engine` (in-process, no network hop).
- **CLI distributed** — `rapidbyte sync --controller <url>` sends
  requests to a running controller via gRPC.
- **`rapidbyte serve`** (planned standalone mode) — the controller
  embeds the engine, serves REST + gRPC on the same port.
- **`rapidbyte serve --metadata-database-url`** (distributed) — the
  controller coordinates remote agents via gRPC, serves REST.

```
┌──────────────────┐     ┌──────────────────────────┐
│  CLI (local)     │────→│   rapidbyte-engine       │
│                  │     │   (in-process)            │
└──────────────────┘     └──────────────────────────┘

┌──────────────────┐     ┌──────────────────────────┐
│  CLI             │     │   rapidbyte-controller   │
│  (distributed)   │────→│   (gRPC client)          │
└──────────────────┘     └──────────────────────────┘

┌──────────────────┐     ┌──────────────────────────┐
│  rapidbyte serve │     │   rapidbyte-controller   │
│  (standalone)    │────→│   embeds engine           │
│                  │     │   REST + gRPC             │
└──────────────────┘     └──────────────────────────┘

┌──────────────────┐     ┌──────────────────────────┐
│  rapidbyte serve │     │   rapidbyte-controller   │
│  --metadata-     │────→│   coordinates agents     │
│  database-url    │     │   REST + gRPC             │
└──────────────────┘     └──────────────────────────┘
```

```rust
// Current: start the controller gRPC server
rapidbyte_controller::run(config, otel_guard, secrets).await

// Planned: unified server (gRPC + REST on the same port)
rapidbyte_controller::serve(config).await
```

### Crate dependency graph

```
cli  ──→ controller ──→ types, pipeline-config, secrets, metrics
     ──→ agent     ──→ engine, types, pipeline-config, secrets, metrics, registry
     ──→ engine    ──→ runtime, state, types
```

Note: controller does NOT depend on engine. The agent bridges controller
and engine.

### Crate structure

```
rapidbyte-controller/
├── lib.rs              # re-exports: ControllerConfig, ServerTlsConfig, run()
├── domain/
│   ├── ports/          # driven port traits (RunRepository, TaskRepository, etc.)
│   ├── run.rs          # Run entity + RunState
│   ├── task.rs         # Task entity + TaskState
│   ├── agent.rs        # Agent entity
│   ├── lease.rs        # Lease value object
│   ├── event.rs        # DomainEvent enum
│   └── error.rs        # DomainError
├── application/
│   ├── context.rs      # AppContext (DI container) + AppConfig
│   ├── submit.rs       # submit_pipeline()
│   ├── poll.rs         # poll_task()
│   ├── complete.rs     # complete_task()
│   ├── heartbeat.rs    # heartbeat()
│   ├── cancel.rs       # cancel_run()
│   ├── query.rs        # get_run(), list_runs()
│   ├── register.rs     # register(), deregister()
│   ├── timeout.rs      # handle_task_timeout()
│   ├── background/     # lease_sweep, agent_reaper
│   ├── error.rs        # AppError
│   └── testing.rs      # in-memory fakes
├── adapter/
│   ├── postgres/       # PgRunRepo, PgTaskRepo, PgAgentRepo, PgPipelineStore, PgEventBus
│   ├── grpc/           # PipelineGrpcService, AgentGrpcService, auth, convert
│   ├── rest/           # PLANNED: axum REST handlers (/api/v1/*)
│   ├── clock.rs        # SystemClock
│   └── secrets.rs      # VaultSecretResolver
├── config.rs           # ControllerConfig, AuthConfig, TimerConfig, ServerTlsConfig
├── server.rs           # run() — composition root (gRPC server)
└── proto.rs            # tonic generated code
```

### AppContext: the DI container

`AppContext` is the controller's DI container. It holds all driven-port
trait objects wired at startup. The composition root in `server.rs`
builds it once and passes it to gRPC service implementations.

```rust
pub struct AppContext {
    pub runs: Arc<dyn RunRepository>,
    pub tasks: Arc<dyn TaskRepository>,
    pub agents: Arc<dyn AgentRepository>,
    pub store: Arc<dyn PipelineStore>,
    pub event_bus: Arc<dyn EventBus>,
    pub secrets: Arc<dyn SecretResolver>,
    pub clock: Arc<dyn Clock>,
    pub config: AppConfig,
}
```

The composition root in `server.rs` wires concrete adapters:

```rust
// Current: start the controller gRPC server
let ctx = AppContext { runs, tasks, agents, store, event_bus, secrets, clock, config };
rapidbyte_controller::run(config, otel_guard, secrets).await
```

> **Note:** When REST and standalone mode are added, `AppContext` will
> expand to include driving-port service traits and an optional
> `EngineContext`.

### Base URL & versioning

```
http://localhost:8080/api/v1/
```

All REST endpoints are prefixed with `/api/v1/`. The version will
increment only on breaking changes. Non-breaking additions (new fields,
new endpoints) do not bump the version.

---

## 2. Authentication

All API requests require a bearer token (except
`GET /api/v1/server/health`).

### HTTP header

```
Authorization: Bearer <token>
```

### CLI login / logout (planned)

Tokens are provisioned out-of-band (generated by an admin, CI secret,
etc.). The `login` command stores a token locally for convenience —
it does not issue tokens.

```bash
rapidbyte login --controller http://localhost:8080   # (planned)
# Prompts for token, stores in ~/.rapidbyte/config.yaml

rapidbyte logout                                      # (planned)
# Removes stored token

rapidbyte logout --controller http://localhost:8080   # (planned)
# Removes token for a specific controller
```

Stored config:

```yaml
# ~/.rapidbyte/config.yaml
controller:
  url: http://localhost:8080
  token: rb_tok_abc123...
```

The CLI resolves auth in order:

1. `--auth-token <token>` flag
2. `RAPIDBYTE_AUTH_TOKEN` environment variable
3. `controller.token` in `~/.rapidbyte/config.yaml`

### Unauthenticated mode

For local development, `serve` can start without auth:

```bash
rapidbyte serve --allow-unauthenticated
```

### Error response (401)

```json
{
  "error": {
    "code": "unauthorized",
    "message": "Missing or invalid bearer token"
  }
}
```

---

## 3. Conventions

### Content type

All requests and responses use `application/json` unless noted otherwise
(SSE uses `text/event-stream`).

### Error format

```json
{
  "error": {
    "code": "pipeline_not_found",
    "message": "Pipeline 'salesforce-crm' does not exist"
  }
}
```

Validation errors include field-level details:

```json
{
  "error": {
    "code": "validation_failed",
    "message": "Pipeline configuration is invalid",
    "details": [
      { "field": "source.use", "reason": "unknown connector 'foobar'" },
      { "field": "destination.write_mode", "reason": "must be one of: append, upsert, replace, merge" }
    ]
  }
}
```

### Standard HTTP status codes

| Code | Meaning |
|------|---------|
| 200  | Success |
| 201  | Created (new resource) |
| 202  | Accepted (async operation started) |
| 400  | Bad request / validation error |
| 401  | Unauthorized |
| 404  | Resource not found |
| 409  | Conflict (e.g., pipeline already running) |
| 422  | Unprocessable entity (valid JSON, invalid semantics) |
| 500  | Internal server error |

### Pagination

List endpoints support cursor-based pagination:

```
GET /api/v1/runs?limit=20&cursor=eyJpZCI6MTAwfQ
```

Response includes a `next_cursor` when more results exist:

```json
{
  "items": [],
  "next_cursor": "eyJpZCI6ODB9"
}
```

Default `limit` is 20, maximum is 100.

### Async operations

Operations that take more than a few seconds (`sync`, `check --apply`,
`teardown`) return `202 Accepted` with a run ID:

```json
{
  "run_id": "run_01H8...",
  "status": "pending",
  "links": {
    "self": "/api/v1/runs/run_01H8...",
    "events": "/api/v1/runs/run_01H8.../events"
  }
}
```

Poll `GET /api/v1/runs/{id}` for status, or subscribe to
`GET /api/v1/runs/{id}/events` for real-time SSE updates.

### Server-Sent Events (SSE)

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/v1/runs/run_01H8.../events
```

Events follow the SSE spec:

```
event: started
data: {"run_id":"run_01H8...","pipeline":"salesforce-crm","started_at":"2025-03-19T14:30:00Z"}

event: progress
data: {"phase":"source","stream":"users","records_read":4500,"bytes_read":1048576,"estimated_total_records":12000}

event: progress
data: {"phase":"destination","stream":"users","records_written":4500}

event: complete
data: {"run_id":"run_01H8...","status":"completed","duration_secs":23.4}
```

Event types:

| Event | Payload | When |
|-------|---------|------|
| `started` | Run ID, pipeline, timestamp | Run begins execution |
| `progress` | Phase, stream, counters, estimated total | During execution |
| `log` | Level, message | Log output (when verbose) |
| `complete` | Final result summary | Run finished successfully |
| `failed` | Error details | Run failed |
| `cancelled` | Cancellation reason | Run was cancelled |

The SSE stream closes when the run reaches a terminal state.

---

## 4. Pipelines

> **Design specification:** The service traits and REST endpoints below
> define the planned REST API surface. The controller currently serves
> gRPC only. These traits will be implemented as driving ports inside
> `rapidbyte-controller/traits/` with REST handlers in
> `rapidbyte-controller/adapter/rest/`.

Each domain section below follows the same pattern:

1. **Driving port** — the Rust service trait (planned) that will live in
   `rapidbyte-controller/traits/`. This is the interface that CLI and
   REST adapters call.
2. **REST adapter** — the HTTP endpoints in
   `rapidbyte-controller/adapter/rest/` that translate requests into
   trait method calls and responses back to JSON.

The trait is the source of truth. The REST API is a projection of it.

### Driving port: PipelineService

```rust
/// Application-layer service for pipeline operations.
///
/// This is the primary boundary between driving adapters (CLI, REST)
/// and the execution backend. Handles config loading, connection
/// resolution, project defaults, and delegation to engine (local) or
/// agents (distributed).
#[async_trait]
pub trait PipelineService: Send + Sync {
    /// List all discovered pipelines in the project.
    async fn list(&self, filter: PipelineFilter) -> Result<PaginatedList<PipelineSummary>>;

    /// Get a single pipeline's resolved configuration.
    async fn get(&self, name: &str) -> Result<PipelineDetail>;

    /// Trigger a pipeline sync. Returns a run ID for tracking.
    async fn sync(&self, request: SyncRequest) -> Result<RunHandle>;

    /// Trigger sync for multiple pipelines by tag/filter. Returns a batch handle.
    async fn sync_batch(&self, request: SyncBatchRequest) -> Result<BatchRunHandle>;

    /// Validate pipeline config and plugin connectivity.
    async fn check(&self, name: &str) -> Result<CheckResult>;

    /// Validate and provision resources (apply phase). Async — returns run handle.
    async fn check_apply(&self, name: &str) -> Result<RunHandle>;

    /// Resolve all defaults and env vars, return effective config.
    async fn compile(&self, name: &str) -> Result<ResolvedConfig>;

    /// Detect schema drift since last sync.
    async fn diff(&self, name: &str) -> Result<DiffResult>;

    /// Run data quality assertions for a pipeline.
    async fn assert(&self, request: AssertRequest) -> Result<AssertResult>;

    /// Tear down resources provisioned by a pipeline.
    async fn teardown(&self, request: TeardownRequest) -> Result<RunHandle>;
}

pub struct PipelineFilter {
    pub tag: Option<Vec<String>>,
    /// Directory filter (CLI-only — not exposed via REST).
    pub dir: Option<PathBuf>,
}

pub struct SyncRequest {
    pub pipeline: String,
    pub stream: Option<String>,
    pub full_refresh: bool,
    pub cursor_start: Option<String>,
    pub cursor_end: Option<String>,
    pub dry_run: bool,
}

pub struct SyncBatchRequest {
    pub tag: Option<Vec<String>>,
    pub exclude: Option<Vec<String>>,
    pub full_refresh: bool,
}

pub struct AssertRequest {
    pub pipeline: Option<String>,
    pub tag: Option<Vec<String>>,
}

pub struct TeardownRequest {
    pub pipeline: String,
    pub reason: String,
}
```

#### REST adapter

### List pipelines

```
GET /api/v1/pipelines
GET /api/v1/pipelines?tag=tier-1
GET /api/v1/pipelines?tag=tier-1&tag=crm
```

```json
{
  "items": [
    {
      "name": "salesforce-crm",
      "description": "Salesforce CRM → Snowflake raw.crm",
      "tags": ["crm", "tier-1"],
      "source": "salesforce",
      "destination": "snowflake",
      "schedule": "every 30m",
      "streams": 6,
      "last_run": {
        "run_id": "run_01H8...",
        "status": "completed",
        "finished_at": "2025-03-19T14:30:00Z"
      }
    }
  ],
  "next_cursor": null
}
```

### Get pipeline

```
GET /api/v1/pipelines/{name}
```

```json
{
  "name": "salesforce-crm",
  "description": "Salesforce CRM → Snowflake raw.crm",
  "tags": ["crm", "tier-1"],
  "schedule": "every 30m",
  "source": {
    "use": "salesforce",
    "connection": "salesforce_prod",
    "streams": [
      { "name": "accounts", "sync_mode": "incremental" },
      { "name": "contacts", "sync_mode": "incremental" },
      { "name": "events", "sync_mode": "incremental", "cursor_field": "CreatedDate" }
    ]
  },
  "destination": {
    "use": "snowflake",
    "connection": "snowflake_raw",
    "schema": "crm",
    "write_mode": "upsert"
  },
  "assert": {
    "freshness": { "warn": "1h", "error": "3h" },
    "not_null": ["id"],
    "unique": ["id"]
  },
  "state": "active"
}
```

### Sync pipeline

```
POST /api/v1/pipelines/{name}/sync
```

```json
{
  "stream": "accounts",
  "full_refresh": false,
  "cursor_start": null,
  "cursor_end": null,
  "dry_run": false
}
```

All fields optional. Empty body `{}` syncs all streams with defaults.

Response `202 Accepted`:

```json
{
  "run_id": "run_01H8...",
  "status": "pending",
  "links": {
    "self": "/api/v1/runs/run_01H8...",
    "events": "/api/v1/runs/run_01H8.../events"
  }
}
```

### Sync all / by tag

```
POST /api/v1/pipelines/sync
```

```json
{
  "tag": ["tier-1"],
  "exclude": ["stripe-payments"],
  "full_refresh": false
}
```

Returns a batch run handle:

```json
{
  "batch_id": "batch_01H8...",
  "runs": [
    { "pipeline": "salesforce-crm", "run_id": "run_01H8a..." },
    { "pipeline": "postgres-product", "run_id": "run_01H8b..." }
  ],
  "links": {
    "self": "/api/v1/batches/batch_01H8..."
  }
}
```

### Check pipeline

```
POST /api/v1/pipelines/{name}/check
```

Response `200 OK` (synchronous — check is fast):

```json
{
  "passed": true,
  "checks": {
    "source_manifest": { "ok": true },
    "source_config": { "ok": true },
    "destination_manifest": { "ok": true },
    "destination_config": { "ok": true },
    "state": { "ok": true },
    "source_validation": { "status": "success" },
    "destination_validation": { "status": "success" },
    "source_prerequisites": { "passed": true, "checks": [] },
    "schema_negotiation": [
      { "stream_name": "users", "passed": true, "warnings": [] }
    ]
  }
}
```

### Check and apply

Validate, then provision resources (create tables, replication slots,
etc.). Async — apply can be slow.

```
POST /api/v1/pipelines/{name}/check-apply
```

Response `202 Accepted` with run handle.

### Compile pipeline

```
GET /api/v1/pipelines/{name}/compiled
```

Returns the fully resolved config with all defaults applied and env
vars substituted:

```json
{
  "pipeline": "salesforce-crm",
  "resolved_config": {
    "version": "1.0",
    "pipeline": "salesforce-crm",
    "source": { "...fully resolved..." },
    "destination": { "...fully resolved..." }
  }
}
```

### Diff pipeline

```
GET /api/v1/pipelines/{name}/diff
```

```json
{
  "streams": [
    {
      "stream_name": "users",
      "changes": [
        { "type": "column_added", "column": "phone_verified", "data_type": "boolean", "nullable": true },
        { "type": "type_changed", "column": "zip_code", "from": "varchar(5)", "to": "varchar(10)" }
      ]
    },
    {
      "stream_name": "orders",
      "changes": []
    }
  ]
}
```

### Assert pipeline

```
POST /api/v1/pipelines/{name}/assert
POST /api/v1/pipelines/assert
```

```json
{
  "tag": ["tier-1"]
}
```

Response:

```json
{
  "passed": true,
  "results": [
    {
      "pipeline": "salesforce-crm",
      "stream": "accounts",
      "assertions": [
        { "type": "not_null", "columns": ["id"], "passed": true },
        { "type": "unique", "columns": ["id"], "passed": true },
        { "type": "freshness", "passed": true, "last_sync_age": "12m", "threshold": "1h" }
      ]
    }
  ]
}
```

### Teardown pipeline

```
POST /api/v1/pipelines/{name}/teardown
```

```json
{
  "reason": "pipeline_deleted"
}
```

Response `202 Accepted` with run handle.

---

## 5. Runs

A run is an execution instance of a pipeline. Created when `sync`,
`teardown`, or `check --apply` is triggered.

### Driving port: RunService

```rust
#[async_trait]
pub trait RunService: Send + Sync {
    /// Get a run by ID.
    async fn get(&self, run_id: &str) -> Result<RunDetail>;

    /// List runs, optionally filtered.
    async fn list(&self, filter: RunFilter) -> Result<PaginatedList<RunSummary>>;

    /// Subscribe to real-time progress events (SSE).
    async fn events(&self, run_id: &str) -> Result<EventStream<ProgressEvent>>;

    /// Cancel a running pipeline.
    async fn cancel(&self, run_id: &str) -> Result<RunDetail>;

    /// Get a batch by ID.
    async fn get_batch(&self, batch_id: &str) -> Result<BatchDetail>;

    /// Subscribe to real-time progress events for an entire batch (SSE).
    async fn batch_events(&self, batch_id: &str) -> Result<EventStream<ProgressEvent>>;
}

pub struct BatchDetail {
    pub batch_id: String,
    pub status: RunStatus,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub runs: Vec<BatchRunEntry>,
}

pub struct BatchRunEntry {
    pub pipeline: String,
    pub run_id: String,
    pub status: RunStatus,
}

pub struct RunFilter {
    pub pipeline: Option<String>,
    pub status: Option<RunStatus>,
    pub limit: u32,
    pub cursor: Option<String>,
}

pub enum RunStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}
```

#### REST adapter

### Get run

```
GET /api/v1/runs/{id}
```

```json
{
  "run_id": "run_01H8...",
  "pipeline": "salesforce-crm",
  "status": "completed",
  "started_at": "2025-03-19T14:30:00Z",
  "finished_at": "2025-03-19T14:30:23Z",
  "duration_secs": 23.4,
  "trigger": "api",
  "counts": {
    "records_read": 12847,
    "records_written": 12847,
    "bytes_read": 52428800,
    "bytes_written": 48234496
  },
  "streams": [
    {
      "stream_name": "accounts",
      "records_read": 4201,
      "records_written": 4201,
      "status": "completed"
    },
    {
      "stream_name": "contacts",
      "records_read": 5892,
      "records_written": 5892,
      "status": "completed"
    }
  ],
  "timing": {
    "source_duration_secs": 8.2,
    "dest_duration_secs": 14.1,
    "wasm_overhead_secs": 1.1
  },
  "retry_count": 0,
  "parallelism": 4,
  "error": null
}
```

### List runs

```
GET /api/v1/runs
GET /api/v1/runs?pipeline=salesforce-crm
GET /api/v1/runs?status=failed&limit=10
```

```json
{
  "items": [
    {
      "run_id": "run_01H8...",
      "pipeline": "salesforce-crm",
      "status": "completed",
      "started_at": "2025-03-19T14:30:00Z",
      "duration_secs": 23.4,
      "records_written": 12847
    }
  ],
  "next_cursor": "eyJpZCI6ODB9"
}
```

### Run events (SSE)

```
GET /api/v1/runs/{id}/events
```

```
Accept: text/event-stream
Authorization: Bearer <token>
```

```
event: progress
data: {"phase":"source","stream":"users","records_read":4500,"bytes_read":1048576}

event: progress
data: {"phase":"destination","stream":"users","records_written":4500}

event: complete
data: {"run_id":"run_01H8...","status":"completed","duration_secs":23.4,"counts":{"records_read":12847,"records_written":12847}}
```

If the run is already in a terminal state, the endpoint returns the
final event and closes immediately.

### Cancel run

```
POST /api/v1/runs/{id}/cancel
```

Response `200 OK`:

```json
{
  "run_id": "run_01H8...",
  "status": "cancelled",
  "cancelled_at": "2025-03-19T14:30:15Z"
}
```

Returns `409 Conflict` if the run is already in a terminal state.

### Get batch

Batches group runs triggered by a single `sync_batch` call.

```
GET /api/v1/batches/{id}
```

```json
{
  "batch_id": "batch_01H8...",
  "status": "running",
  "created_at": "2025-03-19T14:30:00Z",
  "finished_at": null,
  "runs": [
    {
      "pipeline": "salesforce-crm",
      "run_id": "run_01H8a...",
      "status": "completed"
    },
    {
      "pipeline": "postgres-product",
      "run_id": "run_01H8b...",
      "status": "running"
    }
  ]
}
```

Batch status is derived from its runs:

| Batch status | Condition |
|---|---|
| `pending` | All runs pending |
| `running` | Any run in progress |
| `completed` | All runs completed |
| `failed` | Any run failed (others may still be running or completed) |
| `cancelled` | All non-completed runs cancelled |

### Batch events (SSE)

```
GET /api/v1/batches/{id}/events
```

Multiplexes progress events from all runs in the batch. Each event
includes a `run_id` field so the client can attribute progress to a
specific pipeline.

```
event: started
data: {"batch_id":"batch_01H8...","total_runs":2}

event: progress
data: {"run_id":"run_01H8a...","pipeline":"salesforce-crm","phase":"source","stream":"accounts","records_read":2100}

event: run_complete
data: {"run_id":"run_01H8a...","pipeline":"salesforce-crm","status":"completed","duration_secs":12.3}

event: progress
data: {"run_id":"run_01H8b...","pipeline":"postgres-product","phase":"destination","stream":"products","records_written":8400}

event: run_complete
data: {"run_id":"run_01H8b...","pipeline":"postgres-product","status":"completed","duration_secs":18.7}

event: complete
data: {"batch_id":"batch_01H8...","status":"completed","total_duration_secs":18.7}
```

If the batch is already in a terminal state, the endpoint returns the
final event and closes immediately.

---

## 6. Connections

Named connections from `connections.yml`. The API exposes them for
introspection and connectivity testing — not for CRUD (connections
are defined in config files, not via API).

### Driving port: ConnectionService

```rust
#[async_trait]
pub trait ConnectionService: Send + Sync {
    /// List all named connections.
    async fn list(&self) -> Result<Vec<ConnectionSummary>>;

    /// Get a connection's metadata (not credentials).
    async fn get(&self, name: &str) -> Result<ConnectionDetail>;

    /// Test connectivity for a named connection.
    async fn test(&self, name: &str) -> Result<ConnectionTestResult>;

    /// Discover available streams/tables at a connection.
    async fn discover(&self, request: DiscoverRequest) -> Result<DiscoverResult>;
}

pub struct DiscoverRequest {
    pub connection: String,
    pub table: Option<String>,
}
```

#### REST adapter

### List connections

```
GET /api/v1/connections
```

```json
{
  "items": [
    {
      "name": "postgres_app",
      "connector": "postgres",
      "used_by": ["postgres-product"]
    },
    {
      "name": "snowflake_raw",
      "connector": "snowflake",
      "used_by": ["salesforce-crm", "stripe-payments"]
    }
  ]
}
```

Credentials are never exposed in API responses.

### Get connection

```
GET /api/v1/connections/{name}
```

```json
{
  "name": "postgres_app",
  "connector": "postgres",
  "config": {
    "host": "db.acme.com",
    "port": 5432,
    "database": "production",
    "ssl_mode": "require"
  },
  "used_by": ["postgres-product"]
}
```

Sensitive fields (`password`, `api_key`, `secret`, `token`, etc.)
are redacted from the response.

### Test connection

```
POST /api/v1/connections/{name}/test
```

```json
{
  "name": "postgres_app",
  "status": "connected",
  "latency_ms": 12,
  "details": {
    "version": "PostgreSQL 15.4",
    "tables_accessible": 42,
    "replication_available": true
  }
}
```

On failure:

```json
{
  "name": "postgres_app",
  "status": "failed",
  "error": {
    "code": "connection_refused",
    "message": "Connection refused: db.acme.com:5432"
  }
}
```

### Discover streams

```
GET /api/v1/connections/{name}/discover
GET /api/v1/connections/{name}/discover?table=users
```

```json
{
  "connection": "postgres_app",
  "streams": [
    {
      "schema": "public",
      "table": "users",
      "estimated_rows": 1204301,
      "columns": 18
    },
    {
      "schema": "public",
      "table": "orders",
      "estimated_rows": 8891204,
      "columns": 24
    }
  ]
}
```

With `?table=users`, returns column-level detail:

```json
{
  "connection": "postgres_app",
  "table": "public.users",
  "estimated_rows": 1204301,
  "columns": [
    { "name": "id", "type": "bigint", "nullable": false, "default": "nextval(...)" },
    { "name": "email", "type": "varchar(255)", "nullable": false, "default": null },
    { "name": "created_at", "type": "timestamptz", "nullable": false, "default": "now()" }
  ],
  "indexes": [
    { "name": "users_pkey", "columns": ["id"], "unique": true },
    { "name": "users_email_key", "columns": ["email"], "unique": true }
  ],
  "suggested_config": {
    "sync_mode": "cdc",
    "primary_key": ["id"],
    "cursor_field": "updated_at",
    "columns": {
      "exclude": ["password_hash"]
    }
  }
}
```

---

## 7. Plugins

Plugin management — listing, searching, installing, and inspecting
WASM connector plugins from OCI registries.

### Driving port: PluginService

```rust
#[async_trait]
pub trait PluginService: Send + Sync {
    /// List locally cached plugins.
    async fn list(&self) -> Result<Vec<PluginSummary>>;

    /// Search for plugins in the registry.
    async fn search(&self, request: PluginSearchRequest) -> Result<Vec<PluginSearchResult>>;

    /// Get detailed info for a plugin (local or remote).
    async fn info(&self, plugin_ref: &str) -> Result<PluginDetail>;

    /// Install (pull) a plugin from the registry.
    async fn install(&self, plugin_ref: &str) -> Result<PluginInstallResult>;

    /// Remove a plugin from the local cache.
    async fn remove(&self, plugin_ref: &str) -> Result<()>;
}

pub struct PluginSearchRequest {
    pub query: String,
    pub plugin_type: Option<PluginType>,  // source | destination | transform
}
```

#### REST adapter

### List installed plugins

```
GET /api/v1/plugins
```

```json
{
  "items": [
    {
      "name": "rapidbyte/source-postgres",
      "version": "1.2.3",
      "type": "source",
      "size_bytes": 4194304,
      "installed_at": "2025-03-15T10:00:00Z"
    },
    {
      "name": "rapidbyte/dest-snowflake",
      "version": "2.0.0",
      "type": "destination",
      "size_bytes": 5242880,
      "installed_at": "2025-03-10T08:30:00Z"
    }
  ]
}
```

### Search registry

```
GET /api/v1/plugins/search?q=postgres
GET /api/v1/plugins/search?q=postgres&type=source
```

```json
{
  "items": [
    {
      "name": "rapidbyte/source-postgres",
      "description": "PostgreSQL source connector with CDC support",
      "latest_version": "1.2.3",
      "type": "source",
      "downloads": 12450
    }
  ]
}
```

### Get plugin info

```
GET /api/v1/plugins/{ref}
GET /api/v1/plugins/rapidbyte/source-postgres
GET /api/v1/plugins/rapidbyte/source-postgres:1.2.3
```

```json
{
  "name": "rapidbyte/source-postgres",
  "version": "1.2.3",
  "type": "source",
  "description": "PostgreSQL source connector with CDC support",
  "size_bytes": 4194304,
  "manifest": {
    "permissions": {
      "network": { "allowed_hosts": ["*:5432"] },
      "env": { "allowed_vars": ["PG_*"] }
    },
    "limits": {
      "max_memory": "256mb",
      "timeout_seconds": 300
    }
  },
  "available_versions": ["1.2.3", "1.2.2", "1.1.0", "1.0.0"],
  "installed": true
}
```

### Install plugin

```
POST /api/v1/plugins/install
```

```json
{
  "plugin_ref": "rapidbyte/source-postgres:1.2.3"
}
```

Response `200 OK`:

```json
{
  "name": "rapidbyte/source-postgres",
  "version": "1.2.3",
  "size_bytes": 4194304,
  "installed": true
}
```

### Remove plugin

```
DELETE /api/v1/plugins/{ref}
DELETE /api/v1/plugins/rapidbyte/source-postgres:1.2.3
```

Response `200 OK`:

```json
{
  "name": "rapidbyte/source-postgres",
  "version": "1.2.3",
  "removed": true
}
```

---

## 8. Operations

Stateful control-plane operations on a running system. These manage
pipeline lifecycle (pause/resume), state (reset), and observability
(freshness, logs).

### Driving port: OperationsService

```rust
#[async_trait]
pub trait OperationsService: Send + Sync {
    /// Dashboard view — all pipelines with schedule, last run, status.
    async fn status(&self) -> Result<Vec<PipelineStatus>>;

    /// Detailed status for a single pipeline.
    async fn pipeline_status(&self, name: &str) -> Result<PipelineStatusDetail>;

    /// Pause scheduled runs for a pipeline.
    async fn pause(&self, name: &str) -> Result<PipelineState>;

    /// Resume scheduled runs for a pipeline.
    async fn resume(&self, name: &str) -> Result<PipelineState>;

    /// Clear sync state (cursors, checkpoints). Next run = full refresh.
    async fn reset(&self, request: ResetRequest) -> Result<ResetResult>;

    /// Check freshness for all pipelines or filtered by tag.
    async fn freshness(&self, filter: FreshnessFilter) -> Result<Vec<FreshnessStatus>>;

    /// Retrieve logs for a pipeline or run.
    async fn logs(&self, request: LogsRequest) -> Result<LogsResult>;

    /// Stream logs in real-time (SSE).
    async fn logs_stream(&self, filter: LogsStreamFilter) -> Result<EventStream<LogEntry>>;
}

pub enum PipelineState {
    Active,
    Paused,
}

pub struct LogsStreamFilter {
    pub pipeline: Option<String>,
}

pub struct ResetRequest {
    pub pipeline: String,
    pub stream: Option<String>,
}

pub struct FreshnessFilter {
    pub tag: Option<Vec<String>>,
}

pub struct LogsRequest {
    pub pipeline: String,
    pub run_id: Option<String>,
    pub limit: u32,
}
```

#### REST adapter

### Status dashboard

```
GET /api/v1/status
```

```json
{
  "items": [
    {
      "pipeline": "salesforce-crm",
      "schedule": "every 30m",
      "state": "active",
      "last_run": {
        "run_id": "run_01H8...",
        "status": "completed",
        "finished_at": "2025-03-19T14:30:00Z",
        "records_written": 12847,
        "duration_secs": 23.4
      },
      "next_run_in": "18m",
      "health": "healthy"
    },
    {
      "pipeline": "s3-clickstream",
      "schedule": "every 1h",
      "state": "paused",
      "last_run": {
        "run_id": "run_01H7...",
        "status": "completed",
        "finished_at": "2025-03-19T07:00:00Z",
        "records_written": 204301,
        "duration_secs": 45.1
      },
      "next_run_in": null,
      "health": "healthy"
    }
  ]
}
```

### Pipeline status detail

```
GET /api/v1/status/{name}
```

```json
{
  "pipeline": "salesforce-crm",
  "schedule": "every 30m",
  "state": "active",
  "health": "healthy",
  "recent_runs": [
    {
      "run_id": "run_01H8...",
      "status": "completed",
      "started_at": "2025-03-19T14:30:00Z",
      "duration_secs": 23.4,
      "streams": 4,
      "records_written": 12847
    },
    {
      "run_id": "run_01H7...",
      "status": "failed",
      "started_at": "2025-03-19T13:30:00Z",
      "error": "connection timeout (attempt 3/3)"
    }
  ],
  "streams": [
    {
      "name": "accounts",
      "sync_mode": "incremental",
      "cursor_value": "2025-03-19T14:28:00Z",
      "last_records_written": 4201
    },
    {
      "name": "contacts",
      "sync_mode": "incremental",
      "cursor_value": "2025-03-19T14:29:12Z",
      "last_records_written": 5892
    }
  ],
  "dlq_rows": 0,
  "assertions": {
    "last_run": "2025-03-19T14:30:00Z",
    "passed": true,
    "failures": 0
  }
}
```

### Pause pipeline

```
POST /api/v1/pipelines/{name}/pause
```

```json
{
  "pipeline": "salesforce-crm",
  "state": "paused",
  "paused_at": "2025-03-19T14:35:00Z"
}
```

### Resume pipeline

```
POST /api/v1/pipelines/{name}/resume
```

```json
{
  "pipeline": "salesforce-crm",
  "state": "active",
  "resumed_at": "2025-03-19T15:00:00Z",
  "next_run_in": "30m"
}
```

### Reset pipeline state

```
POST /api/v1/pipelines/{name}/reset
```

```json
{
  "stream": "events"
}
```

Body optional. Without `stream`, resets all streams in the pipeline.

Response:

```json
{
  "pipeline": "salesforce-crm",
  "streams_reset": ["events"],
  "cursors_cleared": 1,
  "next_sync_mode": "full_refresh"
}
```

### Freshness

```
GET /api/v1/freshness
GET /api/v1/freshness?tag=tier-1
```

```json
{
  "items": [
    {
      "pipeline": "salesforce-crm",
      "last_sync_age": "12m",
      "sla_warn": "1h",
      "sla_error": "3h",
      "status": "fresh"
    },
    {
      "pipeline": "postgres-product",
      "last_sync_age": "8h",
      "sla_warn": "6h",
      "sla_error": "24h",
      "status": "stale"
    }
  ]
}
```

### Logs

```
GET /api/v1/logs?pipeline=salesforce-crm
GET /api/v1/logs?pipeline=salesforce-crm&run_id=latest
GET /api/v1/logs?pipeline=salesforce-crm&run_id=run_01H8...
GET /api/v1/logs?pipeline=salesforce-crm&limit=50
```

```json
{
  "items": [
    {
      "timestamp": "2025-03-19T14:30:01Z",
      "level": "info",
      "pipeline": "salesforce-crm",
      "run_id": "run_01H8...",
      "message": "Pipeline validated",
      "fields": {
        "source": "salesforce",
        "destination": "snowflake",
        "streams": 4
      }
    }
  ],
  "next_cursor": "eyJ0cyI6MTcxMDg1..."
}
```

### Log stream (SSE)

```
GET /api/v1/logs/stream
GET /api/v1/logs/stream?pipeline=salesforce-crm
```

```
event: log
data: {"timestamp":"2025-03-19T14:30:01Z","level":"info","pipeline":"salesforce-crm","message":"Pipeline validated"}

event: log
data: {"timestamp":"2025-03-19T14:30:02Z","level":"info","pipeline":"salesforce-crm","message":"Source connected"}
```

---

## 9. Server

Server introspection and health endpoints. `GET /api/v1/server/health`
is the only unauthenticated endpoint.

### Driving port: ServerService

```rust
#[async_trait]
pub trait ServerService: Send + Sync {
    /// Health check — for load balancers and readiness probes.
    async fn health(&self) -> Result<HealthStatus>;

    /// Version and build info.
    async fn version(&self) -> Result<VersionInfo>;

    /// Server configuration (non-sensitive).
    async fn config(&self) -> Result<ServerConfig>;
}
```

#### REST adapter

### Health check

```
GET /api/v1/server/health
```

No authentication required. Intended for Kubernetes readiness/liveness
probes and load balancer health checks.

```json
{
  "status": "healthy",
  "mode": "serve",
  "uptime_secs": 86400,
  "state_backend": "postgres",
  "state_backend_healthy": true,
  "agents_connected": 0
}
```

In distributed mode (`controller`):

```json
{
  "status": "healthy",
  "mode": "controller",
  "uptime_secs": 172800,
  "state_backend": "postgres",
  "state_backend_healthy": true,
  "agents_connected": 3
}
```

| `status` | Meaning |
|-----------|---------|
| `healthy` | All subsystems operational |
| `degraded` | Running but with issues (e.g., state backend unreachable) |
| `unhealthy` | Critical failure |

HTTP status: `200` for healthy/degraded, `503` for unhealthy.

### Version

```
GET /api/v1/server/version
```

```json
{
  "version": "0.1.0",
  "rustc": "1.82.0",
  "wasmtime": "41.0.0",
  "mode": "serve",
  "plugins": [
    { "name": "rapidbyte/source-postgres", "version": "1.2.3" },
    { "name": "rapidbyte/dest-snowflake", "version": "2.0.0" }
  ]
}
```

### Server config

```
GET /api/v1/server/config
```

Returns non-sensitive server configuration:

```json
{
  "mode": "serve",
  "port": 8080,
  "metrics_port": 9090,
  "state_backend": "postgres",
  "registry_url": "registry.example.com/plugins",
  "trust_policy": "verify",
  "auth_required": true,
  "pipelines_discovered": 4,
  "scheduler_active": true
}
```
