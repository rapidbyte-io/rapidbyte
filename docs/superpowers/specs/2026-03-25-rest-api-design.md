# REST API Implementation Design

**Date:** 2026-03-25
**Status:** Approved
**Scope:** Full REST API surface for `rapidbyte-controller` — all 6 service domains

---

## 1. Summary

Add a complete REST API (axum) to the `rapidbyte-controller` crate, covering Pipelines, Runs, Connections, Plugins, Operations, and Server endpoints. The REST adapter is a new driving adapter alongside the existing gRPC adapter. Both adapters share a single set of driving-port service traits as the canonical boundary into the controller.

### Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Build order | Top-down vertical slices | Working endpoint fast, consumer-driven port design |
| Service traits | Canonical boundary for REST + gRPC | Single driving-port layer, clean hexagonal architecture |
| gRPC migration | Migrate `PipelineGrpcService` to use service traits | Both adapters share identical business logic paths |
| Agent gRPC | Stays on direct application calls | Internal protocol, not user-facing |
| REST port | Separate from gRPC | Simpler to reason about, separate TLS/config |
| SSE | Reuse existing `EventBus` (LISTEN/NOTIFY) | Proven infrastructure, no duplication |
| Auth | Shared validation module | Single source of truth, RBAC-ready |
| New driven ports | Controller-level traits with standalone adapters | Same pattern as existing ports, clean standalone vs distributed |
| Backend implementation | Full for all 6 domains | No stubs — driven ports, adapters, and service impls for everything |

---

## 2. Architecture

### Hexagonal Boundaries

The controller crate follows hexagonal architecture. The REST API adds:

1. **Driving-port service traits** (`traits/`) — the canonical interface consumed by all driving adapters
2. **REST driving adapter** (`adapter/rest/`) — axum handlers translating HTTP to trait calls
3. **New driven-port traits + adapters** — for Connections, Plugins, Operations backends

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │                      DRIVING ADAPTERS                              │
 │  ┌──────────┐  ┌──────────────┐  ┌──────────────┐                 │
 │  │ CLI      │  │ axum (REST)  │  │ gRPC         │                 │
 │  │ (clap)   │  │ (NEW)        │  │ (migrated)   │                 │
 │  └────┬─────┘  └──────┬───────┘  └──────┬───────┘                 │
 │       │               │                 │                          │
 ├───────┴───────────────┴─────────────────┴──────────────────────────┤
 │                    DRIVING PORTS (service traits)                   │
 │  PipelineService · RunService · ConnectionService                  │
 │  PluginService · OperationsService · ServerService                 │
 ├────────────────────────────────────────────────────────────────────┤
 │                    AppServices (trait implementations)              │
 │                    delegates to AppContext driven ports             │
 ├────────────────────────────────────────────────────────────────────┤
 │                    DRIVEN PORTS (traits)                           │
 │  Existing: RunRepository, TaskRepository, AgentRepository,        │
 │            PipelineStore, EventBus, SecretResolver, Clock          │
 │  New: PluginRegistry, ConnectionTester, CursorStore, LogStore     │
 ├────────────────────────────────────────────────────────────────────┤
 │                    DRIVEN ADAPTERS                                 │
 │  Existing: Postgres repos, PgEventBus, Vault, SystemClock         │
 │  New: PgCursorStore, PgLogStore, EnginePluginRegistry,            │
 │       EngineConnectionTester                                       │
 └────────────────────────────────────────────────────────────────────┘
```

---

## 3. Module Layout

```
rapidbyte-controller/src/
├── traits/                        # NEW: driving-port service traits
│   ├── mod.rs
│   ├── error.rs                   # ServiceError enum
│   ├── pipeline.rs                # PipelineService trait + request/response types
│   ├── run.rs                     # RunService trait + types
│   ├── connection.rs              # ConnectionService trait + types
│   ├── plugin.rs                  # PluginService trait + types
│   ├── operations.rs              # OperationsService trait + types
│   └── server.rs                  # ServerService trait + types
│
├── domain/ports/                  # EXTENDED: new driven ports
│   ├── repository.rs              # (existing)
│   ├── event_bus.rs               # (existing, extended with batch_subscribe, log_subscribe)
│   ├── pipeline_store.rs          # (existing)
│   ├── secrets.rs                 # (existing)
│   ├── clock.rs                   # (existing)
│   ├── plugin_registry.rs         # NEW
│   ├── connection_tester.rs       # NEW
│   ├── cursor_store.rs            # NEW
│   └── log_store.rs               # NEW
│
├── application/
│   ├── services/                  # NEW: service trait implementations
│   │   ├── mod.rs                 # AppServices struct
│   │   ├── pipeline.rs            # impl PipelineService for AppServices
│   │   ├── run.rs                 # impl RunService for AppServices
│   │   ├── connection.rs          # impl ConnectionService for AppServices
│   │   ├── plugin.rs              # impl PluginService for AppServices
│   │   ├── operations.rs          # impl OperationsService for AppServices
│   │   └── server.rs              # impl ServerService for AppServices
│   ├── context.rs                 # AppContext (extended with new driven ports)
│   ├── submit.rs                  # (existing, called by service impls)
│   ├── poll.rs                    # (existing)
│   ├── complete.rs                # (existing)
│   ├── cancel.rs                  # (existing)
│   ├── heartbeat.rs               # (existing)
│   ├── query.rs                   # (existing)
│   ├── timeout.rs                 # (existing)
│   ├── background/                # (existing)
│   ├── testing.rs                 # (extended with new fakes)
│   └── error.rs                   # (existing)
│
├── adapter/
│   ├── auth.rs                    # NEW: shared auth (validate_token -> AuthContext)
│   ├── grpc/                      # (existing, PipelineGrpcService migrated to AppServices)
│   ├── rest/                      # NEW: axum REST adapter
│   │   ├── mod.rs                 # Router assembly, middleware stack
│   │   ├── error.rs               # ServiceError -> JSON error response
│   │   ├── pagination.rs          # Cursor-based pagination helpers
│   │   ├── sse.rs                 # EventBus -> SSE stream helpers
│   │   ├── extractors.rs          # AuthContext extractor, query params
│   │   ├── pipelines.rs           # Pipeline endpoint handlers
│   │   ├── runs.rs                # Run + batch endpoint handlers
│   │   ├── connections.rs         # Connection endpoint handlers
│   │   ├── plugins.rs             # Plugin endpoint handlers
│   │   ├── operations.rs          # Operations endpoint handlers
│   │   └── server.rs              # Server endpoint handlers
│   ├── postgres/                  # (existing + new)
│   │   ├── cursor_store.rs        # NEW: PgCursorStore
│   │   ├── log_store.rs           # NEW: PgLogStore
│   │   └── ...                    # (existing repos)
│   ├── engine/                    # NEW: standalone-mode adapters
│   │   ├── mod.rs
│   │   ├── plugin_registry.rs     # EnginePluginRegistry
│   │   └── connection_tester.rs   # EngineConnectionTester
│   ├── clock.rs                   # (existing)
│   └── secrets.rs                 # (existing)
│
├── server.rs                      # EXTENDED: new serve() for REST
├── config.rs                      # EXTENDED: rest_port
├── proto.rs                       # (existing)
└── lib.rs                         # (extended re-exports)
```

---

## 4. Driving-Port Service Traits

All 6 traits live in `traits/`. Request/response types are defined alongside their trait. All return `Result<T, ServiceError>`.

### ServiceError

```rust
// traits/error.rs

pub enum ServiceError {
    NotFound { resource: String, id: String },
    Conflict { message: String },
    ValidationFailed { details: Vec<FieldError> },
    Unauthorized,
    Internal { message: String },
}

pub struct FieldError {
    pub field: String,
    pub reason: String,
}
```

### PipelineService

```rust
#[async_trait]
pub trait PipelineService: Send + Sync {
    async fn list(&self, filter: PipelineFilter) -> Result<PaginatedList<PipelineSummary>, ServiceError>;
    async fn get(&self, name: &str) -> Result<PipelineDetail, ServiceError>;
    async fn sync(&self, request: SyncRequest) -> Result<RunHandle, ServiceError>;
    async fn sync_batch(&self, request: SyncBatchRequest) -> Result<BatchRunHandle, ServiceError>;
    async fn check(&self, name: &str) -> Result<CheckResult, ServiceError>;
    async fn check_apply(&self, name: &str) -> Result<RunHandle, ServiceError>;
    async fn compile(&self, name: &str) -> Result<ResolvedConfig, ServiceError>;
    async fn diff(&self, name: &str) -> Result<DiffResult, ServiceError>;
    async fn assert(&self, request: AssertRequest) -> Result<AssertResult, ServiceError>;
    async fn teardown(&self, request: TeardownRequest) -> Result<RunHandle, ServiceError>;
}
```

### RunService

```rust
#[async_trait]
pub trait RunService: Send + Sync {
    async fn get(&self, run_id: &str) -> Result<RunDetail, ServiceError>;
    async fn list(&self, filter: RunFilter) -> Result<PaginatedList<RunSummary>, ServiceError>;
    async fn events(&self, run_id: &str) -> Result<EventStream<ProgressEvent>, ServiceError>;
    async fn cancel(&self, run_id: &str) -> Result<RunDetail, ServiceError>;
    async fn get_batch(&self, batch_id: &str) -> Result<BatchDetail, ServiceError>;
    async fn batch_events(&self, batch_id: &str) -> Result<EventStream<ProgressEvent>, ServiceError>;
}
```

### ConnectionService

```rust
#[async_trait]
pub trait ConnectionService: Send + Sync {
    async fn list(&self) -> Result<Vec<ConnectionSummary>, ServiceError>;
    async fn get(&self, name: &str) -> Result<ConnectionDetail, ServiceError>;
    async fn test(&self, name: &str) -> Result<ConnectionTestResult, ServiceError>;
    async fn discover(&self, request: DiscoverRequest) -> Result<DiscoverResult, ServiceError>;
}
```

### PluginService

```rust
#[async_trait]
pub trait PluginService: Send + Sync {
    async fn list(&self) -> Result<Vec<PluginSummary>, ServiceError>;
    async fn search(&self, request: PluginSearchRequest) -> Result<Vec<PluginSearchResult>, ServiceError>;
    async fn info(&self, plugin_ref: &str) -> Result<PluginDetail, ServiceError>;
    async fn install(&self, plugin_ref: &str) -> Result<PluginInstallResult, ServiceError>;
    async fn remove(&self, plugin_ref: &str) -> Result<(), ServiceError>;
}
```

### OperationsService

```rust
#[async_trait]
pub trait OperationsService: Send + Sync {
    async fn status(&self) -> Result<Vec<PipelineStatus>, ServiceError>;
    async fn pipeline_status(&self, name: &str) -> Result<PipelineStatusDetail, ServiceError>;
    async fn pause(&self, name: &str) -> Result<PipelineState, ServiceError>;
    async fn resume(&self, name: &str) -> Result<PipelineState, ServiceError>;
    async fn reset(&self, request: ResetRequest) -> Result<ResetResult, ServiceError>;
    async fn freshness(&self, filter: FreshnessFilter) -> Result<Vec<FreshnessStatus>, ServiceError>;
    async fn logs(&self, request: LogsRequest) -> Result<LogsResult, ServiceError>;
    async fn logs_stream(&self, filter: LogsStreamFilter) -> Result<EventStream<LogEntry>, ServiceError>;
}
```

### ServerService

```rust
#[async_trait]
pub trait ServerService: Send + Sync {
    async fn health(&self) -> Result<HealthStatus, ServiceError>;
    async fn version(&self) -> Result<VersionInfo, ServiceError>;
    async fn config(&self) -> Result<ServerConfigInfo, ServiceError>;
}
```

### Shared Types

```rust
pub struct PaginatedList<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

pub type EventStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

pub struct RunHandle {
    pub run_id: String,
    pub status: String,
}
// Note: `links` field is populated by the adapter layer (REST adds URL paths, gRPC omits it)
```

---

## 5. New Driven-Port Traits

### PluginRegistry

```rust
// domain/ports/plugin_registry.rs

#[async_trait]
pub trait PluginRegistry: Send + Sync {
    async fn list_installed(&self) -> Result<Vec<InstalledPlugin>, RegistryError>;
    async fn search(&self, query: &str, plugin_type: Option<PluginType>) -> Result<Vec<RegistryEntry>, RegistryError>;
    async fn info(&self, plugin_ref: &str) -> Result<PluginMetadata, RegistryError>;
    async fn install(&self, plugin_ref: &str) -> Result<InstalledPlugin, RegistryError>;
    async fn remove(&self, plugin_ref: &str) -> Result<(), RegistryError>;
}
```

Standalone adapter: `adapter/engine/plugin_registry.rs` — wraps engine's OCI resolver + local plugin cache directory.

### ConnectionTester

```rust
// domain/ports/connection_tester.rs

#[async_trait]
pub trait ConnectionTester: Send + Sync {
    async fn test(&self, connection_config: &ConnectionConfig) -> Result<TestResult, ConnectionError>;
    async fn discover(&self, connection_config: &ConnectionConfig, table: Option<&str>) -> Result<DiscoveryResult, ConnectionError>;
}
```

Standalone adapter: `adapter/engine/connection_tester.rs` — wraps engine's plugin runner to invoke source plugin's `validate`/`discover` WIT exports.

The service impl resolves connection name to `ConnectionConfig` via pipeline YAML, then passes config to the port. The port doesn't know about pipeline files.

### CursorStore

```rust
// domain/ports/cursor_store.rs

#[async_trait]
pub trait CursorStore: Send + Sync {
    async fn get_cursors(&self, pipeline: &str) -> Result<Vec<StreamCursor>, CursorError>;
    async fn clear(&self, pipeline: &str, stream: Option<&str>) -> Result<u64, CursorError>;
    async fn last_sync_times(&self, pipelines: &[String]) -> Result<Vec<SyncTimestamp>, CursorError>;
}
```

Adapter: `adapter/postgres/cursor_store.rs` — reads from existing cursor/checkpoint tables managed by `rapidbyte-state`. The `clear` operation is the only write.

### LogStore

```rust
// domain/ports/log_store.rs

#[async_trait]
pub trait LogStore: Send + Sync {
    async fn query(&self, filter: LogFilter) -> Result<PaginatedList<StoredLogEntry>, LogError>;
    async fn subscribe(&self, filter: LogStreamFilter) -> Result<LogEventStream, LogError>;
}
```

Adapter: `adapter/postgres/log_store.rs` — queries `run_logs` table. Subscribe uses LISTEN/NOTIFY (same pattern as `EventBus`).

### Error Types

Each driven port has its own error type (`RegistryError`, `ConnectionError`, `CursorError`, `LogError`). The service layer maps them into `ServiceError`. This keeps port errors domain-specific.

---

## 6. Shared Auth

Extract token validation from `BearerAuthInterceptor` into a shared module.

```rust
// adapter/auth.rs

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub token: String,
    // Future: tenant_id, roles, permissions (RBAC)
}

pub fn validate_token(config: &AuthConfig, raw_token: &str) -> Result<AuthContext, AuthError> {
    if config.allow_unauthenticated {
        return Ok(AuthContext { token: raw_token.to_string() });
    }
    if config.tokens.contains(raw_token) {
        Ok(AuthContext { token: raw_token.to_string() })
    } else {
        Err(AuthError::InvalidToken)
    }
}
```

Both adapters become thin wrappers:

- **gRPC:** Extracts bearer from metadata, calls `validate_token`, attaches `AuthContext` to request extensions
- **REST:** `AuthContext` is an axum `FromRequestParts` extractor. Handlers that need auth add it as a parameter. `GET /api/v1/server/health` omits it.

---

## 7. REST Adapter

### Router Assembly

```rust
// adapter/rest/mod.rs

pub fn router(services: Arc<AppServices>, auth_config: AuthConfig) -> Router {
    let public = Router::new()
        .route("/api/v1/server/health", get(server::health));

    let protected = Router::new()
        // Pipelines
        .route("/api/v1/pipelines", get(pipelines::list))
        .route("/api/v1/pipelines/sync", post(pipelines::sync_batch))
        .route("/api/v1/pipelines/assert", post(pipelines::assert_all))
        .route("/api/v1/pipelines/{name}", get(pipelines::get))
        .route("/api/v1/pipelines/{name}/sync", post(pipelines::sync))
        .route("/api/v1/pipelines/{name}/check", post(pipelines::check))
        .route("/api/v1/pipelines/{name}/check-apply", post(pipelines::check_apply))
        .route("/api/v1/pipelines/{name}/compiled", get(pipelines::compile))
        .route("/api/v1/pipelines/{name}/diff", get(pipelines::diff))
        .route("/api/v1/pipelines/{name}/assert", post(pipelines::assert_one))
        .route("/api/v1/pipelines/{name}/teardown", post(pipelines::teardown))
        .route("/api/v1/pipelines/{name}/pause", post(operations::pause))
        .route("/api/v1/pipelines/{name}/resume", post(operations::resume))
        .route("/api/v1/pipelines/{name}/reset", post(operations::reset))
        // Runs
        .route("/api/v1/runs", get(runs::list))
        .route("/api/v1/runs/{id}", get(runs::get))
        .route("/api/v1/runs/{id}/cancel", post(runs::cancel))
        .route("/api/v1/runs/{id}/events", get(runs::events))
        // Batches
        .route("/api/v1/batches/{id}", get(runs::get_batch))
        .route("/api/v1/batches/{id}/events", get(runs::batch_events))
        // Connections
        .route("/api/v1/connections", get(connections::list))
        .route("/api/v1/connections/{name}", get(connections::get))
        .route("/api/v1/connections/{name}/test", post(connections::test))
        .route("/api/v1/connections/{name}/discover", get(connections::discover))
        // Plugins
        .route("/api/v1/plugins", get(plugins::list))
        .route("/api/v1/plugins/search", get(plugins::search))
        .route("/api/v1/plugins/install", post(plugins::install))
        .route("/api/v1/plugins/{ref}", get(plugins::info))
        .route("/api/v1/plugins/{ref}", delete(plugins::remove))
        // Operations
        .route("/api/v1/status", get(operations::status))
        .route("/api/v1/status/{name}", get(operations::pipeline_status))
        .route("/api/v1/freshness", get(operations::freshness))
        .route("/api/v1/logs", get(operations::logs))
        .route("/api/v1/logs/stream", get(operations::logs_stream))
        // Server
        .route("/api/v1/server/version", get(server::version))
        .route("/api/v1/server/config", get(server::config))
        .layer(auth_layer(auth_config));

    public.merge(protected).with_state(services)
}
```

### Handler Pattern

Handlers are thin translation: extract HTTP inputs, call service trait, map response.

```rust
pub async fn get(
    _auth: AuthContext,
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
) -> Result<Json<RunDetail>, RestError> {
    let run = services.runs.get(&id).await?;
    Ok(Json(run))
}
```

### Error Mapping

`ServiceError` maps to HTTP via `IntoResponse`:

| ServiceError | HTTP Status | JSON `error.code` |
|---|---|---|
| `NotFound` | 404 | `{resource}_not_found` |
| `Conflict` | 409 | `conflict` |
| `ValidationFailed` | 422 | `validation_failed` |
| `Unauthorized` | 401 | `unauthorized` |
| `Internal` | 500 | `internal_error` |

JSON body follows the API doc format:

```json
{
  "error": {
    "code": "pipeline_not_found",
    "message": "Pipeline 'salesforce-crm' does not exist",
    "details": null
  }
}
```

### SSE Helpers

Reusable module converting `EventStream<T>` to axum `Sse`:

```rust
// adapter/rest/sse.rs

pub fn to_sse_stream<T: Serialize>(
    stream: EventStream<T>,
    event_name: impl Fn(&T) -> &'static str,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let sse_stream = stream.map(move |item| {
        let name = event_name(&item);
        let data = serde_json::to_string(&item).unwrap_or_default();
        Ok(Event::default().event(name).data(data))
    });
    Sse::new(sse_stream).keep_alive(KeepAlive::default())
}
```

### Pagination

Shared extractor with validation:

```rust
#[derive(Deserialize)]
pub struct PaginationParams {
    pub limit: Option<u32>,    // default 20, max 100
    pub cursor: Option<String>,
}
```

---

## 8. AppServices & AppContext

### AppServices

Single struct implementing all 6 driving-port traits. Holds `AppContext` and delegates to existing application functions + new driven ports.

```rust
// application/services/mod.rs

pub struct AppServices {
    pub(crate) ctx: Arc<AppContext>,
}

impl AppServices {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx }
    }
}
```

### AppContext Extension

Extended with 4 new driven ports:

```rust
pub struct AppContext {
    // Existing
    pub runs: Arc<dyn RunRepository>,
    pub tasks: Arc<dyn TaskRepository>,
    pub agents: Arc<dyn AgentRepository>,
    pub store: Arc<dyn PipelineStore>,
    pub event_bus: Arc<dyn EventBus>,
    pub secrets: Arc<dyn SecretResolver>,
    pub clock: Arc<dyn Clock>,
    pub config: AppConfig,

    // New
    pub plugin_registry: Arc<dyn PluginRegistry>,
    pub connection_tester: Arc<dyn ConnectionTester>,
    pub cursor_store: Arc<dyn CursorStore>,
    pub log_store: Arc<dyn LogStore>,
}
```

### Composition Root

`server.rs` gets a new `serve()` function:

1. Same setup as `run()`: pool, migrations, adapter construction
2. Build new driven-port adapters (`EnginePluginRegistry`, `EngineConnectionTester`, `PgCursorStore`, `PgLogStore`)
3. Build `AppContext` with all ports
4. Build `AppServices`
5. Build REST router
6. Start both REST and gRPC via `tokio::select!` / `tokio::spawn`

Config extended with `rest_port: u16` (default 8080).

---

## 9. gRPC Migration

`PipelineGrpcService` migrates from calling application functions directly to calling through `AppServices`:

| gRPC method | Before | After |
|---|---|---|
| `submit_pipeline` | `submit::submit_pipeline(&ctx, ...)` | `services.pipelines.sync(...)` |
| `get_run` | `query::get_run(&ctx, ...)` | `services.runs.get(...)` |
| `list_runs` | `query::list_runs(&ctx, ...)` | `services.runs.list(...)` |
| `cancel_run` | `cancel::cancel_run(&ctx, ...)` | `services.runs.cancel(...)` |
| `watch_run` | `event_bus.subscribe(...)` | `services.runs.events(...)` |

`AgentGrpcService` stays on direct application calls — agent protocol is internal, not user-facing.

`PipelineGrpcService` constructor changes to take `Arc<AppServices>`. `AgentGrpcService` keeps `Arc<AppContext>`.

---

## 10. Database Migrations

Two new migrations:

### run_logs table

```sql
CREATE TABLE run_logs (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT NOT NULL REFERENCES runs(id),
    pipeline    TEXT NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    level       TEXT NOT NULL,
    message     TEXT NOT NULL,
    fields      JSONB
);

CREATE INDEX idx_run_logs_pipeline ON run_logs(pipeline, timestamp DESC);
CREATE INDEX idx_run_logs_run_id ON run_logs(run_id, timestamp DESC);
```

### pipeline_states table

```sql
CREATE TABLE pipeline_states (
    pipeline    TEXT PRIMARY KEY,
    state       TEXT NOT NULL DEFAULT 'active',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`CursorStore` reads from existing tables managed by `rapidbyte-state` — no new migration needed.

---

## 11. New Dependencies

```toml
[dependencies]
axum = { version = "0.8", features = ["macros"] }
axum-extra = { version = "0.10", features = ["typed-header"] }
tower-http = { version = "0.6", features = ["cors", "trace"] }
```

All other dependencies (tokio, serde, sqlx, chrono, tracing, tower) already exist.

---

## 12. Testing Strategy

### Unit Tests — Service Implementations

Test each service impl against fake driven ports. Extend `application/testing.rs` with fakes for `PluginRegistry`, `ConnectionTester`, `CursorStore`, `LogStore`.

### Integration Tests — REST Handlers

Test the full HTTP stack using axum's `tower::ServiceExt::oneshot`. Validates JSON format, status codes, auth enforcement, pagination, error responses. Uses fakes — no real database.

### Postgres Integration Tests — Driven Port Adapters

Extend `tests/postgres/` with tests for `PgCursorStore` and `PgLogStore`. Runs against real Postgres (existing Docker setup).

### gRPC Regression

Existing gRPC integration tests validate the migration doesn't break anything.

---

## 13. Build Order (Vertical Slices)

### Slice 1: Server

Axum skeleton + auth + error handling + `ServerService`. Proves the infrastructure.

**Delivers:** `curl /api/v1/server/health` works.

### Slice 2: Runs

`RunService` + SSE + pagination. Reuses existing app layer most heavily.

**Delivers:** `GET /api/v1/runs`, `GET /api/v1/runs/{id}/events` (SSE).

### Slice 3: Pipelines + gRPC Migration

`PipelineService` + all pipeline handlers + gRPC migration. Largest slice.

**Delivers:** Full pipeline CRUD via REST. gRPC and REST share code path.

### Slice 4: Operations

`OperationsService` + `CursorStore` + `LogStore` driven ports + migrations.

**Delivers:** Status dashboard, pause/resume, reset, freshness, logs, log streaming.

### Slice 5: Connections

`ConnectionService` + `ConnectionTester` driven port + engine adapter.

**Delivers:** Connection list/get/test/discover via REST.

### Slice 6: Plugins

`PluginService` + `PluginRegistry` driven port + engine adapter.

**Delivers:** Plugin list/search/install/remove via REST.

### Dependency Graph

```
Slice 1 (Server)
  └→ Slice 2 (Runs)
      └→ Slice 3 (Pipelines + gRPC migration)
          ├→ Slice 4 (Operations)  ─┐
          ├→ Slice 5 (Connections)  ├─ independent of each other
          └→ Slice 6 (Plugins)    ─┘
```
