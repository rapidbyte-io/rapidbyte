# rapidbyte-api Phase 1 — Design Spec

> Phase 1 delivers the `rapidbyte-api` crate (service traits + local-mode
> implementations) and an axum REST adapter in the CLI (`rapidbyte serve`).
> Distributed mode (API → controller via gRPC) is designed for but not wired.

---

## 1. Scope

### In scope

- New `rapidbyte-api` crate: 6 driving-port traits, request/response types,
  `ApiContext` DI container, local-mode service implementations.
- axum REST adapter in `rapidbyte-cli`: router, handlers, auth middleware,
  SSE bridge, error mapping, `rapidbyte serve` command.
- Bearer token auth middleware with `--allow-unauthenticated` flag.
  Login/logout CLI commands deferred to Phase 2 (CLI.md).
- Full SSE for run events via broadcast channel bridge from engine's
  `ChannelProgressReporter`.
- ~20 working endpoints covering the pipeline lifecycle (discover → check →
  sync → monitor → inspect).
- ~15 stubbed endpoints returning 501 for features needing new engine work.

### Out of scope

- Distributed mode wiring (API → controller gRPC). The `DeploymentMode`
  enum and `ApiContext` shape support it; Phase 2 fills the slot.
- CLI login/logout commands and `~/.rapidbyte/config.yaml` management.
- Batch operations, pause/resume, reset, freshness, logs, assert, teardown.
- Plugin search/install/remove (registry write operations).
- New E2E tests — existing `tests/e2e.sh` unchanged.

---

## 2. Architecture

### Approach: Thin API crate + axum adapter in CLI

`rapidbyte-api` contains only the domain boundary: traits, types, context,
and service implementations. The axum REST adapter lives in `rapidbyte-cli`
as a `serve` command module — it is a driving adapter, co-located with the
other CLI driving adapter (clap commands).

This follows the existing hexagonal pattern. The API.md spec describes axum
as a "driving adapter" alongside CLI. If a standalone server binary is
needed later, extracting the axum module is straightforward.

### Crate structure

```
crates/rapidbyte-api/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Re-exports traits, context, types
│   │
│   ├── traits/             # Driving port trait definitions
│   │   ├── mod.rs
│   │   ├── pipeline.rs     # PipelineService
│   │   ├── run.rs          # RunService
│   │   ├── connection.rs   # ConnectionService
│   │   ├── plugin.rs       # PluginService
│   │   ├── operations.rs   # OperationsService
│   │   └── server.rs       # ServerService
│   │
│   ├── types.rs            # Request/response DTOs
│   │
│   ├── services/           # Trait implementations (local mode)
│   │   ├── mod.rs
│   │   ├── pipeline.rs     # Delegates to engine use-cases
│   │   ├── run.rs          # RunManager + RunRecordRepository
│   │   ├── connection.rs   # Config introspection + engine discover
│   │   ├── plugin.rs       # PluginResolver / local cache
│   │   ├── operations.rs   # Cursor queries, run history
│   │   └── server.rs       # Health, version, config
│   │
│   ├── context.rs          # ApiContext DI container + builder
│   ├── error.rs            # ApiError type, PipelineError mapping
│   └── testing.rs          # Fake engine context for tests (cfg(test) gated)
```

### axum adapter structure (in CLI)

```
crates/rapidbyte-cli/src/commands/
├── serve/
│   ├── mod.rs              # build_router(), start server
│   ├── routes/
│   │   ├── mod.rs
│   │   ├── pipelines.rs
│   │   ├── runs.rs
│   │   ├── connections.rs
│   │   ├── plugins.rs
│   │   ├── operations.rs
│   │   └── server.rs
│   ├── middleware/
│   │   ├── mod.rs
│   │   └── auth.rs         # Bearer token validation
│   ├── sse.rs              # broadcast::Receiver → Sse<Event>
│   ├── error.rs            # ApiError → axum IntoResponse
│   └── extract.rs          # Pagination, query param extractors
```

---

## 3. Dependency Graph

### New workspace dependencies

```toml
axum = "0.8"
axum-extra = { version = "0.10", features = ["typed-header"] }
tower-http = { version = "0.6", features = ["cors", "trace"] }
ulid = "1"
```

### rapidbyte-api dependencies

```toml
rapidbyte-engine = { path = "../rapidbyte-engine" }
rapidbyte-types = { path = "../rapidbyte-types" }
rapidbyte-pipeline-config = { path = "../rapidbyte-pipeline-config" }
rapidbyte-registry = { path = "../rapidbyte-registry" }
rapidbyte-secrets = { path = "../rapidbyte-secrets" }
tokio = { workspace = true }
async-trait = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
anyhow = { workspace = true }
chrono = { workspace = true }
ulid = { workspace = true }
dashmap = { workspace = true }
tracing = { workspace = true }
```

### rapidbyte-cli additions

```toml
rapidbyte-api = { path = "../rapidbyte-api" }
axum = { workspace = true }
axum-extra = { workspace = true }
tower-http = { workspace = true }
```

### Updated graph

```
types (leaf)
  ├── state → types
  ├── runtime → types, state
  ├── sdk → types
  ├── pipeline-config → secrets
  ├── engine → types, runtime, state, pipeline-config
  ├── api → engine, types, pipeline-config, registry, secrets  ← NEW
  │   controller → types, pipeline-config, secrets, metrics
  ├── dev → engine, runtime, types, state
  └── cli → api, engine, runtime, types, dev, controller       ← UPDATED
            (+ axum, tower-http for serve adapter)
```

> **Phase 1 compromise:** The CLI retains direct dependencies on
> `rapidbyte-engine`, `rapidbyte-runtime`, etc. for its existing commands
> (`run`, `check`, `discover`, `teardown`). These commands pre-date the API
> layer and call engine use-cases directly. Migrating them to go through
> `rapidbyte-api` service traits is planned for Phase 2 (CLI.md), at which
> point the CLI will depend only on `rapidbyte-api` (plus `rapidbyte-dev`
> for the REPL).

---

## 4. Service Traits

Six driving-port traits in `rapidbyte-api/src/traits/`. All use
`#[async_trait]` and return `Result<T, ApiError>`.

### Common types

```rust
/// Cursor-based paginated list. Default limit 20, max 100.
pub struct PaginatedList<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// Type-erased async event stream for SSE endpoints.
pub type EventStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

pub struct PipelineFilter {
    pub tag: Option<Vec<String>>,
    pub dir: Option<PathBuf>,  // CLI-only, not exposed via REST
}

pub struct RunFilter {
    pub pipeline: Option<String>,
    pub status: Option<RunStatus>,
    pub limit: u32,
    pub cursor: Option<String>,
}

pub struct SyncRequest {
    pub pipeline: String,
    pub stream: Option<String>,
    pub full_refresh: bool,
    pub cursor_start: Option<String>,
    pub cursor_end: Option<String>,
    pub dry_run: bool,
}

pub struct DiscoverRequest {
    pub connection: String,
    pub table: Option<String>,
}
```

> **Note on `DiscoverRequest`:** The `connection` field is populated from the
> URL path parameter (`/connections/{name}/discover`). It is an API-layer
> type, distinct from the engine's `DiscoverParams`.

### PipelineService

```rust
#[async_trait]
pub trait PipelineService: Send + Sync {
    async fn list(&self, filter: PipelineFilter) -> Result<PaginatedList<PipelineSummary>>;
    async fn get(&self, name: &str) -> Result<PipelineDetail>;
    async fn sync(&self, request: SyncRequest) -> Result<RunHandle>;
    async fn sync_batch(&self, request: SyncBatchRequest) -> Result<BatchRunHandle>;
    async fn check(&self, name: &str) -> Result<CheckResult>;
    async fn check_apply(&self, name: &str) -> Result<RunHandle>;
    async fn compile(&self, name: &str) -> Result<ResolvedConfig>;
    async fn diff(&self, name: &str) -> Result<DiffResult>;
    async fn assert(&self, request: AssertRequest) -> Result<AssertResult>;
    async fn teardown(&self, request: TeardownRequest) -> Result<RunHandle>;
}
```

> **Note on `assert`:** Matches API.md. While `assert` shadows the `assert!`
> macro name, it is legal as a method name in Rust and reads naturally in
> trait context (`ctx.pipelines.assert(req)`).

### RunService

```rust
#[async_trait]
pub trait RunService: Send + Sync {
    async fn get(&self, run_id: &str) -> Result<RunDetail>;
    async fn list(&self, filter: RunFilter) -> Result<PaginatedList<RunSummary>>;
    async fn events(&self, run_id: &str) -> Result<EventStream<SseEvent>>;
    async fn cancel(&self, run_id: &str) -> Result<RunDetail>;
    async fn get_batch(&self, batch_id: &str) -> Result<BatchDetail>;
    async fn batch_events(&self, batch_id: &str) -> Result<EventStream<SseEvent>>;
}
```

### ConnectionService

```rust
#[async_trait]
pub trait ConnectionService: Send + Sync {
    async fn list(&self) -> Result<Vec<ConnectionSummary>>;
    async fn get(&self, name: &str) -> Result<ConnectionDetail>;
    async fn test(&self, name: &str) -> Result<ConnectionTestResult>;
    async fn discover(&self, request: DiscoverRequest) -> Result<DiscoverResult>;
}
```

### PluginService

```rust
#[async_trait]
pub trait PluginService: Send + Sync {
    async fn list(&self) -> Result<Vec<PluginSummary>>;
    async fn search(&self, request: PluginSearchRequest) -> Result<Vec<PluginSearchResult>>;
    async fn info(&self, plugin_ref: &str) -> Result<PluginDetail>;
    async fn install(&self, plugin_ref: &str) -> Result<PluginInstallResult>;
    async fn remove(&self, plugin_ref: &str) -> Result<()>;
}
```

### OperationsService

```rust
#[async_trait]
pub trait OperationsService: Send + Sync {
    async fn status(&self) -> Result<Vec<PipelineStatus>>;
    async fn pipeline_status(&self, name: &str) -> Result<PipelineStatusDetail>;
    async fn pause(&self, name: &str) -> Result<PipelineState>;
    async fn resume(&self, name: &str) -> Result<PipelineState>;
    async fn reset(&self, request: ResetRequest) -> Result<ResetResult>;
    async fn freshness(&self, filter: FreshnessFilter) -> Result<Vec<FreshnessStatus>>;
    async fn logs(&self, request: LogsRequest) -> Result<LogsResult>;
    async fn logs_stream(&self, filter: LogsStreamFilter) -> Result<EventStream<SseEvent>>;
}
```

### ServerService

```rust
#[async_trait]
pub trait ServerService: Send + Sync {
    async fn health(&self) -> Result<HealthStatus>;
    async fn version(&self) -> Result<VersionInfo>;
    async fn config(&self) -> Result<ServerConfig>;
}
```

---

## 5. ApiContext

```rust
pub struct ApiContext {
    pub pipelines: Arc<dyn PipelineService>,
    pub runs: Arc<dyn RunService>,
    pub connections: Arc<dyn ConnectionService>,
    pub plugins: Arc<dyn PluginService>,
    pub operations: Arc<dyn OperationsService>,
    pub server: Arc<dyn ServerService>,
}

pub enum DeploymentMode {
    Local,
    Distributed { controller_url: String },
}

impl ApiContext {
    pub async fn from_project(
        project_dir: &Path,
        mode: DeploymentMode,
    ) -> Result<Self> {
        match mode {
            DeploymentMode::Local => {
                // Scan project dir for pipeline YAMLs
                // Build EngineContext with concrete adapters
                // Wire local service impls
            }
            DeploymentMode::Distributed { .. } => {
                // Phase 2: wire controller-backed service impls
                anyhow::bail!("distributed mode not yet implemented");
            }
        }
    }
}
```

`ApiContext::from_project` for local mode:

1. Scans `project_dir` for `*.yaml`/`*.yml` pipeline files.
2. Parses them into a `HashMap<String, PipelineConfig>` (the pipeline catalog).
3. Resolves secrets via `SecretProviders`.
4. Builds shared infrastructure: state backend connection, registry config.
5. Creates a `RunManager` for in-flight run tracking.
6. Wires the 6 service implementations, each holding references to the
   catalog, engine factory functions, and `RunManager`.

---

## 6. SSE Event Model

The engine's `ProgressEvent` enum (`PhaseChanged`, `StreamStarted`,
`BatchEmitted`, `StreamCompleted`, `RetryScheduled`) is an internal type
with no concept of run IDs, pipeline names, record counts, or timestamps.

The API layer defines its own `SseEvent` enum that matches the API.md SSE
spec. The `RunManager` translates engine events and synthesizes lifecycle
events:

```rust
/// API-layer SSE event, distinct from engine's ProgressEvent.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", content = "data")]
pub enum SseEvent {
    Started {
        run_id: String,
        pipeline: String,
        started_at: DateTime<Utc>,
    },
    Progress {
        phase: String,
        stream: String,
        records_read: Option<u64>,
        records_written: Option<u64>,
        bytes_read: Option<u64>,
    },
    Log {
        level: String,
        message: String,
    },
    Complete {
        run_id: String,
        status: String,
        duration_secs: f64,
        counts: Option<PipelineCounts>,
    },
    Failed {
        run_id: String,
        error: String,
    },
    Cancelled {
        run_id: String,
        reason: String,
    },
}
```

### Translation from engine events

| Engine `ProgressEvent` | `SseEvent` |
|---|---|
| `PhaseChanged { phase }` | `Progress { phase, .. }` |
| `StreamStarted { stream }` | `Progress { stream, .. }` |
| `BatchEmitted { stream, bytes }` | `Progress { stream, bytes_read: Some(bytes), .. }` |
| `StreamCompleted { stream }` | `Progress { stream, .. }` |
| `RetryScheduled { .. }` | `Log { level: "warn", message: "retry scheduled" }` |
| *(synthesized by RunManager)* | `Started`, `Complete`, `Failed`, `Cancelled` |

The `RunManager` spawned task emits `Started` before calling `run_pipeline`,
and `Complete`/`Failed`/`Cancelled` after it returns.

---

## 7. RunManager

Tracks in-flight and recently completed runs in memory. The engine's
`RunRecordRepository` (Postgres) provides durable storage.

```rust
pub(crate) struct RunManager {
    runs: Arc<DashMap<String, RunState>>,
}

struct RunState {
    run_id: String,
    pipeline: String,
    status: RunStatus,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    result: Option<PipelineResult>,
    error: Option<String>,
    event_tx: broadcast::Sender<SseEvent>,
    cancel: CancellationToken,
}
```

Completed runs are evicted after 1 hour to prevent unbounded memory growth.
A background task runs on a 5-minute interval to sweep expired entries.

### EngineContext lifecycle

`EngineContext` is created **per pipeline run**, not at `ApiContext`
construction time. This is necessary because each pipeline has its own
`state.connection` URL — different pipelines may point to different state
backends. The `ApiContext` holds the pipeline catalog and registry config;
`build_run_context()` is called at the point where a specific pipeline is
being executed.

### Sync flow

1. `PipelineService::sync` is called with pipeline name + options.
2. Generate `run_id` = `"run_"` + ULID.
3. Create `broadcast::channel(256)` for SSE subscribers.
4. Build `EngineContext` via `build_run_context()` for this specific
   pipeline, passing a progress sender that translates engine
   `ProgressEvent`s to `SseEvent`s and forwards to the broadcast channel.
5. Insert `RunState { status: Pending, .. }` into `RunManager`.
6. `tokio::spawn` the pipeline execution task:
   - Send `SseEvent::Started`.
   - Update status to `Running`.
   - Call `run_pipeline(&ctx, &config, cancel_token)`.
   - On completion: update status to `Completed`/`Failed`, store result.
   - Send terminal SSE event (`Complete` or `Failed`).
7. Return `RunHandle { run_id, status: Pending, links }`.

### Lagged SSE subscribers

The broadcast channel has capacity 256. If a subscriber falls behind, it
receives a `RecvError::Lagged(n)` error. The SSE bridge logs a warning
and continues from the latest event — SSE clients are expected to handle
gaps via the `Last-Event-ID` header in future reconnections.

### SSE subscription

1. `RunService::events` looks up `RunState` by `run_id`.
2. Calls `event_tx.subscribe()` to get a `broadcast::Receiver`.
3. Returns it as an `EventStream`.
4. If the run is already terminal, sends the final event and closes.

### Cancellation

1. `RunService::cancel` looks up `RunState`, calls `cancel.cancel()`.
2. The spawned task receives cancellation via `CancellationToken`.

---

## 8. Auth Middleware

```rust
pub struct AuthConfig {
    pub required: bool,
    pub tokens: Vec<String>,
}
```

Tower layer that:
- Extracts `Authorization: Bearer <token>` header.
- Validates against the configured token list.
- Skips validation for `GET /api/v1/server/health`.
- Returns `401` JSON error envelope on failure.
- Passes through all requests when `required = false`
  (`--allow-unauthenticated`).

Token source for Phase 1: `--auth-token` flag or `RAPIDBYTE_AUTH_TOKEN`
env var on the `serve` command. Multiple tokens supported via
comma-separated values.

---

## 9. Error Handling

### ApiError

```rust
#[derive(Debug, Error)]
pub enum ApiError {
    #[error("not found: {message}")]
    NotFound { code: String, message: String },

    #[error("conflict: {message}")]
    Conflict { code: String, message: String },

    #[error("validation failed: {message}")]
    ValidationFailed { message: String, details: Vec<FieldError> },

    #[error("not implemented: {message}")]
    NotImplemented { message: String },

    #[error("unauthorized: {message}")]
    Unauthorized { message: String },

    #[error("internal error: {message}")]
    Internal { message: String },
}

pub struct FieldError {
    pub field: String,
    pub reason: String,
}
```

### Mapping from engine errors

| Engine error | ApiError |
|---|---|
| `PipelineError::Plugin(PluginError { .. })` | `Internal` (with plugin error details) |
| `PipelineError::Infrastructure(..)` | `Internal` |
| `PipelineError::Cancelled` | `Conflict` (run already cancelled) |
| `RepositoryError::Conflict(..)` | `Conflict` |
| `RepositoryError::Other(..)` | `Internal` |
| Pipeline not in catalog | `NotFound` |
| Run ID not found | `NotFound` |
| Stubbed method called | `NotImplemented` |

### HTTP status mapping (axum adapter)

| ApiError | HTTP | JSON body |
|---|---|---|
| `NotFound` | 404 | `{ "error": { "code": "..", "message": ".." } }` |
| `Conflict` | 409 | `{ "error": { "code": "..", "message": ".." } }` |
| `ValidationFailed` | 400 | `{ "error": { "code": "validation_failed", "message": "..", "details": [..] } }` |
| `NotImplemented` | 501 | `{ "error": { "code": "not_implemented", "message": ".." } }` |
| `Unauthorized` | 401 | `{ "error": { "code": "unauthorized", "message": ".." } }` |
| `Internal` | 500 | `{ "error": { "code": "internal_error", "message": ".." } }` |

---

## 10. Endpoint Implementation Status

All endpoints are nested under `/api/v1/` via `Router::new().nest("/api/v1", api)`
in `build_router`. Paths below omit the prefix for brevity.

### Fully implemented (~20 endpoints)

| Endpoint | Service method | Backing |
|---|---|---|
| `GET /pipelines` | `PipelineService::list` | Project dir scan |
| `GET /pipelines/{name}` | `PipelineService::get` | Catalog lookup |
| `POST /pipelines/{name}/sync` | `PipelineService::sync` | `run_pipeline()` |
| `POST /pipelines/{name}/check` | `PipelineService::check` | `check_pipeline()` |
| `GET /pipelines/{name}/compiled` | `PipelineService::compile` | Config resolve |
| `GET /runs/{id}` | `RunService::get` | RunManager + DB |
| `GET /runs` | `RunService::list` | RunRecordRepository |
| `GET /runs/{id}/events` | `RunService::events` | Broadcast → SSE |
| `POST /runs/{id}/cancel` | `RunService::cancel` | CancellationToken |
| `GET /connections` | `ConnectionService::list` | Config introspection |
| `GET /connections/{name}` | `ConnectionService::get` | Config + redaction |
| `POST /connections/{name}/test` | `ConnectionService::test` | Plugin validate |
| `GET /connections/{name}/discover` | `ConnectionService::discover` | `discover_plugin()` |
| `GET /plugins` | `PluginService::list` | Local cache scan |
| `GET /plugins/{ref}` | `PluginService::info` | Manifest inspect |
| `GET /status` | `OperationsService::status` | Catalog + run history |
| `GET /status/{name}` | `OperationsService::pipeline_status` | Run + cursor state |
| `GET /server/health` | `ServerService::health` | Uptime + DB ping |
| `GET /server/version` | `ServerService::version` | Build info |
| `GET /server/config` | `ServerService::config` | Server config |

### Stubbed — 501 (~15 endpoints)

| Endpoint | Reason |
|---|---|
| `POST /pipelines/sync` | Batch orchestration needed |
| `POST /pipelines/{name}/check-apply` | Async apply wrapper needed |
| `GET /pipelines/{name}/diff` | Schema diff logic needed |
| `POST /pipelines/{name}/assert` | Assertion engine needed |
| `POST /pipelines/assert` | Assertion engine needed |
| `POST /pipelines/{name}/teardown` | Async run tracking for teardown |
| `POST /pipelines/{name}/pause` | Pipeline state management needed |
| `POST /pipelines/{name}/resume` | Pipeline state management needed |
| `POST /pipelines/{name}/reset` | Cursor clearing API needed |
| `GET /freshness` | SLA config + calculation needed |
| `GET /logs` | Log storage needed |
| `GET /logs/stream` | Log capture infra needed |
| `GET /plugins/search` | Registry search API needed |
| `POST /plugins/install` | Registry pull needed |
| `DELETE /plugins/{ref}` | Cache management needed |
| `GET /batches/{id}` | Batch tracking needed |
| `GET /batches/{id}/events` | Batch SSE multiplexing needed |

---

## 11. CLI Command

```rust
Serve {
    /// Address to listen on
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: String,

    /// Disable authentication (development only)
    #[arg(long)]
    allow_unauthenticated: bool,

    /// Bearer token(s) for API auth (comma-separated)
    #[arg(long, env = "RAPIDBYTE_AUTH_TOKEN")]
    auth_token: Option<String>,
}
```

The `serve` command:

1. Builds `ApiContext::from_project(&cwd, DeploymentMode::Local)`.
2. Constructs `AuthConfig` from flags/env.
3. Calls `build_router(ctx, auth_config)`.
4. Binds to the `--listen` address.
5. Serves with graceful shutdown on SIGTERM/SIGINT.
   On shutdown, in-flight runs are cancelled via their `CancellationToken`s
   and the server waits up to 30 seconds for them to finish.
6. Logs: `Listening on http://{listen}`.

CORS: permissive (`Access-Control-Allow-Origin: *`) when
`--allow-unauthenticated` is set. When auth is required, CORS is
restrictive (no wildcard origin). Configurable via `--cors-origin` in
a future iteration.

---

## 12. Testing Strategy

### Unit tests (in `rapidbyte-api`)

- Fake engine context in `testing.rs` with in-memory implementations of
  all engine ports.
- `test_api_context()` wires fakes into `ApiContext`.
- Tests per service: list, get, sync lifecycle, cancel, error mapping,
  secret redaction, SSE event delivery.

### HTTP integration tests (in CLI)

- Spin up axum server on random port with `test_api_context()`.
- Real HTTP requests via `reqwest`.
- Cover: route matching, auth middleware (401/pass-through/health skip),
  JSON error format, SSE delivery, pagination, 501 stubs.

### No new E2E tests

Existing `tests/e2e.sh` unchanged. HTTP integration tests with fake engine
contexts provide sufficient coverage for Phase 1.

---

## 13. Conventions

Follows existing crate conventions:

- `#![warn(clippy::pedantic)]`
- Module table in `lib.rs` doc comment
- Top-level re-exports for common public types
- `pub(crate)` for internal structs
- `thiserror` for error types, `anyhow::Result` at API boundaries
- `tracing` for structured logging
