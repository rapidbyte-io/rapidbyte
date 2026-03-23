# rapidbyte-api Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `rapidbyte-api` crate (6 service traits, types, `ApiContext`, local-mode service impls) and an axum REST adapter in the CLI (`rapidbyte serve`).

**Architecture:** Thin API crate with driving-port traits and local-mode service implementations that delegate to `rapidbyte-engine` use-cases. The axum REST adapter lives in `rapidbyte-cli` as a `serve` command module. SSE via `broadcast::channel` bridging engine `ProgressEvent` to API-layer `SseEvent`.

**Tech Stack:** Rust, axum 0.8, tower-http (CORS/tracing), tokio broadcast channels, serde, ulid, dashmap

**Spec:** `docs/superpowers/specs/2026-03-23-rapidbyte-api-phase1-design.md`

---

## File Map

### New crate: `crates/rapidbyte-api/`

| File | Responsibility |
|------|---------------|
| `Cargo.toml` | Crate manifest with deps on engine, types, pipeline-config, registry, secrets |
| `src/lib.rs` | Module table, re-exports of all public types and traits |
| `src/error.rs` | `ApiError` enum, `FieldError`, `From<PipelineError>` impl |
| `src/types.rs` | Request/response DTOs: `PaginatedList<T>`, `EventStream<T>`, `SseEvent`, all filter/request/response structs |
| `src/traits/mod.rs` | Re-exports all 6 service traits |
| `src/traits/pipeline.rs` | `PipelineService` trait |
| `src/traits/run.rs` | `RunService` trait |
| `src/traits/connection.rs` | `ConnectionService` trait |
| `src/traits/plugin.rs` | `PluginService` trait |
| `src/traits/operations.rs` | `OperationsService` trait |
| `src/traits/server.rs` | `ServerService` trait |
| `src/context.rs` | `ApiContext` struct, `DeploymentMode`, `ApiContext::from_project()` builder |
| `src/run_manager.rs` | `RunManager` struct: in-memory run tracking, broadcast, cancel, eviction |
| `src/services/mod.rs` | Re-exports all 6 service impls |
| `src/services/pipeline.rs` | `LocalPipelineService` — delegates to engine |
| `src/services/run.rs` | `LocalRunService` — queries RunManager + RunRecordRepository |
| `src/services/connection.rs` | `LocalConnectionService` — config introspection + engine discover |
| `src/services/plugin.rs` | `LocalPluginService` — local cache, stubs for search/install/remove |
| `src/services/operations.rs` | `LocalOperationsService` — status from catalog + run history, stubs |
| `src/services/server.rs` | `LocalServerService` — health, version, config |
| `src/testing.rs` | `test_api_context()` factory, fake/in-memory service impls for tests (cfg(test) gated) |

### Modified: `crates/rapidbyte-cli/`

| File | Responsibility |
|------|---------------|
| `Cargo.toml` | Add deps: rapidbyte-api, axum, axum-extra, tower-http, async-stream |
| `src/main.rs` | Add `Serve` variant to `Commands` enum, dispatch to serve module |
| `src/commands/mod.rs` | Add `pub mod serve;` |
| `src/commands/serve/mod.rs` | `build_router()`, `execute()` (bind + serve + graceful shutdown) |
| `src/commands/serve/error.rs` | `ApiError` → axum `IntoResponse` impl |
| `src/commands/serve/extract.rs` | Pagination query params, common extractors |
| `src/commands/serve/sse.rs` | `broadcast::Receiver<SseEvent>` → `Sse<Event>` bridge |
| `src/commands/serve/middleware/mod.rs` | Re-export auth layer |
| `src/commands/serve/middleware/auth.rs` | Bearer token tower layer |
| `src/commands/serve/routes/mod.rs` | Re-exports all route modules |
| `src/commands/serve/routes/pipelines.rs` | Pipeline endpoint handlers |
| `src/commands/serve/routes/runs.rs` | Run endpoint handlers |
| `src/commands/serve/routes/connections.rs` | Connection endpoint handlers |
| `src/commands/serve/routes/plugins.rs` | Plugin endpoint handlers |
| `src/commands/serve/routes/operations.rs` | Operations endpoint handlers |
| `src/commands/serve/routes/server.rs` | Server endpoint handlers |

### Modified: root

| File | Change |
|------|--------|
| `Cargo.toml` | Add `rapidbyte-api` to workspace members, add axum/axum-extra/tower-http/ulid/futures/async-stream to workspace deps |

---

## Task 1: Scaffold `rapidbyte-api` crate with error types

**Files:**
- Create: `crates/rapidbyte-api/Cargo.toml`
- Create: `crates/rapidbyte-api/src/lib.rs`
- Create: `crates/rapidbyte-api/src/error.rs`
- Modify: `Cargo.toml` (root workspace)

- [ ] **Step 1: Add workspace deps and member**

Add to root `Cargo.toml`:
- In `[workspace] members`: add `"crates/rapidbyte-api"`
- In `[workspace.dependencies]`: add `axum`, `axum-extra`, `tower-http`, `ulid`

```toml
# In [workspace] members array, after "crates/rapidbyte-agent":
"crates/rapidbyte-api",

# In [workspace.dependencies], add:
axum = "0.8"
axum-extra = { version = "0.10", features = ["typed-header"] }
tower-http = { version = "0.6", features = ["cors", "trace"] }
ulid = "1"
futures = "0.3"
async-stream = "0.3"
```

- [ ] **Step 2: Create `crates/rapidbyte-api/Cargo.toml`**

```toml
[package]
name = "rapidbyte-api"
version = "0.1.0"
edition = "2021"

[dependencies]
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
futures = { workspace = true }
tokio-util = { workspace = true }
```

- [ ] **Step 3: Create `crates/rapidbyte-api/src/error.rs`**

```rust
use rapidbyte_engine::PipelineError;
use serde::Serialize;

/// API-layer error type. Service implementations return this.
/// The axum adapter maps it to HTTP status codes.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("not found: {message}")]
    NotFound { code: String, message: String },

    #[error("conflict: {message}")]
    Conflict { code: String, message: String },

    #[error("validation failed: {message}")]
    ValidationFailed {
        message: String,
        details: Vec<FieldError>,
    },

    #[error("not implemented: {message}")]
    NotImplemented { message: String },

    #[error("unauthorized: {message}")]
    Unauthorized { message: String },

    #[error("internal error: {message}")]
    Internal { message: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldError {
    pub field: String,
    pub reason: String,
}

impl ApiError {
    pub fn not_found(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::NotFound {
            code: code.into(),
            message: message.into(),
        }
    }

    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::NotImplemented {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

impl From<PipelineError> for ApiError {
    fn from(err: PipelineError) -> Self {
        match &err {
            PipelineError::Cancelled => Self::Conflict {
                code: "run_cancelled".into(),
                message: "Pipeline run was cancelled".into(),
            },
            PipelineError::Plugin(_) => Self::Internal {
                message: err.to_string(),
            },
            PipelineError::Infrastructure(_) => Self::Internal {
                message: err.to_string(),
            },
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self::Internal {
            message: err.to_string(),
        }
    }
}
```

- [ ] **Step 4: Create `crates/rapidbyte-api/src/lib.rs`**

```rust
#![warn(clippy::pedantic)]

//! # rapidbyte-api
//!
//! Application-layer service traits and local-mode implementations for
//! the RapidByte data pipeline engine.
//!
//! ## Modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | `traits` | Six driving-port trait definitions |
//! | `types` | Request/response DTOs and SSE event model |
//! | `services` | Local-mode trait implementations |
//! | `context` | `ApiContext` DI container |
//! | `error` | `ApiError` type and engine error mapping |
//! | `run_manager` | In-memory run tracking, broadcast, cancel |

pub mod context;
pub mod error;
pub mod run_manager;
pub mod services;
pub mod traits;
pub mod types;

pub use context::{ApiContext, DeploymentMode};
pub use error::{ApiError, FieldError};
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo check -p rapidbyte-api 2>&1 | head -20`

This will fail because `traits`, `types`, `services`, `context`, `run_manager` modules don't exist yet. That's expected — we create stub modules next.

- [ ] **Step 6: Create stub modules so the crate compiles**

Create empty/minimal files:

`crates/rapidbyte-api/src/traits/mod.rs`:
```rust
pub mod pipeline;
pub mod run;
pub mod connection;
pub mod plugin;
pub mod operations;
pub mod server;
```

Create each trait file as empty for now (will be filled in Task 2):
- `crates/rapidbyte-api/src/traits/pipeline.rs` — empty
- `crates/rapidbyte-api/src/traits/run.rs` — empty
- `crates/rapidbyte-api/src/traits/connection.rs` — empty
- `crates/rapidbyte-api/src/traits/plugin.rs` — empty
- `crates/rapidbyte-api/src/traits/operations.rs` — empty
- `crates/rapidbyte-api/src/traits/server.rs` — empty

`crates/rapidbyte-api/src/types.rs`:
```rust
// Request/response types — populated in Task 2.
```

`crates/rapidbyte-api/src/context.rs`:
```rust
// ApiContext — populated in Task 4.
```

`crates/rapidbyte-api/src/run_manager.rs`:
```rust
// RunManager — populated in Task 5.
```

`crates/rapidbyte-api/src/services/mod.rs`:
```rust
pub mod pipeline;
pub mod run;
pub mod connection;
pub mod plugin;
pub mod operations;
pub mod server;
```

Create each service file as empty:
- `crates/rapidbyte-api/src/services/pipeline.rs` — empty
- `crates/rapidbyte-api/src/services/run.rs` — empty
- `crates/rapidbyte-api/src/services/connection.rs` — empty
- `crates/rapidbyte-api/src/services/plugin.rs` — empty
- `crates/rapidbyte-api/src/services/operations.rs` — empty
- `crates/rapidbyte-api/src/services/server.rs` — empty

- [ ] **Step 7: Verify the crate compiles**

Run: `cargo check -p rapidbyte-api`
Expected: compiles with possible warnings about unused imports

- [ ] **Step 8: Commit**

```bash
git add crates/rapidbyte-api/ Cargo.toml
git commit -m "feat(api): scaffold rapidbyte-api crate with error types"
```

---

## Task 2: Define service traits and request/response types

**Files:**
- Create (content): `crates/rapidbyte-api/src/types.rs`
- Create (content): `crates/rapidbyte-api/src/traits/pipeline.rs`
- Create (content): `crates/rapidbyte-api/src/traits/run.rs`
- Create (content): `crates/rapidbyte-api/src/traits/connection.rs`
- Create (content): `crates/rapidbyte-api/src/traits/plugin.rs`
- Create (content): `crates/rapidbyte-api/src/traits/operations.rs`
- Create (content): `crates/rapidbyte-api/src/traits/server.rs`
- Modify: `crates/rapidbyte-api/src/traits/mod.rs`
- Modify: `crates/rapidbyte-api/src/lib.rs`

- [ ] **Step 1: Write `types.rs` with all DTOs**

This file defines all request/response types used by the service traits.
Types are serde-serializable and distinct from engine domain types.
The service implementations handle conversion.

Reference the spec's Section 4 "Common types" and API.md for JSON shapes.

Key types to define:
- `PaginatedList<T>` — `{ items, next_cursor }`
- `EventStream<T>` — type alias for `Pin<Box<dyn Stream<Item = T> + Send>>`
- `SseEvent` — enum with `Started`, `Progress`, `Log`, `Complete`, `Failed`, `Cancelled`
- `PipelineFilter`, `RunFilter`, `SyncRequest`, `SyncBatchRequest`, `AssertRequest`, `TeardownRequest`
- `DiscoverRequest`, `PluginSearchRequest`, `ResetRequest`, `FreshnessFilter`, `LogsRequest`, `LogsStreamFilter`
- `PipelineSummary`, `PipelineDetail`, `RunHandle`, `BatchRunHandle`
- `RunDetail`, `RunSummary`, `RunStatus`, `BatchDetail`, `BatchRunEntry`
- `CheckResult` (API-layer, not engine's `CheckResult`)
- `ResolvedConfig`, `DiffResult`, `AssertResult`
- `ConnectionSummary`, `ConnectionDetail`, `ConnectionTestResult`, `DiscoverResult`
- `PluginSummary`, `PluginDetail`, `PluginSearchResult`, `PluginInstallResult`
- `PipelineStatus`, `PipelineStatusDetail`, `PipelineState`, `ResetResult`, `FreshnessStatus`
- `LogsResult`, `LogEntry`
- `HealthStatus`, `VersionInfo`, `ServerConfig`

All structs derive `Debug, Clone, Serialize, Deserialize` (except `EventStream`).
`RunStatus` is an enum: `Pending, Running, Completed, Failed, Cancelled`.
`PipelineState` is an enum: `Active, Paused`.

Reference: spec Section 4 (lines 177–218), API.md Sections 4–9.

- [ ] **Step 2: Write all 6 trait files**

Each trait file defines one `#[async_trait]` trait.
All methods return `Result<T, ApiError>`.

`traits/pipeline.rs` — `PipelineService` with 10 methods (spec lines 221–242).
`traits/run.rs` — `RunService` with 6 methods (spec lines 247–254).
`traits/connection.rs` — `ConnectionService` with 4 methods (spec lines 259–264).
`traits/plugin.rs` — `PluginService` with 5 methods (spec lines 269–277).
`traits/operations.rs` — `OperationsService` with 8 methods (spec lines 282–294).
`traits/server.rs` — `ServerService` with 3 methods (spec lines 299–305).

Reference: spec Section 4, API.md service trait definitions.

- [ ] **Step 3: Update `traits/mod.rs` to re-export traits**

```rust
pub mod pipeline;
pub mod run;
pub mod connection;
pub mod plugin;
pub mod operations;
pub mod server;

pub use pipeline::PipelineService;
pub use run::RunService;
pub use connection::ConnectionService;
pub use plugin::PluginService;
pub use operations::OperationsService;
pub use server::ServerService;
```

- [ ] **Step 4: Update `lib.rs` re-exports**

Add re-exports for all trait and type names used by consumers (CLI, tests):

```rust
// Traits
pub use traits::{
    PipelineService, RunService, ConnectionService,
    PluginService, OperationsService, ServerService,
};

// Types (selective — the most-used ones)
pub use types::{
    PaginatedList, EventStream, SseEvent, RunStatus, PipelineState,
    SyncRequest, RunFilter, PipelineFilter,
    RunHandle, RunDetail, RunSummary,
    PipelineSummary, PipelineDetail, CheckResult,
    HealthStatus, VersionInfo, ServerConfig,
};
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo check -p rapidbyte-api`
Expected: compiles (service impls are still empty stubs)

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-api/src/
git commit -m "feat(api): define 6 service traits and request/response types"
```

---

## Task 3: Implement `ApiError` → engine error mapping tests

**Files:**
- Modify: `crates/rapidbyte-api/src/error.rs` (add tests)

- [ ] **Step 1: Write tests for error mapping**

Add `#[cfg(test)] mod tests` to `error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_cancelled_maps_to_conflict() {
        let err: ApiError = PipelineError::Cancelled.into();
        assert!(matches!(err, ApiError::Conflict { .. }));
    }

    #[test]
    fn infrastructure_error_maps_to_internal() {
        let err: ApiError = PipelineError::Infrastructure(
            anyhow::anyhow!("db connection failed"),
        ).into();
        assert!(matches!(err, ApiError::Internal { .. }));
        assert!(err.to_string().contains("db connection failed"));
    }

    #[test]
    fn not_found_helper() {
        let err = ApiError::not_found("pipeline_not_found", "Pipeline 'foo' does not exist");
        assert!(matches!(err, ApiError::NotFound { code, message }
            if code == "pipeline_not_found" && message.contains("foo")));
    }

    #[test]
    fn not_implemented_helper() {
        let err = ApiError::not_implemented("batch sync");
        assert!(matches!(err, ApiError::NotImplemented { message }
            if message == "batch sync"));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-api`
Expected: 4 tests pass

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-api/src/error.rs
git commit -m "test(api): add ApiError mapping tests"
```

---

## Task 4: Create shared test infrastructure (`testing.rs`)

**Files:**
- Create: `crates/rapidbyte-api/src/testing.rs`
- Modify: `crates/rapidbyte-api/src/lib.rs`

Shared test utilities used by all service impl tests and HTTP integration
tests. Creates a `test_api_context()` factory that wires fake service
implementations backed by in-memory state.

> **Spec note:** The spec lists `testing.rs` as a `cfg(test)` gated module.
> The `RunManager` is the actual implementation used in tests (it's already
> in-memory). The fake services wrap a real `RunManager` + in-memory catalog.

- [ ] **Step 1: Create `testing.rs`**

```rust
#![cfg(test)]

use crate::context::ApiContext;
use crate::run_manager::RunManager;
use crate::types::*;
use rapidbyte_pipeline_config::PipelineConfig;
use std::collections::HashMap;
use std::sync::Arc;

/// Build an `ApiContext` for tests with an empty pipeline catalog.
/// No real engine, state backend, or WASM runtime — all service impls
/// use in-memory state only.
pub fn test_api_context() -> ApiContext {
    test_api_context_with_catalog(HashMap::new())
}

/// Build an `ApiContext` with a pre-populated pipeline catalog.
pub fn test_api_context_with_catalog(
    catalog: HashMap<String, PipelineConfig>,
) -> ApiContext {
    let catalog = Arc::new(catalog);
    let run_manager = Arc::new(RunManager::new());

    // Wire all 6 local service impls with test-friendly config:
    // - No registry config (plugin operations will fail gracefully or stub)
    // - No secrets
    // - Shared catalog and run_manager
    // Exact wiring depends on service impl constructors from Tasks 6–10.
    // Placeholder: fill in as services are implemented.
    todo!("Wire service impls — completed in Task 11 after all services exist")
}
```

This file will be iteratively completed as each service is implemented.
The `todo!()` is replaced in Task 11 when `ApiContext::from_project` and
all services are wired. In the interim, individual service tests construct
their service impls directly (e.g., `LocalServerService::new(Instant::now())`).

- [ ] **Step 2: Add to `lib.rs`**

```rust
#[cfg(test)]
pub mod testing;
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-api`
Expected: compiles (testing module only included in test builds)

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-api/src/testing.rs crates/rapidbyte-api/src/lib.rs
git commit -m "feat(api): add shared test infrastructure module"
```

---

## Task 5: Implement `RunManager`

**Files:**
- Create (content): `crates/rapidbyte-api/src/run_manager.rs`

The `RunManager` tracks in-flight and recently completed runs in memory.
It provides broadcast channels for SSE and cancellation tokens.

Reference: spec Section 7, engine's `ProgressEvent` at `crates/rapidbyte-engine/src/domain/progress.rs`.

- [ ] **Step 1: Write tests for `RunManager`**

Add tests at the bottom of `run_manager.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_run_and_get() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("test-pipeline".into());
        assert!(run_id.starts_with("run_"));

        let state = mgr.get(&run_id).unwrap();
        assert_eq!(state.pipeline, "test-pipeline");
        assert_eq!(state.status, RunStatus::Pending);
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let mgr = RunManager::new();
        assert!(mgr.get("run_nonexistent").is_none());
    }

    #[test]
    fn update_status() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("p".into());
        mgr.update_status(&run_id, RunStatus::Running);
        assert_eq!(mgr.get(&run_id).unwrap().status, RunStatus::Running);
    }

    #[test]
    fn cancel_sets_token() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("p".into());
        let token = mgr.cancel_token(&run_id).unwrap();
        assert!(!token.is_cancelled());
        mgr.cancel(&run_id);
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn subscribe_receives_events() {
        let mgr = RunManager::new();
        let run_id = mgr.create_run("p".into());
        let mut rx = mgr.subscribe(&run_id).unwrap();
        let event = SseEvent::Progress {
            phase: "source".into(),
            stream: "users".into(),
            records_read: Some(100),
            records_written: None,
            bytes_read: Some(4096),
        };
        mgr.send_event(&run_id, event.clone());
        let received = rx.recv().await.unwrap();
        assert!(matches!(received, SseEvent::Progress { .. }));
    }
}
```

- [ ] **Step 2: Implement `RunManager`**

Core struct with `DashMap<String, RunState>`:
- `create_run(pipeline: String) -> String` — generates ULID run_id, inserts pending state
- `get(run_id: &str) -> Option<RunSnapshot>` — returns a clone of run state
- `update_status(run_id: &str, status: RunStatus)` — updates status
- `complete(run_id: &str, result: PipelineResult)` — sets Completed + result
- `fail(run_id: &str, error: String)` — sets Failed + error
- `cancel(run_id: &str)` — triggers CancellationToken
- `cancel_token(run_id: &str) -> Option<CancellationToken>` — returns clone of token
- `subscribe(run_id: &str) -> Option<broadcast::Receiver<SseEvent>>` — SSE subscription
- `send_event(run_id: &str, event: SseEvent)` — broadcast to subscribers
- `list_runs() -> Vec<RunSnapshot>` — returns all runs

`RunSnapshot` is a read-only view (clone of RunState fields minus channels/tokens).

Eviction: `start_eviction_task(&self)` spawns a tokio task that removes entries
older than 1 hour every 5 minutes.

Reference: spec Section 7, `tokio::sync::broadcast`, `tokio_util::sync::CancellationToken`.

- [ ] **Step 3: Run tests**

Run: `cargo test -p rapidbyte-api -- run_manager`
Expected: all 5 tests pass

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-api/src/run_manager.rs
git commit -m "feat(api): implement RunManager with broadcast and cancel"
```

---

## Task 6: Implement `ApiContext` and `ServerService`

**Files:**
- Create (content): `crates/rapidbyte-api/src/context.rs`
- Create (content): `crates/rapidbyte-api/src/services/server.rs`
- Modify: `crates/rapidbyte-api/src/services/mod.rs`

Start with the simplest service (ServerService) to validate the full trait → impl → context wiring.

- [ ] **Step 1: Write test for `ServerService`**

In `services/server.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_returns_healthy() {
        let svc = LocalServerService::new(Instant::now());
        let health = svc.health().await.unwrap();
        assert_eq!(health.status, "healthy");
        assert_eq!(health.mode, "serve");
        assert!(health.uptime_secs >= 0);
    }

    #[tokio::test]
    async fn version_returns_cargo_version() {
        let svc = LocalServerService::new(Instant::now());
        let ver = svc.version().await.unwrap();
        assert_eq!(ver.version, env!("CARGO_PKG_VERSION"));
    }
}
```

- [ ] **Step 2: Implement `LocalServerService`**

Holds `started_at: Instant`. Methods return static info:
- `health()` — `HealthStatus { status: "healthy", mode: "serve", uptime_secs, .. }`
- `version()` — `VersionInfo { version: env!("CARGO_PKG_VERSION"), .. }`
- `config()` — `ServerConfig { mode: "serve", auth_required, port, .. }`

- [ ] **Step 3: Implement `ApiContext` with `DeploymentMode`**

`context.rs` defines:
- `DeploymentMode` enum (`Local`, `Distributed { controller_url }`)
- `ApiContext` struct with 6 `Arc<dyn Trait>` fields
- `ApiContext::from_project(project_dir, mode, secrets, registry_config)`:
  - Scans for `*.yaml`/`*.yml` pipeline files in `project_dir`
  - Parses each into `PipelineConfig`, builds `HashMap<String, PipelineConfig>` catalog
  - Creates `RunManager`
  - Wires all 6 local service impls
  - `Distributed` variant returns `anyhow::bail!("distributed mode not yet implemented")`

Reference: engine factory at `crates/rapidbyte-engine/src/adapter/engine_factory.rs:191`,
pipeline loading at `crates/rapidbyte-cli/src/commands/mod.rs:31`.

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-api`
Expected: server tests pass

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-api/src/context.rs crates/rapidbyte-api/src/services/
git commit -m "feat(api): implement ApiContext and ServerService"
```

---

## Task 7: Implement `PipelineService` (local mode)

**Files:**
- Create (content): `crates/rapidbyte-api/src/services/pipeline.rs`

The core service. `sync` spawns a pipeline run via `RunManager`.
`check` delegates to `check_pipeline()`. `list`/`get`/`compile` work from the pipeline catalog.

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn list_returns_catalog_pipelines() { .. }

    #[tokio::test]
    async fn get_returns_pipeline_detail() { .. }

    #[tokio::test]
    async fn get_nonexistent_returns_not_found() { .. }

    #[tokio::test]
    async fn sync_returns_run_handle() { .. }

    #[tokio::test]
    async fn sync_batch_returns_not_implemented() { .. }

    #[tokio::test]
    async fn check_delegates_to_engine() { .. }

    #[tokio::test]
    async fn compile_returns_resolved_config() { .. }
}
```

Tests use a test catalog with 1–2 pipeline configs built in-memory
(not from YAML files). Mock/fake the engine context for `check` and `sync`.

Reference: engine `check_pipeline` at `crates/rapidbyte-engine/src/application/check.rs:37`,
`run_pipeline` at `crates/rapidbyte-engine/src/application/run.rs:131`.

- [ ] **Step 2: Implement `LocalPipelineService`**

Struct holds:
- `catalog: Arc<HashMap<String, PipelineConfig>>` — parsed pipeline configs
- `run_manager: Arc<RunManager>` — for sync/cancel
- `registry_config: Arc<RegistryConfig>` — for building engine contexts
- `secrets: Arc<SecretProviders>` — for config resolution

Methods:
- `list` — iterate catalog, build `PipelineSummary` for each, apply tag filter
- `get` — lookup by name, return `PipelineDetail` or `NotFound`
- `sync` — create run in `RunManager`, spawn task that calls `build_run_context` then `run_pipeline`
- `check` — `build_lightweight_context` then `check_pipeline`, convert engine `CheckResult` to API `CheckResult`
- `compile` — lookup config, return as `ResolvedConfig`
- `sync_batch`, `check_apply`, `diff`, `assert`, `teardown` — return `ApiError::not_implemented(..)`

The `sync` spawn task:
1. Send `SseEvent::Started` via `run_manager.send_event()`
2. Update status to `Running`
3. Create a `tokio::sync::mpsc::unbounded_channel::<ProgressEvent>()` pair
4. Spawn a bridge task: receives `ProgressEvent` from the mpsc receiver,
   translates each to `SseEvent` (see spec Section 6 translation table),
   and calls `run_manager.send_event()` to broadcast to SSE subscribers
5. Build `EngineContext` via `build_run_context(&config, Some(mpsc_tx), &registry_config)`
   — the engine's `ChannelProgressReporter` sends events to the mpsc sender
6. Call `run_pipeline(&ctx, &config, cancel_token)`
7. On success: `run_manager.complete()`, send `SseEvent::Complete`
8. On failure: `run_manager.fail()`, send `SseEvent::Failed`

Note: `build_run_context` accepts `Option<tokio_mpsc::UnboundedSender<ProgressEvent>>`
as its progress channel. The bridge task converts these engine-internal events to
the API-layer `SseEvent` type before broadcasting.

- [ ] **Step 3: Run tests**

Run: `cargo test -p rapidbyte-api -- services::pipeline`
Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-api/src/services/pipeline.rs
git commit -m "feat(api): implement PipelineService with sync, check, list"
```

---

## Task 8: Implement `RunService` (local mode)

**Files:**
- Create (content): `crates/rapidbyte-api/src/services/run.rs`

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn get_active_run() { .. }

    #[tokio::test]
    async fn get_nonexistent_returns_not_found() { .. }

    #[tokio::test]
    async fn cancel_active_run() { .. }

    #[tokio::test]
    async fn cancel_completed_run_returns_conflict() { .. }

    #[tokio::test]
    async fn events_returns_stream() { .. }

    #[tokio::test]
    async fn get_batch_returns_not_implemented() { .. }
}
```

- [ ] **Step 2: Implement `LocalRunService`**

Holds `run_manager: Arc<RunManager>`.

- `get` — `run_manager.get(run_id)`, convert `RunSnapshot` → `RunDetail`, or `NotFound`
- `list` — `run_manager.list_runs()`, apply filter, paginate, return `PaginatedList<RunSummary>`
- `events` — `run_manager.subscribe(run_id)`, wrap receiver in `EventStream<SseEvent>`, or `NotFound`
- `cancel` — check status (if terminal → `Conflict`), else `run_manager.cancel(run_id)`
- `get_batch`, `batch_events` — return `ApiError::not_implemented(..)`

For `events`: if run is already terminal, return a stream that emits the final event and closes.

- [ ] **Step 3: Run tests**

Run: `cargo test -p rapidbyte-api -- services::run`
Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-api/src/services/run.rs
git commit -m "feat(api): implement RunService with get, list, events, cancel"
```

---

## Task 9: Implement `ConnectionService` and `PluginService`

**Files:**
- Create (content): `crates/rapidbyte-api/src/services/connection.rs`
- Create (content): `crates/rapidbyte-api/src/services/plugin.rs`

- [ ] **Step 1: Write tests for ConnectionService**

```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn list_returns_connections_from_catalog() { .. }

    #[tokio::test]
    async fn get_redacts_sensitive_fields() { .. }

    #[tokio::test]
    async fn get_nonexistent_returns_not_found() { .. }
}
```

- [ ] **Step 2: Implement `LocalConnectionService`**

Holds:
- `catalog: Arc<HashMap<String, PipelineConfig>>` — extracts connection info from source/dest configs
- `registry_config: Arc<RegistryConfig>` — for discover context

Methods:
- `list` — scan catalog for unique connection names from source/dest configs
- `get` — find connection config, redact sensitive fields (password, secret, token, api_key), return `ConnectionDetail`
- `test` — `build_discover_context` → `validate_plugin` with connection config, return result
- `discover` — `build_discover_context` → `discover_plugin`, convert to `DiscoverResult`

Sensitive field redaction: iterate config JSON, replace values for keys matching
`password|secret|token|api_key|private_key` with `"***REDACTED***"`.

- [ ] **Step 3: Write tests for PluginService**

```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn list_returns_empty_when_no_plugins() { .. }

    #[tokio::test]
    async fn search_returns_not_implemented() { .. }

    #[tokio::test]
    async fn install_returns_not_implemented() { .. }

    #[tokio::test]
    async fn remove_returns_not_implemented() { .. }
}
```

- [ ] **Step 4: Implement `LocalPluginService`**

Holds `registry_config: Arc<RegistryConfig>`.

- `list` — scan local plugin cache directory, return summaries
- `info` — resolve plugin ref, return manifest details
- `search`, `install`, `remove` — return `ApiError::not_implemented(..)`

Reference: registry crate at `crates/rapidbyte-registry/src/lib.rs`.

- [ ] **Step 5: Run tests**

Run: `cargo test -p rapidbyte-api -- services::connection services::plugin`
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-api/src/services/connection.rs crates/rapidbyte-api/src/services/plugin.rs
git commit -m "feat(api): implement ConnectionService and PluginService"
```

---

## Task 10: Implement `OperationsService`

**Files:**
- Create (content): `crates/rapidbyte-api/src/services/operations.rs`

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn status_returns_all_pipelines() { .. }

    #[tokio::test]
    async fn pipeline_status_returns_detail() { .. }

    #[tokio::test]
    async fn pipeline_status_not_found() { .. }

    #[tokio::test]
    async fn pause_returns_not_implemented() { .. }

    #[tokio::test]
    async fn resume_returns_not_implemented() { .. }

    #[tokio::test]
    async fn reset_returns_not_implemented() { .. }

    #[tokio::test]
    async fn freshness_returns_not_implemented() { .. }

    #[tokio::test]
    async fn logs_returns_not_implemented() { .. }
}
```

- [ ] **Step 2: Implement `LocalOperationsService`**

Holds:
- `catalog: Arc<HashMap<String, PipelineConfig>>`
- `run_manager: Arc<RunManager>`

Methods:
- `status` — iterate catalog, for each pipeline build `PipelineStatus` from config + last run from `RunManager`
- `pipeline_status` — lookup pipeline, get recent runs, stream cursor state
- `pause`, `resume`, `reset`, `freshness`, `logs`, `logs_stream` — return `ApiError::not_implemented(..)`

- [ ] **Step 3: Run tests**

Run: `cargo test -p rapidbyte-api -- services::operations`
Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-api/src/services/operations.rs
git commit -m "feat(api): implement OperationsService with status, stubs for rest"
```

---

## Task 11: Wire `ApiContext::from_project` and `testing.rs` end-to-end

**Files:**
- Modify: `crates/rapidbyte-api/src/context.rs`
- Modify: `crates/rapidbyte-api/src/services/mod.rs`
- Modify: `crates/rapidbyte-api/src/testing.rs`

Now that all 6 services are implemented, wire them into `ApiContext::from_project`
and complete the `testing.rs` module (replace the `todo!()` from Task 4).

- [ ] **Step 1: Write integration test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn from_project_local_mode_with_no_pipelines() {
        let dir = TempDir::new().unwrap();
        let ctx = ApiContext::from_project(
            dir.path(),
            DeploymentMode::Local,
            /* secrets, registry_config */
        ).await.unwrap();

        let health = ctx.server.health().await.unwrap();
        assert_eq!(health.status, "healthy");

        let pipelines = ctx.pipelines.list(PipelineFilter::default()).await.unwrap();
        assert!(pipelines.items.is_empty());
    }

    #[tokio::test]
    async fn distributed_mode_returns_error() {
        let dir = TempDir::new().unwrap();
        let result = ApiContext::from_project(
            dir.path(),
            DeploymentMode::Distributed { controller_url: "http://localhost:9090".into() },
            /* ... */
        ).await;
        assert!(result.is_err());
    }
}
```

- [ ] **Step 2: Implement the builder**

`from_project`:
1. Glob `project_dir/**/*.yaml` and `**/*.yml`
2. Parse each with `rapidbyte_pipeline_config::parse()` (skip files that fail to parse, log warning)
3. Build catalog: `HashMap<String, PipelineConfig>` keyed by `config.pipeline`
4. Create `RunManager::new()` + start eviction task
5. Create `Arc`-wrapped service impls, passing shared catalog, run_manager, registry_config, secrets
6. Return `ApiContext { pipelines, runs, connections, plugins, operations, server }`

- [ ] **Step 3: Run tests**

Run: `cargo test -p rapidbyte-api -- context`
Expected: both tests pass

- [ ] **Step 4: Run full crate tests**

Run: `cargo test -p rapidbyte-api`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-api/src/context.rs crates/rapidbyte-api/src/services/mod.rs
git commit -m "feat(api): wire ApiContext::from_project with all 6 services"
```

---

## Task 12: Add axum deps to CLI and scaffold serve command

**Files:**
- Modify: `crates/rapidbyte-cli/Cargo.toml`
- Modify: `crates/rapidbyte-cli/src/main.rs`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`
- Create: `crates/rapidbyte-cli/src/commands/serve/mod.rs`
- Create: `crates/rapidbyte-cli/src/commands/serve/error.rs`
- Create: `crates/rapidbyte-cli/src/commands/serve/extract.rs`
- Create: `crates/rapidbyte-cli/src/commands/serve/sse.rs`
- Create: `crates/rapidbyte-cli/src/commands/serve/middleware/mod.rs`
- Create: `crates/rapidbyte-cli/src/commands/serve/middleware/auth.rs`
- Create: `crates/rapidbyte-cli/src/commands/serve/routes/mod.rs`

- [ ] **Step 1: Add deps to CLI Cargo.toml**

Add to `[dependencies]`:

```toml
rapidbyte-api = { path = "../rapidbyte-api" }
axum = { workspace = true }
axum-extra = { workspace = true }
tower-http = { workspace = true }
async-stream = { workspace = true }
```

- [ ] **Step 2: Add `Serve` command variant to `main.rs`**

Add to the `Commands` enum (after `Dev`, before `Controller`):

```rust
/// Start the REST API server
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
},
```

Add dispatch in the `match` block:

```rust
Commands::Serve { listen, allow_unauthenticated, auth_token } => {
    commands::serve::execute(
        &listen,
        allow_unauthenticated,
        auth_token.as_deref(),
        &registry_config,
        &secrets,
    ).await?;
}
```

Reference: CLI main.rs commands enum at `crates/rapidbyte-cli/src/main.rs:113`.

- [ ] **Step 3: Add `pub mod serve;` to commands/mod.rs**

- [ ] **Step 4: Create stub serve module and all sub-files**

`serve/mod.rs` — define `execute()` that prints "serve not yet implemented" and returns Ok.
`serve/error.rs` — empty.
`serve/extract.rs` — empty.
`serve/sse.rs` — empty.
`serve/middleware/mod.rs` — `pub mod auth;`
`serve/middleware/auth.rs` — empty.
`serve/routes/mod.rs` — empty with all 6 sub-module declarations.
`serve/routes/pipelines.rs` through `server.rs` — all empty.

- [ ] **Step 5: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: compiles

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-cli/
git commit -m "feat(cli): scaffold serve command with axum deps"
```

---

## Task 13: Implement axum error mapping and extractors

**Files:**
- Create (content): `crates/rapidbyte-cli/src/commands/serve/error.rs`
- Create (content): `crates/rapidbyte-cli/src/commands/serve/extract.rs`

- [ ] **Step 1: Write test for error → response mapping**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    #[test]
    fn not_found_returns_404() {
        let err = ApiError::not_found("pipeline_not_found", "Pipeline 'foo' not found");
        let resp = ApiErrorResponse(err).into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn not_implemented_returns_501() {
        let err = ApiError::not_implemented("batch sync");
        let resp = ApiErrorResponse(err).into_response();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[test]
    fn unauthorized_returns_401() {
        let err = ApiError::Unauthorized { message: "bad token".into() };
        let resp = ApiErrorResponse(err).into_response();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
```

- [ ] **Step 2: Implement `ApiErrorResponse`**

Wrapper struct `ApiErrorResponse(pub ApiError)` that implements `IntoResponse`.
Maps to JSON body `{ "error": { "code": "..", "message": ".." } }` with correct HTTP status.

```rust
pub struct ApiErrorResponse(pub ApiError);

impl IntoResponse for ApiErrorResponse {
    fn into_response(self) -> Response {
        let (status, code, message, details) = match &self.0 {
            ApiError::NotFound { code, message } => (StatusCode::NOT_FOUND, code.clone(), message.clone(), None),
            ApiError::Conflict { code, message } => (StatusCode::CONFLICT, code.clone(), message.clone(), None),
            ApiError::ValidationFailed { message, details } => (StatusCode::BAD_REQUEST, "validation_failed".into(), message.clone(), Some(details.clone())),
            ApiError::NotImplemented { message } => (StatusCode::NOT_IMPLEMENTED, "not_implemented".into(), message.clone(), None),
            ApiError::Unauthorized { message } => (StatusCode::UNAUTHORIZED, "unauthorized".into(), message.clone(), None),
            ApiError::Internal { message } => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error".into(), message.clone(), None),
        };
        // Build JSON envelope and return (status, Json(body))
    }
}

impl From<ApiError> for ApiErrorResponse {
    fn from(err: ApiError) -> Self { Self(err) }
}
```

- [ ] **Step 3: Implement pagination extractor**

`extract.rs`:

```rust
#[derive(Debug, Deserialize)]
pub struct PaginationParams {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl PaginationParams {
    pub fn limit(&self) -> u32 {
        self.limit.unwrap_or(20).min(100)
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-cli -- serve::error`
Expected: 3 tests pass

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/serve/error.rs crates/rapidbyte-cli/src/commands/serve/extract.rs
git commit -m "feat(cli): implement API error response mapping and extractors"
```

---

## Task 14: Implement auth middleware

**Files:**
- Create (content): `crates/rapidbyte-cli/src/commands/serve/middleware/auth.rs`
- Modify: `crates/rapidbyte-cli/src/commands/serve/middleware/mod.rs`

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, routing::get, body::Body};
    use http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn handler() -> &'static str { "ok" }

    #[tokio::test]
    async fn allows_request_with_valid_token() {
        let config = AuthConfig { required: true, tokens: vec!["test-token".into()] };
        let app = Router::new()
            .route("/test", get(handler))
            .layer(auth_layer(config));
        let req = Request::builder()
            .uri("/test")
            .header("Authorization", "Bearer test-token")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rejects_request_without_token() {
        let config = AuthConfig { required: true, tokens: vec!["test-token".into()] };
        let app = Router::new()
            .route("/test", get(handler))
            .layer(auth_layer(config));
        let req = Request::builder()
            .uri("/test")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn allows_unauthenticated_when_not_required() {
        let config = AuthConfig { required: false, tokens: vec![] };
        let app = Router::new()
            .route("/test", get(handler))
            .layer(auth_layer(config));
        let req = Request::builder()
            .uri("/test")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_endpoint_skips_auth() {
        let config = AuthConfig { required: true, tokens: vec!["secret".into()] };
        let app = Router::new()
            .route("/api/v1/server/health", get(handler))
            .layer(auth_layer(config));
        let req = Request::builder()
            .uri("/api/v1/server/health")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
```

- [ ] **Step 2: Implement auth middleware**

`AuthConfig { required: bool, tokens: Vec<String> }` + `auth_layer(config) -> impl Layer`.

Use a `tower::Layer` / `tower::Service` that:
1. If path is `/api/v1/server/health` → pass through
2. If `!config.required` → pass through
3. Extract `Authorization` header, parse `Bearer <token>`
4. Check `config.tokens.contains(&token)`
5. If invalid → return 401 JSON response
6. If valid → call inner service

- [ ] **Step 3: Run tests**

Run: `cargo test -p rapidbyte-cli -- serve::middleware::auth`
Expected: 4 tests pass

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/serve/middleware/
git commit -m "feat(cli): implement bearer token auth middleware"
```

---

## Task 15: Implement SSE bridge

**Files:**
- Create (content): `crates/rapidbyte-cli/src/commands/serve/sse.rs`

- [ ] **Step 1: Implement SSE bridge**

Converts a `broadcast::Receiver<SseEvent>` into an `Sse<impl Stream<Item = Result<Event, Infallible>>>`.

```rust
use axum::response::sse::{Event, Sse};
use futures::stream::Stream;
use rapidbyte_api::types::SseEvent;
use std::convert::Infallible;
use tokio::sync::broadcast;

pub fn sse_stream(
    mut rx: broadcast::Receiver<SseEvent>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let (event_type, data) = event.to_sse_parts();
                    yield Ok(Event::default().event(event_type).data(data));
                    if event.is_terminal() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "SSE subscriber lagged");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream)
}
```

Add `to_sse_parts(&self) -> (&str, String)` and `is_terminal(&self) -> bool` methods to `SseEvent` in `rapidbyte-api/src/types.rs`.

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: compiles

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/serve/sse.rs crates/rapidbyte-api/src/types.rs
git commit -m "feat(cli): implement SSE bridge from broadcast to axum Sse"
```

---

## Task 16: Implement server and pipeline route handlers

**Files:**
- Create (content): `crates/rapidbyte-cli/src/commands/serve/routes/server.rs`
- Create (content): `crates/rapidbyte-cli/src/commands/serve/routes/pipelines.rs`
- Modify: `crates/rapidbyte-cli/src/commands/serve/routes/mod.rs`

- [ ] **Step 1: Implement server routes**

3 handlers, all straightforward:

```rust
pub async fn health(State(ctx): State<Arc<ApiContext>>) -> Result<Json<HealthStatus>, ApiErrorResponse> { .. }
pub async fn version(State(ctx): State<Arc<ApiContext>>) -> Result<Json<VersionInfo>, ApiErrorResponse> { .. }
pub async fn config(State(ctx): State<Arc<ApiContext>>) -> Result<Json<ServerConfig>, ApiErrorResponse> { .. }
```

- [ ] **Step 2: Implement pipeline routes**

Handlers:

```rust
pub async fn list(State(ctx), Query(params)) -> Result<Json<PaginatedList<PipelineSummary>>, ApiErrorResponse> { .. }
pub async fn get(State(ctx), Path(name)) -> Result<Json<PipelineDetail>, ApiErrorResponse> { .. }
pub async fn sync(State(ctx), Path(name), Json(body)) -> Result<(StatusCode, Json<RunHandle>), ApiErrorResponse> {
    // IMPORTANT: Return StatusCode::ACCEPTED (202), not 200
    let handle = ctx.pipelines.sync(request).await.map_err(ApiErrorResponse::from)?;
    Ok((StatusCode::ACCEPTED, Json(handle)))
}
pub async fn check(State(ctx), Path(name)) -> Result<Json<CheckResult>, ApiErrorResponse> { .. }
pub async fn compile(State(ctx), Path(name)) -> Result<Json<ResolvedConfig>, ApiErrorResponse> { .. }

// Stubs:
pub async fn sync_batch(..) -> Result<.., ApiErrorResponse> { Err(ApiError::not_implemented("batch sync").into()) }
pub async fn check_apply(..) -> Result<.., ApiErrorResponse> { Err(ApiError::not_implemented("check and apply").into()) }
pub async fn diff(..) -> Result<.., ApiErrorResponse> { Err(ApiError::not_implemented("diff").into()) }
pub async fn assert_one(..) -> Result<.., ApiErrorResponse> { Err(ApiError::not_implemented("assert").into()) }
pub async fn assert_all(..) -> Result<.., ApiErrorResponse> { Err(ApiError::not_implemented("assert all").into()) }
pub async fn teardown(..) -> Result<.., ApiErrorResponse> { Err(ApiError::not_implemented("teardown").into()) }
```

- [ ] **Step 3: Update routes/mod.rs**

```rust
pub mod server;
pub mod pipelines;
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: compiles

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/serve/routes/
git commit -m "feat(cli): implement server and pipeline route handlers"
```

---

## Task 17: Implement remaining route handlers

**Files:**
- Create (content): `crates/rapidbyte-cli/src/commands/serve/routes/runs.rs`
- Create (content): `crates/rapidbyte-cli/src/commands/serve/routes/connections.rs`
- Create (content): `crates/rapidbyte-cli/src/commands/serve/routes/plugins.rs`
- Create (content): `crates/rapidbyte-cli/src/commands/serve/routes/operations.rs`
- Modify: `crates/rapidbyte-cli/src/commands/serve/routes/mod.rs`

- [ ] **Step 1: Implement run route handlers**

```rust
pub async fn get(State(ctx), Path(id)) -> Result<Json<RunDetail>, ApiErrorResponse> { .. }
pub async fn list(State(ctx), Query(params)) -> Result<Json<PaginatedList<RunSummary>>, ApiErrorResponse> { .. }
pub async fn events(State(ctx), Path(id)) -> Result<Sse<impl Stream>, ApiErrorResponse> {
    let stream = ctx.runs.events(&id).await.map_err(ApiErrorResponse::from)?;
    // Use sse_stream() bridge
}
pub async fn cancel(State(ctx), Path(id)) -> Result<Json<RunDetail>, ApiErrorResponse> { .. }
pub async fn get_batch(..) -> Result<.., ApiErrorResponse> { Err(not_implemented) }
pub async fn batch_events(..) -> Result<.., ApiErrorResponse> { Err(not_implemented) }
```

- [ ] **Step 2: Implement connection route handlers**

```rust
pub async fn list(State(ctx)) -> Result<Json<..>, ApiErrorResponse> { .. }
pub async fn get(State(ctx), Path(name)) -> Result<Json<..>, ApiErrorResponse> { .. }
pub async fn test(State(ctx), Path(name)) -> Result<Json<..>, ApiErrorResponse> { .. }
pub async fn discover(State(ctx), Path(name), Query(params)) -> Result<Json<..>, ApiErrorResponse> { .. }
```

- [ ] **Step 3: Implement plugin route handlers**

```rust
pub async fn list(State(ctx)) -> Result<Json<..>, ApiErrorResponse> { .. }
pub async fn info(State(ctx), Path(plugin_ref)) -> Result<Json<..>, ApiErrorResponse> { .. }
// search, install, remove — all stub with 501
```

- [ ] **Step 4: Implement operations route handlers**

```rust
pub async fn status(State(ctx)) -> Result<Json<..>, ApiErrorResponse> { .. }
pub async fn status_detail(State(ctx), Path(name)) -> Result<Json<..>, ApiErrorResponse> { .. }
// pause, resume, reset, freshness, logs, logs_stream — all stub with 501
```

- [ ] **Step 5: Update routes/mod.rs with all modules**

```rust
pub mod server;
pub mod pipelines;
pub mod runs;
pub mod connections;
pub mod plugins;
pub mod operations;
```

- [ ] **Step 6: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: compiles

- [ ] **Step 7: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/serve/routes/
git commit -m "feat(cli): implement run, connection, plugin, operations route handlers"
```

---

## Task 18: Wire router and serve command

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/serve/mod.rs`

- [ ] **Step 1: Implement `build_router`**

```rust
use axum::{Router, routing::{get, post, delete}};
use tower_http::cors::CorsLayer;

pub fn build_router(ctx: ApiContext, auth_config: AuthConfig) -> Router {
    let cors = if !auth_config.required {
        CorsLayer::permissive()
    } else {
        CorsLayer::new() // restrictive default
    };

    let api = Router::new()
        // Pipelines
        .route("/pipelines", get(routes::pipelines::list))
        .route("/pipelines/sync", post(routes::pipelines::sync_batch))
        .route("/pipelines/assert", post(routes::pipelines::assert_all))
        .route("/pipelines/{name}", get(routes::pipelines::get))
        .route("/pipelines/{name}/sync", post(routes::pipelines::sync))
        .route("/pipelines/{name}/check", post(routes::pipelines::check))
        .route("/pipelines/{name}/check-apply", post(routes::pipelines::check_apply))
        .route("/pipelines/{name}/compiled", get(routes::pipelines::compile))
        .route("/pipelines/{name}/diff", get(routes::pipelines::diff))
        .route("/pipelines/{name}/assert", post(routes::pipelines::assert_one))
        .route("/pipelines/{name}/teardown", post(routes::pipelines::teardown))
        .route("/pipelines/{name}/pause", post(routes::operations::pause))
        .route("/pipelines/{name}/resume", post(routes::operations::resume))
        .route("/pipelines/{name}/reset", post(routes::operations::reset))
        // Runs
        .route("/runs", get(routes::runs::list))
        .route("/runs/{id}", get(routes::runs::get))
        .route("/runs/{id}/events", get(routes::runs::events))
        .route("/runs/{id}/cancel", post(routes::runs::cancel))
        // Batches
        .route("/batches/{id}", get(routes::runs::get_batch))
        .route("/batches/{id}/events", get(routes::runs::batch_events))
        // Connections
        .route("/connections", get(routes::connections::list))
        .route("/connections/{name}", get(routes::connections::get))
        .route("/connections/{name}/test", post(routes::connections::test))
        .route("/connections/{name}/discover", get(routes::connections::discover))
        // Plugins
        .route("/plugins", get(routes::plugins::list))
        .route("/plugins/search", get(routes::plugins::search))
        .route("/plugins/install", post(routes::plugins::install))
        .route("/plugins/{org}/{name}", get(routes::plugins::info))
        .route("/plugins/{org}/{name}", delete(routes::plugins::remove))
        .route("/plugins/{org}/{name}:{version}", get(routes::plugins::info))
        .route("/plugins/{org}/{name}:{version}", delete(routes::plugins::remove))
        // Operations
        .route("/status", get(routes::operations::status))
        .route("/status/{name}", get(routes::operations::status_detail))
        .route("/freshness", get(routes::operations::freshness))
        .route("/logs", get(routes::operations::logs))
        .route("/logs/stream", get(routes::operations::logs_stream))
        // Server
        .route("/server/health", get(routes::server::health))
        .route("/server/version", get(routes::server::version))
        .route("/server/config", get(routes::server::config))
        .layer(middleware::auth::auth_layer(auth_config))
        .layer(cors)
        .with_state(Arc::new(ctx));

    Router::new().nest("/api/v1", api)
}
```

Plugin ref format is `{org}/{name}` or `{org}/{name}:{version}`.
Routes use explicit path segments rather than wildcards for clarity.
If axum 0.8 has issues with the colon in `{org}/{name}:{version}`,
fall back to a catch-all `/*plugin_ref` with manual parsing at
implementation time.

- [ ] **Step 2: Implement `execute` function**

```rust
pub async fn execute(
    listen: &str,
    allow_unauthenticated: bool,
    auth_token: Option<&str>,
    registry_config: &RegistryConfig,
    secrets: &SecretProviders,
) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let ctx = ApiContext::from_project(&cwd, DeploymentMode::Local, secrets, registry_config).await?;

    let tokens = auth_token
        .map(|t| t.split(',').map(str::trim).map(String::from).collect())
        .unwrap_or_default();

    let auth_config = AuthConfig {
        required: !allow_unauthenticated,
        tokens,
    };

    let app = build_router(ctx, auth_config);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("Listening on http://{listen}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
    tracing::info!("Shutting down...");
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p rapidbyte-cli`
Expected: compiles

- [ ] **Step 4: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/serve/mod.rs
git commit -m "feat(cli): wire build_router and serve command execute"
```

---

## Task 19: HTTP integration tests

**Files:**
- Create: `crates/rapidbyte-cli/tests/serve_integration.rs` (or inline in serve module)

- [ ] **Step 1: Write integration tests**

Tests that spin up the server on a random port with a test `ApiContext`:

```rust
use axum::body::Body;
use http::{Request, StatusCode};
use tower::ServiceExt;

fn test_app() -> Router {
    // Build ApiContext with empty catalog, no auth required
    let ctx = /* test context with empty catalog */;
    build_router(ctx, AuthConfig { required: false, tokens: vec![] })
}

#[tokio::test]
async fn health_returns_200() {
    let app = test_app();
    let req = Request::get("/api/v1/server/health").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn version_returns_200() {
    let app = test_app();
    let req = Request::get("/api/v1/server/version").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn list_pipelines_returns_empty() {
    let app = test_app();
    let req = Request::get("/api/v1/pipelines").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn get_nonexistent_pipeline_returns_404() {
    let app = test_app();
    let req = Request::get("/api/v1/pipelines/nonexistent").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn sync_batch_returns_501() {
    let app = test_app();
    let req = Request::post("/api/v1/pipelines/sync")
        .header("content-type", "application/json")
        .body(Body::from("{}")).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn auth_required_rejects_without_token() {
    let ctx = /* test context */;
    let app = build_router(ctx, AuthConfig { required: true, tokens: vec!["secret".into()] });
    let req = Request::get("/api/v1/pipelines").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn auth_required_allows_health_without_token() {
    let ctx = /* test context */;
    let app = build_router(ctx, AuthConfig { required: true, tokens: vec!["secret".into()] });
    let req = Request::get("/api/v1/server/health").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn error_response_has_correct_json_format() {
    let app = test_app();
    let req = Request::get("/api/v1/pipelines/nonexistent").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body: serde_json::Value = /* read body */;
    assert!(body["error"]["code"].is_string());
    assert!(body["error"]["message"].is_string());
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p rapidbyte-cli -- serve`
Expected: all tests pass

- [ ] **Step 3: Commit**

```bash
git add crates/rapidbyte-cli/tests/ crates/rapidbyte-cli/src/commands/serve/
git commit -m "test(cli): add HTTP integration tests for serve command"
```

---

## Task 20: Final verification

**Files:** None (verification only)

- [ ] **Step 1: Run full workspace tests**

Run: `cargo test --workspace`
Expected: all existing tests still pass, all new tests pass

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: no errors (warnings allowed on new code, fix if any)

- [ ] **Step 3: Run fmt**

Run: `cargo fmt --check`
Expected: no formatting issues

- [ ] **Step 4: Verify the binary runs**

Run: `cargo run -- serve --help`
Expected: shows serve command help with `--listen`, `--allow-unauthenticated`, `--auth-token` flags

- [ ] **Step 5: Quick smoke test**

Run: `cargo run -- serve --allow-unauthenticated &`
Then: `curl -s http://localhost:8080/api/v1/server/health | jq .`
Expected: `{ "status": "healthy", "mode": "serve", ... }`

Kill the background process.

- [ ] **Step 6: Final commit if any fixups**

Stage only the specific files that were fixed, then commit:

```bash
git commit -am "fix: address clippy/fmt issues from API phase 1"
```
