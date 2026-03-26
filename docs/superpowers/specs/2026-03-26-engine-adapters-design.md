# Engine Adapters + PipelineService Completion — Design Spec

**Date:** 2026-03-26
**Status:** Approved
**Scope:** Make the REST API fully functional — zero 501s. Engine adapters, embedded local agent, pipeline discovery, PipelineService/ConnectionService completion.
**Prerequisite:** REST API implementation (merged on `main`)
**Follow-up:** Spec B — CLI-over-REST (CLI command rewrites to use REST client)

---

## 1. Summary

The REST API has 6 service domains but most `PipelineService` and `ConnectionService` methods return 501 because they need engine access. This spec adds:

1. **Embedded local agent** — runs in-process alongside the controller, executes pipeline tasks using the engine
2. **Pipeline filesystem discovery** — `PipelineSource` driven port for listing/reading pipeline YAML files
3. **Engine-backed driven port adapters** — `EngineConnectionTester`, `EnginePluginRegistry`, `EnginePipelineInspector` in the CLI crate
4. **PipelineService completion** — all 10 methods return real results
5. **ConnectionService completion** — test/discover return real results
6. **Task model extension** — `TaskOperation` enum for sync/check/teardown/assert

### Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Pipeline execution model | Embedded local agent | Reuses agent poll/complete flow, exercises same code path as distributed |
| Engine context ownership | CLI builds `EngineContext`, passes to controller | CLI already knows how to build engine contexts via factory functions |
| Pipeline discovery | Filesystem + inline YAML | Matches CLI.md: `sync pipeline.yml` (file) and `sync --pipeline name` (lookup) |
| State backend | Postgres always | Project convention, no SQLite |
| Sync operations | `check()`, `compile()`, `diff()` | Fast, no task needed |
| Async operations | `sync()`, `check_apply()`, `teardown()`, `assert()` | Long-running, task-based via agent |
| Build order | Top-down vertical slices | Working sync e2e first, proves architecture |

---

## 2. Architecture

### Embedded Agent Model

When the controller starts in embedded mode, a local agent runs in-process:

```
┌─────────────────────────────────────────────────────────┐
│                    rapidbyte serve                       │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐ │
│  │ REST API     │  │ gRPC API     │  │ Embedded      │ │
│  │ (axum)       │  │ (tonic)      │  │ Agent         │ │
│  └──────┬───────┘  └──────┬───────┘  └───────┬───────┘ │
│         │                 │                   │         │
│  ┌──────┴─────────────────┴───────────────────┴───────┐ │
│  │              AppContext (driven ports)              │ │
│  │  RunRepository · TaskRepository · EventBus         │ │
│  │  PipelineStore · PipelineSource · PipelineInspector│ │
│  │  ConnectionTester · PluginRegistry · CursorStore   │ │
│  └────────────────────────────────────────────────────┘ │
│                          │                              │
│  ┌───────────────────────┴────────────────────────────┐ │
│  │              EngineContext (from CLI)               │ │
│  │  PluginRunner · PluginResolver · StateBackend      │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

The embedded agent loop:

1. Register with controller via `register()` application function
2. Loop: `poll_task()` → if task → execute based on `TaskOperation` → `complete_task()`
3. Progress events published to `EventBus` during execution
4. Heartbeats sent on interval to prevent lease expiry

### Composition Root

The CLI builds everything and passes it to the controller:

```rust
pub struct ServeContext {
    pub otel_guard: Arc<OtelGuard>,
    pub secrets: SecretProviders,
    pub connection_tester: Arc<dyn ConnectionTester>,
    pub plugin_registry: Arc<dyn PluginRegistry>,
    pub pipeline_source: Arc<dyn PipelineSource>,
    pub pipeline_inspector: Arc<dyn PipelineInspector>,
    pub engine_ctx: Arc<EngineContext>,
}
```

`serve()` signature becomes:

```rust
pub async fn serve(config: ControllerConfig, ctx: ServeContext) -> Result<()>
```

The function:
1. Runs existing setup (pool, migrations, adapters)
2. Builds `AppContext` with engine-backed driven ports from `ServeContext`
3. Spawns embedded agent (passing `engine_ctx`)
4. Starts gRPC + REST servers

---

## 3. New Driven Ports

### PipelineSource

Filesystem-based pipeline discovery:

```rust
// domain/ports/pipeline_source.rs

#[derive(Debug, Clone)]
pub struct PipelineInfo {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineSourceError {
    #[error("pipeline not found: {0}")]
    NotFound(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("invalid pipeline YAML: {0}")]
    InvalidYaml(String),
}

#[async_trait]
pub trait PipelineSource: Send + Sync {
    async fn list(&self) -> Result<Vec<PipelineInfo>, PipelineSourceError>;
    async fn get(&self, name: &str) -> Result<String, PipelineSourceError>;
}
```

Adapter: `cli/src/engine_adapters/pipeline_source.rs` — `FsPipelineSource` scans a project directory for `*.yml`/`*.yaml` files, extracts pipeline names via `extract_pipeline_name()`.

### PipelineInspector

Synchronous engine operations (check, diff):

```rust
// domain/ports/pipeline_inspector.rs

#[derive(Debug, thiserror::Error)]
pub enum InspectorError {
    #[error("pipeline error: {0}")]
    Pipeline(String),
    #[error("plugin error: {0}")]
    Plugin(String),
}

#[async_trait]
pub trait PipelineInspector: Send + Sync {
    async fn check(&self, pipeline_yaml: &str, secrets: &str) -> Result<CheckOutput, InspectorError>;
    async fn diff(&self, pipeline_yaml: &str) -> Result<DiffOutput, InspectorError>;
}
```

`CheckOutput` and `DiffOutput` are engine-domain types mapped to the REST response types in the service layer.

Adapter: `cli/src/engine_adapters/pipeline_inspector.rs` — wraps `check_pipeline()` and schema comparison.

---

## 4. Task Model Extension

### TaskOperation enum

```rust
// domain/task.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskOperation {
    Sync,
    CheckApply,
    Teardown,
    Assert,
}

impl TaskOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::CheckApply => "check_apply",
            Self::Teardown => "teardown",
            Self::Assert => "assert",
        }
    }
}

impl FromStr for TaskOperation {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "sync" => Ok(Self::Sync),
            "check_apply" => Ok(Self::CheckApply),
            "teardown" => Ok(Self::Teardown),
            "assert" => Ok(Self::Assert),
            _ => Err(()),
        }
    }
}
```

### Task entity changes

`Task` gains an `operation: TaskOperation` field. Default is `Sync` for backwards compatibility.

### Migration

```sql
ALTER TABLE tasks ADD COLUMN operation TEXT NOT NULL DEFAULT 'sync';
```

### submit_pipeline extension

`submit_pipeline()` gains an `operation: TaskOperation` parameter:

```rust
pub async fn submit_pipeline(
    ctx: &AppContext,
    idempotency_key: Option<String>,
    pipeline_yaml: String,
    max_retries: u32,
    timeout_seconds: Option<u64>,
    operation: TaskOperation,  // NEW
) -> Result<SubmitResult, AppError>
```

The `PipelineService` methods call this with the appropriate operation:
- `sync()` → `TaskOperation::Sync`
- `check_apply()` → `TaskOperation::CheckApply`
- `teardown()` → `TaskOperation::Teardown`
- `assert()` → `TaskOperation::Assert`

---

## 5. Embedded Agent

### Module: `application/embedded_agent.rs`

```rust
pub struct EmbeddedAgentConfig {
    pub poll_interval: Duration,
    pub max_concurrent_tasks: u32,
}

impl Default for EmbeddedAgentConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            max_concurrent_tasks: 4,
        }
    }
}

pub async fn run_embedded_agent(
    app_ctx: Arc<AppContext>,
    engine_ctx: Arc<EngineContext>,
    config: EmbeddedAgentConfig,
) -> Result<()> {
    // 1. Register agent
    let agent_id = register::register(
        &app_ctx,
        AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: config.max_concurrent_tasks,
        },
    ).await?;

    // 2. Poll-execute loop
    loop {
        match poll::poll_task(&app_ctx, &agent_id).await? {
            Some(assignment) => {
                let engine = engine_ctx.clone();
                tokio::spawn(async move {
                    execute_task(assignment, engine).await
                });
            }
            None => tokio::time::sleep(config.poll_interval).await,
        }
    }
}

async fn execute_task(assignment: TaskAssignment, engine_ctx: Arc<EngineContext>) {
    let result = match assignment.operation {
        TaskOperation::Sync => {
            run_pipeline(&engine_ctx, &config).await
        }
        TaskOperation::CheckApply => {
            check_pipeline(&engine_ctx, &config).await
            // then apply
        }
        TaskOperation::Teardown => {
            teardown_pipeline(&engine_ctx, &config).await
        }
        TaskOperation::Assert => {
            // run assertions
        }
    };

    // Complete or fail the task based on result
    match result {
        Ok(metrics) => complete_task(&app_ctx, task_id, metrics).await,
        Err(e) => fail_task(&app_ctx, task_id, e).await,
    }
}
```

The agent uses the existing application functions (`register`, `poll_task`, `complete_task`, `heartbeat`) directly on `AppContext` — no gRPC overhead.

Progress events are forwarded from the engine's `ProgressReporter` to the controller's `EventBus` during execution, so SSE streams work for `watch`.

---

## 6. PipelineService Completion

### Sync operations (return result directly)

| Method | Implementation |
|---|---|
| `list()` | `pipeline_source.list()` + enrich with last run from `RunRepository` |
| `get()` | `pipeline_source.get(name)` + parse YAML + return structured config |
| `check()` | `pipeline_inspector.check(yaml)` → `CheckResult` |
| `compile()` | `pipeline_source.get(name)` + resolve secrets/env vars |
| `diff()` | `pipeline_inspector.diff(yaml)` → `DiffResult` |

### Async operations (create task, return 202 + run handle)

| Method | Implementation |
|---|---|
| `sync()` | `pipeline_source.get(name)` → `submit_pipeline(yaml, Sync)` |
| `sync_batch()` | For each pipeline matching filter: `submit_pipeline(yaml, Sync)`. Return batch handle. |
| `check_apply()` | `pipeline_source.get(name)` → `submit_pipeline(yaml, CheckApply)` |
| `teardown()` | `submit_pipeline(yaml, Teardown)` |
| `assert()` | `submit_pipeline(yaml, Assert)` |

### ConnectionService completion

| Method | Implementation |
|---|---|
| `list()` | Parse `connections.yml` from project dir (via `PipelineSource` or new method) |
| `get()` | Parse + redact sensitive fields |
| `test()` | Resolve connection config → `connection_tester.test(config)` |
| `discover()` | Resolve connection config → `connection_tester.discover(config, table)` |

---

## 7. Module Layout Changes

### New files in controller

| File | Responsibility |
|---|---|
| `domain/ports/pipeline_source.rs` | `PipelineSource` trait |
| `domain/ports/pipeline_inspector.rs` | `PipelineInspector` trait |
| `domain/task.rs` (modified) | `TaskOperation` enum |
| `application/embedded_agent.rs` | Embedded agent loop |
| `migrations/*_add_task_operation.sql` | Add operation column |

### New files in CLI

| File | Responsibility |
|---|---|
| `engine_adapters/pipeline_source.rs` | `FsPipelineSource` |
| `engine_adapters/pipeline_inspector.rs` | `EnginePipelineInspector` |
| `engine_adapters/connection_tester.rs` (updated) | Real implementation replacing stub |
| `engine_adapters/plugin_registry.rs` | `EnginePluginRegistry` real implementation |

### Modified files

| File | Changes |
|---|---|
| `controller/src/server.rs` | `serve()` takes `ServeContext`, spawns embedded agent |
| `controller/src/application/context.rs` | `AppContext` gains `pipeline_source`, `pipeline_inspector` |
| `controller/src/application/submit.rs` | Add `operation` parameter |
| `controller/src/application/services/pipeline.rs` | Implement all methods |
| `controller/src/application/services/connection.rs` | Implement all methods |
| `controller/src/application/testing.rs` | Fakes for new ports |
| `cli/src/commands/controller.rs` | Build `ServeContext` with engine adapters |

---

## 8. Build Order (Vertical Slices)

### Slice 1: Sync end-to-end

1. `TaskOperation` enum + migration
2. `PipelineSource` driven port + `FsPipelineSource` adapter
3. Extend `submit_pipeline()` with `TaskOperation` parameter
4. `PipelineService::sync()` implementation
5. Embedded agent module
6. `ServeContext` struct + updated `serve()` signature
7. CLI wiring
8. Tests

**Delivers:** `POST /api/v1/pipelines/{name}/sync` triggers real pipeline execution via embedded agent.

### Slice 2: Pipeline discovery (list/get/compile)

1. `PipelineService::list()` implementation
2. `PipelineService::get()` implementation
3. `PipelineService::compile()` implementation
4. Tests

**Delivers:** `GET /api/v1/pipelines` returns real data.

### Slice 3: Check + Discover

1. `PipelineInspector` driven port + adapter
2. `PipelineService::check()` implementation
3. `PipelineService::check_apply()` implementation
4. `EngineConnectionTester` real implementation
5. `ConnectionService` completion
6. Tests

**Delivers:** Check and discover return real results.

### Slice 4: Teardown + Diff + Assert + Plugins

1. `PipelineService::teardown()` implementation
2. `PipelineService::diff()` implementation
3. `PipelineService::assert()` implementation
4. `EnginePluginRegistry` real implementation
5. Tests

**Delivers:** All REST endpoints return real data. Zero 501s.

### Dependency graph

```
Slice 1 (Sync e2e)
  ├→ Slice 2 (List/Get/Compile)
  ├→ Slice 3 (Check/Discover)
  └→ Slice 4 (Teardown/Diff/Assert/Plugins)
```

Slices 2-4 are independent after Slice 1.

---

## 9. Testing Strategy

### Unit tests — service implementations

Each method with fakes: `FakePipelineSource` (in-memory YAML map), `FakePipelineInspector` (canned results). Existing fakes for other ports.

### Integration tests — embedded agent e2e

Full stack in-process: `AppContext` with fakes + `FakePipelineSource` + mock `EngineContext`. Submit sync, assert Run transitions Pending → Running → Completed, assert progress events flow.

### REST integration tests

Extend `tests/rest/`: sync returns 202 (not 501), list returns data, check returns results.

### CLI integration tests

`ServeContext` built with real engine adapters compiles and wires correctly.
