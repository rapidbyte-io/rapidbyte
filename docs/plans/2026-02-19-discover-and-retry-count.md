# Discover Command & Retry Count Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `rapidbyte discover` CLI command to expose connector catalog discovery, and add `retry_count` to PipelineResult for pipeline-level retry observability.

**Architecture:** The discover command loads a connector WASM, opens it, calls `rb_discover`, and prints the catalog. retry_count is threaded from the orchestrator's existing `attempt` counter into PipelineResult and surfaced in CLI output + bench JSON.

**Tech Stack:** Rust, rapidbyte-cli, rapidbyte-core, rapidbyte-sdk

---

## Research Summary

| Feature | Connector Side | Host Side | Gap |
|---------|---------------|-----------|-----|
| `rb_discover()` | Exported & implemented in source-postgres (`main.rs:196-234`) | `ConnectorHandle::discover()` exists (`connector_handle.rs:52-70`) | No CLI command, orchestrator never calls it |
| `retry_count` | N/A (pipeline-level) | Orchestrator tracks `attempt` (`orchestrator.rs:28`) | Not in `PipelineResult`, not in CLI output |

---

### Task 1: Add `rapidbyte discover` CLI command

**Files:**
- Create: `crates/rapidbyte-cli/src/commands/discover.rs`
- Modify: `crates/rapidbyte-cli/src/main.rs:24-37`
- Modify: `crates/rapidbyte-cli/src/commands/mod.rs`
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs` (add `discover_connector` function)

**Step 1: Add `discover_connector` to the orchestrator**

In `crates/rapidbyte-core/src/engine/orchestrator.rs`, add a public function after `check_pipeline`:

```rust
/// Discover the catalog (available streams/schemas) from a connector.
/// Loads the WASM module, opens it with the given config, calls rb_discover,
/// then closes cleanly.
pub async fn discover_connector(
    connector_ref: &str,
    config: &serde_json::Value,
) -> Result<rapidbyte_sdk::protocol::Catalog> {
    let wasm_path = wasm_runtime::resolve_connector_path(connector_ref)?;

    // Load manifest for permissions (if available)
    let manifest = wasm_runtime::load_connector_manifest(&wasm_path)?;
    let permissions = manifest.as_ref().map(|m| m.permissions.clone());

    let (connector_id, connector_version) = parse_connector_ref(connector_ref);

    let config_clone = config.clone();
    tokio::task::spawn_blocking(move || -> Result<rapidbyte_sdk::protocol::Catalog> {
        run_discover(
            &wasm_path,
            &connector_id,
            &connector_version,
            &config_clone,
            permissions.as_ref(),
        )
    })
    .await?
}
```

Also add the `run_discover` helper to `crates/rapidbyte-core/src/engine/runner.rs`:

```rust
use rapidbyte_sdk::protocol::Catalog;

pub(crate) fn run_discover(
    wasm_path: &std::path::Path,
    connector_id: &str,
    connector_version: &str,
    config: &serde_json::Value,
    permissions: Option<&Permissions>,
) -> Result<Catalog> {
    let runtime = WasmRuntime::new()?;
    let module = runtime.load_module(wasm_path)?;

    let state = Arc::new(SqliteStateBackend::in_memory()?);

    let host_state = HostState {
        pipeline_name: "discover".to_string(),
        current_stream: Arc::new(Mutex::new("discover".to_string())),
        state_backend: state,
        stats: Arc::new(Mutex::new(RunStats::default())),
        batch_sender: None,
        next_batch_id: 1,
        batch_receiver: None,
        pending_batch: None,
        last_error: None,
        compression: None,
        source_checkpoints: Arc::new(Mutex::new(Vec::new())),
        dest_checkpoints: Arc::new(Mutex::new(Vec::new())),
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = create_secure_wasi_module(permissions)?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);

    // Lifecycle: open -> discover -> close
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(config.clone()),
        connector_id: connector_id.to_string(),
        connector_version: connector_version.to_string(),
    };

    handle.open(&open_ctx)?;
    let catalog = handle.discover()?;

    if let Err(e) = handle.close() {
        tracing::warn!("Connector close failed during discover: {}", e);
    }

    Ok(catalog)
}
```

Add the public re-export in `orchestrator.rs` imports:

```rust
use super::runner::{run_destination, run_source, run_transform, validate_connector, run_discover};
```

**Step 2: Add the CLI `discover` command module**

Create `crates/rapidbyte-cli/src/commands/discover.rs`:

```rust
use std::path::Path;

use anyhow::{Context, Result};

use rapidbyte_core::engine::orchestrator;
use rapidbyte_core::pipeline::parser;

/// Execute the `discover` command: load a pipeline config and discover the source catalog.
pub async fn execute(pipeline_path: &Path) -> Result<()> {
    let config = parser::parse_pipeline(pipeline_path)
        .with_context(|| format!("Failed to parse pipeline: {}", pipeline_path.display()))?;

    tracing::info!(
        pipeline = config.pipeline,
        source = config.source.use_ref,
        "Discovering source catalog"
    );

    let catalog =
        orchestrator::discover_connector(&config.source.use_ref, &config.source.config).await?;

    println!("Catalog for '{}' (source: {})", config.pipeline, config.source.use_ref);
    println!("  Streams: {}", catalog.streams.len());

    for stream in &catalog.streams {
        println!();
        println!("  Stream: {}", stream.name);
        if !stream.supported_sync_modes.is_empty() {
            let modes: Vec<String> = stream
                .supported_sync_modes
                .iter()
                .map(|m| format!("{:?}", m))
                .collect();
            println!("    Sync modes:  {}", modes.join(", "));
        }
        if let Some(ref cursor) = stream.source_defined_cursor {
            println!("    Cursor:      {}", cursor);
        }
        if let Some(ref pk) = stream.source_defined_primary_key {
            if !pk.is_empty() {
                println!("    Primary key: {}", pk.join(", "));
            }
        }
        if !stream.schema.is_empty() {
            println!("    Columns:");
            for col in &stream.schema {
                let nullable = if col.nullable { "NULL" } else { "NOT NULL" };
                println!("      {} {} {}", col.name, col.data_type, nullable);
            }
        }
    }

    // Machine-readable JSON output
    let json = serde_json::to_string_pretty(&catalog)?;
    println!("\n@@CATALOG_JSON@@{}", json);

    Ok(())
}
```

**Step 3: Wire up the CLI command**

In `crates/rapidbyte-cli/src/main.rs`, add the `Discover` variant to the `Commands` enum:

```rust
#[derive(Subcommand)]
enum Commands {
    /// Run a data pipeline
    Run {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
    },
    /// Validate pipeline configuration and connectivity
    Check {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
    },
    /// Discover available streams from a source connector
    Discover {
        /// Path to pipeline YAML file
        pipeline: PathBuf,
    },
    /// List available connectors
    Connectors,
}
```

And add the match arm in `main()`:

```rust
Commands::Discover { pipeline } => commands::discover::execute(&pipeline).await,
```

In `crates/rapidbyte-cli/src/commands/mod.rs`, add:

```rust
pub mod discover;
```

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: All pass (no existing tests affected)

**Step 5: Build host binary**

Run: `cargo build`
Expected: Compiles cleanly

**Step 6: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/discover.rs \
       crates/rapidbyte-cli/src/commands/mod.rs \
       crates/rapidbyte-cli/src/main.rs \
       crates/rapidbyte-core/src/engine/orchestrator.rs \
       crates/rapidbyte-core/src/engine/runner.rs
git commit -m "feat(cli): add 'rapidbyte discover' command for source catalog discovery"
```

---

### Task 2: Add retry_count to PipelineResult

**Files:**
- Modify: `crates/rapidbyte-core/src/engine/runner.rs:26-52`
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs:26-89` and `448-470`
- Modify: `crates/rapidbyte-cli/src/commands/run.rs:29-91`
- Test: `crates/rapidbyte-core/src/engine/errors.rs` (add test)

**Step 1: Add `retry_count` field to PipelineResult**

In `crates/rapidbyte-core/src/engine/runner.rs`, add to the `PipelineResult` struct (after `transform_module_load_ms`):

```rust
    pub transform_module_load_ms: Vec<u64>,
    // Retry tracking
    pub retry_count: u32,
```

**Step 2: Thread retry_count from orchestrator to PipelineResult**

In `crates/rapidbyte-core/src/engine/orchestrator.rs`, change `run_pipeline` to pass `attempt` into `execute_pipeline_once` and thread it into PipelineResult.

Change the `run_pipeline` function signature of `execute_pipeline_once` to accept the attempt number. Specifically, change `execute_pipeline_once(config).await` to `execute_pipeline_once(config, attempt).await`.

Update `execute_pipeline_once` signature:

```rust
async fn execute_pipeline_once(config: &PipelineConfig, attempt: u32) -> Result<PipelineResult, PipelineError> {
```

Then in the `Ok(PipelineResult { ... })` construction (line 448-470), add:

```rust
                retry_count: attempt - 1,
```

**Step 3: Display retry_count in CLI output**

In `crates/rapidbyte-cli/src/commands/run.rs`, add after the module load lines (before `@@BENCH_JSON@@`):

```rust
    if result.retry_count > 0 {
        println!("  Retries:         {}", result.retry_count);
    }
```

And add to the bench JSON object:

```rust
        "retry_count": result.retry_count,
```

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: All pass

**Step 5: Build host binary**

Run: `cargo build`
Expected: Compiles cleanly

**Step 6: Commit**

```bash
git add crates/rapidbyte-core/src/engine/runner.rs \
       crates/rapidbyte-core/src/engine/orchestrator.rs \
       crates/rapidbyte-cli/src/commands/run.rs
git commit -m "feat(core): add retry_count to PipelineResult for pipeline retry observability"
```

---

### Task 3: Verification

**Step 1: Run all host tests**

Run: `cargo test --workspace`
Expected: All pass, no regressions

**Step 2: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: No warnings

**Step 3: Build both connectors**

Run:
```bash
cd connectors/source-postgres && cargo build --target wasm32-wasip1 --release
cd connectors/dest-postgres && cargo build --target wasm32-wasip1 --release
```
Expected: Both build cleanly (no connector changes in this plan)

**Step 4: Verify the new `discover` command compiles into help**

Run: `cargo run -- --help`
Expected: Shows `discover` subcommand in help output

Run: `cargo run -- discover --help`
Expected: Shows usage for discover command
