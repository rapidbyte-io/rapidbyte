# State Backend Factory — Cross-Crate Responsibility Fix

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans to implement this plan.

**Goal:** Move state backend construction into `rapidbyte-state` crate so it owns its own factory. Remove engine's knowledge of concrete backend types.

**Architecture:** The engine currently imports `SqliteStateBackend` and `PostgresStateBackend` directly and constructs them with backend-specific logic. This violates single responsibility — the state crate should own construction of its own types. The engine should only pass config values through.

**Tech Stack:** Rust workspace crates (`rapidbyte-state`, `rapidbyte-engine`, `rapidbyte-types`)

---

## Current Problem

```
engine/plugin/sandbox.rs
├── imports SqliteStateBackend, PostgresStateBackend (concrete types)
├── knows default SQLite path (~/.rapidbyte/state.db)
├── knows Postgres default connstr
└── matches on StateBackendKind (defined in engine/config/types.rs)
```

The engine knows too much about state backend internals. Adding a new backend (e.g., DynamoDB) would require changes in BOTH the state crate AND the engine.

## Target

```
rapidbyte-state/
├── pub enum BackendKind { Sqlite, Postgres }
├── pub fn open(kind, connection) → Arc<dyn StateBackend>
├── pub fn default_sqlite_path() → PathBuf
└── owns ALL construction logic

rapidbyte-engine/
├── deserializes BackendKind from YAML (via rapidbyte_state::BackendKind)
├── resolves default connection string from config
└── calls rapidbyte_state::open(kind, &connection)  ← single call, no concrete types
```

---

## Tasks

### Task 1: Move BackendKind to rapidbyte-state

**Files:**
- Modify: `crates/rapidbyte-state/src/lib.rs`
- Modify: `crates/rapidbyte-engine/src/config/types.rs`

- [ ] **Step 1: Define BackendKind in rapidbyte-state**

Add to `crates/rapidbyte-state/src/lib.rs`:
```rust
/// Supported state backend implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    Sqlite,
    Postgres,
}

impl Default for BackendKind {
    fn default() -> Self {
        Self::Sqlite
    }
}
```

- [ ] **Step 2: Update engine's StateBackendKind to re-export**

In `crates/rapidbyte-engine/src/config/types.rs`, replace the local `StateBackendKind` enum with a re-export:
```rust
pub use rapidbyte_state::BackendKind as StateBackendKind;
```

Or if we prefer no backward compat, just use `BackendKind` directly everywhere and delete `StateBackendKind`.

- [ ] **Step 3: Run tests**

Run: `cargo test --workspace --exclude rapidbyte-benchmarks`

- [ ] **Step 4: Commit**

```bash
git commit -m "refactor(state): move BackendKind enum to rapidbyte-state crate"
```

---

### Task 2: Add factory function to rapidbyte-state

**Files:**
- Modify: `crates/rapidbyte-state/src/lib.rs`

- [ ] **Step 1: Add open_backend factory**

```rust
/// Open a state backend by kind and connection string.
///
/// For SQLite, `connection` is a file path. For Postgres, it's a connection string.
///
/// # Errors
///
/// Returns an error if the database cannot be opened or connected to.
pub fn open_backend(kind: BackendKind, connection: &str) -> Result<Arc<dyn StateBackend>> {
    match kind {
        BackendKind::Sqlite => {
            let backend = SqliteStateBackend::open(std::path::Path::new(connection))
                .context("Failed to open SQLite state backend")?;
            Ok(Arc::new(backend) as Arc<dyn StateBackend>)
        }
        BackendKind::Postgres => {
            let backend = PostgresStateBackend::open(connection)
                .map_err(|e| anyhow::anyhow!("Failed to open Postgres state backend: {e}"))?;
            Ok(Arc::new(backend) as Arc<dyn StateBackend>)
        }
    }
}

/// Default SQLite state database path: `~/.rapidbyte/state.db`
#[must_use]
pub fn default_sqlite_path() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    std::path::PathBuf::from(home).join(".rapidbyte").join("state.db")
}
```

- [ ] **Step 2: Add tests**

```rust
#[test]
fn test_open_backend_sqlite_in_memory() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("test.db");
    let backend = open_backend(BackendKind::Sqlite, db_path.to_str().unwrap()).unwrap();
    // Verify it works
    let run_id = backend.start_run(
        &rapidbyte_types::state::PipelineId::new("test"),
        &rapidbyte_types::state::StreamName::new("all"),
    ).unwrap();
    assert!(run_id > 0);
}

#[test]
fn test_default_sqlite_path() {
    let path = default_sqlite_path();
    assert!(path.to_str().unwrap().contains(".rapidbyte"));
    assert!(path.to_str().unwrap().ends_with("state.db"));
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p rapidbyte-state`

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(state): add open_backend factory and default_sqlite_path"
```

---

### Task 3: Simplify engine — delete create_state_backend, inline the call

Once the state crate owns its factory, the engine's `create_state_backend()` becomes ~3 lines. Don't wrap it in a function or a new module — just inline it at the call site.

**Files:**
- Modify: `crates/rapidbyte-engine/src/plugin/sandbox.rs` — delete `create_state_backend` and `check_state_backend`
- Modify: `crates/rapidbyte-engine/src/orchestrator.rs` — inline the state backend creation

- [ ] **Step 1: Delete `create_state_backend` and `check_state_backend` from plugin/sandbox.rs**

These functions no longer need to exist. The engine just calls the state crate directly.

- [ ] **Step 2: Inline state backend creation in orchestrator.rs**

At the call site where `create_state_backend(&config)` was called, replace with:
```rust
let connection = config.state.connection.clone().unwrap_or_else(|| {
    rapidbyte_state::default_connection(config.state.backend)
});
let state = tokio::task::spawn_blocking(move || {
    rapidbyte_state::open_backend(config_for_state.state.backend, &connection)
})
.await
.map_err(|e| PipelineError::task_panicked("open_state_backend", e))??;
```

- [ ] **Step 3: Inline check_state_backend in check_pipeline**

Replace the call to `check_state_backend(&config)` with a direct inline:
```rust
let state = match rapidbyte_state::open_backend(
    config.state.backend,
    &config.state.connection.clone().unwrap_or_else(|| rapidbyte_state::default_connection(config.state.backend)),
) {
    Ok(_) => CheckStatus { ok: true, message: String::new() },
    Err(e) => CheckStatus { ok: false, message: e.to_string() },
};
```

- [ ] **Step 4: Remove concrete backend imports from engine production code**

Remove `use rapidbyte_state::{SqliteStateBackend, PostgresStateBackend}` from production code. Test code may still use `SqliteStateBackend::in_memory()`.

- [ ] **Step 5: Run full test suite**

Run: `cargo test --workspace --exclude rapidbyte-benchmarks`

- [ ] **Step 6: Commit**

```bash
git commit -m "refactor(engine): inline state backend creation, remove concrete backend imports"
```

---

## Verification

After all tasks:
- Engine no longer imports `SqliteStateBackend` or `PostgresStateBackend` in production code (test code may still use `SqliteStateBackend::in_memory()`)
- No `create_state_backend` wrapper function exists — the call is inlined
- `plugin/sandbox.rs` contains ONLY `build_sandbox_overrides()` (a true plugin concern)
- Adding a new state backend requires ONLY changes to `rapidbyte-state`

---

*— End of Plan —*
