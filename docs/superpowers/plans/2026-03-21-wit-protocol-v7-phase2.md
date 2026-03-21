# WIT Protocol v7 — Phase 2: SDK & Runtime Integration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the SDK and runtime compile and work with v7 types, implement new host imports, update plugin traits, and clean up v6 cruft.

**Architecture:** Fix compilation breaks from Phase 1 type changes, then layer in v7 functionality: updated plugin traits with new lifecycle methods, new Context API methods (cancellation, transactional checkpoints, stream errors, batch metadata), new host import implementations, and updated proc macro for v7 dispatch. Clean up dead code throughout.

**Tech Stack:** Rust, wasmtime component model, wit-bindgen, proc-macro2/quote/syn

**Spec:** `docs/superpowers/specs/2026-03-20-wit-protocol-v7-design.md`

**Depends on:** Phase 1 complete (v7 WIT file + types crate)

---

## Refactoring scope (integrated into tasks, not separate)

- Remove `SchemaHint` enum from `catalog.rs` (dead — `StreamContext` now uses `StreamSchema`)
- Remove `Catalog` wrapper struct (v7 discovery returns `Vec<DiscoveredStream>` directly)
- Remove old `ValidationResult`/`ValidationStatus` re-exports from SDK prelude
- Remove `state_compare_and_set` from `HostImports` trait (v7 drops `state-cas`)
- Remove `PluginInfo` from `init()` return — v7 uses `spec()` export instead
- Make error category match arms exhaustive (no catch-all `_ =>`)
- Consolidate SDK re-exports to use `rapidbyte_types::prelude` where possible

---

### Task 1: Fix SDK compilation — update imports and remove dead re-exports

**Files:**
- Modify: `crates/rapidbyte-sdk/src/prelude.rs`
- Modify: `crates/rapidbyte-sdk/src/plugin.rs`
- Modify: `crates/rapidbyte-sdk/src/context.rs`
- Modify: `crates/rapidbyte-sdk/src/host_ffi.rs`
- Modify: `crates/rapidbyte-sdk/src/features.rs`
- Modify: `crates/rapidbyte-sdk/src/lib.rs`

**What's broken:** Phase 1 deleted `ValidationResult` and `ValidationStatus` from `rapidbyte_types::error` and renamed `PartitionStrategy::Mod` to `ModHash`. The SDK imports these deleted types.

- [ ] **Step 1: Read all broken SDK files to understand current imports**

Read each file listed above, noting every import of `ValidationResult`, `ValidationStatus`, `SchemaHint`, `Catalog`, `ColumnSchema`, `Stream` from old locations.

- [ ] **Step 2: Update prelude.rs**

Replace old re-exports with v7 types:
```rust
// Remove these:
pub use crate::error::{ValidationResult, ValidationStatus};
pub use crate::catalog::{Catalog, ColumnSchema, Stream};

// Add these:
pub use rapidbyte_types::validation::{
    ValidationReport, ValidationStatus, PrerequisitesReport, PrerequisiteCheck,
};
pub use rapidbyte_types::schema::{SchemaField, StreamSchema, FieldConstraint, FieldRequirement};
pub use rapidbyte_types::discovery::{DiscoveredStream, PluginSpec};
pub use rapidbyte_types::lifecycle::{ApplyRequest, ApplyReport, TeardownRequest, TeardownReport};
pub use rapidbyte_types::batch::BatchMetadata;
pub use rapidbyte_types::run::{RunRequest, RunSummary, StreamResult};
pub use rapidbyte_types::checkpoint::{CursorUpdate, StateMutation};
```

Keep existing re-exports that are still valid (PluginError, SyncMode, WriteMode, etc.). Remove `Catalog`, `ColumnSchema`, `Stream`, `SchemaHint` — these are v6 types.

- [ ] **Step 3: Update plugin.rs imports**

Replace:
```rust
use crate::error::{PluginError, ValidationResult, ValidationStatus};
```
With:
```rust
use rapidbyte_types::error::PluginError;
use rapidbyte_types::validation::{ValidationReport, ValidationStatus};
```

Update trait method signatures to use `ValidationReport` instead of `ValidationResult`. For now, keep the same method signatures — just fix the type name. The full v7 trait update happens in Task 2.

- [ ] **Step 4: Update context.rs, host_ffi.rs, features.rs imports**

Fix any remaining broken imports. Use `rapidbyte_types::prelude::*` where it simplifies things, or explicit imports.

- [ ] **Step 5: Remove dead code from SDK catalog/schema modules**

If the SDK has its own `catalog.rs` or re-exports `SchemaHint`, remove or update. Search for any remaining references to `SchemaHint`, `Catalog` (the old wrapper), `ColumnSchema`, `Stream` (the old discovery type).

- [ ] **Step 6: Verify SDK compiles**

Run: `cargo check -p rapidbyte-sdk`
Expected: PASS (or identify remaining issues)

If there are remaining errors, fix them iteratively. The goal is `cargo check -p rapidbyte-sdk` passes.

- [ ] **Step 7: Commit**

```bash
git commit -m "fix(sdk): update imports for v7 types, remove dead v6 re-exports"
```

---

### Task 2: Update SDK plugin traits to v7 lifecycle

**Files:**
- Modify: `crates/rapidbyte-sdk/src/plugin.rs`

**What changes:** Source/Destination/Transform traits get new lifecycle methods with default impls. `init()` no longer returns `PluginInfo` (v7 uses `spec()` instead). `validate()` signature changes to accept optional upstream schema.

- [ ] **Step 1: Read current plugin.rs**

- [ ] **Step 2: Update Source trait**

```rust
pub trait Source: Sized {
    type Config: DeserializeOwned;

    /// Return plugin spec. Called before open, no instance needed.
    fn spec() -> PluginSpec { PluginSpec::from_manifest() }

    /// Initialize the plugin with parsed config.
    async fn init(config: Self::Config) -> Result<Self, PluginError>;

    /// Pre-flight checks.
    async fn prerequisites(&self, ctx: &Context) -> Result<PrerequisitesReport, PluginError> {
        Ok(PrerequisitesReport::passed())
    }

    /// Discover available streams.
    async fn discover(&self, ctx: &Context) -> Result<Vec<DiscoveredStream>, PluginError>;

    /// Validate config, optionally against upstream schema.
    async fn validate(
        &self, ctx: &Context, upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Validation not implemented"))
    }

    /// Create/prepare external resources.
    async fn apply(&self, ctx: &Context, request: ApplyRequest) -> Result<ApplyReport, PluginError> {
        Ok(ApplyReport::noop())
    }

    /// Read data from a single stream.
    async fn read(&self, ctx: &Context, stream: StreamContext) -> Result<ReadSummary, PluginError>;

    /// Clean up session.
    async fn close(&self, ctx: &Context) -> Result<(), PluginError> { Ok(()) }

    /// Tear down persistent resources.
    async fn teardown(&self, ctx: &Context, request: TeardownRequest) -> Result<TeardownReport, PluginError> {
        Ok(TeardownReport::noop())
    }
}
```

Key changes from v6:
- `init()` returns `Result<Self, PluginError>` (not `Result<(Self, PluginInfo), PluginError>`)
- `spec()` is new (static, no `&self`)
- `validate()` takes `&self` + optional `StreamSchema` (not `config: &Self::Config`)
- `discover()` returns `Vec<DiscoveredStream>` (not `Catalog`)
- New: `prerequisites()`, `apply()`, `teardown()` with default impls

- [ ] **Step 3: Update Destination trait similarly**

Same changes, but `write()` instead of `read()`, no `discover()`.

- [ ] **Step 4: Update Transform trait**

Smaller interface — no `prerequisites()`, `apply()`, `teardown()`. Keep: `spec()`, `init()`, `validate()`, `transform()`, `close()`.

- [ ] **Step 5: Add `PluginSpec::from_manifest()` helper**

If it doesn't exist yet, add a basic implementation that returns a default `PluginSpec` (will be populated by the proc macro from the embedded manifest):

```rust
impl PluginSpec {
    pub fn from_manifest() -> Self {
        Self {
            protocol_version: 7,
            config_schema_json: "{}".into(),
            resource_schema_json: None,
            documentation_url: None,
            features: vec![],
            supported_sync_modes: vec![],
            supported_write_modes: None,
        }
    }
}
```

- [ ] **Step 6: Verify compiles**

Run: `cargo check -p rapidbyte-sdk`

- [ ] **Step 7: Commit**

```bash
git commit -m "feat(sdk): update plugin traits to v7 lifecycle

- init() no longer returns PluginInfo (use spec() instead)
- validate() takes optional upstream StreamSchema
- discover() returns Vec<DiscoveredStream>
- Add spec(), prerequisites(), apply(), teardown() with defaults"
```

---

### Task 3: Add v7 feature traits (MultiStreamSource, MultiStreamCdcSource, BulkDestination)

**Files:**
- Modify: `crates/rapidbyte-sdk/src/features.rs`
- Modify: `crates/rapidbyte-sdk/src/prelude.rs`

- [ ] **Step 1: Read current features.rs**

- [ ] **Step 2: Add new feature traits**

Add after existing traits:

```rust
/// Multi-stream source — receives all streams in one call.
pub trait MultiStreamSource: Source {
    async fn read_streams(
        &self,
        ctx: &Context,
        streams: Vec<StreamContext>,
    ) -> Result<RunSummary, PluginError>;
}

/// Multi-stream CDC — one replication slot, many tables.
pub trait MultiStreamCdcSource: MultiStreamSource + CdcSource {
    async fn read_all_changes(
        &self,
        ctx: &Context,
        streams: Vec<StreamContext>,
    ) -> Result<RunSummary, PluginError>;
}

/// Bulk-optimized destination.
pub trait BulkDestination: Destination {
    async fn write_bulk(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError>;
}
```

Note: Move `write_bulk` from the Destination trait default to BulkDestination. Remove it from Destination.

- [ ] **Step 3: Update prelude to re-export new traits**

Add: `pub use crate::features::{MultiStreamSource, MultiStreamCdcSource, BulkDestination};`

- [ ] **Step 4: Verify compiles**

Run: `cargo check -p rapidbyte-sdk`

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(sdk): add v7 feature traits (MultiStreamSource, MultiStreamCdcSource, BulkDestination)"
```

---

### Task 4: Update SDK host_ffi and Context with v7 methods

**Files:**
- Modify: `crates/rapidbyte-sdk/src/host_ffi.rs`
- Modify: `crates/rapidbyte-sdk/src/context.rs`

- [ ] **Step 1: Read both files**

- [ ] **Step 2: Update HostImports trait**

Remove:
- `state_compare_and_set()` (v7 drops `state-cas`)

Add:
```rust
// Transactional checkpoints
fn checkpoint_begin(&self, kind: CheckpointKind) -> Result<u64, PluginError>;
fn checkpoint_set_cursor(&self, txn: u64, cursor: &CursorUpdate) -> Result<(), PluginError>;
fn checkpoint_set_state(&self, txn: u64, mutation: &StateMutation) -> Result<(), PluginError>;
fn checkpoint_commit(&self, txn: u64, records: u64, bytes: u64) -> Result<(), PluginError>;
fn checkpoint_abort(&self, txn: u64);

// Cancellation
fn is_cancelled(&self) -> bool;

// Per-stream error
fn stream_error(&self, stream_index: u32, error: &PluginError) -> Result<(), PluginError>;

// Batch with metadata
fn emit_batch_with_metadata(&self, handle: u64, metadata: &BatchMetadata) -> Result<(), PluginError>;
fn next_batch_metadata(&self, handle: u64) -> Result<BatchMetadata, PluginError>;
```

Update the existing `emit_batch` to call `emit_batch_with_metadata` internally (the old signature still works for single-stream plugins via the SDK).

- [ ] **Step 3: Update the WasmImports implementation**

Update the concrete implementation that calls the WIT-generated functions. For now, implement the new methods by calling the corresponding WIT host imports. The WIT imports are generated in the plugin's `__rb_bindings` module.

- [ ] **Step 4: Update Context with new public methods**

Add to Context:
```rust
// Cancellation
pub fn is_cancelled(&self) -> bool
pub fn check_cancelled(&self) -> Result<(), PluginError>  // Returns Err if cancelled

// Batch for multi-stream
pub fn emit_batch_for_stream(&self, stream_index: u32, batch: &RecordBatch) -> Result<(), PluginError>

// Per-stream errors
pub fn stream_error(&self, stream_index: u32, error: PluginError) -> Result<(), PluginError>

// Transactional checkpoints
pub fn begin_checkpoint(&self, kind: CheckpointKind) -> Result<CheckpointTxn, PluginError>
```

The simple `ctx.checkpoint()` method stays but now internally calls `begin → set_cursor → commit`.

Add the `CheckpointTxn` struct:
```rust
pub struct CheckpointTxn { handle: u64 }

impl CheckpointTxn {
    pub fn set_cursor(&mut self, stream: &str, field: &str, value: CursorValue) -> Result<(), PluginError> { ... }
    pub fn set_state(&mut self, scope: StateScope, key: &str, value: &str) -> Result<(), PluginError> { ... }
    pub fn commit(self, records: u64, bytes: u64) -> Result<(), PluginError> { ... }
    pub fn abort(self) { ... }
}

impl Drop for CheckpointTxn {
    fn drop(&mut self) { /* abort if not committed — log warning */ }
}
```

- [ ] **Step 5: Update `emit_batch` to include metadata**

The existing `ctx.emit_batch(&batch)` should automatically fill `BatchMetadata` with `stream_index: 0`, auto-incrementing `sequence_number`, and computed `record_count` / `byte_count`. Store a sequence counter in Context (add a field or use a thread-local).

- [ ] **Step 6: Verify compiles**

Run: `cargo check -p rapidbyte-sdk`

- [ ] **Step 7: Commit**

```bash
git commit -m "feat(sdk): add v7 Context API — cancellation, transactional checkpoints, stream errors, batch metadata

- Remove state_compare_and_set (v7 drops state-cas)
- Add is_cancelled/check_cancelled for cooperative cancellation
- Add CheckpointTxn for transactional checkpoint building
- Add emit_batch_for_stream for multi-stream sources
- Add stream_error for per-stream failure reporting
- emit_batch now attaches BatchMetadata automatically"
```

---

### Task 5: Clean up dead v6 code from SDK

**Files:**
- Modify: `crates/rapidbyte-sdk/src/lib.rs`
- Modify or delete: SDK files that re-export old types
- Modify: `crates/rapidbyte-sdk/src/conformance.rs` (if it references old types)

- [ ] **Step 1: Search for remaining v6 references**

Grep the entire `crates/rapidbyte-sdk/` for: `ValidationResult`, `SchemaHint`, `Catalog` (the struct), `ColumnSchema`, `Stream` (the old discovery type), `PluginInfo`, `state_compare_and_set`, `state_cas`.

- [ ] **Step 2: Remove or update each reference**

- If the SDK has its own type aliases or re-exports for these, remove them
- If conformance tests reference old types, update them to v7 types
- If `PluginInfo` is still referenced (old init return), update callers

- [ ] **Step 3: Remove `write_bulk` default from Destination trait**

It's now on `BulkDestination`. Remove the default impl from `Destination::write_bulk` if it still exists.

- [ ] **Step 4: Verify compiles and tests pass**

Run: `cargo check -p rapidbyte-sdk && cargo test -p rapidbyte-sdk`

- [ ] **Step 5: Commit**

```bash
git commit -m "refactor(sdk): remove dead v6 code — SchemaHint, Catalog, ValidationResult, PluginInfo from init, state_cas"
```

---

### Task 6: Fix runtime compilation — bindings type converters

**Files:**
- Modify: `crates/rapidbyte-runtime/src/bindings.rs`
- Modify: `crates/rapidbyte-runtime/src/error.rs` (if it exists with match arms)

**What's broken:** The WIT file changed from v6 to v7, so `wasmtime::component::bindgen!` generates new types. The manual converter code in `bindings.rs` references old types and signatures.

- [ ] **Step 1: Read bindings.rs and understand the macro structure**

The file uses `wasmtime::component::bindgen!` to auto-generate types from WIT, then provides manual converter functions between those generated types and `rapidbyte_types`.

- [ ] **Step 2: Update error category converters**

Add explicit `ErrorCategory::Cancelled` match arm. Remove catch-all `_ =>` pattern — make all matches exhaustive.

```rust
ErrorCategory::Cancelled => CErrorCategory::Cancelled,
```

And the reverse:
```rust
CErrorCategory::Cancelled => ErrorCategory::Cancelled,
```

- [ ] **Step 3: Update validation converter**

The WIT now generates a `ValidationReport` with `output_schema` and `field_requirements` fields. Update the converter to map these:

```rust
pub fn validation_from_component(value: ComponentValidationReport) -> ValidationReport {
    ValidationReport {
        status: match value.status { ... },
        message: value.message,
        warnings: value.warnings,
        output_schema: value.output_schema.map(|s| convert_stream_schema(s)),
        field_requirements: value.field_requirements.map(|reqs| reqs.into_iter().map(convert_field_requirement).collect()),
    }
}
```

Add helper converters for `StreamSchema`, `SchemaField`, `FieldRequirement` between WIT-generated and `rapidbyte_types` versions.

- [ ] **Step 4: Update host trait implementation signatures**

The WIT host interface changed:
- `emit_batch` now takes `(handle, metadata)` instead of just `(handle)`
- `checkpoint` replaced by `checkpoint_begin/set_cursor/set_state/commit/abort`
- `state_cas` removed
- New: `is_cancelled`, `stream_error`, `next_batch_metadata`

Update the `impl Host for ComponentHostState` block to match new signatures.

- [ ] **Step 5: Handle new lifecycle export types**

The WIT source/destination/transform interfaces now have `spec`, `prerequisites`, `apply`, `teardown` exports. The bindings auto-generate these. Add any needed type converters for `PluginSpec`, `PrerequisitesReport`, `ApplyRequest/Report`, `TeardownRequest/Report`, `DiscoveredStream`.

- [ ] **Step 6: Fix any remaining compilation errors**

Run: `cargo check -p rapidbyte-runtime` and fix iteratively.

- [ ] **Step 7: Commit**

```bash
git commit -m "feat(runtime): update bindings for v7 WIT — new type converters, exhaustive error matches, transactional checkpoint signatures"
```

---

### Task 7: Implement v7 host imports in host_state.rs

**Files:**
- Modify: `crates/rapidbyte-runtime/src/host_state.rs`

This is the host-side implementation of new v7 imports.

- [ ] **Step 1: Read host_state.rs — understand ComponentHostState and existing impl methods**

- [ ] **Step 2: Add transactional checkpoint state**

Add to `ComponentHostState`:
```rust
pub(crate) checkpoint_txns: CheckpointTxnTable,
```

Create a simple transaction table:
```rust
struct CheckpointTxnTable {
    next_id: u64,
    active: HashMap<u64, PendingCheckpointTxn>,
}

struct PendingCheckpointTxn {
    kind: CheckpointKind,
    cursor_updates: Vec<CursorUpdate>,
    state_mutations: Vec<StateMutation>,
}
```

- [ ] **Step 3: Implement checkpoint transaction methods**

```rust
fn checkpoint_begin_impl(&mut self, kind: CheckpointKind) -> Result<u64, PluginError>
fn checkpoint_set_cursor_impl(&mut self, txn: u64, cursor: CursorUpdate) -> Result<(), PluginError>
fn checkpoint_set_state_impl(&mut self, txn: u64, mutation: StateMutation) -> Result<(), PluginError>
fn checkpoint_commit_impl(&mut self, txn: u64, records: u64, bytes: u64) -> Result<(), PluginError>
fn checkpoint_abort_impl(&mut self, txn: u64)
```

`checkpoint_commit_impl` should: collect the pending txn, build a `Checkpoint` from it (or a new `CheckpointTransaction` type), and send it through the existing checkpoint collector. Then clean up the txn entry.

- [ ] **Step 4: Implement is_cancelled**

Add a cancellation flag to `ComponentHostState`:
```rust
pub(crate) cancel_flag: Arc<AtomicBool>,
```

Implement:
```rust
fn is_cancelled_impl(&self) -> bool {
    self.cancel_flag.load(Ordering::Relaxed)
}
```

The host sets this flag when `cancel_requested` is true on the run.

- [ ] **Step 5: Implement stream_error**

```rust
fn stream_error_impl(&mut self, stream_index: u32, error: PluginError) -> Result<(), PluginError>
```

Store stream errors in a collector on `ComponentHostState`. The orchestrator reads them after run completes.

- [ ] **Step 6: Update emit_batch to accept metadata**

```rust
fn emit_batch_impl(&mut self, handle: u64, metadata: BatchMetadata) -> Result<(), PluginError>
```

The metadata's `stream_index` is used to route batches to the correct channel. For single-stream, `stream_index` is always 0 (single channel). For multi-stream, the host creates multiple channels and routes by index.

- [ ] **Step 7: Implement next_batch_metadata**

```rust
fn next_batch_metadata_impl(&mut self, handle: u64) -> Result<BatchMetadata, PluginError>
```

Store metadata alongside frames in the frame table or batch router.

- [ ] **Step 8: Remove state_cas_impl**

Delete the `state_cas_impl` method — v7 drops `state-cas`.

- [ ] **Step 9: Verify compiles and existing tests pass**

Run: `cargo check -p rapidbyte-runtime && cargo test -p rapidbyte-runtime`

- [ ] **Step 10: Commit**

```bash
git commit -m "feat(runtime): implement v7 host imports — transactional checkpoints, cancellation, stream errors, batch metadata

- Add CheckpointTxnTable for transactional checkpoint state
- Add cancel_flag (Arc<AtomicBool>) for cooperative cancellation
- Add stream error collector for per-stream failure reporting
- emit_batch now accepts BatchMetadata for stream routing
- Remove state_cas_impl (v7 drops state-cas)"
```

---

### Task 8: Update proc macro for v7 lifecycle

**Files:**
- Modify: `crates/rapidbyte-sdk/macros/src/plugin.rs`

This is complex — the macro generates WIT glue code for all lifecycle methods.

- [ ] **Step 1: Read the full macro file**

Understand:
- How `gen_wit_bindings` generates the `__rb_bindings` module
- How `gen_lifecycle_methods` generates `open`, `validate`, `close`, `run`, `discover`
- How `gen_read_dispatch` routes to feature traits
- How `to_component_error` / `to_component_validation` convert types

- [ ] **Step 2: Update lifecycle method generation**

Add new v7 exports to the generated impl block:

```rust
fn spec() -> Result<PluginSpec, PluginError> {
    // Call T::spec() (static method, no instance needed)
}

fn prerequisites(session: u64) -> Result<PrerequisitesReport, PluginError> {
    // Get instance, call T::prerequisites(&ctx)
}

fn apply(session: u64, request: ApplyRequest) -> Result<ApplyReport, PluginError> {
    // Get instance, call T::apply(&ctx, request)
}

fn teardown(session: u64, request: TeardownRequest) -> Result<TeardownReport, PluginError> {
    // Get instance, call T::teardown(&ctx, request)
}
```

Update `validate` generation to pass optional `StreamSchema`.

Update `discover` to return `list<discovered-stream>` instead of `string` (JSON).

- [ ] **Step 3: Update `init` / `open` to not return PluginInfo**

v6 `open` returned `(Self, PluginInfo)` and serialized PluginInfo.
v7 `open` returns just `Self`. PluginInfo is replaced by `spec()`.

Update the generated `open` function accordingly.

- [ ] **Step 4: Update error/validation converters in macro**

Add `Cancelled` match arm to `to_component_error`.
Update `to_component_validation` to pass through `output_schema` and `field_requirements`.

- [ ] **Step 5: Update read dispatch for new feature traits**

Add dispatch rules:
1. If `multi-stream` + `cdc` AND multiple CDC streams → `read_all_changes()`
2. If `cdc` AND sync_mode is CDC → `read_changes()`
3. If `multi-stream` AND multiple streams → `read_streams()`
4. If `partitioned-read` AND partition coords → `read_partition()`
5. Otherwise → `read()`

- [ ] **Step 6: Update WIT world for transforms**

Transform interface is smaller — no `prerequisites`, `apply`, `teardown`. The macro should only generate these for source/destination roles.

- [ ] **Step 7: Verify compiles**

Run: `cargo check -p rapidbyte-sdk-macros`
Then: `cargo check -p rapidbyte-sdk` (which uses the macro)

- [ ] **Step 8: Commit**

```bash
git commit -m "feat(sdk-macros): update proc macro for v7 lifecycle

- Generate spec(), prerequisites(), apply(), teardown() exports
- Update init/open to not return PluginInfo
- Update validate to accept optional StreamSchema
- Update discover to return Vec<DiscoveredStream>
- Add v7 read dispatch rules (multi-stream, multi-stream-cdc)
- Add Cancelled error category to converters"
```

---

### Task 9: Fix engine compilation and update runner ports

**Files:**
- Modify: `crates/rapidbyte-engine/src/domain/ports/runner.rs`
- Modify: `crates/rapidbyte-engine/src/application/run.rs`
- Modify: `crates/rapidbyte-engine/src/application/check.rs`
- Modify: `crates/rapidbyte-engine/src/application/discover.rs`
- Modify: other engine files as needed for compilation

**This task is about compilation, not adding new orchestration logic** (that's Phase 3).

- [ ] **Step 1: Run `cargo check -p rapidbyte-engine` to see all errors**

- [ ] **Step 2: Update PluginRunner trait**

The runner trait methods reference old types (ValidationResult, Catalog, PluginInfo). Update to:
- `validate()` returns `ValidationReport`
- `discover()` returns `Vec<DiscoveredStream>`
- Run params don't carry `PluginInfo`

- [ ] **Step 3: Update application layer imports**

Fix all broken imports in `run.rs`, `check.rs`, `discover.rs`. These files use `ValidationResult`, `Catalog`, `SchemaHint` etc.

- [ ] **Step 4: Fix adapter layer**

Update `adapter/wasm_runner.rs` to match new runner trait signatures.

- [ ] **Step 5: Verify engine compiles**

Run: `cargo check -p rapidbyte-engine`

- [ ] **Step 6: Run existing engine tests**

Run: `cargo test -p rapidbyte-engine`
Fix any test failures from type changes.

- [ ] **Step 7: Commit**

```bash
git commit -m "fix(engine): update ports and application layer for v7 types

- PluginRunner returns ValidationReport, Vec<DiscoveredStream>
- Remove PluginInfo from run params
- Update all imports from old to new type locations"
```

---

### Task 10: Fix remaining workspace compilation

**Files:**
- Various files across all crates

- [ ] **Step 1: Run `cargo check --workspace` to find all remaining errors**

Exclude plugins (they target wasm32-wasip2 and need separate treatment):
```bash
cargo check --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform
```

- [ ] **Step 2: Fix errors iteratively**

Common fixes:
- CLI crate importing old types
- Controller crate using old ValidationResult
- Dev crate references

- [ ] **Step 3: Run workspace tests**

```bash
cargo test --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform
```

Fix any failures.

- [ ] **Step 4: Run clippy**

```bash
cargo clippy --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform -- -D warnings
```

- [ ] **Step 5: Commit**

```bash
git commit -m "fix: resolve remaining v7 compilation issues across workspace"
```

---

### Task 11: Verify Phase 2 complete

- [ ] **Step 1: Full workspace check (excluding plugins)**

```bash
cargo check --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform
```
Expected: PASS

- [ ] **Step 2: Full workspace tests (excluding plugins)**

```bash
cargo test --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform
```
Expected: ALL PASS

- [ ] **Step 3: Clippy clean**

```bash
cargo clippy --workspace --exclude source-postgres --exclude dest-postgres --exclude transform-sql --exclude test-source --exclude test-destination --exclude test-transform -- -D warnings
```
Expected: PASS

Phase 2 complete. The SDK, runtime, engine, and CLI all compile and test clean against v7 types. Plugins are excluded — they're rebuilt in Phase 4.
