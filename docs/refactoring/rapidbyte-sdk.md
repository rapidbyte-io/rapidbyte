# Refactoring Guide: `rapidbyte-sdk`

> Organized around [`REFACTORING_PRINCIPLES.md`](../../REFACTORING_PRINCIPLES.md).
> Each item is tagged with a **priority tier** and **effort estimate**.

## Legend

| Tag | Meaning |
|-----|---------|
| **P0** | Do first — blocks other refactors or has outsized safety/quality impact |
| **P1** | Important — clear value, moderate risk |
| **P2** | Polish — nice-to-have, low risk, do when touching nearby code |
| **S** | Small — < 1 hour |
| **M** | Medium — 1-4 hours |
| **L** | Large — half day or more |

---

## File Inventory

| File | Lines | Domain | Tests? |
|------|------:|--------|--------|
| `protocol.rs` | 805 | Wire types, enums, serde models | Yes (460 lines) |
| `connector.rs` | 563 | Traits + `*_connector_main!` macros | Yes (trait shape only) |
| `errors.rs` | 462 | Error model, builder, Display | Yes |
| `manifest.rs` | 448 | Connector manifest, permissions, roles | Yes |
| `host_ffi.rs` | 308 | Guest-side host import wrappers | No |
| `host_tcp.rs` | 121 | AsyncRead/AsyncWrite over host sockets | No |
| `validation.rs` | 96 | PG identifier validation | Yes |
| `prelude.rs` | 15 | Re-exports | No |
| `lib.rs` | 10 | Module declarations | No |
| **Total** | **~2,828** | | |

---

## 1. Structural & Type-Level Principles

> *REFACTORING_PRINCIPLES.md* &sect;2 — Make invalid states unrepresentable.

### 1.1 Replace `protocol_version: String` with an enum &mdash; P0 / S

**File:** `protocol.rs:51-55` (`ConnectorInfo`), `manifest.rs:180` (`ConnectorManifest`), `host_ffi.rs:173-176`

**Problem:** `protocol_version` is a free-form `String` in `ConnectorInfo`, `ConnectorManifest`, and the `serde_json::json!` envelopes in `host_ffi.rs`. Nothing prevents `"banana"` from being used as a protocol version. The host already hard-codes `"2"` everywhere.

**After:**
```rust
/// Wire protocol version for host/guest communication.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ProtocolVersion {
    #[serde(rename = "2")]
    V2,
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        Self::V2
    }
}
```

Then replace `protocol_version: String` with `protocol_version: ProtocolVersion` in:
- `ConnectorInfo`
- `ConnectorManifest`
- `PayloadEnvelope`

And remove all hard-coded `"2"` string literals in `host_ffi.rs:173`, `host_ffi.rs:199`.

**Why P0:** This is the simplest "make invalid states unrepresentable" win. It prevents protocol mismatch bugs at compile time, and adding a future `V3` forces handling everywhere via exhaustive match.

---

### 1.2 Type `ColumnSchema.data_type` as an enum &mdash; P1 / M

**File:** `protocol.rs:12-16`

**Problem:** `data_type` is a `String`. Connectors produce values like `"Int64"`, `"Utf8"`, `"Timestamp"` etc., but nothing constrains the vocabulary. A typo like `"int64"` vs `"Int64"` would silently pass serialization and fail at runtime.

**After:**
```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "PascalCase")]
pub enum ArrowDataType {
    Boolean,
    Int8, Int16, Int32, Int64,
    UInt8, UInt16, UInt32, UInt64,
    Float16, Float32, Float64,
    Utf8, LargeUtf8,
    Binary, LargeBinary,
    Date32, Date64,
    #[serde(rename = "Timestamp[ms]")]
    TimestampMillis,
    #[serde(rename = "Timestamp[us]")]
    TimestampMicros,
    #[serde(rename = "Timestamp[ns]")]
    TimestampNanos,
    Decimal128,
    Json,
    /// Escape hatch for types not yet enumerated.
    #[serde(untagged)]
    Other(String),
}
```

The `Other(String)` variant ensures backwards compatibility while guiding connector authors toward valid types.

**Impact:** Touches `ColumnSchema`, `SchemaHint::Columns`, and every connector that constructs column schemas. Coordinate with `source-postgres` and `dest-postgres`.

---

### 1.3 Newtype `ErrorCode` for `ConnectorError.code` &mdash; P2 / S

**File:** `errors.rs:94`

**Problem:** `code` is a bare `String`. Error codes like `"MISSING_HOST"`, `"BATCH_TOO_LARGE"` are stringly-typed conventions. A newtype would document expectations without breaking the wire format.

**After:**
```rust
/// Opaque error code following SCREAMING_SNAKE_CASE convention.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ErrorCode(pub String);

impl ErrorCode {
    pub fn new(code: impl Into<String>) -> Self {
        Self(code.into())
    }
}
```

Builder methods like `ConnectorError::config("MISSING_HOST", ...)` would accept `impl Into<ErrorCode>`, which auto-converts from `&str` via `From`.

---

### 1.4 Fix `DlqRecord` stringly-typed fields &mdash; P1 / S

**File:** `protocol.rs:328-334`

**Problem:** `DlqRecord` uses `String` for `error_category` and `failed_at`:

```rust
pub struct DlqRecord {
    pub error_category: String,   // should be ErrorCategory
    pub failed_at: String,        // should be a timestamp type or at least documented format
    ...
}
```

`error_category` should reuse `ErrorCategory` from `errors.rs`. `failed_at` should at minimum be documented as ISO 8601, or better, use `chrono::DateTime<Utc>` (if the dependency is acceptable) or a newtype `Iso8601Timestamp(String)`.

**After:**
```rust
pub struct DlqRecord {
    pub stream_name: String,
    pub record_json: String,
    pub error_message: String,
    pub error_category: ErrorCategory,
    pub failed_at: String, // ISO 8601 — consider newtype later
}
```

This reuses an existing enum rather than relying on string matching.

---

### 1.5 Split `protocol.rs` god module &mdash; P0 / M

**File:** `protocol.rs` (805 lines)

**Problem:** `protocol.rs` contains 30+ types spanning at least 4 domains:
- **Wire types:** `SyncMode`, `WriteMode`, `SchemaHint`, `CursorType`, `CursorValue`, `CursorInfo`
- **Summaries:** `ReadSummary`, `WriteSummary`, `TransformSummary`, `ReadPerf`, `WritePerf`
- **Checkpoints & state:** `Checkpoint`, `CheckpointKind`, `StateScope`, `Metric`, `MetricValue`
- **Stream config:** `StreamContext`, `StreamLimits`, `StreamPolicies`, schema evolution policy types
- **Envelopes:** `PayloadEnvelope`, `DlqRecord`

At 805 lines this is approaching the &sect;4 "avoid god-modules" threshold. The tests alone are 460 lines, making navigation difficult.

**Proposed split:**

```
protocol/
  mod.rs           # Re-exports everything (public API unchanged)
  wire.rs          # SyncMode, WriteMode, Feature, ConnectorRole, ColumnSchema, SchemaHint
  cursor.rs        # CursorType, CursorValue, CursorInfo
  stream.rs        # StreamContext, StreamLimits, StreamPolicies, schema evolution types
  checkpoint.rs    # Checkpoint, CheckpointKind, StateScope
  summary.rs       # ReadSummary, WriteSummary, TransformSummary, ReadPerf, WritePerf
  envelope.rs      # PayloadEnvelope, DlqRecord, ConnectorInfo, Metric, MetricValue
```

**Migration strategy:**
1. Create `protocol/mod.rs` with `pub use` re-exports for every public type.
2. Move types one sub-module at a time, compiling between each move.
3. The public API (`use rapidbyte_sdk::protocol::SyncMode`) stays identical.

**Why P0:** This is the highest-leverage structural change. It makes every subsequent refactoring item in this guide easier because files stay small and focused.

---

### 1.6 Split `manifest.rs` by domain &mdash; P1 / M

**File:** `manifest.rs` (448 lines)

**Problem:** `manifest.rs` mixes three domains:
- **Permissions** (lines 9-65): `TlsRequirement`, `NetworkPermissions`, `FsPermissions`, `EnvPermissions`, `Permissions`
- **Roles & capabilities** (lines 71-124): `SourceFeature`, `SourceCapabilities`, `DestinationFeature`, etc.
- **Manifest root** (lines 130-211): `ArtifactInfo`, `ConnectorManifest`

**Proposed split:**

```
manifest/
  mod.rs           # Re-exports, ConnectorManifest
  permissions.rs   # TlsRequirement, NetworkPermissions, FsPermissions, EnvPermissions, Permissions
  roles.rs         # SourceFeature, SourceCapabilities, DestinationFeature, etc.
  artifact.rs      # ArtifactInfo
```

---

### 1.7 Extract `rapidbyte-types` crate &mdash; P0 / L

**Problem:** `rapidbyte-core` (the host-side orchestrator) has a Cargo dependency on `rapidbyte-sdk` (the guest-side connector SDK). While the actual imports are clean today &mdash; core only uses `protocol`, `errors`, and `manifest` types, never `host_ffi`, `host_tcp`, or connector traits &mdash; the Cargo-level dependency is a boundary leak:

1. `rapidbyte-core` pulls in `wit-bindgen = "0.46"` and `tokio` (with WASI `io-util` features) transitively through SDK, even though the host never needs these.
2. Nothing at the Cargo level prevents a future developer from `use rapidbyte_sdk::host_ffi` in host code.
3. The SDK's purpose is "connector authoring" &mdash; the host shouldn't depend on it at all.

**Current dependency graph:**
```
connectors ──→ rapidbyte-sdk (wit-bindgen, tokio/wasi)
rapidbyte-core ──→ rapidbyte-sdk (LEAKS wit-bindgen, tokio/wasi into host)
rapidbyte-cli ──→ rapidbyte-core
```

**Proposed dependency graph:**
```
connectors ──→ rapidbyte-sdk ──→ rapidbyte-types
rapidbyte-core ──→ rapidbyte-types (NO sdk dependency)
rapidbyte-cli ──→ rapidbyte-core ──→ rapidbyte-types
```

**Types that move to `rapidbyte-types`** (everything core imports from SDK today):
- `protocol::*`: `Catalog`, `Checkpoint`, `CheckpointKind`, `ColumnPolicy`, `ConnectorRole`, `CursorInfo`, `CursorType`, `CursorValue`, `DataErrorPolicy`, `DlqRecord`, `Iso8601Timestamp`, `NullabilityPolicy`, `PayloadEnvelope`, `ProtocolVersion`, `ReadSummary`, `SchemaEvolutionPolicy`, `SchemaHint`, `StateScope`, `StreamContext`, `StreamLimits`, `StreamPolicies`, `SyncMode`, `TransformSummary`, `TypeChangePolicy`, `WriteMode`, `WriteSummary`
- `errors::*`: `BackoffClass`, `CommitState`, `ConnectorError`, `ErrorCategory`, `ErrorScope`, `ValidationResult`, `ValidationStatus`
- `manifest::*`: `ConnectorManifest`, `Permissions` (and all sub-types)

**What stays in `rapidbyte-sdk`** (guest-only code):
- `connector.rs` &mdash; traits (`Source`, `Destination`, `Transform`) + macros (`*_connector_main!`)
- `host_ffi.rs` &mdash; guest-side host import wrappers + `wit_bindgen` bindings
- `host_tcp.rs` &mdash; `AsyncRead`/`AsyncWrite` over host sockets
- `prelude.rs` &mdash; re-exports from both `rapidbyte-types` + sdk-specific items

**`rapidbyte-types` dependencies** (minimal):
- `serde`, `serde_json` &mdash; for `Serialize`/`Deserialize`
- `thiserror` &mdash; for error `Display` impls

**SDK dependencies that stay SDK-only:**
- `wit-bindgen = "0.46"` &mdash; only needed for guest WIT bindings
- `tokio = { ..., features = ["io-util"] }` &mdash; only needed for `host_tcp` `AsyncRead`/`AsyncWrite`

**CLI note:** `rapidbyte-cli/src/commands/scaffold.rs` uses `rapidbyte_sdk::prelude::*` in string templates for code generation. These string references to `rapidbyte_sdk` are correct since scaffold generates guest code. The CLI's `check.rs` only needs `ValidationStatus`, which moves to `rapidbyte-types`.

**Migration strategy:**
1. Create `crates/rapidbyte-types/` with `protocol/`, `errors/`, `manifest/` modules moved from SDK.
2. Add `rapidbyte-types` to the workspace `Cargo.toml`.
3. Update `rapidbyte-sdk` to `pub use rapidbyte_types::*` so downstream connectors see no API change.
4. Replace `rapidbyte-core`'s `rapidbyte-sdk` dependency with `rapidbyte-types`.
5. Update `rapidbyte-cli` imports (only `ValidationStatus` changes).

**Why P0:** This is a prerequisite for all other refactoring items. Extracting shared types first establishes the correct dependency boundary, ensuring that subsequent changes to protocol/error/manifest types don't accidentally couple the host to guest-only SDK code.

---

## 2. DRY & Macro Consolidation

> *REFACTORING_PRINCIPLES.md* &sect;6 — Ruthless DRY.

### 2.1 Consolidate three `*_connector_main!` macros &mdash; P0 / L

**File:** `connector.rs:268-446`

**Problem:** `source_connector_main!`, `dest_connector_main!`, and `transform_connector_main!` are ~90% identical. The shared code is already factored into `__rb_connector_common!` and `__rb_guest_lifecycle_methods!`, which is good. But the outer macros still duplicate:
- The `struct Rapidbyte*Component;` declaration
- The `impl ... Guest for ...` block structure
- The `export!()` invocation
- The `fn main() {}` stub

The role-specific code (e.g., `run_read`, `run_write`, `run_transform`) differs in:
1. The WIT world name (`"rapidbyte-source"`, `"rapidbyte-destination"`, `"rapidbyte-transform"`)
2. The trait method called (`read`, `write`, `transform`)
3. The summary type returned (`ReadSummary`, `WriteSummary`, `TransformSummary`)
4. The WIT export path

**Approach:** Introduce a single `connector_main!` macro that accepts a `role` parameter:

```rust
/// Export a connector component. Usage:
///
/// ```ignore
/// connector_main!(source, MySource);
/// connector_main!(destination, MyDest);
/// connector_main!(transform, MyTransform);
/// ```
#[macro_export]
macro_rules! connector_main {
    (source, $connector_type:ty) => { /* source variant */ };
    (destination, $connector_type:ty) => { /* dest variant */ };
    (transform, $connector_type:ty) => { /* transform variant */ };
}
```

Keep the old macro names as thin wrappers for backwards compatibility during migration:

```rust
#[macro_export]
macro_rules! source_connector_main {
    ($t:ty) => { $crate::connector_main!(source, $t); };
}
```

**Why P0:** Macro duplication is the single largest DRY violation in the SDK (~180 duplicated lines). Every new field added to a summary type requires editing three macros.

---

### 2.2 Unify error conversion between connector.rs and host_ffi.rs &mdash; P1 / M

**Files:** `connector.rs:124-166` (`to_component_error`), `host_ffi.rs:19-74` (`from_component_error`)

**Problem:** These two functions are exact mirrors — one converts SDK types to WIT component types, the other converts back. Both contain exhaustive `match` arms for `ErrorCategory`, `ErrorScope`, `BackoffClass`, and `CommitState`. When a new error variant is added, both must be updated.

**Approach:** Generate both conversion directions from a single mapping. Options:
1. **Macro approach** — A `map_enum!` helper macro that generates both `From` impls.
2. **Trait approach** — Implement `From<ErrorCategory> for bindings::...::ErrorCategory` and the reverse on the WIT-generated types.

Option 2 is cleaner but constrained by orphan rules (WIT types are generated in a `mod bindings` block). Option 1 is pragmatic:

```rust
macro_rules! map_enum_variants {
    ($sdk:ty, $wit:ty, $( $sdk_variant:ident => $wit_variant:ident ),+ $(,)?) => {
        fn to_wit(v: $sdk) -> $wit {
            match v { $( <$sdk>::$sdk_variant => <$wit>::$wit_variant, )+ }
        }
        fn from_wit(v: $wit) -> $sdk {
            match v { $( <$wit>::$wit_variant => <$sdk>::$sdk_variant, )+ }
        }
    };
}
```

---

### 2.3 Consolidate default validation and close methods &mdash; P2 / S

**File:** `connector.rs:20-25, 44-48, 64-68` (validate defaults), `connector.rs:31-33, 52-54, 73-75` (close defaults)

**Problem:** The default `validate` and `close` implementations are copy-pasted across all three traits:

```rust
// Identical in SourceConnector, DestinationConnector, TransformConnector
async fn validate(_config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: "Validation not implemented".to_string(),
    })
}

async fn close(&mut self) -> Result<(), ConnectorError> {
    Ok(())
}
```

**Approach:** Extract free functions:

```rust
/// Default validation response for connectors that don't implement validation.
pub fn default_validation<C>(_config: &C) -> Result<ValidationResult, ConnectorError> {
    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: "Validation not implemented".to_string(),
    })
}
```

Then reference from trait defaults: `async fn validate(config: &Self::Config) -> ... { default_validation(config) }`.

Alternatively, since each default body is a single expression, this is a minor DRY issue — acceptable to leave if the refactoring feels forced.

---

## 3. Narrative Flow & Readability

> *REFACTORING_PRINCIPLES.md* &sect;3 — Make the code easy to read top-to-bottom.

### 3.1 Use `thiserror` for Display impl &mdash; P2 / S

**File:** `errors.rs:64-88` (`Display` for `ErrorCategory`, `ErrorScope`), `errors.rs:283-295` (`Display` for `ConnectorError`)

**Problem:** Hand-written `Display` impls for enums are boilerplate-heavy:

```rust
impl std::fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config => write!(f, "config"),
            Self::Auth => write!(f, "auth"),
            // ... 7 more arms
        }
    }
}
```

**After (with `thiserror`):**
```rust
#[derive(Debug, Clone, Copy, thiserror::Error, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    #[error("config")]
    Config,
    #[error("auth")]
    Auth,
    // ...
}
```

For `ConnectorError`:
```rust
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize, PartialEq)]
#[error("[{category}/{scope}] {code} ({}): {message}", if *.retryable { "retryable" } else { "fatal" })]
pub struct ConnectorError { ... }
```

This removes the manual `impl std::error::Error for ConnectorError {}` line and all manual `Display` impls (~30 lines).

**Note:** `thiserror` is a dev-ergonomics dependency with zero runtime cost. Evaluate whether adding it to a WASM-targeting SDK crate is acceptable. If not, leave as-is.

---

### 3.2 Implement `TryFrom<i32>` for `StateScope` &mdash; P2 / S

**File:** `protocol.rs:248-260`

**Problem:** Manual conversion methods when idiomatic Rust uses `TryFrom`:

```rust
impl StateScope {
    pub fn from_i32(v: i32) -> Option<Self> { ... }
    pub fn to_i32(self) -> i32 { self as i32 }
}
```

**After:**
```rust
impl TryFrom<i32> for StateScope {
    type Error = i32; // or a dedicated error type
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Pipeline),
            1 => Ok(Self::Stream),
            2 => Ok(Self::ConnectorInstance),
            _ => Err(value),
        }
    }
}
```

Keep `to_i32()` as-is (it's just `self as i32`, which is idiomatic for `#[repr(i32)]` enums). Deprecate `from_i32()` in favor of `StateScope::try_from(v)`.

---

### 3.3 Typed validation errors in `validation.rs` &mdash; P2 / S

**File:** `validation.rs:1-40`

**Problem:** `validate_pg_identifier` returns `Result<(), String>`. String errors lose structure and can't be pattern-matched.

**After:**
```rust
#[derive(Debug, Clone, PartialEq)]
pub enum IdentifierError {
    Empty,
    TooLong { name: String, len: usize },
    InvalidStart { char: char },
    InvalidChar { char: char },
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "identifier must not be empty"),
            Self::TooLong { name, len } =>
                write!(f, "identifier '{}' exceeds 63 bytes (got {})", name, len),
            Self::InvalidStart { char } =>
                write!(f, "identifier must start with a letter or underscore, got '{}'", char),
            Self::InvalidChar { char } =>
                write!(f, "identifier contains invalid character '{}'", char),
        }
    }
}
```

---

## 4. Architecture & Tooling

> *REFACTORING_PRINCIPLES.md* &sect;4 — Module structure, Clippy, encapsulation.

### 4.1 Add module-level rustdoc to all files &mdash; P1 / S

**Problem:** Only `connector.rs`, `host_ffi.rs`, `host_tcp.rs`, and `prelude.rs` have `//!` module docs. Missing from:
- `protocol.rs` — No module doc
- `errors.rs` — No module doc
- `manifest.rs` — No module doc
- `validation.rs` — No module doc
- `lib.rs` — No module doc

**Action:** Add `//!` doc comments to every module. Example for `lib.rs`:

```rust
//! Rapidbyte Connector SDK.
//!
//! Provides traits, protocol types, and host-import wrappers for building
//! WASI-based data pipeline connectors. Connector authors should use the
//! [`prelude`] module for common imports and the `*_connector_main!` macros
//! to wire up the WIT component model.
```

**Why P1:** Module docs are the first thing a new contributor reads. Per &sect;6, "documentation is part of the API."

---

### 4.2 Replace `cfg` duplication in `host_ffi.rs` with a trait &mdash; P2 / L

**File:** `host_ffi.rs` (all 308 lines)

**Problem:** Every public function in `host_ffi.rs` is duplicated with `#[cfg(target_arch = "wasm32")]` / `#[cfg(not(target_arch = "wasm32"))]` blocks:

```rust
#[cfg(target_arch = "wasm32")]
pub fn log(level: i32, message: &str) {
    bindings::rapidbyte::connector::host::log(level as u32, message);
}

#[cfg(not(target_arch = "wasm32"))]
pub fn log(_level: i32, _message: &str) {}
```

This pattern repeats 11 times across `log`, `emit_batch`, `next_batch`, `state_get`, `state_put`, `state_compare_and_set`, `checkpoint`, `metric`, `emit_dlq_record`, `connect_tcp`, `socket_read`, `socket_write`, `socket_close`.

**Approach:** Define a `HostImports` trait and provide two implementations:

```rust
pub trait HostImports {
    fn log(&self, level: i32, message: &str);
    fn emit_batch(&self, ipc_bytes: &[u8]) -> Result<(), ConnectorError>;
    fn next_batch(&self, buf: &mut Vec<u8>, max_bytes: u64) -> Result<Option<usize>, ConnectorError>;
    // ... etc.
}

#[cfg(target_arch = "wasm32")]
pub struct WasmHostImports;

#[cfg(not(target_arch = "wasm32"))]
pub struct StubHostImports;
```

**Trade-off:** This changes the API surface from free functions to trait methods. It enables testing connectors against mock host imports on native targets — a significant testability win. However, it's a large change that ripples through every connector. Mark as P2 / L and tackle after the P0 items stabilize.

**Alternative (lighter touch):** Keep free functions, but extract a single `cfg`-gated `mod wasm_impl` and `mod stub_impl`, then delegate:

```rust
#[cfg(target_arch = "wasm32")]
mod wasm_impl;
#[cfg(not(target_arch = "wasm32"))]
mod stub_impl;

#[cfg(target_arch = "wasm32")]
pub use wasm_impl::*;
#[cfg(not(target_arch = "wasm32"))]
pub use stub_impl::*;
```

---

## 5. Polish: Naming, Layout, and Documentation

> *REFACTORING_PRINCIPLES.md* &sect;6 — Naming, DRY, layout, robustness.

### 5.1 Serde consistency: mixed `default` patterns &mdash; P2 / S

**Files:** `protocol.rs`, `manifest.rs`

**Problem:** The codebase mixes three patterns for serde defaults:

1. Named default functions: `#[serde(default = "default_checkpoint_interval_bytes")]`
2. Type-level `#[serde(default)]` on fields
3. Standalone `Default` impl blocks

Examples of inconsistency:
- `StreamLimits` uses a named function `default_checkpoint_interval_bytes()` (line 96-98) for one field but a manual `Default` impl (line 116-128) for the whole struct, where both specify `64 * 1024 * 1024`.
- `ConnectorManifest` uses `default_manifest_version()` (line 147-149) and `default_tls()` (line 36-38).

**Recommendation:** Standardize on `#[serde(default)]` at field level combined with a single `Default` impl. Remove named default functions when the `Default` impl already covers them. This prevents the value from being specified in two places.

---

### 5.2 Extract magic numbers as named constants &mdash; P1 / S

**File:** `protocol.rs:96-98, 117-128`

**Problem:** Magic numbers are scattered across default functions and `Default` impls:

```rust
fn default_checkpoint_interval_bytes() -> u64 {
    64 * 1024 * 1024 // 64 MB
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            max_batch_bytes: 64 * 1024 * 1024,  // 64 MB
            max_record_bytes: 16 * 1024 * 1024, // 16 MB
            max_inflight_batches: 16,
            ...
        }
    }
}
```

**After:**
```rust
/// Default maximum batch size: 64 MiB.
pub const DEFAULT_MAX_BATCH_BYTES: u64 = 64 * 1024 * 1024;
/// Default maximum record size: 16 MiB.
pub const DEFAULT_MAX_RECORD_BYTES: u64 = 16 * 1024 * 1024;
/// Default checkpoint interval: 64 MiB.
pub const DEFAULT_CHECKPOINT_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;
/// Default maximum in-flight batches.
pub const DEFAULT_MAX_INFLIGHT_BATCHES: u32 = 16;
```

These constants become the single source of truth. The `Default` impl and serde default functions reference them.

---

### 5.3 Fix naming stutters &mdash; P2 / S

**Files:** Various

Per &sect;6 "Avoid Stuttering":

| Current | Proposed | Location |
|---------|----------|----------|
| `ConnectorError::internal(...)` | Fine as-is (constructor pattern) | `errors.rs` |
| `connector::SourceConnector` | `connector::Source` | `connector.rs:15` |
| `connector::DestinationConnector` | `connector::Destination` | `connector.rs:38` |
| `connector::TransformConnector` | `connector::Transform` | `connector.rs:59` |

When accessed as `rapidbyte_sdk::connector::SourceConnector`, the word "Connector" appears twice. Renaming to `Source` / `Destination` / `Transform` reads better: `connector::Source`.

**Migration:** Type aliases for backwards compatibility:
```rust
pub type SourceConnector = Source;
pub type DestinationConnector = Destination;
pub type TransformConnector = Transform;
```

Deprecate old names and remove in next major version.

---

### 5.4 Expand `prelude.rs` &mdash; P2 / S

**File:** `prelude.rs`

**Problem:** The prelude re-exports protocol types and traits but omits several commonly-needed items:

Missing from prelude:
- `ConnectorResult` (the `Result`/`Err` tagged enum from `errors.rs`)
- `ErrorCategory`, `ErrorScope`, `BackoffClass`, `CommitState` (needed when constructing errors with builder methods)
- `Catalog`, `Stream`, `ColumnSchema` (needed in every source connector's `discover()`)
- `StateScope` (needed for `host_ffi::state_get/state_put`)

**Action:** Add the commonly-used types. Keep the prelude focused — don't re-export internal types like `PayloadEnvelope`.

---

### 5.5 Test coverage gaps &mdash; P1 / M

**Problem:** Several modules have zero test coverage:

| Module | Lines | Tests |
|--------|------:|-------|
| `host_ffi.rs` | 308 | 0 |
| `host_tcp.rs` | 121 | 0 |

`host_ffi.rs` is untestable on native because the `cfg(not(target_arch = "wasm32"))` stubs return hard-coded values. This is a direct consequence of the `cfg` duplication (see &sect;4.2). The trait-based approach would unlock testing with mock host imports.

**Short-term action:** Add tests for the non-`cfg`-gated code:
- `SocketReadResult` / `SocketWriteResult` enum construction
- The `from_component_error` conversion (would need to be made `pub(crate)` or tested in integration)

For `host_tcp.rs`, test the `HostTcpStream` state machine logic (close idempotency, closed flag behavior) by mocking `host_ffi` functions.

---

## 6. Performance

> *REFACTORING_PRINCIPLES.md* &sect;5 — Make it correct first, then fast. Measure.

### 6.1 Avoid `json!` intermediate allocation in `host_ffi.rs` &mdash; P2 / S

**File:** `host_ffi.rs:172-180, 197-204`

**Problem:** The `checkpoint` and `metric` functions build a `serde_json::Value` via `json!()` then immediately serialize it to a string:

```rust
let envelope = serde_json::json!({
    "protocol_version": "2",
    "connector_id": connector_id,
    "stream_name": stream_name,
    "payload": cp,
});
let payload_json = serde_json::to_string(&envelope)?;
```

`json!()` allocates a `serde_json::Value` tree (heap-allocated `Map`, `String` values), then `to_string` walks it again. This is two traversals and extra allocations.

**After:** Use `PayloadEnvelope<&Checkpoint>` directly (or a local struct):

```rust
let envelope = PayloadEnvelope {
    protocol_version: ProtocolVersion::V2, // after item 1.1
    connector_id: connector_id.to_string(),
    stream_name: stream_name.to_string(),
    payload: cp,
};
let payload_json = serde_json::to_string(&envelope)?;
```

This serializes directly from the struct — a single traversal, no intermediate `Value`.

**Caveat:** `PayloadEnvelope` uses `#[serde(flatten)]` on its payload field, which has known performance issues in serde (disables some optimizations). Profile before assuming this is faster. The real fix may be to replace `flatten` with explicit fields.

---

### 6.2 Avoid double copy in `next_batch` &mdash; P2 / S

**File:** `host_ffi.rs:95-111`

**Problem:**
```rust
pub fn next_batch(buf: &mut Vec<u8>, max_bytes: u64) -> Result<Option<usize>, ConnectorError> {
    let next = bindings::rapidbyte::connector::host::next_batch()
        .map_err(from_component_error)?;
    match next {
        Some(batch) => {
            // ...
            buf.clear();
            buf.extend_from_slice(&batch); // copies batch into buf
            Ok(Some(batch.len()))
        }
        // ...
    }
}
```

The host returns `batch: Vec<u8>`, then we copy it into `buf`. If the API returned the `Vec` directly, the caller could take ownership without copying.

**After (API change):**
```rust
pub fn next_batch(max_bytes: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
    let next = bindings::rapidbyte::connector::host::next_batch()
        .map_err(from_component_error)?;
    match next {
        Some(batch) if batch.len() as u64 > max_bytes => {
            Err(ConnectorError::internal("BATCH_TOO_LARGE", ...))
        }
        Some(batch) => Ok(Some(batch)),
        None => Ok(None),
    }
}
```

**Trade-off:** This changes the public API. The caller previously reused a `buf` across calls (amortizing allocation). Returning `Vec<u8>` transfers ownership but allocates each time. Profile to determine which pattern is better for the hot path.

---

## 7. Execution Order

### Phase 1: Structural Foundation (P0 items)

```
1.7  Extract rapidbyte-types    ─── Do first (prerequisite for all others)
     │
     ▼
1.5  Split protocol.rs          ─┐
1.1  ProtocolVersion enum        ├─ Can run in parallel (after 1.7)
2.1  Consolidate macros         ─┘
```

Do **1.7** first — it establishes the correct crate boundary so all subsequent type changes land in the right crate. Then **1.5** makes protocol files small and focused.

### Phase 2: Type Safety & DRY (P1 items)

```
1.2  ArrowDataType enum        (depends on 1.5 — touches protocol types)
1.4  DlqRecord type fix        (depends on 1.5)
1.6  Split manifest.rs         (independent)
2.2  Unify error conversion    (independent)
4.1  Module-level rustdoc      (independent — do alongside any file you touch)
5.2  Magic number constants    (depends on 1.5)
5.5  Test coverage gaps        (independent — ongoing)
```

### Phase 3: Polish (P2 items)

```
1.3  ErrorCode newtype
2.3  Default method consolidation
3.1  thiserror adoption
3.2  TryFrom for StateScope
3.3  Typed validation errors
4.2  Trait-based host_ffi
5.1  Serde consistency
5.3  Naming stutters
5.4  Prelude expansion
6.1  json! intermediate allocation
6.2  next_batch double copy
```

Phase 3 items can be done opportunistically whenever touching nearby code.

---

## 8. Summary Table

| # | Item | Principle | Priority | Effort | Primary File(s) |
|---|------|-----------|----------|--------|-----------------|
| 1.1 | `ProtocolVersion` enum | &sect;2 Types | **P0** | S | `protocol.rs`, `manifest.rs`, `host_ffi.rs` |
| 1.2 | `ArrowDataType` enum | &sect;2 Types | P1 | M | `protocol.rs` |
| 1.3 | `ErrorCode` newtype | &sect;2 Types | P2 | S | `errors.rs` |
| 1.4 | `DlqRecord` type fix | &sect;2 Types | P1 | S | `protocol.rs`, `errors.rs` |
| 1.5 | Split `protocol.rs` | &sect;4 Architecture | **P0** | M | `protocol.rs` |
| 1.6 | Split `manifest.rs` | &sect;4 Architecture | P1 | M | `manifest.rs` |
| 1.7 | Extract `rapidbyte-types` crate | &sect;4 Architecture | **P0** | L | `protocol.rs`, `errors.rs`, `manifest.rs`, `Cargo.toml` |
| 2.1 | Consolidate macros | &sect;6 DRY | **P0** | L | `connector.rs` |
| 2.2 | Unify error conversion | &sect;6 DRY | P1 | M | `connector.rs`, `host_ffi.rs` |
| 2.3 | Default method consolidation | &sect;6 DRY | P2 | S | `connector.rs` |
| 3.1 | `thiserror` adoption | &sect;3 Readability | P2 | S | `errors.rs` |
| 3.2 | `TryFrom<i32>` for `StateScope` | &sect;3 Readability | P2 | S | `protocol.rs` |
| 3.3 | Typed validation errors | &sect;3 Readability | P2 | S | `validation.rs` |
| 4.1 | Module-level rustdoc | &sect;6 Documentation | P1 | S | All files |
| 4.2 | Trait-based `host_ffi` | &sect;4 Architecture | P2 | L | `host_ffi.rs` |
| 5.1 | Serde consistency | &sect;6 Polish | P2 | S | `protocol.rs`, `manifest.rs` |
| 5.2 | Magic number constants | &sect;6 Polish | P1 | S | `protocol.rs` |
| 5.3 | Naming stutters | &sect;6 Naming | P2 | S | `connector.rs` |
| 5.4 | Prelude expansion | &sect;6 Polish | P2 | S | `prelude.rs` |
| 5.5 | Test coverage gaps | &sect;4 Tooling | P1 | M | `host_ffi.rs`, `host_tcp.rs` |
| 6.1 | `json!` intermediate alloc | &sect;5 Performance | P2 | S | `host_ffi.rs` |
| 6.2 | `next_batch` double copy | &sect;5 Performance | P2 | S | `host_ffi.rs` |

**Totals:** 4 P0, 7 P1, 11 P2 &mdash; 22 items.
