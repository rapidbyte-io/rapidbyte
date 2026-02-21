# Async-First SDK DX Overhaul

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate `Default` + `Option<Config>` anti-patterns from connector traits by introducing an associated `Config` type, turning `open()` into an async constructor (`connect()`), and absorbing all Tokio/WIT boilerplate into the SDK macros.

**Architecture:** The new SDK traits use `async fn` and an associated `type Config: DeserializeOwned`. The macros create the Tokio runtime, deserialize config JSON, call the async constructor, and store the fully-initialized connector instance. Connectors never touch `OnceLock`, `RefCell`, `block_on`, or `OpenContext` directly. No backward compatibility — clean break.

**Tech Stack:** Rust, `serde::de::DeserializeOwned`, `tokio` (current_thread), `wit-bindgen 0.46`, `wasmtime 41` component model

---

## Language Agnosticism

This plan changes **only the Rust SDK layer** — it does NOT touch the WIT interface (`wit/rapidbyte-connector.wit`), which is the true language-agnostic contract. The architecture has three layers:

```
┌─────────────────────────────────────────────────────┐
│  Layer 3: Language SDKs (Rust, Go, Python, ...)     │  ← This plan changes the Rust SDK only
│  - Idiomatic DX per language                        │
│  - Config parsing, async runtime, type safety       │
│  - Generates WIT bindings under the hood            │
├─────────────────────────────────────────────────────┤
│  Layer 2: WIT Interface (rapidbyte:connector@2.0.0) │  ← UNCHANGED — the real contract
│  - open(config-json: string)                        │
│  - run-read(ctx-json: string) → read-summary        │
│  - Host imports: emit-batch, next-batch, etc.       │
├─────────────────────────────────────────────────────┤
│  Layer 1: Wasmtime Component Runtime (host)         │  ← UNCHANGED
│  - Loads .wasm, calls exports, provides imports     │
└─────────────────────────────────────────────────────┘
```

**Any language that can compile to `wasm32-wasip2` and implement the WIT exports can be a connector.** The Rust SDK is one convenience layer; Go, Python, or C connectors would implement the same WIT interfaces directly (or via their own language SDK).

### What the WIT contract requires (language-agnostic)

A connector is a WASI component that exports one of:
- `source-connector` interface: `open`, `discover`, `validate`, `run-read`, `close`
- `dest-connector` interface: `open`, `validate`, `run-write`, `close`
- `transform-connector` interface: `open`, `validate`, `run-transform`, `close`

All data crosses the boundary as:
- **Config:** JSON string (`open(config-json: string)`)
- **Stream context:** JSON string (`run-read(ctx-json: string)`)
- **Batches:** Arrow IPC bytes (`list<u8>` via `emit-batch`/`next-batch`)
- **State/checkpoints:** JSON strings
- **Errors:** structured `connector-error` record

### DX pattern equivalence in other languages

The Rust SDK patterns map cleanly to other languages:

| Rust SDK concept | Go equivalent | Python equivalent |
|------------------|--------------|-------------------|
| `type Config: DeserializeOwned` | `type Config struct` + `json.Unmarshal` | `@dataclass Config` + `json.loads` |
| `async fn connect(config) -> (Self, OpenInfo)` | `func Connect(config Config) (*Connector, OpenInfo, error)` | `@classmethod async def connect(cls, config: Config) -> tuple[Self, OpenInfo]` |
| `async fn read(&mut self, ctx)` | `func (c *Connector) Read(ctx StreamContext) (ReadSummary, error)` | `async def read(self, ctx: StreamContext) -> ReadSummary` |
| `source_connector_main!` macro | `func main()` + `wit-bindgen-go` generated exports | `if __name__` + `componentize-py` |
| Tokio runtime in macro | goroutine-native (no runtime needed) | `asyncio.run` in component shim |

A Go connector implementing the same WIT interface would look like:

```go
// connectors/source-mysql/main.go
type SourceMySQL struct {
    config Config
    db     *sql.DB
}

func Connect(config Config) (*SourceMySQL, OpenInfo, error) {
    db, err := sql.Open("mysql", config.DSN())
    if err != nil {
        return nil, OpenInfo{}, err
    }
    return &SourceMySQL{config: config, db: db},
           OpenInfo{ProtocolVersion: "2", Features: []string{}},
           nil
}

func (s *SourceMySQL) Read(ctx StreamContext) (ReadSummary, error) {
    // Pure business logic — no framework boilerplate
}
```

### Invariants this plan preserves

1. **WIT interface unchanged** — no modifications to `wit/rapidbyte-connector.wit`
2. **JSON-over-string protocol** — config and context are opaque JSON strings at the WIT boundary
3. **Arrow IPC for data** — `list<u8>` byte arrays, language-agnostic
4. **No Rust types in the contract** — `connector-error`, `read-summary`, etc. are WIT records, not Rust structs
5. **Host doesn't know or care what language** produced the `.wasm` component

---

## Current Pain Points (for context)

| Problem | Where | Root cause |
|---------|-------|------------|
| `config: Option<Config>` + `.unwrap()` in every method | `source-postgres/src/main.rs:14`, `dest-postgres/src/main.rs:13` | Trait requires `Default`, config only available after `open()` |
| `config::create_runtime()` + `rt.block_on(...)` in every method | `source-postgres/src/main.rs:39,55,83` | Trait methods are sync, but real work is async |
| `Config::from_open_context()` boilerplate in every connector | `source-postgres/src/config.rs:22-29`, `dest-postgres/src/config.rs:34-51` | SDK doesn't handle config parsing |
| `to_component_error`, `parse_open_context`, `parse_stream_context` duplicated 3x | `connector.rs:70-164, 279-373, 475-569` | Each macro duplicates all helper functions |

---

## Task 1: New SDK Traits (`connector.rs` trait definitions)

**Files:**
- Modify: `crates/rapidbyte-sdk/src/connector.rs:1-41`

**Step 1: Write the failing test**

Create a compile-test that asserts the new trait shape is usable. Add to end of `crates/rapidbyte-sdk/src/connector.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{ConnectorError, ValidationResult, ValidationStatus};
    use crate::protocol::{Catalog, OpenInfo, ReadSummary, StreamContext, WriteSummary, TransformSummary};
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestConfig {
        host: String,
    }

    struct TestSource {
        config: TestConfig,
    }

    impl SourceConnector for TestSource {
        type Config = TestConfig;

        async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {
            Ok((
                Self { config },
                OpenInfo {
                    protocol_version: "2".to_string(),
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn discover(&mut self) -> Result<Catalog, ConnectorError> {
            Ok(Catalog { streams: vec![] })
        }

        async fn read(&mut self, _ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {
            Ok(ReadSummary {
                records_read: 0,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
                perf: None,
            })
        }
    }

    struct TestDest {
        config: TestConfig,
    }

    impl DestinationConnector for TestDest {
        type Config = TestConfig;

        async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {
            Ok((
                Self { config },
                OpenInfo {
                    protocol_version: "2".to_string(),
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn write(&mut self, _ctx: StreamContext) -> Result<WriteSummary, ConnectorError> {
            Ok(WriteSummary {
                records_written: 0,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
                perf: None,
            })
        }
    }

    struct TestTransform {
        config: TestConfig,
    }

    impl TransformConnector for TestTransform {
        type Config = TestConfig;

        async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {
            Ok((
                Self { config },
                OpenInfo {
                    protocol_version: "2".to_string(),
                    features: vec![],
                    default_max_batch_bytes: 64 * 1024 * 1024,
                },
            ))
        }

        async fn transform(&mut self, _ctx: StreamContext) -> Result<TransformSummary, ConnectorError> {
            Ok(TransformSummary {
                records_in: 0,
                records_out: 0,
                bytes_in: 0,
                bytes_out: 0,
                batches_processed: 0,
            })
        }
    }

    #[test]
    fn test_trait_shapes_compile() {
        // Compile-time verification that the trait shapes work.
        // The test passing means the associated types and async fns compile.
        fn assert_source<T: SourceConnector>() {}
        fn assert_dest<T: DestinationConnector>() {}
        fn assert_transform<T: TransformConnector>() {}
        assert_source::<TestSource>();
        assert_dest::<TestDest>();
        assert_transform::<TestTransform>();
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-sdk -- tests::test_trait_shapes_compile`
Expected: FAIL — traits still require `Default`, don't have `Config` associated type or `connect()`

**Step 3: Replace the trait definitions**

Replace lines 1-41 of `crates/rapidbyte-sdk/src/connector.rs` (the trait block only, not the macros) with:

```rust
//! Async-first connector traits.
//!
//! Connectors implement one of the traits below. The `*_connector_main!`
//! macros handle Tokio runtime, config deserialization, and WIT bindings.

use serde::de::DeserializeOwned;

use crate::errors::{ConnectorError, ValidationResult, ValidationStatus};
use crate::protocol::{
    Catalog, OpenInfo, ReadSummary, StreamContext, TransformSummary, WriteSummary,
};

/// Source connector lifecycle.
///
/// `Config` is your connector's strongly-typed configuration struct.
/// The SDK macro deserializes the JSON config and passes it to `connect()`.
/// By the time `read()` is called, the connector is fully initialized.
pub trait SourceConnector: Sized {
    type Config: DeserializeOwned;

    /// Async constructor. Replaces `Default` + `open()`.
    /// Return the initialized connector and its capabilities.
    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError>;

    /// Validate connectivity. Default: success.
    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: "Validation not implemented".to_string(),
        })
    }

    /// Discover available streams.
    async fn discover(&mut self) -> Result<Catalog, ConnectorError>;

    /// Read a single stream.
    async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError>;

    /// Teardown. Default: no-op.
    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Destination connector lifecycle.
pub trait DestinationConnector: Sized {
    type Config: DeserializeOwned;

    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError>;

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: "Validation not implemented".to_string(),
        })
    }

    async fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError>;

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Transform connector lifecycle.
pub trait TransformConnector: Sized {
    type Config: DeserializeOwned;

    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError>;

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: "Validation not implemented".to_string(),
        })
    }

    async fn transform(&mut self, ctx: StreamContext) -> Result<TransformSummary, ConnectorError>;

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}
```

**Step 4: Update prelude**

Remove `OpenContext` and `ConfigBlob` from `crates/rapidbyte-sdk/src/prelude.rs` (connectors no longer need them):

```rust
pub use crate::protocol::{
    Catalog, Checkpoint, CheckpointKind, CursorInfo, CursorValue, Feature, Metric,
    MetricValue, OpenInfo, ReadPerf, ReadSummary, SchemaHint, StreamContext,
    StreamLimits, SyncMode, TransformSummary, WriteMode, WritePerf, WriteSummary,
};
```

Note: `OpenContext` and `ConfigBlob` stay in `protocol.rs` (the macros still use them internally), they're just removed from the public prelude since connector authors don't interact with them anymore.

**Step 5: Add `serde` dependency to SDK Cargo.toml**

The `DeserializeOwned` bound requires the `serde` crate in scope. Check `crates/rapidbyte-sdk/Cargo.toml` — `serde` is already a dependency with `derive` feature. No change needed.

**Step 6: Run test to verify it passes**

Run: `cargo test -p rapidbyte-sdk -- tests::test_trait_shapes_compile`
Expected: PASS

**Step 7: Commit**

```bash
git add crates/rapidbyte-sdk/src/connector.rs crates/rapidbyte-sdk/src/prelude.rs
git commit -m "feat(sdk): async-first connector traits with associated Config type"
```

---

## Task 2: Rewrite SDK Macros (absorb all boilerplate)

**Files:**
- Modify: `crates/rapidbyte-sdk/src/connector.rs` (macro sections, lines 43-645)

The three macros (`source_connector_main!`, `dest_connector_main!`, `transform_connector_main!`) need rewriting. The key changes:

1. Create Tokio runtime in the macro (connectors never see it)
2. Parse config JSON → call `<T>::connect(config)` in `open()` → store the connector instance directly in `CONNECTOR` `OnceLock`
3. `CONNECTOR` holds `Option<$connector_type>` — no wrapper struct, just the user's type
4. Store raw `config_json: String` in a separate `OnceLock<String>` so `validate()` can re-parse it (since `validate` takes `&Config`, not `&mut self`, and the config was consumed by `connect()`)
5. Keep `to_component_error`, `parse_stream_context`, `to_component_validation` as-is (they're macro-internal)
6. Remove `parse_open_context` — the macro handles config parsing directly

**Step 1: No separate test** — this is a macro rewrite that will be tested via the connector builds in Task 3.

**Step 2: Rewrite `source_connector_main!`**

Replace the entire `source_connector_main!` macro (lines 43-250) with:

```rust
/// Export a source connector component for `rapidbyte-source` world.
#[macro_export]
macro_rules! source_connector_main {
    ($connector_type:ty) => {
        mod __rb_source_bindings {
            rapidbyte_sdk::wit_bindgen::generate!({
                path: "../../wit",
                world: "rapidbyte-source",
            });
        }

        use std::cell::RefCell;
        use std::sync::OnceLock;

        use rapidbyte_sdk::errors::{BackoffClass, CommitState, ErrorCategory, ErrorScope};

        // Note: These statics live inside the macro expansion, so each connector
        // type that invokes the macro gets its own distinct static variables.
        // This is correct and thread-safe within the WASI single-threaded environment.
        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        static CONFIG_JSON: OnceLock<String> = OnceLock::new();

        fn get_runtime() -> &'static tokio::runtime::Runtime {
            RUNTIME.get_or_init(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create guest tokio runtime")
            })
        }

        struct SyncRefCell(RefCell<Option<$connector_type>>);
        unsafe impl Sync for SyncRefCell {}

        static CONNECTOR: OnceLock<SyncRefCell> = OnceLock::new();

        fn get_state() -> &'static RefCell<Option<$connector_type>> {
            &CONNECTOR.get_or_init(|| SyncRefCell(RefCell::new(None))).0
        }

        fn to_component_error(
            error: rapidbyte_sdk::errors::ConnectorError,
        ) -> __rb_source_bindings::rapidbyte::connector::types::ConnectorError {
            use __rb_source_bindings::rapidbyte::connector::types::{
                BackoffClass as CBClass, CommitState as CState, ConnectorError as CErr,
                ErrorCategory as Cat, ErrorScope as Scope,
            };

            CErr {
                category: match error.category {
                    ErrorCategory::Config => Cat::Config,
                    ErrorCategory::Auth => Cat::Auth,
                    ErrorCategory::Permission => Cat::Permission,
                    ErrorCategory::RateLimit => Cat::RateLimit,
                    ErrorCategory::TransientNetwork => Cat::TransientNetwork,
                    ErrorCategory::TransientDb => Cat::TransientDb,
                    ErrorCategory::Data => Cat::Data,
                    ErrorCategory::Schema => Cat::Schema,
                    ErrorCategory::Internal => Cat::Internal,
                },
                scope: match error.scope {
                    ErrorScope::Stream => Scope::PerStream,
                    ErrorScope::Batch => Scope::PerBatch,
                    ErrorScope::Record => Scope::PerRecord,
                },
                code: error.code,
                message: error.message,
                retryable: error.retryable,
                retry_after_ms: error.retry_after_ms,
                backoff_class: match error.backoff_class {
                    BackoffClass::Fast => CBClass::Fast,
                    BackoffClass::Normal => CBClass::Normal,
                    BackoffClass::Slow => CBClass::Slow,
                },
                safe_to_retry: error.safe_to_retry,
                commit_state: error.commit_state.map(|s| match s {
                    CommitState::BeforeCommit => CState::BeforeCommit,
                    CommitState::AfterCommitUnknown => CState::AfterCommitUnknown,
                    CommitState::AfterCommitConfirmed => CState::AfterCommitConfirmed,
                }),
                details_json: error.details.map(|v| v.to_string()),
            }
        }

        fn parse_stream_context(
            ctx_json: String,
        ) -> Result<
            rapidbyte_sdk::protocol::StreamContext,
            __rb_source_bindings::rapidbyte::connector::types::ConnectorError,
        > {
            serde_json::from_str(&ctx_json).map_err(|e| {
                to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                    "INVALID_STREAM_CTX",
                    format!("Invalid StreamContext JSON: {}", e),
                ))
            })
        }

        fn to_component_validation(
            result: rapidbyte_sdk::errors::ValidationResult,
        ) -> __rb_source_bindings::rapidbyte::connector::types::ValidationResult {
            use __rb_source_bindings::rapidbyte::connector::types::{
                ValidationResult as CValidationResult, ValidationStatus as CValidationStatus,
            };
            use rapidbyte_sdk::errors::ValidationStatus;

            CValidationResult {
                status: match result.status {
                    ValidationStatus::Success => CValidationStatus::Success,
                    ValidationStatus::Failed => CValidationStatus::Failed,
                    ValidationStatus::Warning => CValidationStatus::Warning,
                },
                message: result.message,
            }
        }

        struct RapidbyteSourceComponent;

        impl __rb_source_bindings::exports::rapidbyte::connector::source_connector::Guest
            for RapidbyteSourceComponent
        {
            fn open(
                config_json: String,
            ) -> Result<(), __rb_source_bindings::rapidbyte::connector::types::ConnectorError>
            {
                // Store raw JSON for validate() to re-parse later
                let _ = CONFIG_JSON.set(config_json.clone());

                let config: <$connector_type as rapidbyte_sdk::connector::SourceConnector>::Config =
                    serde_json::from_str(&config_json).map_err(|e| {
                        to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                            "INVALID_CONFIG",
                            format!("Config parse error: {}", e),
                        ))
                    })?;

                let rt = get_runtime();
                let (instance, _open_info) = rt
                    .block_on(
                        <$connector_type as rapidbyte_sdk::connector::SourceConnector>::connect(
                            config,
                        ),
                    )
                    .map_err(to_component_error)?;

                // Store just the initialized connector instance
                let state_cell = get_state();
                *state_cell.borrow_mut() = Some(instance);

                Ok(())
            }

            fn discover(
            ) -> Result<String, __rb_source_bindings::rapidbyte::connector::types::ConnectorError>
            {
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                let conn = state_ref.as_mut().expect("Connector not opened");

                let catalog = rt
                    .block_on(
                        <$connector_type as rapidbyte_sdk::connector::SourceConnector>::discover(
                            conn,
                        ),
                    )
                    .map_err(to_component_error)?;

                serde_json::to_string(&catalog).map_err(|e| {
                    to_component_error(rapidbyte_sdk::errors::ConnectorError::internal(
                        "SERIALIZE_CATALOG",
                        e.to_string(),
                    ))
                })
            }

            fn validate(
            ) -> Result<
                __rb_source_bindings::rapidbyte::connector::types::ValidationResult,
                __rb_source_bindings::rapidbyte::connector::types::ConnectorError,
            > {
                let json = CONFIG_JSON.get().expect("open must be called before validate");
                let config: <$connector_type as rapidbyte_sdk::connector::SourceConnector>::Config =
                    serde_json::from_str(json).map_err(|e| {
                        to_component_error(rapidbyte_sdk::errors::ConnectorError::config(
                            "INVALID_CONFIG",
                            format!("Config parse error: {}", e),
                        ))
                    })?;

                let rt = get_runtime();
                rt.block_on(
                    <$connector_type as rapidbyte_sdk::connector::SourceConnector>::validate(
                        &config,
                    ),
                )
                .map(to_component_validation)
                .map_err(to_component_error)
            }

            fn run_read(
                ctx_json: String,
            ) -> Result<
                __rb_source_bindings::rapidbyte::connector::types::ReadSummary,
                __rb_source_bindings::rapidbyte::connector::types::ConnectorError,
            > {
                let ctx = parse_stream_context(ctx_json)?;
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                let conn = state_ref.as_mut().expect("Connector not opened");

                let summary = rt
                    .block_on(
                        <$connector_type as rapidbyte_sdk::connector::SourceConnector>::read(
                            conn,
                            ctx,
                        ),
                    )
                    .map_err(to_component_error)?;

                Ok(__rb_source_bindings::rapidbyte::connector::types::ReadSummary {
                    records_read: summary.records_read,
                    bytes_read: summary.bytes_read,
                    batches_emitted: summary.batches_emitted,
                    checkpoint_count: summary.checkpoint_count,
                    records_skipped: summary.records_skipped,
                })
            }

            fn close(
            ) -> Result<(), __rb_source_bindings::rapidbyte::connector::types::ConnectorError> {
                let rt = get_runtime();
                let state_cell = get_state();
                let mut state_ref = state_cell.borrow_mut();
                if let Some(conn) = state_ref.as_mut() {
                    rt.block_on(
                        <$connector_type as rapidbyte_sdk::connector::SourceConnector>::close(
                            conn,
                        ),
                    )
                    .map_err(to_component_error)?;
                }
                *state_ref = None;
                Ok(())
            }
        }

        __rb_source_bindings::export!(
            RapidbyteSourceComponent with_types_in __rb_source_bindings
        );

        fn main() {}
    };
}
```

**Step 3: Rewrite `dest_connector_main!`**

Same pattern as source, but with `DestinationConnector` trait and `run_write` instead of `run_read`/`discover`. Replace the entire `dest_connector_main!` macro. The structure is identical — `CONFIG_JSON`, `get_state()`, `get_runtime()`, same helpers — but references `DestinationConnector` trait and `__rb_dest_bindings`. The `Guest` impl has `open`, `validate`, `run_write`, `close` (no `discover`). Instance stored directly in `OnceLock<SyncRefCell<Option<$connector_type>>>` (no wrapper struct).

**Step 4: Rewrite `transform_connector_main!`**

Same pattern with `TransformConnector` trait and `run_transform`. The `Guest` impl has `open`, `validate`, `run_transform`, `close`. Same `CONFIG_JSON` + direct instance storage pattern.

**Step 5: Run SDK tests**

Run: `cargo test -p rapidbyte-sdk`
Expected: PASS (the trait test from Task 1 still compiles)

**Step 6: Commit**

```bash
git add crates/rapidbyte-sdk/src/connector.rs
git commit -m "feat(sdk): rewrite connector macros to absorb tokio runtime and config parsing"
```

---

## Task 3: Migrate `source-postgres`

**Files:**
- Modify: `connectors/source-postgres/src/main.rs` (full rewrite)
- Modify: `connectors/source-postgres/src/config.rs` (remove `from_open_context`, `create_runtime`)
- Modify: `connectors/source-postgres/src/source.rs` (no change expected — `read_stream` stays)
- Modify: `connectors/source-postgres/src/schema.rs` (no change expected — `discover_catalog` stays)

**Step 1: Simplify `config.rs`**

Remove `from_open_context()`, `create_runtime()`, and the `rapidbyte_sdk` imports. The config struct becomes a pure data object:

```rust
use serde::Deserialize;

/// PostgreSQL connection config from pipeline YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: String,
    pub database: String,
}

fn default_port() -> u16 {
    5432
}

impl Config {
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}
```

**Step 2: Rewrite `main.rs`**

```rust
pub mod config;
pub mod schema;
pub mod source;

use std::time::Instant;

use rapidbyte_sdk::connector::SourceConnector;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult, ValidationStatus};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{Catalog, OpenInfo, ReadSummary, StreamContext};

pub struct SourcePostgres {
    config: config::Config,
}

impl SourceConnector for SourcePostgres {
    type Config = config::Config;

    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {
        host_ffi::log(
            2,
            &format!(
                "source-postgres: open with host={} db={}",
                config.host, config.database
            ),
        );
        Ok((
            Self { config },
            OpenInfo {
                protocol_version: "2".to_string(),
                features: vec![],
                default_max_batch_bytes: 64 * 1024 * 1024,
            },
        ))
    }

    async fn discover(&mut self) -> Result<Catalog, ConnectorError> {
        let client = source::connect(&self.config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        schema::discover_catalog(&client)
            .await
            .map(|streams| Catalog { streams })
            .map_err(|e| ConnectorError::transient_db("DISCOVERY_FAILED", e))
    }

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        let client = source::connect(config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        client
            .query_one("SELECT 1", &[])
            .await
            .map_err(|e| {
                ConnectorError::transient_network(
                    "CONNECTION_TEST_FAILED",
                    format!("Connection test failed: {}", e),
                )
            })?;
        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: format!("Connected to {}:{}/{}", config.host, config.port, config.database),
        })
    }

    async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {
        let connect_start = Instant::now();
        let client = source::connect(&self.config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();
        source::read_stream(&client, &ctx, connect_secs)
            .await
            .map_err(|e| ConnectorError::internal("READ_FAILED", e))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        host_ffi::log(2, "source-postgres: close (no-op)");
        Ok(())
    }
}

rapidbyte_sdk::source_connector_main!(SourcePostgres);
```

Key changes:
- No `#[derive(Default)]`, no `Option<Config>`
- `config` is `Config` (not `Option<Config>`)
- No `config::create_runtime()` or `rt.block_on()` — methods are `async`
- `validate` is a static method taking `&Config`
- All `.config.as_ref().ok_or_else(...)` unwraps are gone

**Step 3: Build the connector**

Run: `cd connectors/source-postgres && cargo build`
Expected: Build succeeds targeting `wasm32-wasip2`

**Step 4: Commit**

```bash
git add connectors/source-postgres/src/main.rs connectors/source-postgres/src/config.rs
git commit -m "feat(source-postgres): migrate to async-first SDK traits"
```

---

## Task 4: Migrate `dest-postgres`

**Files:**
- Modify: `connectors/dest-postgres/src/main.rs` (full rewrite)
- Modify: `connectors/dest-postgres/src/config.rs` (remove `from_open_context`, `create_runtime`)
- Modify: `connectors/dest-postgres/src/sink.rs` (convert `write_stream` to `async fn` — **critical** to avoid nested Tokio runtime panic)

**Step 1: Simplify `config.rs`**

```rust
use rapidbyte_sdk::errors::ConnectorError;
use serde::Deserialize;

/// PostgreSQL connection config from pipeline YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: String,
    pub database: String,
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "default_load_method")]
    pub load_method: String,
}

fn default_port() -> u16 {
    5432
}

fn default_schema() -> String {
    "public".to_string()
}

fn default_load_method() -> String {
    "insert".to_string()
}

impl Config {
    /// Validate config values that serde can't enforce.
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.load_method != "insert" && self.load_method != "copy" {
            return Err(ConnectorError::config(
                "INVALID_CONFIG",
                format!(
                    "Invalid load_method: '{}'. Must be 'insert' or 'copy'",
                    self.load_method
                ),
            ));
        }
        Ok(())
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}
```

Note: The `load_method` validation that was in `from_open_context()` moves to a `validate()` method on `Config`, called in `connect()`.

**Step 2: Rewrite `main.rs`**

```rust
mod config;
mod ddl;
mod format;
mod loader;
pub mod sink;

use rapidbyte_sdk::connector::DestinationConnector;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult, ValidationStatus};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{Feature, OpenInfo, StreamContext, WriteSummary};

pub struct DestPostgres {
    config: config::Config,
}

impl DestinationConnector for DestPostgres {
    type Config = config::Config;

    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {
        config.validate()?;

        host_ffi::log(
            2,
            &format!(
                "dest-postgres: open with host={} db={} schema={} load_method={}",
                config.host, config.database, config.schema, config.load_method
            ),
        );

        let mut features = vec![Feature::ExactlyOnce];
        if config.load_method == "copy" {
            features.push(Feature::BulkLoadCopy);
        }

        Ok((
            Self { config },
            OpenInfo {
                protocol_version: "2".to_string(),
                features,
                default_max_batch_bytes: 64 * 1024 * 1024,
            },
        ))
    }

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        let client = sink::connect(config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;

        client
            .query_one("SELECT 1", &[])
            .await
            .map_err(|e| {
                ConnectorError::transient_network(
                    "CONNECTION_TEST_FAILED",
                    format!("Connection test failed: {}", e),
                )
            })?;

        let schema_check = client
            .query_one(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
                &[&config.schema],
            )
            .await;

        let message = match schema_check {
            Ok(_) => format!(
                "Connected to {}:{}/{} (schema: {})",
                config.host, config.port, config.database, config.schema
            ),
            Err(_) => format!(
                "Connected to {}:{}/{} (schema '{}' does not exist, will be created)",
                config.host, config.port, config.database, config.schema
            ),
        };

        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message,
        })
    }

    async fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError> {
        sink::write_stream(&self.config, &ctx).await
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        host_ffi::log(2, "dest-postgres: close (no-op)");
        Ok(())
    }
}

rapidbyte_sdk::dest_connector_main!(DestPostgres);
```

**Important:** `sink::write_stream` must be converted to `async fn` (see next step). The macro wraps `write()` in `get_runtime().block_on(...)`, so if `write_stream` still creates its own internal Tokio runtime, you get a nested runtime panic ("Cannot start a runtime from within a runtime").

**Step 3: Convert `sink::write_stream` to `async fn`**

The current `write_stream` is sync and creates its own Tokio runtime internally:

```rust
// BEFORE (sink.rs:~69) — will panic with nested runtime!
pub fn write_stream(config: &crate::config::Config, ctx: &StreamContext) -> Result<WriteSummary, ConnectorError> {
    // ...identifier validation...
    let rt = crate::config::create_runtime();
    rt.block_on(async {
        let client = connect(config).await?;
        let mut session = WriteSession::begin(&client, ...).await?;
        // ...pull loop...
        session.commit().await
    })
}
```

Convert to async by removing the runtime wrapper — all internal methods (`connect`, `WriteSession::begin/process_batch/commit`) are already async:

```rust
// AFTER — pure async, no internal runtime
pub async fn write_stream(config: &crate::config::Config, ctx: &StreamContext) -> Result<WriteSummary, ConnectorError> {
    // ...identifier validation (stays sync, no change)...
    let client = connect(config).await?;
    let mut session = WriteSession::begin(&client, ...).await?;
    // ...pull loop (stays the same, just loses the async { } wrapper)...
    session.commit().await
}
```

This is safe because the macro's `get_runtime().block_on(...)` already provides the Tokio runtime context. Also remove `create_runtime` from `config.rs` (no longer used).

**Step 4: Build the connector**

Run: `cd connectors/dest-postgres && cargo build`
Expected: Build succeeds targeting `wasm32-wasip2`

**Step 5: Commit**

```bash
git add connectors/dest-postgres/src/main.rs connectors/dest-postgres/src/config.rs connectors/dest-postgres/src/sink.rs
git commit -m "feat(dest-postgres): migrate to async-first SDK traits"
```

---

## Task 5: Update Scaffold Templates

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Update `gen_source_main()`**

Replace the `gen_source_main` function with the new async-first template:

```rust
fn gen_source_main(struct_name: &str) -> String {
    format!(
        r#"mod config;
mod schema;
mod source;

use rapidbyte_sdk::prelude::*;

pub struct {struct_name} {{
    config: config::Config,
}}

impl SourceConnector for {struct_name} {{
    type Config = config::Config;

    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {{
        host_ffi::log(2, &format!("{{}}: open with host={{}} db={{}}",
            env!("CARGO_PKG_NAME"), config.host, config.database));
        Ok((
            Self {{ config }},
            OpenInfo {{
                protocol_version: "2".to_string(),
                features: vec![],
                default_max_batch_bytes: 64 * 1024 * 1024,
            }},
        ))
    }}

    async fn discover(&mut self) -> Result<Catalog, ConnectorError> {{
        schema::discover_catalog(&self.config)
    }}

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {{
        // TODO: Connect and run a test query
        Ok(ValidationResult {{
            status: ValidationStatus::Success,
            message: "Validation not yet implemented".to_string(),
        }})
    }}

    async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {{
        source::read_stream(&self.config, &ctx)
    }}

    async fn close(&mut self) -> Result<(), ConnectorError> {{
        host_ffi::log(2, &format!("{{}}: close", env!("CARGO_PKG_NAME")));
        Ok(())
    }}
}}

rapidbyte_sdk::source_connector_main!({struct_name});
"#
    )
}
```

**Step 2: Update `gen_dest_main()`**

```rust
fn gen_dest_main(struct_name: &str) -> String {
    format!(
        r#"mod config;
mod sink;

use rapidbyte_sdk::prelude::*;

pub struct {struct_name} {{
    config: config::Config,
}}

impl DestinationConnector for {struct_name} {{
    type Config = config::Config;

    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {{
        host_ffi::log(2, &format!("{{}}: open with host={{}} db={{}}",
            env!("CARGO_PKG_NAME"), config.host, config.database));
        Ok((
            Self {{ config }},
            OpenInfo {{
                protocol_version: "2".to_string(),
                features: vec![],
                default_max_batch_bytes: 64 * 1024 * 1024,
            }},
        ))
    }}

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {{
        Ok(ValidationResult {{
            status: ValidationStatus::Success,
            message: "Validation not yet implemented".to_string(),
        }})
    }}

    async fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError> {{
        sink::write_stream(&self.config, &ctx).await
    }}

    async fn close(&mut self) -> Result<(), ConnectorError> {{
        host_ffi::log(2, &format!("{{}}: close", env!("CARGO_PKG_NAME")));
        Ok(())
    }}
}}

rapidbyte_sdk::dest_connector_main!({struct_name});
"#
    )
}
```

**Step 3: Update `gen_config()`**

Remove `from_open_context()` and `create_runtime()`:

```rust
fn gen_config() -> String {
    r#"use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: String,
    pub database: String,
}

fn default_port() -> u16 {
    3306 // TODO: Change to your connector's default port
}

impl Config {
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}
"#
    .to_string()
}
```

**Step 4: Update `gen_source_rs()` and `gen_sink_rs()`**

The scaffold stubs now take `&Config` directly (not `&OpenContext`):

```rust
fn gen_source_rs() -> String {
    r#"use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub fn read_stream(config: &Config, ctx: &StreamContext) -> Result<ReadSummary, ConnectorError> {
    // TODO: Implement stream reading
    Ok(ReadSummary {
        records_read: 0,
        bytes_read: 0,
        batches_emitted: 0,
        checkpoint_count: 0,
        records_skipped: 0,
        perf: None,
    })
}
"#
    .to_string()
}
```

(`gen_sink_rs` stays the same — it already takes `&Config`.)

**Step 5: Build host crate**

Run: `cargo build -p rapidbyte-cli`
Expected: PASS

**Step 6: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "feat(scaffold): update templates for async-first SDK traits"
```

---

## Task 6: Full Verification

**Step 1: Host tests**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 2: Clippy**

Run: `cargo clippy --workspace`
Expected: No new warnings

**Step 3: Build both connectors**

Run: `just build-connectors`
Expected: Both `.wasm` binaries produced

**Step 4: E2E test**

Run: `just e2e`
Expected: Full pipeline completes — source reads, dest writes, data verified

**Step 5: Final commit (if any fixups needed)**

```bash
git commit -m "fix: address review feedback from async SDK migration"
```

---

## Task 7: Update PROTOCOL.md for Language Agnosticism

**Files:**
- Modify: `docs/PROTOCOL.md` (section 10)

**Step 1: Replace section 10**

Current section 10 ("SDK Expectations") implies Rust-only. Replace it with a language-agnostic section:

```markdown
## 10. Building Connectors

### 10.1 Language-Agnostic Contract

Any language that compiles to `wasm32-wasip2` and implements the WIT interface can be a Rapidbyte connector. The WIT file (`wit/rapidbyte-connector.wit`) is the source of truth — not any particular SDK.

A valid connector is a WASI component that:
1. Exports one of `source-connector`, `dest-connector`, or `transform-connector`
2. Accepts JSON config via `open(config-json: string)`
3. Exchanges Arrow IPC batches via host imports (`emit-batch`/`next-batch`)
4. Returns structured `connector-error` records on failure

### 10.2 Rust SDK (recommended for Rust connectors)

The `rapidbyte-sdk` crate provides ergonomic Rust traits and macros:

- `source_connector_main!(Type)` — exports a source component
- `dest_connector_main!(Type)` — exports a destination component
- `transform_connector_main!(Type)` — exports a transform component

The SDK handles WIT binding generation, config JSON deserialization, Tokio runtime management, and error type conversion.

For TCP clients (e.g. `tokio-postgres`), use `rapidbyte_sdk::host_tcp::HostTcpStream` with `connect_raw` to route through host-proxied networking.

### 10.3 Other Languages

Connectors can be written in any language with WASI component support:
- **Go:** Use `wit-bindgen-go` to generate bindings from the WIT file
- **Python:** Use `componentize-py` to compile Python to a WASI component
- **C/C++:** Use `wit-bindgen-c` for C bindings

The connector must implement the same WIT exports and call the same WIT imports regardless of language.
```

**Step 2: Verify no broken links**

Run: `grep -n "section 10\|SDK Expectations" docs/PROTOCOL.md`
Expected: Only the new section 10 heading

**Step 3: Commit**

```bash
git add docs/PROTOCOL.md
git commit -m "docs: update PROTOCOL.md section 10 for language-agnostic connector development"
```

---

## Design Decisions & Rationale

| Decision | Rationale |
|----------|-----------|
| `validate(&Config)` is static (not `&self`) | Validation often needs a fresh connection, not the connector's existing state. Avoids borrowing conflicts. |
| Instance stored directly (no `ConnectorState` wrapper) | The connector already owns its config (e.g. `Self { config, client }`). Wrapping it in another struct is redundant. The `OnceLock` just holds `Option<$connector_type>`. |
| Raw `config_json` stored in separate `OnceLock<String>` | `validate()` needs `&Config` but the config was consumed by `connect()`. Storing the raw JSON lets validate re-parse a fresh copy. Cheap and avoids `Clone` bound on Config. |
| `OnceLock<SyncRefCell<Option<$connector_type>>>` | Same safety model as before. `Option` because we can't construct the connector until `open()` is called. `SyncRefCell` because WASI is single-threaded. |
| No `Clone` bound on `Config` | Not all config types are `Clone`. `DeserializeOwned` is sufficient — we re-parse from stored JSON when needed. |
| `sink::write_stream` converted to `async fn` | The macro wraps `write()` in `get_runtime().block_on(...)`. If `write_stream` created its own internal runtime, Tokio would panic with "Cannot start a runtime from within a runtime." All internal methods are already async, so removing the runtime wrapper is trivial. |
| WIT interface unchanged | The WIT file is the language-agnostic contract. All Rust DX improvements happen in the SDK layer above WIT, preserving the ability to write connectors in Go/Python/C. |
| JSON-over-string at WIT boundary | Config and context are opaque JSON strings crossing the component boundary. This keeps the contract simple for any language to implement. |
