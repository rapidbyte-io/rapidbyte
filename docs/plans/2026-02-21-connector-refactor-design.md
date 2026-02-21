# Connector Standardization & DX Refactor — Design

**Date:** 2026-02-21
**Status:** Approved

## Problem

Current connector code has inconsistent structure, misleading method names, and no enforced layout convention. Specific pain points:

1. `connect()` doesn't actually open a connection — it parses config and returns capabilities. The real connections happen inside `validate()`, `read()`, and `write()`.
2. Source connector (`source-postgres`) is monolithic — 739 lines mixing SQL, Arrow encoding, cursor logic, and connection management.
3. No standardized file layout — dest-postgres has 5 focused modules while source-postgres has 2 large ones.
4. No guidance for future non-DB connectors (HTTP APIs, S3, file-based, transforms).

## Industry Research

Analyzed Airbyte, Fivetran, and Estuary Flow connector SDKs to inform design decisions.

### Airbyte (CDK)
- Lifecycle: `spec` → `check` → `discover` → `read`/`write`
- Separate process per invocation — fully stateless between calls
- Each method opens and closes its own connection
- `check()` is the validation step — opens throwaway connection, runs test query
- One stream per `read()` call; orchestrator handles multi-stream dispatch

### Fivetran (SDK)
- Lifecycle: `configuration_form` → `test` → `schema` → `update`/`insert`
- gRPC server model but stateless per-call — no connection sharing between methods
- `test()` validates connectivity (equivalent to Airbyte's `check`)
- Each operation manages its own DB client lifecycle

### Estuary Flow (SDK)
- Lifecycle: `Open` → transact loop → `Destroy`
- Long-lived persistent connections via gRPC streams
- Designed for real-time/streaming — different paradigm than batch ETL

### Decision
We follow the **Airbyte/Fivetran model**: stateless stages, each method manages its own connections. This matches our WASI component model where connectors run as short-lived guest processes.

## Design

### 1. Trait Lifecycle — Rename & Clarify

Rename `connect()` → `init()` across all three traits. Rename `OpenInfo` → `ConnectorInfo`.

**Lifecycle order:** `init()` → `validate()` → `discover()` → `read()`/`write()`/`transform()` → `close()`

```rust
pub trait SourceConnector: Sized {
    type Config: DeserializeOwned;

    /// Lightweight constructor. Parse config, return capabilities.
    /// Does NOT open database connections.
    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;

    /// Static validation. Opens throwaway connection to test reachability.
    /// No &self — takes config directly.
    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        Ok(ValidationResult::success("Validation not implemented"))
    }

    /// Discover available streams and their schemas.
    async fn discover(&mut self) -> Result<Catalog, ConnectorError>;

    /// Read one stream. Orchestrator calls this once per stream.
    async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError>;

    /// Cleanup hook (default: no-op).
    async fn close(&mut self) -> Result<(), ConnectorError> { Ok(()) }
}
```

```rust
pub trait DestinationConnector: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        Ok(ValidationResult::success("Validation not implemented"))
    }

    async fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError>;

    async fn close(&mut self) -> Result<(), ConnectorError> { Ok(()) }
}
```

```rust
pub trait TransformConnector: Sized {
    type Config: DeserializeOwned;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        Ok(ValidationResult::success("Validation not implemented"))
    }

    // No discover() — transforms don't define schema

    async fn transform(&mut self, ctx: StreamContext) -> Result<TransformSummary, ConnectorError>;

    async fn close(&mut self) -> Result<(), ConnectorError> { Ok(()) }
}
```

**Trait comparison:**

| Method | Source | Destination | Transform |
|---|---|---|---|
| `init()` | Required | Required | Required |
| `validate()` | Optional (default: success) | Optional (default: success) | Optional (default: success) |
| `discover()` | Required | N/A | N/A |
| `read()` | Required | N/A | N/A |
| `write()` | N/A | Required | N/A |
| `transform()` | N/A | N/A | Required |
| `close()` | Optional (default: no-op) | Optional (default: no-op) | Optional (default: no-op) |

All three traits share: `type Config`, `init()`, `validate()`, `close()`.

### 2. Standardized File Layout

Every connector follows this structure. Scaffold enforces it.

**Source connectors:**
```
connectors/source-<name>/
├── Cargo.toml
├── manifest.json
└── src/
    ├── main.rs        # struct definition, trait impl, source_connector_main! macro
    ├── config.rs      # Config struct (serde)
    ├── client.rs      # connection/client helpers (DB pool, HTTP client, S3 client)
    ├── reader.rs      # core read logic (query, pagination, Arrow encoding)
    └── schema.rs      # schema discovery (catalog building)
```

**Destination connectors:**
```
connectors/dest-<name>/
├── Cargo.toml
├── manifest.json
└── src/
    ├── main.rs        # struct definition, trait impl, dest_connector_main! macro
    ├── config.rs      # Config struct
    ├── client.rs      # connection/client helpers
    ├── writer.rs      # core write logic (batch processing, commit)
    └── schema.rs      # DDL, table creation, schema drift (if applicable)
```

**Transform connectors:**
```
connectors/transform-<name>/
├── Cargo.toml
├── manifest.json
└── src/
    ├── main.rs        # struct definition, trait impl, transform_connector_main! macro
    ├── config.rs      # Config struct
    └── transform.rs   # core transform logic
```

**Rules:**
- `main.rs` is always thin — struct definition + trait impl delegating to module functions
- `config.rs` is only the `Config` struct with serde derives
- `client.rs` is optional — omit for connectors with no external connections (e.g., file transforms, pure computation)
- Additional modules are fine when complexity warrants it (e.g., dest-postgres keeps `loader.rs`, `format.rs`, `ddl.rs`)

### 3. main.rs Pattern

`main.rs` is a thin delegation layer. All business logic lives in dedicated modules.

```rust
mod config;
mod client;
mod reader;
mod schema;

use rapidbyte_sdk::prelude::*;

pub struct SourcePostgres {
    config: config::Config,
}

impl SourceConnector for SourcePostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        let info = ConnectorInfo {
            protocol_version: "2".to_string(),
            features: vec![Feature::IncrementalSync],
            default_max_batch_bytes: 64 * 1024 * 1024,
        };
        Ok((Self { config }, info))
    }

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        client::validate(config).await
    }

    async fn discover(&mut self) -> Result<Catalog, ConnectorError> {
        schema::discover_catalog(&self.config).await
    }

    async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {
        reader::read_stream(&self.config, &ctx).await
    }
}

rapidbyte_sdk::source_connector_main!(SourcePostgres);
```

### 4. Non-DB Connector Examples

The design is generic — same traits work for any data source or destination.

**HTTP API Source** (REST API poller):
```
connectors/source-http/src/
├── main.rs        # struct HttpSource { config }
├── config.rs      # url, headers, auth, pagination settings
├── client.rs      # HTTP client, auth token refresh
├── reader.rs      # pagination loop, JSON→Arrow conversion
└── schema.rs      # infer schema from sample response
```

**S3 Source** (CSV/Parquet files):
```
connectors/source-s3/src/
├── main.rs
├── config.rs      # bucket, prefix, credentials, format
├── client.rs      # S3 client wrapper
├── reader.rs      # list objects, stream files→Arrow
└── schema.rs      # infer from first file
```

**File Destination** (local Parquet/CSV):
```
connectors/dest-file/src/
├── main.rs
├── config.rs      # output path, format, compression
└── writer.rs      # write Arrow batches to file
```

No `client.rs` needed — just file I/O.

### 5. WIT & Macro Implications

**WIT interfaces stay unchanged.** The rename from `connect()` to `init()` is SDK-level only. The WIT `open` export continues to map to the SDK trait method — just a different name:

```rust
// Inside source_connector_main! macro:
fn open(config_json: String) -> Result<...> {
    let config = serde_json::from_str(&config_json)?;
    let (instance, info) = rt.block_on(
        <$connector_type as SourceConnector>::init(config)  // was ::connect
    )?;
    // ... rest unchanged
}
```

**`ConnectorInfo` rename** (from `OpenInfo`): Rename the struct in `protocol.rs` and update all references. No WIT change — the macro serializes to JSON.

Three macros need the same one-line change: `::connect(config)` → `::init(config)`.

### 6. Change Scope Summary

| Area | Change | Files |
|---|---|---|
| Trait renames | `connect()` → `init()` | `connector.rs` |
| Type rename | `OpenInfo` → `ConnectorInfo` | `protocol.rs`, `connector.rs` |
| Macro updates | `::connect()` → `::init()` | `connector.rs` (3 macros) |
| Source PG restructure | Split monolithic `source.rs` into `client.rs`, `reader.rs`, `schema.rs` | `connectors/source-postgres/src/` |
| Dest PG restructure | Rename `sink.rs` → `writer.rs`, minor reorg | `connectors/dest-postgres/src/` |
| Scaffold update | New file layout, `init()` in templates | `scaffold.rs` |
| No WIT changes | Component interface stays the same | — |

### 7. Design Principles

1. **Stateless stages**: each trait method manages its own connections. No cross-method state sharing beyond parsed config.
2. **`init()` is cheap**: parse config, return capabilities. Never opens network connections.
3. **`validate()` is static**: `&Config` only, no `&self`. Opens throwaway connection, checks reachability.
4. **One stream per call**: orchestrator dispatches. Connector handles one stream at a time.
5. **Thin `main.rs`**: struct + trait impl + macro. All logic in dedicated modules.
6. **Generic traits**: work for DB, HTTP, S3, file, and transform connectors. No database-specific assumptions in the SDK.
7. **SDK-level renames only**: WIT interfaces are stable. Trait renames don't affect the component model.
