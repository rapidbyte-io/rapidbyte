# Connector Standardization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rename `connect()` → `init()` and `OpenInfo` → `ConnectorInfo` across all traits, macros, connectors, and scaffolding; restructure source-postgres into standardized file layout.

**Architecture:** SDK-level renames only — WIT interfaces stay unchanged. The macro's `open()` WIT export still maps to the trait's `init()` method. Source-postgres's monolithic `source.rs` gets split into `client.rs` (connection), `reader.rs` (read logic), and `schema.rs` (already exists). Dest-postgres gets `sink.rs` renamed to `writer.rs` with a `client.rs` extracted for the connection function.

**Tech Stack:** Rust, Arrow IPC, tokio-postgres, WASI component model (wit-bindgen)

---

### Task 1: Rename `OpenInfo` → `ConnectorInfo` in protocol.rs

**Files:**
- Modify: `crates/rapidbyte-sdk/src/protocol.rs:51-55` (struct definition)
- Modify: `crates/rapidbyte-sdk/src/protocol.rs:393-401` (test)

**Step 1: Rename the struct**

In `crates/rapidbyte-sdk/src/protocol.rs`, find:

```rust
pub struct OpenInfo {
    pub protocol_version: String,
    pub features: Vec<Feature>,
    pub default_max_batch_bytes: u64,
}
```

Replace with:

```rust
pub struct ConnectorInfo {
    pub protocol_version: String,
    pub features: Vec<Feature>,
    pub default_max_batch_bytes: u64,
}
```

**Step 2: Update the roundtrip test in the same file**

Find the test `test_open_info_roundtrip` and rename it to `test_connector_info_roundtrip`. Update all `OpenInfo` references inside it to `ConnectorInfo`.

Before:
```rust
    #[test]
    fn test_open_info_roundtrip() {
        let info = OpenInfo {
```

After:
```rust
    #[test]
    fn test_connector_info_roundtrip() {
        let info = ConnectorInfo {
```

Also update the `back` variable type:
```rust
        let back: ConnectorInfo = serde_json::from_str(&json).unwrap();
```

**Step 3: Run tests to verify**

Run: `cargo test --workspace`
Expected: All 157 tests pass (nothing outside SDK directly constructs `OpenInfo` at compile time — connectors build separately)

**Step 4: Commit**

```bash
git add crates/rapidbyte-sdk/src/protocol.rs
git commit -m "refactor: rename OpenInfo to ConnectorInfo in protocol"
```

---

### Task 2: Update prelude.rs and connector.rs imports for ConnectorInfo

**Files:**
- Modify: `crates/rapidbyte-sdk/src/prelude.rs:11-15`
- Modify: `crates/rapidbyte-sdk/src/connector.rs:9-11` (import)
- Modify: `crates/rapidbyte-sdk/src/connector.rs:18` (SourceConnector trait)
- Modify: `crates/rapidbyte-sdk/src/connector.rs:41` (DestinationConnector trait)
- Modify: `crates/rapidbyte-sdk/src/connector.rs:62` (TransformConnector trait)
- Modify: `crates/rapidbyte-sdk/src/connector.rs:448-563` (test module)

**Step 1: Update the import in connector.rs**

In `crates/rapidbyte-sdk/src/connector.rs` line 10, change:

```rust
use crate::protocol::{
    Catalog, OpenInfo, ReadSummary, StreamContext, TransformSummary, WriteSummary,
};
```

to:

```rust
use crate::protocol::{
    Catalog, ConnectorInfo, ReadSummary, StreamContext, TransformSummary, WriteSummary,
};
```

**Step 2: Update the three trait definitions**

In `crates/rapidbyte-sdk/src/connector.rs`, for each of the three traits, change the `connect` method signature from `OpenInfo` to `ConnectorInfo`:

Line 18 (SourceConnector):
```rust
    async fn connect(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;
```

Line 41 (DestinationConnector):
```rust
    async fn connect(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;
```

Line 62 (TransformConnector):
```rust
    async fn connect(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;
```

**Step 3: Update the test module**

In `crates/rapidbyte-sdk/src/connector.rs` test module:

Line 453 — update import:
```rust
    use crate::protocol::{Catalog, ConnectorInfo, ReadSummary, StreamContext, WriteSummary, TransformSummary};
```

Lines 468, 471 (TestSource) — change `OpenInfo` to `ConnectorInfo`:
```rust
        async fn connect(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
            Ok((
                Self { config },
                ConnectorInfo {
```

Lines 502, 505 (TestDest) — same change.

Lines 532, 535 (TestTransform) — same change.

**Step 4: Update prelude.rs**

In `crates/rapidbyte-sdk/src/prelude.rs` lines 11-15, replace `OpenInfo` with `ConnectorInfo`:

```rust
pub use crate::protocol::{
    Catalog, Checkpoint, CheckpointKind, CursorInfo, CursorValue, Feature, Metric, MetricValue,
    ConnectorInfo, ReadPerf, ReadSummary, SchemaHint, StreamContext, StreamLimits, SyncMode,
    TransformSummary, WriteMode, WritePerf, WriteSummary,
};
```

**Step 5: Run tests**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 6: Commit**

```bash
git add crates/rapidbyte-sdk/src/connector.rs crates/rapidbyte-sdk/src/prelude.rs
git commit -m "refactor: update ConnectorInfo references in connector traits and prelude"
```

---

### Task 3: Rename `connect()` → `init()` in traits and macro

**Files:**
- Modify: `crates/rapidbyte-sdk/src/connector.rs:18` (SourceConnector trait)
- Modify: `crates/rapidbyte-sdk/src/connector.rs:41` (DestinationConnector trait)
- Modify: `crates/rapidbyte-sdk/src/connector.rs:62` (TransformConnector trait)
- Modify: `crates/rapidbyte-sdk/src/connector.rs:233` (`__rb_guest_lifecycle_methods!` macro)
- Modify: `crates/rapidbyte-sdk/src/connector.rs:448-563` (test module)

**Step 1: Rename `connect` → `init` in all three trait definitions**

In `crates/rapidbyte-sdk/src/connector.rs`:

Line 18 (SourceConnector):
```rust
    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;
```

Line 41 (DestinationConnector):
```rust
    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;
```

Line 62 (TransformConnector):
```rust
    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>;
```

**Step 2: Update the `__rb_guest_lifecycle_methods!` macro**

Line 233 — change `::connect(config)` to `::init(config)`:

```rust
            let (instance, _open_info) = rt
                .block_on(<$connector_type as $connector_trait>::init(config))
                .map_err(to_component_error)?;
```

**Step 3: Update the test implementations**

In the test module, change all three test structs:

TestSource (line 468):
```rust
        async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
```

TestDest (line 502):
```rust
        async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
```

TestTransform (line 532):
```rust
        async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
```

**Step 4: Run tests**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 5: Commit**

```bash
git add crates/rapidbyte-sdk/src/connector.rs
git commit -m "refactor: rename connect() to init() in connector traits and macro"
```

---

### Task 4: Update source-postgres connector — rename `connect` to `init` and extract `client.rs`

**Files:**
- Modify: `connectors/source-postgres/src/main.rs`
- Create: `connectors/source-postgres/src/client.rs`
- Modify: `connectors/source-postgres/src/source.rs:23-51` (move connect functions out)

**Step 1: Create `connectors/source-postgres/src/client.rs`**

Move the `connect` and `connect_inner` functions from `source.rs` into a new `client.rs`. Also move the `validate` logic (currently inline in `main.rs`) into a `validate` function.

Create `connectors/source-postgres/src/client.rs`:

```rust
use anyhow::{anyhow, Context};
use tokio_postgres::{Client, Config as PgConfig, NoTls};

use rapidbyte_sdk::errors::{ConnectorError, ValidationResult, ValidationStatus};
use rapidbyte_sdk::host_ffi;

/// Connect to PostgreSQL using the provided config.
pub async fn connect(config: &crate::config::Config) -> Result<Client, String> {
    connect_inner(config).await.map_err(|e| e.to_string())
}

async fn connect_inner(config: &crate::config::Config) -> anyhow::Result<Client> {
    let mut pg = PgConfig::new();
    pg.host(&config.host);
    pg.port(config.port);
    pg.user(&config.user);
    if !config.password.is_empty() {
        pg.password(&config.password);
    }
    pg.dbname(&config.database);

    let stream = rapidbyte_sdk::host_tcp::HostTcpStream::connect(&config.host, config.port)
        .map_err(|e| anyhow!("Connection failed: {e}"))?;
    let (client, connection) = pg
        .connect_raw(stream, NoTls)
        .await
        .context("Connection failed")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            host_ffi::log(0, &format!("PostgreSQL connection error: {}", e));
        }
    });

    Ok(client)
}

/// Validate PostgreSQL connectivity.
pub async fn validate(config: &crate::config::Config) -> Result<ValidationResult, ConnectorError> {
    let client = connect(config)
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
```

**Step 2: Remove `connect` and `connect_inner` from `source.rs`**

In `connectors/source-postgres/src/source.rs`, remove lines 1-51 (the two connect functions and their imports). The file should start at the old line 53 (`const BATCH_SIZE`). Update the imports at the top — remove `anyhow::anyhow` (if no longer needed after connect removal — check), keep the rest. Also replace any `connect(` call inside this file with `crate::client::connect(`.

Update `read_stream` and `read_stream_inner` — these stay in `source.rs` (which will be renamed to `reader.rs` in the next task). The file now needs to import `crate::client` for the connection. But actually `read_stream` in `main.rs` calls `source::connect()` then `source::read_stream()` — we'll change main.rs to call `client::connect()` instead.

Remove the following from the top of `source.rs`:
- The `connect` and `connect_inner` functions (lines 23-51)
- The `anyhow::anyhow` import (line 4) — **keep it** if used elsewhere in the file (it is — line 118, 201, etc.)
- The `tokio_postgres::{Client, Config as PgConfig, NoTls}` import — change to just `tokio_postgres::Client` since `PgConfig` and `NoTls` were only used by `connect_inner`

The new imports for `source.rs` should be:

```rust
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context};
use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, SecondsFormat, Utc};
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    ColumnSchema, CursorType, CursorValue, Metric, MetricValue, ReadSummary, StreamContext,
    SyncMode,
};
use rapidbyte_sdk::validation::validate_pg_identifier;
```

**Step 3: Update `main.rs` — rename `connect` → `init`, add `client` module, delegate `validate`**

Rewrite `connectors/source-postgres/src/main.rs`:

```rust
pub mod config;
mod client;
pub mod schema;
pub mod source;

use std::time::Instant;

use rapidbyte_sdk::connector::SourceConnector;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{Catalog, ConnectorInfo, ReadSummary, StreamContext};

pub struct SourcePostgres {
    config: config::Config,
}

impl SourceConnector for SourcePostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        host_ffi::log(
            2,
            &format!(
                "source-postgres: open with host={} db={}",
                config.host, config.database
            ),
        );
        Ok((
            Self { config },
            ConnectorInfo {
                protocol_version: "2".to_string(),
                features: vec![],
                default_max_batch_bytes: 64 * 1024 * 1024,
            },
        ))
    }

    async fn discover(&mut self) -> Result<Catalog, ConnectorError> {
        let client = client::connect(&self.config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        schema::discover_catalog(&client)
            .await
            .map(|streams| Catalog { streams })
            .map_err(|e| ConnectorError::transient_db("DISCOVERY_FAILED", e))
    }

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        client::validate(config).await
    }

    async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
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

**Step 4: Build the connector**

Run: `cd connectors/source-postgres && cargo build`
Expected: Successful wasm32-wasip2 build

**Step 5: Commit**

```bash
git add connectors/source-postgres/src/main.rs connectors/source-postgres/src/client.rs connectors/source-postgres/src/source.rs
git commit -m "refactor: extract client.rs, rename connect→init in source-postgres"
```

---

### Task 5: Rename source-postgres `source.rs` → `reader.rs`

**Files:**
- Rename: `connectors/source-postgres/src/source.rs` → `connectors/source-postgres/src/reader.rs`
- Modify: `connectors/source-postgres/src/main.rs` (update module name)

**Step 1: Rename the file**

```bash
mv connectors/source-postgres/src/source.rs connectors/source-postgres/src/reader.rs
```

**Step 2: Update main.rs module declarations**

In `connectors/source-postgres/src/main.rs`, change:

```rust
pub mod source;
```

to:

```rust
mod reader;
```

And update the two references:
- `source::read_stream(` → `reader::read_stream(`

**Step 3: Build the connector**

Run: `cd connectors/source-postgres && cargo build`
Expected: Successful build

**Step 4: Commit**

```bash
git add connectors/source-postgres/src/main.rs connectors/source-postgres/src/reader.rs
git rm connectors/source-postgres/src/source.rs
git commit -m "refactor: rename source.rs to reader.rs in source-postgres"
```

---

### Task 6: Update dest-postgres connector — rename `connect` → `init`, extract `client.rs`, rename `sink.rs` → `writer.rs`

**Files:**
- Modify: `connectors/dest-postgres/src/main.rs`
- Create: `connectors/dest-postgres/src/client.rs`
- Rename: `connectors/dest-postgres/src/sink.rs` → `connectors/dest-postgres/src/writer.rs`
- Modify: `connectors/dest-postgres/src/writer.rs` (remove `connect` function, update visibility)

**Step 1: Create `connectors/dest-postgres/src/client.rs`**

Extract the `connect` function from `sink.rs` into its own module:

```rust
use tokio_postgres::{Client, Config as PgConfig, NoTls};

use rapidbyte_sdk::errors::{ConnectorError, ValidationResult, ValidationStatus};
use rapidbyte_sdk::host_ffi;

/// Connect to PostgreSQL using the provided config.
pub(crate) async fn connect(config: &crate::config::Config) -> Result<Client, String> {
    let mut pg = PgConfig::new();
    pg.host(&config.host);
    pg.port(config.port);
    pg.user(&config.user);
    if !config.password.is_empty() {
        pg.password(&config.password);
    }
    pg.dbname(&config.database);

    let stream = rapidbyte_sdk::host_tcp::HostTcpStream::connect(&config.host, config.port)
        .map_err(|e| format!("Connection failed: {}", e))?;
    let (client, connection) = pg
        .connect_raw(stream, NoTls)
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            host_ffi::log(0, &format!("PostgreSQL connection error: {}", e));
        }
    });

    Ok(client)
}

/// Validate PostgreSQL connectivity and target schema.
pub(crate) async fn validate(config: &crate::config::Config) -> Result<ValidationResult, ConnectorError> {
    let client = connect(config)
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
```

**Step 2: Rename `sink.rs` → `writer.rs`**

```bash
mv connectors/dest-postgres/src/sink.rs connectors/dest-postgres/src/writer.rs
```

**Step 3: Update `writer.rs` — remove `connect` function, use `crate::client::connect`**

In `connectors/dest-postgres/src/writer.rs`:

Remove the `connect` function (old lines 20-47). Replace its import of `Config as PgConfig, NoTls` — these are no longer needed. Update the import line:

Before:
```rust
use tokio_postgres::{Client, Config as PgConfig, NoTls};
```

After:
```rust
use tokio_postgres::Client;
```

In the `write_stream` function (around old line 71), change `connect(config)` to `crate::client::connect(config)`:

```rust
    let client = crate::client::connect(config)
        .await
        .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
```

**Step 4: Update `main.rs`**

Rewrite `connectors/dest-postgres/src/main.rs`:

```rust
mod config;
mod client;
mod ddl;
mod format;
mod loader;
mod writer;

use rapidbyte_sdk::connector::DestinationConnector;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{ConnectorInfo, Feature, StreamContext, WriteSummary};

pub struct DestPostgres {
    config: config::Config,
}

impl DestinationConnector for DestPostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
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
            ConnectorInfo {
                protocol_version: "2".to_string(),
                features,
                default_max_batch_bytes: 64 * 1024 * 1024,
            },
        ))
    }

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        client::validate(config).await
    }

    async fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError> {
        writer::write_stream(&self.config, &ctx).await
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        host_ffi::log(2, "dest-postgres: close (no-op)");
        Ok(())
    }
}

rapidbyte_sdk::dest_connector_main!(DestPostgres);
```

**Step 5: Build the connector**

Run: `cd connectors/dest-postgres && cargo build`
Expected: Successful wasm32-wasip2 build

**Step 6: Commit**

```bash
git add connectors/dest-postgres/src/main.rs connectors/dest-postgres/src/client.rs connectors/dest-postgres/src/writer.rs
git rm connectors/dest-postgres/src/sink.rs
git commit -m "refactor: extract client.rs, rename sink.rs→writer.rs, connect→init in dest-postgres"
```

---

### Task 7: Update scaffold templates

**Files:**
- Modify: `crates/rapidbyte-cli/src/commands/scaffold.rs`

**Step 1: Update `gen_source_main`**

In `crates/rapidbyte-cli/src/commands/scaffold.rs`, function `gen_source_main` (line 279):

- Change `pub mod source;` → `mod reader;`
- Add `mod client;` after `mod config;`
- Change `async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError>` → `async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>`
- Change `OpenInfo {{` → `ConnectorInfo {{`
- Change `source::read_stream` → `reader::read_stream`

The full new function:

```rust
fn gen_source_main(struct_name: &str) -> String {
    format!(
        r#"pub mod config;
mod client;
mod reader;
pub mod schema;

use rapidbyte_sdk::prelude::*;

pub struct {struct_name} {{
    config: config::Config,
}}

impl SourceConnector for {struct_name} {{
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {{
        host_ffi::log(2, &format!("{{}}: init with host={{}} db={{}}",
            env!("CARGO_PKG_NAME"), config.host, config.database));
        Ok((
            Self {{ config }},
            ConnectorInfo {{
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
        client::validate(config).await
    }}

    async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {{
        reader::read_stream(&self.config, &ctx).await
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

**Step 2: Update `gen_dest_main`**

Function `gen_dest_main` (line 335):

- Change `mod sink;` → `mod client;\nmod writer;`
- Change `async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError>` → `async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError>`
- Change `OpenInfo {{` → `ConnectorInfo {{`
- Change `sink::write_stream` → `writer::write_stream`

The full new function:

```rust
fn gen_dest_main(struct_name: &str) -> String {
    format!(
        r#"mod config;
mod client;
mod writer;

use rapidbyte_sdk::prelude::*;

pub struct {struct_name} {{
    config: config::Config,
}}

impl DestinationConnector for {struct_name} {{
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {{
        host_ffi::log(2, &format!("{{}}: init with host={{}} db={{}}",
            env!("CARGO_PKG_NAME"), config.host, config.database));
        Ok((
            Self {{ config }},
            ConnectorInfo {{
                protocol_version: "2".to_string(),
                features: vec![],
                default_max_batch_bytes: 64 * 1024 * 1024,
            }},
        ))
    }}

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {{
        client::validate(config).await
    }}

    async fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError> {{
        writer::write_stream(&self.config, &ctx).await
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

**Step 3: Update `gen_source_rs` → `gen_reader_rs`**

Rename the function `gen_source_rs` to `gen_reader_rs` (line 417). No content change needed — the stub is already correct.

**Step 4: Update `gen_sink_rs` → `gen_writer_rs`**

Rename the function `gen_sink_rs` to `gen_writer_rs` (line 450). No content change needed — the stub is already correct.

**Step 5: Add `gen_client_rs` function**

Add a new function to generate the client stub:

```rust
fn gen_client_rs() -> String {
    r#"use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub async fn validate(config: &Config) -> Result<ValidationResult, ConnectorError> {
    // TODO: Connect and run a test query
    let _ = config;
    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: "Validation not yet implemented".to_string(),
    })
}
"#
    .to_string()
}
```

**Step 6: Update the `run` function — file generation calls**

In the `run` function (line 94-130), update the file generation calls:

For `Role::Source` (line 94):
- Change `gen_source_rs()` → `gen_reader_rs()` and filename `"source.rs"` → `"reader.rs"`
- Add `gen_client_rs()` call for `"client.rs"`

For `Role::Destination` (line 117):
- Change `gen_sink_rs()` → `gen_writer_rs()` and filename `"sink.rs"` → `"writer.rs"`
- Add `gen_client_rs()` call for `"client.rs"`

Updated source block:
```rust
        Role::Source => {
            write_file(&src_dir.join("main.rs"), &gen_source_main(&struct_name), &mut created_files)?;
            write_file(&src_dir.join("config.rs"), &gen_config(), &mut created_files)?;
            write_file(&src_dir.join("client.rs"), &gen_client_rs(), &mut created_files)?;
            write_file(&src_dir.join("reader.rs"), &gen_reader_rs(), &mut created_files)?;
            write_file(&src_dir.join("schema.rs"), &gen_schema_rs(), &mut created_files)?;
        }
```

Updated destination block:
```rust
        Role::Destination => {
            write_file(&src_dir.join("main.rs"), &gen_dest_main(&struct_name), &mut created_files)?;
            write_file(&src_dir.join("config.rs"), &gen_config(), &mut created_files)?;
            write_file(&src_dir.join("client.rs"), &gen_client_rs(), &mut created_files)?;
            write_file(&src_dir.join("writer.rs"), &gen_writer_rs(), &mut created_files)?;
        }
```

**Step 7: Update the "Next steps" help text**

In the `run` function (around line 143-151), update the printed next steps:

```rust
    match role {
        Role::Source => {
            println!("  3. Implement connection validation in src/client.rs");
            println!("  4. Implement schema discovery in src/schema.rs");
            println!("  5. Implement stream reading in src/reader.rs");
        }
        Role::Destination => {
            println!("  3. Implement connection validation in src/client.rs");
            println!("  4. Implement stream writing in src/writer.rs");
        }
    }
```

**Step 8: Run tests**

Run: `cargo test --workspace`
Expected: All tests pass (scaffold tests are part of the workspace)

**Step 9: Commit**

```bash
git add crates/rapidbyte-cli/src/commands/scaffold.rs
git commit -m "refactor: update scaffold templates for init/ConnectorInfo and standardized layout"
```

---

### Task 8: Run full workspace tests and build both connectors

**Step 1: Run workspace tests**

Run: `cargo test --workspace`
Expected: All 157+ tests pass

**Step 2: Run clippy**

Run: `cargo clippy --workspace`
Expected: 0 warnings

**Step 3: Build source-postgres**

Run: `cd connectors/source-postgres && cargo build`
Expected: Successful wasm32-wasip2 build

**Step 4: Build dest-postgres**

Run: `cd connectors/dest-postgres && cargo build`
Expected: Successful wasm32-wasip2 build

**Step 5: Verify standardized file layout**

```bash
ls connectors/source-postgres/src/
# Expected: main.rs config.rs client.rs reader.rs schema.rs

ls connectors/dest-postgres/src/
# Expected: main.rs config.rs client.rs writer.rs ddl.rs loader.rs format.rs
```

No commit needed — this is a verification step.
