use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

/// Connector role derived from the name prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Source,
    Destination,
}

impl Role {
    fn as_str(&self) -> &'static str {
        match self {
            Role::Source => "source",
            Role::Destination => "destination",
        }
    }
}

/// Run the scaffold command.
pub fn run(name: &str, output: Option<&str>) -> Result<()> {
    // Determine role from name prefix
    let role = if name.starts_with("source-") {
        Role::Source
    } else if name.starts_with("dest-") {
        Role::Destination
    } else {
        bail!(
            "Connector name must start with 'source-' or 'dest-', got '{}'",
            name
        );
    };

    // Compute output directory
    let base_dir = match output {
        Some(dir) => PathBuf::from(dir).join(name),
        None => PathBuf::from("connectors").join(name),
    };

    if base_dir.exists() {
        bail!("Directory already exists: {}", base_dir.display());
    }

    // Derive names
    let name_underscored = name.replace('-', "_");
    let struct_name = to_struct_name(name);
    let service_name = extract_service_name(name, role);
    let display_name = format!(
        "Rapidbyte {} {}",
        if role == Role::Source {
            "Source"
        } else {
            "Destination"
        },
        service_name
    );

    // Create directories
    let src_dir = base_dir.join("src");
    let cargo_dir = base_dir.join(".cargo");
    fs::create_dir_all(&src_dir).context("Failed to create src directory")?;
    fs::create_dir_all(&cargo_dir).context("Failed to create .cargo directory")?;

    // Track created files for summary
    let mut created_files: Vec<PathBuf> = Vec::new();

    // Write Cargo.toml
    let cargo_toml = gen_cargo_toml(name, &name_underscored);
    write_file(&base_dir.join("Cargo.toml"), &cargo_toml, &mut created_files)?;

    // Write .cargo/config.toml
    let cargo_config = gen_cargo_config();
    write_file(
        &cargo_dir.join("config.toml"),
        &cargo_config,
        &mut created_files,
    )?;

    // Write manifest.json
    let manifest = gen_manifest(name, &name_underscored, &display_name, role, &service_name);
    write_file(
        &base_dir.join("manifest.json"),
        &manifest,
        &mut created_files,
    )?;

    // Write source files
    match role {
        Role::Source => {
            write_file(
                &src_dir.join("main.rs"),
                &gen_source_main(&struct_name),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("config.rs"),
                &gen_config(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("source.rs"),
                &gen_source_rs(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("schema.rs"),
                &gen_schema_rs(),
                &mut created_files,
            )?;
        }
        Role::Destination => {
            write_file(
                &src_dir.join("main.rs"),
                &gen_dest_main(&struct_name),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("config.rs"),
                &gen_config(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("sink.rs"),
                &gen_sink_rs(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("format.rs"),
                &gen_format_rs(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("ddl.rs"),
                &gen_ddl_rs(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("loader.rs"),
                &gen_loader_rs(),
                &mut created_files,
            )?;
        }
    }

    // Print summary
    println!("Scaffolded {} connector '{}'", role.as_str(), name);
    println!();
    println!("Created files:");
    for f in &created_files {
        println!("  {}", f.display());
    }
    println!();
    println!("Next steps:");
    println!("  1. cd {}", base_dir.display());
    println!("  2. Edit src/config.rs with your connection parameters");
    match role {
        Role::Source => {
            println!("  3. Implement schema discovery in src/schema.rs");
            println!("  4. Implement stream reading in src/source.rs");
        }
        Role::Destination => {
            println!("  3. Implement value formatting in src/format.rs");
            println!("  4. Implement table DDL in src/ddl.rs");
            println!("  5. Implement batch loading in src/loader.rs");
            println!("  6. Wire it together in src/sink.rs");
        }
    }
    println!("  Then: cargo build --release");

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_file(path: &Path, content: &str, created: &mut Vec<PathBuf>) -> Result<()> {
    fs::write(path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    created.push(path.to_path_buf());
    Ok(())
}

/// Convert `source-mysql` -> `SourceMysql`, `dest-snowflake` -> `DestSnowflake`.
fn to_struct_name(name: &str) -> String {
    name.split('-')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
            }
        })
        .collect()
}

/// Extract the service name: `source-mysql` -> `MySQL`, `dest-snowflake` -> `Snowflake`.
/// Falls back to title-casing the suffix.
fn extract_service_name(name: &str, role: Role) -> String {
    let prefix = match role {
        Role::Source => "source-",
        Role::Destination => "dest-",
    };
    let suffix = name.strip_prefix(prefix).unwrap_or(name);

    // Title-case: first char uppercase, rest lowercase
    let mut chars = suffix.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().to_string() + chars.as_str(),
    }
}

// ---------------------------------------------------------------------------
// Template generators
// ---------------------------------------------------------------------------

fn gen_cargo_toml(name: &str, name_underscored: &str) -> String {
    format!(
        r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[workspace]

[[bin]]
name = "{name_underscored}"
path = "src/main.rs"

[dependencies]
rapidbyte-sdk = {{ path = "../../crates/rapidbyte-sdk" }}
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
tokio = {{ version = "=1.36", features = ["rt", "net", "macros", "io-util"] }}
arrow = {{ version = "53", features = ["ipc"] }}

[patch.crates-io]
tokio = {{ git = "https://github.com/second-state/wasi_tokio.git", branch = "v1.36.x" }}
socket2 = {{ git = "https://github.com/second-state/socket2.git", branch = "v0.5.x" }}
"#
    )
}

fn gen_cargo_config() -> String {
    r#"[build]
target = "wasm32-wasip1"

[target.wasm32-wasip1]
rustflags = ["--cfg", "wasmedge", "--cfg", "tokio_unstable", "--cfg", "skip_wasi_unsupported"]
"#
    .to_string()
}

fn gen_manifest(
    name: &str,
    name_underscored: &str,
    display_name: &str,
    role: Role,
    service_name: &str,
) -> String {
    let role_str = role.as_str();
    format!(
        r#"{{
    "manifest_version": "1.0",
    "id": "rapidbyte/{name}",
    "name": "{display_name}",
    "version": "0.1.0",
    "description": "{role_str} connector for {service_name}",
    "protocol_version": "1",
    "artifact": {{
        "entry_point": "{name_underscored}.wasm"
    }},
    "permissions": {{
        "network": {{
            "tls": "optional",
            "allow_runtime_config_domains": true
        }}
    }},
    "roles": {{
        "{role_str}": {{
            "supported_sync_modes": ["full_refresh"],
            "features": []
        }}
    }},
    "config_schema": {{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["host", "user", "database"],
        "properties": {{
            "host": {{ "type": "string" }},
            "port": {{ "type": "integer", "default": 3306 }},
            "user": {{ "type": "string" }},
            "password": {{ "type": "string", "default": "" }},
            "database": {{ "type": "string" }}
        }}
    }}
}}
"#
    )
}

fn gen_source_main(struct_name: &str) -> String {
    format!(
        r#"mod config;
mod schema;
mod source;

use rapidbyte_sdk::prelude::*;

#[derive(Default)]
pub struct {struct_name} {{
    config: Option<config::Config>,
}}

impl SourceConnector for {struct_name} {{
    fn open(&mut self, ctx: OpenContext) -> Result<OpenInfo, ConnectorError> {{
        let config = config::Config::from_open_context(&ctx)?;
        host_ffi::log(2, &format!("{{}}: open with host={{}} db={{}}",
            env!("CARGO_PKG_NAME"), config.host, config.database));
        self.config = Some(config);
        Ok(OpenInfo {{
            protocol_version: "1".to_string(),
            features: vec![],
            default_max_batch_bytes: 64 * 1024 * 1024,
        }})
    }}

    fn discover(&mut self) -> Result<Catalog, ConnectorError> {{
        let config = self.config.as_ref()
            .ok_or_else(|| ConnectorError::config("NO_CONFIG", "Call open first"))?;
        schema::discover_catalog(config)
    }}

    fn validate(&mut self) -> Result<ValidationResult, ConnectorError> {{
        let _config = self.config.as_ref()
            .ok_or_else(|| ConnectorError::config("NO_CONFIG", "Call open first"))?;
        // TODO: Connect and run a test query
        Ok(ValidationResult {{
            status: ValidationStatus::Success,
            message: "Validation not yet implemented".to_string(),
        }})
    }}

    fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {{
        let config = self.config.as_ref()
            .ok_or_else(|| ConnectorError::config("NO_CONFIG", "Call open first"))?;
        source::read_stream(config, &ctx)
    }}

    fn close(&mut self) -> Result<(), ConnectorError> {{
        host_ffi::log(2, &format!("{{}}: close", env!("CARGO_PKG_NAME")));
        Ok(())
    }}
}}

rapidbyte_sdk::source_connector_main!({struct_name});
"#
    )
}

fn gen_dest_main(struct_name: &str) -> String {
    format!(
        r#"mod config;
mod ddl;
mod format;
mod loader;
pub mod sink;

use rapidbyte_sdk::prelude::*;

#[derive(Default)]
pub struct {struct_name} {{
    config: Option<config::Config>,
}}

impl DestinationConnector for {struct_name} {{
    fn open(&mut self, ctx: OpenContext) -> Result<OpenInfo, ConnectorError> {{
        let config = config::Config::from_open_context(&ctx)?;
        host_ffi::log(2, &format!("{{}}: open with host={{}} db={{}}",
            env!("CARGO_PKG_NAME"), config.host, config.database));
        self.config = Some(config);
        Ok(OpenInfo {{
            protocol_version: "1".to_string(),
            features: vec![],
            default_max_batch_bytes: 64 * 1024 * 1024,
        }})
    }}

    fn validate(&mut self) -> Result<ValidationResult, ConnectorError> {{
        let _config = self.config.as_ref()
            .ok_or_else(|| ConnectorError::config("NO_CONFIG", "Call open first"))?;
        Ok(ValidationResult {{
            status: ValidationStatus::Success,
            message: "Validation not yet implemented".to_string(),
        }})
    }}

    fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError> {{
        let config = self.config.as_ref()
            .ok_or_else(|| ConnectorError::config("NO_CONFIG", "Call open first"))?;
        sink::write_stream(config, &ctx)
    }}

    fn close(&mut self) -> Result<(), ConnectorError> {{
        host_ffi::log(2, &format!("{{}}: close", env!("CARGO_PKG_NAME")));
        Ok(())
    }}
}}

rapidbyte_sdk::dest_connector_main!({struct_name});
"#
    )
}

fn gen_config() -> String {
    r#"use rapidbyte_sdk::prelude::*;
use serde::Deserialize;

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
    pub fn from_open_context(ctx: &OpenContext) -> Result<Self, ConnectorError> {
        let value = match &ctx.config {
            ConfigBlob::Json(v) => v.clone(),
        };
        serde_json::from_value(value).map_err(|e| {
            ConnectorError::config("INVALID_CONFIG", format!("Config parse error: {}", e))
        })
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}

pub fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
}
"#
    .to_string()
}

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

fn gen_schema_rs() -> String {
    r#"use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub fn discover_catalog(config: &Config) -> Result<Catalog, ConnectorError> {
    // TODO: Implement schema discovery
    Ok(Catalog { streams: vec![] })
}
"#
    .to_string()
}

fn gen_sink_rs() -> String {
    r#"use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub fn write_stream(config: &Config, ctx: &StreamContext) -> Result<WriteSummary, ConnectorError> {
    // TODO: Implement stream writing
    Ok(WriteSummary {
        records_written: 0,
        bytes_written: 0,
        batches_written: 0,
        checkpoint_count: 0,
        records_failed: 0,
        perf: None,
    })
}
"#
    .to_string()
}

fn gen_format_rs() -> String {
    r#"use std::fmt::Write as FmtWrite;

use arrow::array::{
    Array, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

/// Pre-downcast Arrow column reference for zero-allocation value formatting.
pub(crate) enum TypedCol<'a> {
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Boolean(&'a BooleanArray),
    Utf8(&'a arrow::array::StringArray),
    Null,
}

/// Pre-downcast active columns from a RecordBatch.
pub(crate) fn downcast_columns<'a>(batch: &'a RecordBatch, active_cols: &[usize]) -> Vec<TypedCol<'a>> {
    active_cols.iter().map(|&i| {
        let col = batch.column(i);
        match col.data_type() {
            DataType::Int16 => TypedCol::Int16(col.as_any().downcast_ref().unwrap()),
            DataType::Int32 => TypedCol::Int32(col.as_any().downcast_ref().unwrap()),
            DataType::Int64 => TypedCol::Int64(col.as_any().downcast_ref().unwrap()),
            DataType::Float32 => TypedCol::Float32(col.as_any().downcast_ref().unwrap()),
            DataType::Float64 => TypedCol::Float64(col.as_any().downcast_ref().unwrap()),
            DataType::Boolean => TypedCol::Boolean(col.as_any().downcast_ref().unwrap()),
            DataType::Utf8 => TypedCol::Utf8(col.as_string::<i32>()),
            _ => TypedCol::Null,
        }
    }).collect()
}

/// Write a SQL literal for the value at `row_idx` into `buf` (no heap allocation).
pub(crate) fn write_sql_value(buf: &mut String, col: &TypedCol, row_idx: usize) {
    // TODO: Implement for your connector's data types
    match col {
        TypedCol::Null => buf.push_str("NULL"),
        _ => buf.push_str("NULL"), // placeholder — implement per-type formatting
    }
}
"#
    .to_string()
}

fn gen_ddl_rs() -> String {
    r#"use std::collections::HashSet;

use arrow::datatypes::DataType;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{SchemaEvolutionPolicy, WriteMode};

/// Map Arrow data types to database column types.
pub(crate) fn arrow_to_pg_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Boolean => "BOOLEAN",
        DataType::Utf8 => "TEXT",
        _ => "TEXT",
    }
}

/// Create the target table if it doesn't exist, based on the Arrow schema.
pub(crate) async fn ensure_table(
    client: &Client,
    qualified_table: &str,
    arrow_schema: &arrow::datatypes::Schema,
) -> Result<(), String> {
    let columns_ddl: Vec<String> = arrow_schema.fields().iter().map(|field| {
        let pg_type = arrow_to_pg_type(field.data_type());
        let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
        format!("\"{}\" {}{}", field.name(), pg_type, nullable)
    }).collect();

    let ddl = format!("CREATE TABLE IF NOT EXISTS {} ({})", qualified_table, columns_ddl.join(", "));
    host_ffi::log(3, &format!("ensuring table: {}", ddl));
    client.execute(&ddl, &[]).await
        .map_err(|e| format!("Failed to create table {}: {}", qualified_table, e))?;
    Ok(())
}

/// Ensure the target schema and table exist, handling schema drift.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn ensure_table_and_schema(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    created_tables: &mut HashSet<String>,
    _write_mode: Option<&WriteMode>,
    _schema_policy: Option<&SchemaEvolutionPolicy>,
    arrow_schema: &arrow::datatypes::Schema,
    _ignored_columns: &mut HashSet<String>,
    _type_null_columns: &mut HashSet<String>,
) -> Result<(), String> {
    let qualified_table = format!("\"{}\".\"{}\"", target_schema, stream_name);
    if created_tables.contains(&qualified_table) {
        return Ok(());
    }
    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", target_schema);
    client.execute(&create_schema, &[]).await
        .map_err(|e| format!("Failed to create schema: {}", e))?;
    ensure_table(client, &qualified_table, arrow_schema).await?;
    created_tables.insert(qualified_table);
    // TODO: Add schema drift detection and policy enforcement
    Ok(())
}
"#
    .to_string()
}

fn gen_loader_rs() -> String {
    r#"use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{DataErrorPolicy, SchemaEvolutionPolicy, WriteMode};

use crate::ddl::ensure_table_and_schema;
use crate::format::{downcast_columns, write_sql_value};

/// Result of a write operation with per-row error tracking.
pub(crate) struct WriteResult {
    pub rows_written: u64,
    pub rows_failed: u64,
}

/// Bundled parameters for batch write operations.
pub(crate) struct WriteContext<'a> {
    pub client: &'a Client,
    pub target_schema: &'a str,
    pub stream_name: &'a str,
    pub created_tables: &'a mut HashSet<String>,
    pub write_mode: Option<&'a WriteMode>,
    pub schema_policy: Option<&'a SchemaEvolutionPolicy>,
    pub on_data_error: DataErrorPolicy,
    pub load_method: &'a str,
    pub ignored_columns: &'a mut HashSet<String>,
    pub type_null_columns: &'a mut HashSet<String>,
}

/// Decode Arrow IPC bytes into schema and record batches.
fn decode_ipc(ipc_bytes: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("Failed to read Arrow IPC: {}", e))?;
    let schema = reader.schema();
    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read batches: {}", e))?;
    Ok((schema, batches))
}

/// Write an Arrow IPC batch to the database.
pub(crate) async fn write_batch(ctx: &mut WriteContext<'_>, ipc_bytes: &[u8]) -> Result<(WriteResult, u64), String> {
    let decode_start = Instant::now();
    let (arrow_schema, batches) = decode_ipc(ipc_bytes)?;
    let decode_nanos = decode_start.elapsed().as_nanos() as u64;

    if batches.is_empty() {
        return Ok((WriteResult { rows_written: 0, rows_failed: 0 }, decode_nanos));
    }

    // TODO: Implement INSERT and/or COPY batch writing
    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    host_ffi::log(2, &format!("wrote {} rows (stub)", total_rows));
    Ok((WriteResult { rows_written: total_rows, rows_failed: 0 }, decode_nanos))
}
"#
    .to_string()
}
