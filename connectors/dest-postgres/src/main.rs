pub mod sink;

use std::sync::OnceLock;
use std::time::Instant;

use rapidbyte_sdk::errors::{ConnectorError, ConnectorResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::memory::{pack_ptr_len, write_guest_bytes};
use rapidbyte_sdk::protocol::{
    ConfigBlob, Feature, OpenContext, OpenInfo, StreamContext, WriteMode, WritePerf, WriteSummary,
};
use rapidbyte_sdk::validation::validate_pg_identifier;

use serde::Deserialize;
use tokio_postgres::NoTls;

static CONFIG: OnceLock<PgConfig> = OnceLock::new();

// Re-export allocator functions from SDK so the host can call them
pub use rapidbyte_sdk::memory::{rb_allocate, rb_deallocate};

/// PostgreSQL connection config from pipeline YAML.
#[derive(Debug, Deserialize)]
struct PgConfig {
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    user: String,
    #[serde(default)]
    password: String,
    database: String,
    #[serde(default = "default_schema")]
    schema: String,
    #[serde(default = "default_load_method")]
    load_method: String,
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

impl PgConfig {
    fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}

/// Helper: read input bytes from (ptr, len), run handler, serialize result.
fn protocol_handler<F>(input_ptr: i32, input_len: i32, handler: F) -> i64
where
    F: FnOnce(&[u8]) -> Vec<u8>,
{
    let input = unsafe {
        std::slice::from_raw_parts(input_ptr as *const u8, input_len as usize)
    };
    let result_bytes = handler(input);
    let (ptr, len) = write_guest_bytes(&result_bytes);
    pack_ptr_len(ptr, len)
}

fn make_ok_response<T: serde::Serialize>(data: T) -> Vec<u8> {
    let result: ConnectorResult<T> = ConnectorResult::Ok { data };
    serde_json::to_vec(&result).unwrap()
}

fn make_err_response(code: &str, message: &str) -> Vec<u8> {
    let result: ConnectorResult<()> = ConnectorResult::Err {
        error: ConnectorError {
            code: code.to_string(),
            message: message.to_string(),
        },
    };
    serde_json::to_vec(&result).unwrap()
}

/// Create a tokio runtime suitable for the Wasm environment.
fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
}

/// Connect to PostgreSQL using the provided config.
async fn connect(
    config: &PgConfig,
) -> Result<tokio_postgres::Client, String> {
    let conn_str = config.connection_string();
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            host_ffi::log(0, &format!("PostgreSQL connection error: {}", e));
        }
    });

    Ok(client)
}

// === v1 Exported Protocol Functions ===

#[no_mangle]
pub extern "C" fn rb_open(config_ptr: i32, config_len: i32) -> i64 {
    protocol_handler(config_ptr, config_len, |input| {
        let open_ctx: OpenContext = match serde_json::from_slice(input) {
            Ok(ctx) => ctx,
            Err(e) => {
                return make_err_response(
                    "INVALID_OPEN_CTX",
                    &format!("Invalid OpenContext: {}", e),
                );
            }
        };

        // Extract config from ConfigBlob
        let config_value = match &open_ctx.config {
            ConfigBlob::Json(v) => v.clone(),
        };

        let pg_config: PgConfig = match serde_json::from_value(config_value) {
            Ok(c) => c,
            Err(e) => {
                return make_err_response(
                    "INVALID_CONFIG",
                    &format!("Invalid PG config: {}", e),
                );
            }
        };

        // Validate load_method
        if pg_config.load_method != "insert" && pg_config.load_method != "copy" {
            return make_err_response(
                "INVALID_CONFIG",
                &format!(
                    "Invalid load_method: '{}'. Must be 'insert' or 'copy'",
                    pg_config.load_method
                ),
            );
        }

        host_ffi::log(
            2,
            &format!(
                "dest-postgres: open with host={} db={} schema={} load_method={}",
                pg_config.host, pg_config.database, pg_config.schema, pg_config.load_method
            ),
        );

        let features = if pg_config.load_method == "copy" {
            vec![Feature::BulkLoadCopy]
        } else {
            vec![]
        };

        if CONFIG.set(pg_config).is_err() {
            host_ffi::log(
                1,
                "dest-postgres: rb_open called more than once; using first config",
            );
        }

        make_ok_response(OpenInfo {
            protocol_version: "1".to_string(),
            features,
            default_max_batch_bytes: 64 * 1024 * 1024,
        })
    })
}

#[no_mangle]
pub extern "C" fn rb_validate(config_ptr: i32, config_len: i32) -> i64 {
    protocol_handler(config_ptr, config_len, |input| {
        // v1: try to parse as OpenContext first, fall back to raw PgConfig for v0 compat
        let config: PgConfig = if let Ok(open_ctx) = serde_json::from_slice::<OpenContext>(input) {
            match &open_ctx.config {
                ConfigBlob::Json(v) => match serde_json::from_value(v.clone()) {
                    Ok(c) => c,
                    Err(e) => {
                        return make_err_response(
                            "INVALID_CONFIG",
                            &format!("Invalid config: {}", e),
                        );
                    }
                },
            }
        } else {
            host_ffi::log(
                1,
                "dest-postgres: rb_validate received non-OpenContext input (v0 compat)",
            );
            match serde_json::from_slice(input) {
                Ok(c) => c,
                Err(e) => {
                    return make_err_response(
                        "INVALID_CONFIG",
                        &format!("Invalid config: {}", e),
                    );
                }
            }
        };

        let rt = create_runtime();
        rt.block_on(async {
            match connect(&config).await {
                Ok(client) => {
                    match client.query_one("SELECT 1", &[]).await {
                        Ok(_) => {
                            // Also verify the target schema exists
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

                            let result = rapidbyte_sdk::errors::ValidationResult {
                                status: rapidbyte_sdk::errors::ValidationStatus::Success,
                                message,
                            };
                            make_ok_response(result)
                        }
                        Err(e) => make_err_response(
                            "CONNECTION_TEST_FAILED",
                            &format!("Connection test failed: {}", e),
                        ),
                    }
                }
                Err(e) => make_err_response("CONNECTION_FAILED", &e),
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn rb_run_write(request_ptr: i32, request_len: i32) -> i64 {
    protocol_handler(request_ptr, request_len, |input| {
        let stream_ctx: StreamContext = match serde_json::from_slice(input) {
            Ok(c) => c,
            Err(e) => {
                return make_err_response(
                    "INVALID_STREAM_CTX",
                    &format!("Invalid StreamContext: {}", e),
                );
            }
        };

        let config = match CONFIG.get() {
            Some(c) => c,
            None => {
                return make_err_response(
                    "NO_CONFIG",
                    "No config available. Call rb_open first.",
                );
            }
        };

        // Validate identifiers before interpolating into SQL
        if let Err(e) = validate_pg_identifier(&stream_ctx.stream_name) {
            return make_err_response(
                "INVALID_IDENTIFIER",
                &format!("Invalid stream name: {}", e),
            );
        }
        if let Err(e) = validate_pg_identifier(&config.schema) {
            return make_err_response(
                "INVALID_IDENTIFIER",
                &format!("Invalid schema name: {}", e),
            );
        }

        let rt = create_runtime();
        rt.block_on(async {
            // Phase 1: Connect
            let connect_start = Instant::now();
            let client = match connect(config).await {
                Ok(c) => c,
                Err(e) => return make_err_response("CONNECTION_FAILED", &e),
            };
            let connect_secs = connect_start.elapsed().as_secs_f64();

            // Phase 2: BEGIN first transaction
            // Chunked commits: when bytes_since_commit exceeds checkpoint_interval_bytes,
            // we COMMIT, emit a checkpoint, and BEGIN a new transaction.
            // If checkpoint_interval_bytes is 0, the entire stream stays in one transaction.
            if let Err(e) = client.execute("BEGIN", &[]).await {
                return make_err_response("TX_FAILED", &format!("BEGIN failed: {}", e));
            }

            // Phase 3: Pull loop — read batches from host and write immediately
            let flush_start = Instant::now();
            let mut total_rows: u64 = 0;
            let mut total_bytes: u64 = 0;
            let mut batches_written: u64 = 0;
            let mut created_tables = std::collections::HashSet::new();
            let mut buf: Vec<u8> = Vec::new();
            let mut loop_error: Option<String> = None;
            let mut checkpoint_count: u64 = 0;
            let mut bytes_since_commit: u64 = 0;
            let checkpoint_interval = stream_ctx.limits.checkpoint_interval_bytes;

            // Upsert mode is not compatible with COPY — fall back to INSERT
            let use_copy = config.load_method == "copy"
                && !matches!(stream_ctx.write_mode, Some(WriteMode::Upsert { .. }));
            if config.load_method == "copy"
                && matches!(stream_ctx.write_mode, Some(WriteMode::Upsert { .. }))
            {
                host_ffi::log(
                    1,
                    "dest-postgres: upsert mode not compatible with COPY, falling back to INSERT",
                );
            }

            loop {
                match host_ffi::next_batch(&mut buf, stream_ctx.limits.max_batch_bytes) {
                    Ok(None) => {
                        // EOF — no more batches
                        break;
                    }
                    Ok(Some(n)) => {
                        let ipc_bytes = &buf[..n];

                        let write_result = if use_copy {
                            sink::write_batch_copy(
                                &client,
                                &config.schema,
                                &stream_ctx.stream_name,
                                ipc_bytes,
                                &mut created_tables,
                                stream_ctx.write_mode.as_ref(),
                            )
                            .await
                        } else {
                            sink::write_batch(
                                &client,
                                &config.schema,
                                &stream_ctx.stream_name,
                                ipc_bytes,
                                &mut created_tables,
                                stream_ctx.write_mode.as_ref(),
                            )
                            .await
                        };

                        match write_result {
                            Ok(count) => {
                                total_rows += count;
                                total_bytes += n as u64;
                                bytes_since_commit += n as u64;
                                batches_written += 1;
                            }
                            Err(e) => {
                                loop_error = Some(e);
                                break;
                            }
                        }

                        // Chunked commit: if we've written enough bytes, commit and start new txn
                        if checkpoint_interval > 0 && bytes_since_commit >= checkpoint_interval {
                            if let Err(e) = client.execute("COMMIT", &[]).await {
                                loop_error = Some(format!("Checkpoint COMMIT failed: {}", e));
                                break;
                            }

                            // Emit checkpoint so host can track progress
                            let cp = rapidbyte_sdk::protocol::Checkpoint {
                                id: checkpoint_count + 1,
                                kind: rapidbyte_sdk::protocol::CheckpointKind::Dest,
                                stream: stream_ctx.stream_name.clone(),
                                cursor_field: None,
                                cursor_value: None,
                                records_processed: total_rows,
                                bytes_processed: total_bytes,
                            };
                            let _ = host_ffi::checkpoint("dest-postgres", &stream_ctx.stream_name, &cp);
                            checkpoint_count += 1;
                            bytes_since_commit = 0;

                            host_ffi::log(3, &format!(
                                "dest-postgres: checkpoint {} — committed {} rows, {} bytes so far",
                                checkpoint_count, total_rows, total_bytes
                            ));

                            // Begin new transaction for next chunk
                            if let Err(e) = client.execute("BEGIN", &[]).await {
                                loop_error = Some(format!(
                                    "Post-checkpoint BEGIN failed after {} bytes committed: {}",
                                    total_bytes - bytes_since_commit, e
                                ));
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        loop_error = Some(format!("next_batch failed: {}", e));
                        break;
                    }
                }
            }

            let flush_secs = flush_start.elapsed().as_secs_f64();

            // If there was an error during the loop, ROLLBACK and return
            if let Some(err) = loop_error {
                let _ = client.execute("ROLLBACK", &[]).await;
                return make_err_response("WRITE_FAILED", &err);
            }

            // Phase 4: COMMIT
            let commit_start = Instant::now();
            if let Err(e) = client.execute("COMMIT", &[]).await {
                return make_err_response("TX_FAILED", &format!("COMMIT failed: {}", e));
            }
            let commit_secs = commit_start.elapsed().as_secs_f64();

            // Final checkpoint for the remaining data (only if this chunk has uncommitted data)
            if bytes_since_commit > 0 {
                let cp = rapidbyte_sdk::protocol::Checkpoint {
                    id: checkpoint_count + 1,
                    kind: rapidbyte_sdk::protocol::CheckpointKind::Dest,
                    stream: stream_ctx.stream_name.clone(),
                    cursor_field: None,
                    cursor_value: None,
                    records_processed: total_rows,
                    bytes_processed: total_bytes,
                };
                let _ = host_ffi::checkpoint("dest-postgres", &stream_ctx.stream_name, &cp);
                checkpoint_count += 1;
            }

            host_ffi::log(
                2,
                &format!(
                    "dest-postgres: flushed {} rows in {} batches via {} (connect={:.3}s flush={:.3}s commit={:.3}s)",
                    total_rows, batches_written, config.load_method, connect_secs, flush_secs, commit_secs
                ),
            );

            let summary = WriteSummary {
                records_written: total_rows,
                bytes_written: total_bytes,
                batches_written,
                checkpoint_count,
                perf: Some(WritePerf {
                    connect_secs,
                    flush_secs,
                    commit_secs,
                }),
            };
            make_ok_response(summary)
        })
    })
}

#[no_mangle]
pub extern "C" fn rb_close() -> i32 {
    host_ffi::log(2, "dest-postgres: close (no-op)");
    0
}

fn main() {
    // Wasm entry point — not used directly, protocol functions are called by the host
}
