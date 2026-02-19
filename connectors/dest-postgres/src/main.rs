pub mod sink;

use std::sync::OnceLock;
use std::time::Instant;

use rapidbyte_sdk::errors::{CommitState, ConnectorError, ConnectorResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::memory::{pack_ptr_len, write_guest_bytes};
use rapidbyte_sdk::protocol::{
    ConfigBlob, Feature, OpenContext, OpenInfo, StreamContext, WritePerf, WriteSummary,
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

fn make_err_response(error: ConnectorError) -> Vec<u8> {
    let result: ConnectorResult<()> = ConnectorResult::Err { error };
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

// === Exported Protocol Functions ===

#[no_mangle]
pub extern "C" fn rb_open(config_ptr: i32, config_len: i32) -> i64 {
    protocol_handler(config_ptr, config_len, |input| {
        let open_ctx: OpenContext = match serde_json::from_slice(input) {
            Ok(ctx) => ctx,
            Err(e) => {
                return make_err_response(
                    ConnectorError::config("INVALID_OPEN_CTX", format!("Invalid OpenContext: {}", e)),
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
                    ConnectorError::config("INVALID_CONFIG", format!("Invalid PG config: {}", e)),
                );
            }
        };

        // Validate load_method
        if pg_config.load_method != "insert" && pg_config.load_method != "copy" {
            return make_err_response(
                ConnectorError::config("INVALID_CONFIG", format!(
                    "Invalid load_method: '{}'. Must be 'insert' or 'copy'",
                    pg_config.load_method
                )),
            );
        }

        host_ffi::log(
            2,
            &format!(
                "dest-postgres: open with host={} db={} schema={} load_method={}",
                pg_config.host, pg_config.database, pg_config.schema, pg_config.load_method
            ),
        );

        let mut features = vec![Feature::ExactlyOnce];
        if pg_config.load_method == "copy" {
            features.push(Feature::BulkLoadCopy);
        }

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
        let open_ctx: OpenContext = match serde_json::from_slice(input) {
            Ok(ctx) => ctx,
            Err(e) => {
                return make_err_response(ConnectorError::config("INVALID_OPEN_CTX", format!("Invalid OpenContext: {}", e)));
            }
        };
        let config: PgConfig = match &open_ctx.config {
            ConfigBlob::Json(v) => match serde_json::from_value(v.clone()) {
                Ok(c) => c,
                Err(e) => {
                    return make_err_response(ConnectorError::config("INVALID_CONFIG", format!("Invalid config: {}", e)));
                }
            },
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
                            ConnectorError::transient_network("CONNECTION_TEST_FAILED", format!("Connection test failed: {}", e)),
                        ),
                    }
                }
                Err(e) => make_err_response(ConnectorError::transient_network("CONNECTION_FAILED", e)),
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
                    ConnectorError::config("INVALID_STREAM_CTX", format!("Invalid StreamContext: {}", e)),
                );
            }
        };

        let config = match CONFIG.get() {
            Some(c) => c,
            None => {
                return make_err_response(
                    ConnectorError::config("NO_CONFIG", "No config available. Call rb_open first."),
                );
            }
        };

        // Validate identifiers before interpolating into SQL
        if let Err(e) = validate_pg_identifier(&stream_ctx.stream_name) {
            return make_err_response(
                ConnectorError::config("INVALID_IDENTIFIER", format!("Invalid stream name: {}", e)),
            );
        }
        if let Err(e) = validate_pg_identifier(&config.schema) {
            return make_err_response(
                ConnectorError::config("INVALID_IDENTIFIER", format!("Invalid schema name: {}", e)),
            );
        }

        let rt = create_runtime();
        rt.block_on(async {
            // Phase 1: Connect
            let connect_start = Instant::now();
            let client = match connect(config).await {
                Ok(c) => c,
                Err(e) => return make_err_response(ConnectorError::transient_network("CONNECTION_FAILED", e)),
            };
            let connect_secs = connect_start.elapsed().as_secs_f64();

            // Phase 2: Session lifecycle
            let mut session = match sink::WriteSession::begin(
                &client,
                &config.schema,
                sink::SessionConfig {
                    stream_name: stream_ctx.stream_name.clone(),
                    write_mode: stream_ctx.write_mode.clone(),
                    load_method: config.load_method.clone(),
                    schema_policy: stream_ctx.policies.schema_evolution,
                    on_data_error: stream_ctx.policies.on_data_error,
                    checkpoint: sink::CheckpointConfig {
                        interval_bytes: stream_ctx.limits.checkpoint_interval_bytes,
                        interval_rows: stream_ctx.limits.checkpoint_interval_rows,
                        interval_seconds: stream_ctx.limits.checkpoint_interval_seconds,
                    },
                },
            )
            .await
            {
                Ok(s) => s,
                Err(e) => {
                    return make_err_response(ConnectorError::transient_db("SESSION_BEGIN_FAILED", e));
                }
            };

            // Phase 3: Pull loop — read batches from host
            let mut buf: Vec<u8> = Vec::new();
            let mut loop_error: Option<String> = None;

            loop {
                match host_ffi::next_batch(&mut buf, stream_ctx.limits.max_batch_bytes) {
                    Ok(None) => break,
                    Ok(Some(n)) => {
                        if let Err(e) = session.process_batch(&buf[..n]).await {
                            loop_error = Some(e);
                            break;
                        }
                    }
                    Err(e) => {
                        loop_error = Some(format!("next_batch failed: {}", e));
                        break;
                    }
                }
            }

            // Handle errors
            if let Some(err) = loop_error {
                session.rollback().await;
                return make_err_response(
                    ConnectorError::transient_db("WRITE_FAILED", err)
                        .with_commit_state(CommitState::BeforeCommit),
                );
            }

            // Phase 4: Commit
            let result = match session.commit().await {
                Ok(r) => r,
                Err(e) => {
                    return make_err_response(
                        ConnectorError::transient_db("COMMIT_FAILED", e)
                            .with_commit_state(CommitState::AfterCommitUnknown),
                    );
                }
            };

            let summary = WriteSummary {
                records_written: result.total_rows,
                bytes_written: result.total_bytes,
                batches_written: result.batches_written,
                checkpoint_count: result.checkpoint_count,
                records_failed: result.total_failed,
                perf: Some(WritePerf {
                    connect_secs,
                    flush_secs: result.flush_secs,
                    commit_secs: result.commit_secs,
                    arrow_decode_secs: result.arrow_decode_secs,
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
