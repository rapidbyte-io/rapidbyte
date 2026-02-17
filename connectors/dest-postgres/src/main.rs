pub mod sink;

use std::sync::Mutex;
use std::sync::OnceLock;

use rapidbyte_sdk::errors::{ConnectorError, ConnectorResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::memory::{pack_ptr_len, write_guest_bytes};

use serde::Deserialize;
use tokio_postgres::NoTls;

static CONFIG: OnceLock<PgConfig> = OnceLock::new();
static BATCH_BUFFER: OnceLock<Mutex<Vec<(String, Vec<u8>)>>> = OnceLock::new();

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
}

fn default_port() -> u16 {
    5432
}

fn default_schema() -> String {
    "public".to_string()
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

// === Exported Protocol Functions ===

#[no_mangle]
pub extern "C" fn rb_init(config_ptr: i32, config_len: i32) -> i64 {
    protocol_handler(config_ptr, config_len, |input| {
        match serde_json::from_slice::<PgConfig>(input) {
            Ok(config) => {
                host_ffi::log(
                    2,
                    &format!(
                        "dest-postgres: init with host={} db={} schema={}",
                        config.host, config.database, config.schema
                    ),
                );

                // Store config for later use by rb_write_batch
                let _ = CONFIG.set(config);
                make_ok_response(())
            }
            Err(e) => make_err_response("INVALID_CONFIG", &format!("Invalid config: {}", e)),
        }
    })
}

#[no_mangle]
pub extern "C" fn rb_validate(config_ptr: i32, config_len: i32) -> i64 {
    protocol_handler(config_ptr, config_len, |input| {
        let config: PgConfig = match serde_json::from_slice(input) {
            Ok(c) => c,
            Err(e) => {
                return make_err_response("INVALID_CONFIG", &format!("Invalid config: {}", e));
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
pub extern "C" fn rb_write_batch(
    stream_ptr: i32,
    stream_len: i32,
    batch_ptr: i32,
    batch_len: i32,
) -> i64 {
    if CONFIG.get().is_none() {
        let result = make_err_response("NO_CONFIG", "No config available. Call rb_init first.");
        let (ptr, len) = write_guest_bytes(&result);
        return pack_ptr_len(ptr, len);
    }

    let stream_name = unsafe {
        let bytes = std::slice::from_raw_parts(stream_ptr as *const u8, stream_len as usize);
        String::from_utf8_lossy(bytes).to_string()
    };

    let ipc_bytes = unsafe {
        std::slice::from_raw_parts(batch_ptr as *const u8, batch_len as usize)
    };

    let buffer = BATCH_BUFFER.get_or_init(|| Mutex::new(Vec::new()));
    buffer.lock().unwrap().push((stream_name, ipc_bytes.to_vec()));

    let result = make_ok_response(());
    let (ptr, len) = write_guest_bytes(&result);
    pack_ptr_len(ptr, len)
}

#[no_mangle]
pub extern "C" fn rb_write_finalize(input_ptr: i32, input_len: i32) -> i64 {
    protocol_handler(input_ptr, input_len, |_input| {
        let config = match CONFIG.get() {
            Some(c) => c,
            None => return make_err_response("NO_CONFIG", "No config available. Call rb_init first."),
        };

        let buffer = match BATCH_BUFFER.get() {
            Some(b) => b,
            None => {
                let summary = rapidbyte_sdk::protocol::WriteSummary {
                    records_written: 0,
                    bytes_written: 0,
                };
                return make_ok_response(summary);
            }
        };

        let batches: Vec<(String, Vec<u8>)> = {
            let mut guard = buffer.lock().unwrap();
            std::mem::take(&mut *guard)
        };

        if batches.is_empty() {
            let summary = rapidbyte_sdk::protocol::WriteSummary {
                records_written: 0,
                bytes_written: 0,
            };
            return make_ok_response(summary);
        }

        host_ffi::log(2, &format!("dest-postgres: write finalize — flushing {} batches", batches.len()));

        let rt = create_runtime();
        rt.block_on(async {
            match connect(&config).await {
                Ok(client) => {
                    let mut total_rows: u64 = 0;
                    let mut total_bytes: u64 = 0;

                    if let Err(e) = client.execute("BEGIN", &[]).await {
                        return make_err_response("TX_FAILED", &format!("BEGIN failed: {}", e));
                    }

                    let mut created_tables = std::collections::HashSet::new();

                    for (stream_name, ipc_bytes) in &batches {
                        match sink::write_batch(&client, &config.schema, stream_name, ipc_bytes, &mut created_tables).await {
                            Ok(count) => {
                                total_rows += count;
                                total_bytes += ipc_bytes.len() as u64;
                                host_ffi::report_progress(count, ipc_bytes.len() as u64);
                            }
                            Err(e) => {
                                let _ = client.execute("ROLLBACK", &[]).await;
                                return make_err_response("WRITE_FAILED", &e);
                            }
                        }
                    }

                    if let Err(e) = client.execute("COMMIT", &[]).await {
                        return make_err_response("TX_FAILED", &format!("COMMIT failed: {}", e));
                    }

                    host_ffi::log(2, &format!("dest-postgres: flushed {} rows in {} batches", total_rows, batches.len()));

                    let summary = rapidbyte_sdk::protocol::WriteSummary {
                        records_written: total_rows,
                        bytes_written: total_bytes,
                    };
                    make_ok_response(summary)
                }
                Err(e) => make_err_response("CONNECTION_FAILED", &e),
            }
        })
    })
}

fn main() {
    // Wasm entry point — not used directly, protocol functions are called by the host
}
