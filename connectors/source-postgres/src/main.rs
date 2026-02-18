pub mod schema;
pub mod source;

use std::sync::OnceLock;

use rapidbyte_sdk::errors::{ConnectorError, ConnectorResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::memory::{pack_ptr_len, write_guest_bytes};
use rapidbyte_sdk::protocol::{
    Catalog, ConfigBlob, OpenContext, OpenInfo, StreamContext,
};

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
}

fn default_port() -> u16 {
    5432
}

impl PgConfig {
    fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}

/// Helper: read input bytes from (ptr, len), deserialize JSON, run function, serialize result.
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
                return make_err_response("INVALID_OPEN_CTX", &format!("Invalid OpenContext: {}", e));
            }
        };

        // Extract config from ConfigBlob
        let config_value = match &open_ctx.config {
            ConfigBlob::Json(v) => v.clone(),
        };

        let pg_config: PgConfig = match serde_json::from_value(config_value) {
            Ok(c) => c,
            Err(e) => {
                return make_err_response("INVALID_CONFIG", &format!("Invalid PG config: {}", e));
            }
        };

        host_ffi::log(
            2,
            &format!(
                "source-postgres: open with host={} db={}",
                pg_config.host, pg_config.database
            ),
        );

        if CONFIG.set(pg_config).is_err() {
            host_ffi::log(
                1,
                "source-postgres: rb_open called more than once; using first config",
            );
        }

        make_ok_response(OpenInfo {
            protocol_version: "1".to_string(),
            features: vec![],
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
                        return make_err_response("INVALID_CONFIG", &format!("Invalid config: {}", e));
                    }
                },
            }
        } else {
            host_ffi::log(1, "source-postgres: rb_validate received non-OpenContext input (v0 compat)");
            match serde_json::from_slice(input) {
                Ok(c) => c,
                Err(e) => {
                    return make_err_response("INVALID_CONFIG", &format!("Invalid config: {}", e));
                }
            }
        };

        let rt = create_runtime();
        rt.block_on(async {
            match connect(&config).await {
                Ok(client) => {
                    match client.query_one("SELECT 1", &[]).await {
                        Ok(_) => {
                            let result = rapidbyte_sdk::errors::ValidationResult {
                                status: rapidbyte_sdk::errors::ValidationStatus::Success,
                                message: format!(
                                    "Connected to {}:{}/{}",
                                    config.host, config.port, config.database
                                ),
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
pub extern "C" fn rb_discover(config_ptr: i32, config_len: i32) -> i64 {
    protocol_handler(config_ptr, config_len, |input| {
        // Try OpenContext first, fall back to raw config
        let config: PgConfig = if let Ok(open_ctx) = serde_json::from_slice::<OpenContext>(input) {
            match &open_ctx.config {
                ConfigBlob::Json(v) => match serde_json::from_value(v.clone()) {
                    Ok(c) => c,
                    Err(_) => {
                        return make_err_response("NO_CONFIG", "Discover requires valid config");
                    }
                },
            }
        } else {
            host_ffi::log(1, "source-postgres: rb_discover received non-OpenContext input (v0 compat)");
            match serde_json::from_slice(input) {
                Ok(c) => c,
                Err(_) => {
                    // If input is empty/dummy, use stored config
                    match CONFIG.get() {
                        Some(c) => {
                            // Can't clone PgConfig (no Clone), reconnect with stored config
                            let rt = create_runtime();
                            return rt.block_on(async {
                                match connect(c).await {
                                    Ok(client) => match schema::discover_catalog(&client).await {
                                        Ok(streams) => make_ok_response(Catalog { streams }),
                                        Err(e) => make_err_response("DISCOVERY_FAILED", &e),
                                    },
                                    Err(e) => make_err_response("CONNECTION_FAILED", &e),
                                }
                            });
                        }
                        None => {
                            return make_err_response("NO_CONFIG", "Discover requires PostgreSQL config");
                        }
                    }
                }
            }
        };

        let rt = create_runtime();
        rt.block_on(async {
            match connect(&config).await {
                Ok(client) => match schema::discover_catalog(&client).await {
                    Ok(streams) => make_ok_response(Catalog { streams }),
                    Err(e) => make_err_response("DISCOVERY_FAILED", &e),
                },
                Err(e) => make_err_response("CONNECTION_FAILED", &e),
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn rb_run_read(request_ptr: i32, request_len: i32) -> i64 {
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

        let rt = create_runtime();
        rt.block_on(async {
            match connect(config).await {
                Ok(client) => match source::read_stream_v1(&client, &stream_ctx).await {
                    Ok(summary) => make_ok_response(summary),
                    Err(e) => make_err_response("READ_FAILED", &e),
                },
                Err(e) => make_err_response("CONNECTION_FAILED", &e),
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn rb_close() -> i32 {
    host_ffi::log(2, "source-postgres: close (no-op)");
    0
}

fn main() {
    // Wasm entry point — not used directly, protocol functions are called by the host
}
