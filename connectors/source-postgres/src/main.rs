pub mod schema;
pub mod source;

use rapidbyte_sdk::errors::{ConnectorError, ConnectorResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::memory::{pack_ptr_len, write_guest_bytes};
use rapidbyte_sdk::protocol::{Catalog, ReadRequest};

use serde::Deserialize;
use tokio_postgres::NoTls;

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

// === Exported Protocol Functions ===

#[no_mangle]
pub extern "C" fn rb_init(config_ptr: i32, config_len: i32) -> i64 {
    protocol_handler(config_ptr, config_len, |input| {
        match serde_json::from_slice::<PgConfig>(input) {
            Ok(config) => {
                host_ffi::log(
                    2,
                    &format!("source-postgres: init with host={} db={}", config.host, config.database),
                );

                // Verify we can parse the config, but don't connect yet
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
                    // Test the connection with a simple query
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
        // The discover input is the config (passed during init, but we re-parse here)
        // For the protocol, discover receives a dummy {} but we need the config.
        // We'll use the config that was passed during init.
        // For v0.1, the host passes the config again.
        let config: PgConfig = match serde_json::from_slice(input) {
            Ok(c) => c,
            Err(_) => {
                // If input is empty/dummy, we can't discover without config
                return make_err_response(
                    "NO_CONFIG",
                    "Discover requires PostgreSQL config",
                );
            }
        };

        let rt = create_runtime();
        rt.block_on(async {
            match connect(&config).await {
                Ok(client) => match schema::discover_catalog(&client).await {
                    Ok(streams) => {
                        let catalog = Catalog { streams };
                        make_ok_response(catalog)
                    }
                    Err(e) => make_err_response("DISCOVERY_FAILED", &e),
                },
                Err(e) => make_err_response("CONNECTION_FAILED", &e),
            }
        })
    })
}

#[no_mangle]
pub extern "C" fn rb_read(request_ptr: i32, request_len: i32) -> i64 {
    protocol_handler(request_ptr, request_len, |input| {
        let request: ReadRequest = match serde_json::from_slice(input) {
            Ok(r) => r,
            Err(e) => {
                return make_err_response(
                    "INVALID_REQUEST",
                    &format!("Invalid read request: {}", e),
                );
            }
        };

        // For v0.1, the config is passed as part of the state
        // In practice, the host calls rb_init first, which stores the config.
        // We need the config to connect. For now, use state or env vars.
        // Actually, looking at the protocol flow: init stores config, read uses it.
        // We need global state for this. For v0.1, read the config from state.

        // Read config from state (set during rb_init via host)
        let config_json = match host_ffi::get_state("pg_config") {
            Some(v) => v,
            None => {
                // Fallback: try to read from the request's state field
                match &request.state {
                    Some(state) => match serde_json::from_value::<PgConfig>(state.clone()) {
                        Ok(_) => state.clone(),
                        Err(e) => {
                            return make_err_response(
                                "NO_CONFIG",
                                &format!("No config available for read: {}", e),
                            );
                        }
                    },
                    None => {
                        return make_err_response(
                            "NO_CONFIG",
                            "No config available. Call rb_init first.",
                        );
                    }
                }
            }
        };

        let config: PgConfig = match serde_json::from_value(config_json) {
            Ok(c) => c,
            Err(e) => {
                return make_err_response(
                    "INVALID_CONFIG",
                    &format!("Invalid stored config: {}", e),
                );
            }
        };

        let rt = create_runtime();
        rt.block_on(async {
            match connect(&config).await {
                Ok(client) => match source::read_streams(&client, &request).await {
                    Ok(summary) => make_ok_response(summary),
                    Err(e) => make_err_response("READ_FAILED", &e),
                },
                Err(e) => make_err_response("CONNECTION_FAILED", &e),
            }
        })
    })
}

fn main() {
    // Wasm entry point — not used directly, protocol functions are called by the host
}
