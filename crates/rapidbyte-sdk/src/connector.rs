//! Standard connector traits.
//!
//! Every Rapidbyte connector implements one or both of these traits.
//! The `source_connector_main!` and `dest_connector_main!` macros
//! generate the FFI glue (`rb_open`, `rb_run_read`, etc.) that the
//! host calls.

use crate::errors::{ConnectorError, ValidationResult};
use crate::protocol::{Catalog, OpenContext, OpenInfo, ReadSummary, StreamContext, WriteSummary};

/// Source connector trait.
///
/// Lifecycle: `open` -> (`discover` | `validate` | `read`) -> `close`
pub trait SourceConnector: Default {
    fn open(&mut self, ctx: OpenContext) -> Result<OpenInfo, ConnectorError>;
    fn discover(&mut self) -> Result<Catalog, ConnectorError>;
    fn validate(&mut self) -> Result<ValidationResult, ConnectorError>;
    fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError>;
    fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

/// Destination connector trait.
///
/// Lifecycle: `open` -> (`validate` | `write`) -> `close`
pub trait DestinationConnector: Default {
    fn open(&mut self, ctx: OpenContext) -> Result<OpenInfo, ConnectorError>;
    fn validate(&mut self) -> Result<ValidationResult, ConnectorError>;
    fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError>;
    fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FFI macro: source_connector_main!
// ---------------------------------------------------------------------------

/// Generate all `rb_*` FFI exports for a source connector.
///
/// Usage:
/// ```ignore
/// use rapidbyte_sdk::source_connector_main;
///
/// struct MySource { /* ... */ }
/// impl Default for MySource { /* ... */ }
/// impl rapidbyte_sdk::connector::SourceConnector for MySource { /* ... */ }
///
/// source_connector_main!(MySource);
/// ```
#[macro_export]
macro_rules! source_connector_main {
    ($connector_type:ty) => {
        use std::cell::RefCell;
        use std::sync::OnceLock;

        static CONNECTOR: OnceLock<RefCell<$connector_type>> = OnceLock::new();

        fn get_connector() -> &'static RefCell<$connector_type> {
            CONNECTOR.get_or_init(|| RefCell::new(<$connector_type>::default()))
        }

        fn protocol_handler<F>(input_ptr: i32, input_len: i32, handler: F) -> i64
        where
            F: FnOnce(&[u8]) -> Vec<u8>,
        {
            let input =
                unsafe { std::slice::from_raw_parts(input_ptr as *const u8, input_len as usize) };
            let result_bytes = handler(input);
            let (ptr, len) = rapidbyte_sdk::memory::write_guest_bytes(&result_bytes);
            rapidbyte_sdk::memory::pack_ptr_len(ptr, len)
        }

        fn make_ok<T: serde::Serialize>(data: T) -> Vec<u8> {
            let result: rapidbyte_sdk::errors::ConnectorResult<T> =
                rapidbyte_sdk::errors::ConnectorResult::Ok { data };
            serde_json::to_vec(&result).unwrap()
        }

        fn make_err(error: rapidbyte_sdk::errors::ConnectorError) -> Vec<u8> {
            let result: rapidbyte_sdk::errors::ConnectorResult<()> =
                rapidbyte_sdk::errors::ConnectorResult::Err { error };
            serde_json::to_vec(&result).unwrap()
        }

        #[no_mangle]
        pub extern "C" fn rb_open(ptr: i32, len: i32) -> i64 {
            protocol_handler(ptr, len, |input| {
                let ctx: rapidbyte_sdk::protocol::OpenContext = match serde_json::from_slice(input) {
                    Ok(c) => c,
                    Err(e) => {
                        return make_err(rapidbyte_sdk::errors::ConnectorError::config(
                            "INVALID_OPEN_CTX",
                            format!("Invalid OpenContext: {}", e),
                        ));
                    }
                };

                let mut conn = get_connector().borrow_mut();
                match <$connector_type as rapidbyte_sdk::connector::SourceConnector>::open(
                    &mut *conn, ctx,
                ) {
                    Ok(info) => make_ok(info),
                    Err(e) => make_err(e),
                }
            })
        }

        #[no_mangle]
        pub extern "C" fn rb_discover(ptr: i32, len: i32) -> i64 {
            protocol_handler(ptr, len, |_input| {
                let mut conn = get_connector().borrow_mut();
                match <$connector_type as rapidbyte_sdk::connector::SourceConnector>::discover(
                    &mut *conn,
                ) {
                    Ok(catalog) => make_ok(catalog),
                    Err(e) => make_err(e),
                }
            })
        }

        #[no_mangle]
        pub extern "C" fn rb_validate(ptr: i32, len: i32) -> i64 {
            protocol_handler(ptr, len, |_input| {
                let mut conn = get_connector().borrow_mut();
                match <$connector_type as rapidbyte_sdk::connector::SourceConnector>::validate(
                    &mut *conn,
                ) {
                    Ok(result) => make_ok(result),
                    Err(e) => make_err(e),
                }
            })
        }

        #[no_mangle]
        pub extern "C" fn rb_run_read(ptr: i32, len: i32) -> i64 {
            protocol_handler(ptr, len, |input| {
                let ctx: rapidbyte_sdk::protocol::StreamContext =
                    match serde_json::from_slice(input) {
                        Ok(c) => c,
                        Err(e) => {
                            return make_err(rapidbyte_sdk::errors::ConnectorError::config(
                                "INVALID_STREAM_CTX",
                                format!("Invalid StreamContext: {}", e),
                            ));
                        }
                    };

                let mut conn = get_connector().borrow_mut();
                match <$connector_type as rapidbyte_sdk::connector::SourceConnector>::read(
                    &mut *conn, ctx,
                ) {
                    Ok(summary) => make_ok(summary),
                    Err(e) => make_err(e),
                }
            })
        }

        #[no_mangle]
        pub extern "C" fn rb_close() -> i32 {
            let mut conn = get_connector().borrow_mut();
            match <$connector_type as rapidbyte_sdk::connector::SourceConnector>::close(&mut *conn)
            {
                Ok(()) => 0,
                Err(_) => -1,
            }
        }

        pub use rapidbyte_sdk::memory::{rb_allocate, rb_deallocate};

        fn main() {
            // Wasm entry point -- not used directly, protocol functions are called by the host
        }
    };
}

// ---------------------------------------------------------------------------
// FFI macro: dest_connector_main!
// ---------------------------------------------------------------------------

/// Generate all `rb_*` FFI exports for a destination connector.
///
/// Usage:
/// ```ignore
/// use rapidbyte_sdk::dest_connector_main;
///
/// struct MyDest { /* ... */ }
/// impl Default for MyDest { /* ... */ }
/// impl rapidbyte_sdk::connector::DestinationConnector for MyDest { /* ... */ }
///
/// dest_connector_main!(MyDest);
/// ```
#[macro_export]
macro_rules! dest_connector_main {
    ($connector_type:ty) => {
        use std::cell::RefCell;
        use std::sync::OnceLock;

        static CONNECTOR: OnceLock<RefCell<$connector_type>> = OnceLock::new();

        fn get_connector() -> &'static RefCell<$connector_type> {
            CONNECTOR.get_or_init(|| RefCell::new(<$connector_type>::default()))
        }

        fn protocol_handler<F>(input_ptr: i32, input_len: i32, handler: F) -> i64
        where
            F: FnOnce(&[u8]) -> Vec<u8>,
        {
            let input =
                unsafe { std::slice::from_raw_parts(input_ptr as *const u8, input_len as usize) };
            let result_bytes = handler(input);
            let (ptr, len) = rapidbyte_sdk::memory::write_guest_bytes(&result_bytes);
            rapidbyte_sdk::memory::pack_ptr_len(ptr, len)
        }

        fn make_ok<T: serde::Serialize>(data: T) -> Vec<u8> {
            let result: rapidbyte_sdk::errors::ConnectorResult<T> =
                rapidbyte_sdk::errors::ConnectorResult::Ok { data };
            serde_json::to_vec(&result).unwrap()
        }

        fn make_err(error: rapidbyte_sdk::errors::ConnectorError) -> Vec<u8> {
            let result: rapidbyte_sdk::errors::ConnectorResult<()> =
                rapidbyte_sdk::errors::ConnectorResult::Err { error };
            serde_json::to_vec(&result).unwrap()
        }

        #[no_mangle]
        pub extern "C" fn rb_open(ptr: i32, len: i32) -> i64 {
            protocol_handler(ptr, len, |input| {
                let ctx: rapidbyte_sdk::protocol::OpenContext = match serde_json::from_slice(input) {
                    Ok(c) => c,
                    Err(e) => {
                        return make_err(rapidbyte_sdk::errors::ConnectorError::config(
                            "INVALID_OPEN_CTX",
                            format!("Invalid OpenContext: {}", e),
                        ));
                    }
                };

                let mut conn = get_connector().borrow_mut();
                match <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::open(
                    &mut *conn, ctx,
                ) {
                    Ok(info) => make_ok(info),
                    Err(e) => make_err(e),
                }
            })
        }

        #[no_mangle]
        pub extern "C" fn rb_validate(ptr: i32, len: i32) -> i64 {
            protocol_handler(ptr, len, |_input| {
                let mut conn = get_connector().borrow_mut();
                match <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::validate(
                    &mut *conn,
                ) {
                    Ok(result) => make_ok(result),
                    Err(e) => make_err(e),
                }
            })
        }

        #[no_mangle]
        pub extern "C" fn rb_run_write(ptr: i32, len: i32) -> i64 {
            protocol_handler(ptr, len, |input| {
                let ctx: rapidbyte_sdk::protocol::StreamContext =
                    match serde_json::from_slice(input) {
                        Ok(c) => c,
                        Err(e) => {
                            return make_err(rapidbyte_sdk::errors::ConnectorError::config(
                                "INVALID_STREAM_CTX",
                                format!("Invalid StreamContext: {}", e),
                            ));
                        }
                    };

                let mut conn = get_connector().borrow_mut();
                match <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::write(
                    &mut *conn, ctx,
                ) {
                    Ok(summary) => make_ok(summary),
                    Err(e) => make_err(e),
                }
            })
        }

        #[no_mangle]
        pub extern "C" fn rb_close() -> i32 {
            let mut conn = get_connector().borrow_mut();
            match <$connector_type as rapidbyte_sdk::connector::DestinationConnector>::close(
                &mut *conn,
            ) {
                Ok(()) => 0,
                Err(_) => -1,
            }
        }

        pub use rapidbyte_sdk::memory::{rb_allocate, rb_deallocate};

        fn main() {
            // Wasm entry point -- not used directly, protocol functions are called by the host
        }
    };
}
