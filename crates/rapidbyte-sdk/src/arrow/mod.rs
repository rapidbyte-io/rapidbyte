//! Arrow integration for Rapidbyte plugins.
//!
//! All data in Rapidbyte flows as Arrow RecordBatches. This module provides
//! the tools plugin authors need to work with Arrow:
//!
//! - [`build_arrow_schema`] — convert v7 `SchemaField` values to Arrow `Schema`
//! - [`encode_ipc`] / [`decode_ipc`] — IPC serialization (used internally by
//!   `host_ffi::emit_batch` and `host_ffi::next_batch`)
//! - [`arrow_data_type`] — convert protocol `ArrowDataType` to Arrow `DataType`
//!
//! Arrow crate types are re-exported directly so plugins never need a
//! separate `arrow` dependency:
//!
//! ```ignore
//! use rapidbyte_sdk::arrow::array::Int32Array;
//! use rapidbyte_sdk::arrow::datatypes::{DataType, Field, Schema};
//! use rapidbyte_sdk::arrow::record_batch::RecordBatch;
//! ```

pub mod ipc;
pub mod schema;
pub mod types;

pub use ipc::{decode_ipc, encode_ipc, encode_ipc_into};
pub use schema::build_arrow_schema;
pub use types::arrow_data_type;

// Re-export arrow crate submodules so plugins use `rapidbyte_sdk::arrow::*`
// instead of depending on `arrow` directly.
pub use arrow::array;
pub use arrow::datatypes;
pub use arrow::record_batch;
