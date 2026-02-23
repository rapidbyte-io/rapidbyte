//! Arrow batch write dispatch (INSERT/COPY).
//!
//! This module dispatches decoded Arrow batches to COPY or INSERT.
//! Per-row fallback paths are intentionally removed: batch write failures are
//! terminal and return an error immediately.

mod copy;
mod copy_format;
mod insert;
mod typed_col;

use std::collections::HashSet;
use std::sync::Arc;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::protocol::SchemaEvolutionPolicy;

use crate::config::LoadMethod;

use self::copy::copy_batch;
use self::insert::insert_batch;

/// Maximum rows per multi-value INSERT statement.
pub(crate) const INSERT_CHUNK_SIZE: usize = 1000;

/// Default COPY flush buffer size.
pub(crate) const DEFAULT_COPY_FLUSH_BYTES: usize = 4 * 1024 * 1024;

/// Result of a write operation with per-row error tracking.
pub(crate) struct WriteResult {
    pub(crate) rows_written: u64,
    pub(crate) rows_failed: u64,
}

/// Bundled parameters for batch write operations.
pub(crate) struct WriteContext<'a> {
    pub(crate) client: &'a Client,
    pub(crate) target_schema: &'a str,
    /// Physical write target (staging table in Replace mode).
    pub(crate) stream_name: &'a str,
    pub(crate) created_tables: &'a mut HashSet<String>,
    pub(crate) write_mode: Option<&'a WriteMode>,
    pub(crate) schema_policy: Option<&'a SchemaEvolutionPolicy>,
    pub(crate) load_method: LoadMethod,
    pub(crate) ignored_columns: &'a mut HashSet<String>,
    pub(crate) type_null_columns: &'a mut HashSet<String>,
    pub(crate) copy_flush_bytes: Option<usize>,
}

/// Write decoded Arrow batches to PostgreSQL.
///
/// Dispatches to COPY or INSERT based on `ctx.load_method`. If the load method
/// is COPY but write mode is Upsert, dispatches to INSERT because COPY does not
/// support `ON CONFLICT`.
pub(crate) async fn write_batch(
    sdk_ctx: &Context,
    ctx: &mut WriteContext<'_>,
    arrow_schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<WriteResult, String> {
    if batches.is_empty() {
        return Ok(WriteResult {
            rows_written: 0,
            rows_failed: 0,
        });
    }

    let use_copy =
        ctx.load_method == LoadMethod::Copy && !matches!(ctx.write_mode, Some(WriteMode::Upsert { .. }));

    let (result, method_name) = if use_copy {
        (copy_batch(sdk_ctx, ctx, arrow_schema, batches).await, "COPY")
    } else {
        (insert_batch(sdk_ctx, ctx, arrow_schema, batches).await, "INSERT")
    };

    let rows_written = result.map_err(|e| {
        format!("{} failed for stream {}: {e}", method_name, ctx.stream_name)
    })?;

    Ok(WriteResult {
        rows_written,
        rows_failed: 0,
    })
}
