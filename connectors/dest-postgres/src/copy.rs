//! COPY FROM STDIN write path.
//!
//! Streams Arrow RecordBatch data to PostgreSQL via the COPY text protocol.
//! Flushes at configurable byte thresholds to bound memory usage.

use std::sync::Arc;

use bytes::Bytes;
use futures_util::SinkExt;
use pg_escape::quote_identifier;
use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;

use crate::decode::{downcast_columns, format_copy_value};

/// Default COPY flush buffer size (4 MB).
const DEFAULT_FLUSH_BYTES: usize = 4 * 1024 * 1024;

/// Write batches via COPY FROM STDIN. Returns rows written.
///
/// Parameters are all pre-computed by the session layer:
/// - `qualified_table`: schema-qualified table name
/// - `active_cols`: indices of non-ignored columns
/// - `type_null_flags`: per-column flag forcing NULL for type-incompatible columns
/// - `flush_bytes`: optional flush threshold override
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write(
    ctx: &Context,
    client: &Client,
    qualified_table: &str,
    active_cols: &[usize],
    arrow_schema: &Arc<Schema>,
    batches: &[RecordBatch],
    type_null_flags: &[bool],
    flush_bytes: Option<usize>,
) -> Result<u64, String> {
    if batches.is_empty() || active_cols.is_empty() {
        return Ok(0);
    }

    let col_list = active_cols
        .iter()
        .map(|&i| quote_identifier(arrow_schema.field(i).name()))
        .collect::<Vec<_>>()
        .join(", ");
    let copy_stmt = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT text)",
        qualified_table, col_list
    );

    let sink = client
        .copy_in(&copy_stmt)
        .await
        .map_err(|e| format!("COPY start failed: {e}"))?;
    let mut sink = Box::pin(sink);

    let flush_threshold = flush_bytes.unwrap_or(DEFAULT_FLUSH_BYTES).max(1);
    let mut total_rows: u64 = 0;
    let mut buf = Vec::with_capacity(flush_threshold);

    for batch in batches {
        let typed_cols = downcast_columns(batch, active_cols)?;

        for row_idx in 0..batch.num_rows() {
            for (pos, typed_col) in typed_cols.iter().enumerate() {
                if pos > 0 {
                    buf.push(b'\t');
                }
                if type_null_flags[pos] {
                    buf.extend_from_slice(b"\\N");
                } else {
                    format_copy_value(&mut buf, typed_col, row_idx);
                }
            }
            buf.push(b'\n');
            total_rows += 1;

            if buf.len() >= flush_threshold {
                sink.send(Bytes::from(std::mem::take(&mut buf)))
                    .await
                    .map_err(|e| format!("COPY send failed: {e}"))?;
                buf = Vec::with_capacity(flush_threshold);
            }
        }
    }

    if !buf.is_empty() {
        sink.send(Bytes::from(buf))
            .await
            .map_err(|e| format!("COPY send failed: {e}"))?;
    }

    let _rows = sink
        .as_mut()
        .finish()
        .await
        .map_err(|e| format!("COPY finish failed: {e}"))?;

    ctx.log(
        LogLevel::Info,
        &format!(
            "dest-postgres: COPY wrote {} rows to {}",
            total_rows, qualified_table
        ),
    );

    Ok(total_rows)
}
