//! COPY FROM STDIN write path.

use std::sync::Arc;

use anyhow::Context;
use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures_util::SinkExt;
use pg_escape::quote_identifier;

use rapidbyte_sdk::host_ffi;

use crate::batch::copy_format::format_copy_typed_value;
use crate::batch::insert::active_column_indices;
use crate::batch::typed_col::downcast_columns;
use crate::batch::{WriteContext, DEFAULT_COPY_FLUSH_BYTES};
use crate::ddl::ensure_table_and_schema;

/// Write via PostgreSQL COPY protocol and return rows written.
pub(crate) async fn copy_batch(
    ctx: &mut WriteContext<'_>,
    arrow_schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> anyhow::Result<u64> {
    if batches.is_empty() {
        return Ok(0);
    }

    let qualified_table = format!(
        "{}.{}",
        quote_identifier(ctx.target_schema),
        quote_identifier(ctx.stream_name)
    );

    ensure_table_and_schema(
        ctx.client,
        ctx.target_schema,
        ctx.stream_name,
        ctx.created_tables,
        ctx.write_mode,
        ctx.schema_policy,
        arrow_schema,
        ctx.ignored_columns,
        ctx.type_null_columns,
    )
    .await
    .with_context(|| format!("Failed to ensure table/schema for {}", ctx.stream_name))?;

    let active_cols = active_column_indices(arrow_schema, ctx.ignored_columns);
    if active_cols.is_empty() {
        host_ffi::log(1, "dest-postgres: all columns ignored, skipping COPY batch");
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

    let sink = ctx
        .client
        .copy_in(&copy_stmt)
        .await
        .context("COPY start failed")?;
    let mut sink = Box::pin(sink);

    let flush_threshold = ctx
        .copy_flush_bytes
        .unwrap_or(DEFAULT_COPY_FLUSH_BYTES)
        .max(1);

    let mut total_rows: u64 = 0;
    let mut buf = Vec::with_capacity(flush_threshold);

    for batch in batches {
        let typed_cols = downcast_columns(batch, &active_cols).map_err(|e| anyhow::anyhow!(e))?;
        let type_null_flags: Vec<bool> = active_cols
            .iter()
            .map(|&i| ctx.type_null_columns.contains(arrow_schema.field(i).name()))
            .collect();

        for row_idx in 0..batch.num_rows() {
            for (pos, typed_col) in typed_cols.iter().enumerate() {
                if pos > 0 {
                    buf.push(b'\t');
                }
                if type_null_flags[pos] {
                    buf.extend_from_slice(b"\\N");
                } else {
                    format_copy_typed_value(&mut buf, typed_col, row_idx);
                }
            }
            buf.push(b'\n');
            total_rows += 1;

            if buf.len() >= flush_threshold {
                sink.send(Bytes::from(std::mem::take(&mut buf)))
                    .await
                    .context("COPY send failed")?;
                buf = Vec::with_capacity(flush_threshold);
            }
        }
    }

    if !buf.is_empty() {
        sink.send(Bytes::from(buf))
            .await
            .context("COPY send failed")?;
    }

    let _rows = sink.as_mut().finish().await.context("COPY finish failed")?;

    host_ffi::log(
        2,
        &format!(
            "dest-postgres: COPY wrote {} rows to {}",
            total_rows, qualified_table
        ),
    );

    Ok(total_rows)
}
