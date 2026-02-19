use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures_util::SinkExt;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{DataErrorPolicy, SchemaEvolutionPolicy, WriteMode};
use rapidbyte_sdk::validation::validate_pg_identifier;

use crate::ddl::ensure_table_and_schema;
use crate::format::{downcast_columns, format_copy_value, write_sql_value};

/// Result of a write operation with per-row error tracking.
pub(crate) struct WriteResult {
    pub(crate) rows_written: u64,
    pub(crate) rows_failed: u64,
}

/// Bundled parameters for batch write operations.
///
/// Groups the common arguments needed by `write_batch`
/// to keep function signatures under clippy's argument limit.
pub(crate) struct WriteContext<'a> {
    pub(crate) client: &'a Client,
    pub(crate) target_schema: &'a str,
    pub(crate) stream_name: &'a str,
    pub(crate) created_tables: &'a mut HashSet<String>,
    pub(crate) write_mode: Option<&'a WriteMode>,
    pub(crate) schema_policy: Option<&'a SchemaEvolutionPolicy>,
    pub(crate) on_data_error: DataErrorPolicy,
    pub(crate) load_method: &'a str,
    pub(crate) ignored_columns: &'a mut HashSet<String>,
    pub(crate) type_null_columns: &'a mut HashSet<String>,
}

/// Maximum rows per multi-value INSERT statement.
/// Keeps SQL string size manageable (~350KB for 7 columns).
const INSERT_CHUNK_SIZE: usize = 1000;

/// Buffer size for COPY data before flushing to the sink.
const COPY_FLUSH_BYTES: usize = 4 * 1024 * 1024; // 4MB

/// Decode Arrow IPC bytes into schema and record batches.
fn decode_ipc(ipc_bytes: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("Failed to read Arrow IPC: {}", e))?;
    let schema = reader.schema();
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read batches: {}", e))?;
    Ok((schema, batches))
}

/// Write an Arrow IPC batch to PostgreSQL.
///
/// Dispatches to COPY or INSERT based on `ctx.load_method`. If the load method
/// is "copy" but the write mode is Upsert, automatically falls back to INSERT
/// (COPY cannot handle ON CONFLICT). On batch failure with Skip/Dlq error
/// policy, falls back to single-row INSERTs.
pub(crate) async fn write_batch(ctx: &mut WriteContext<'_>, ipc_bytes: &[u8]) -> Result<(WriteResult, u64), String> {
    // Decode Arrow IPC with timing
    let decode_start = Instant::now();
    let (arrow_schema, batches) = decode_ipc(ipc_bytes)?;
    let decode_nanos = decode_start.elapsed().as_nanos() as u64;

    if batches.is_empty() {
        return Ok((WriteResult { rows_written: 0, rows_failed: 0 }, decode_nanos));
    }

    let use_copy = ctx.load_method == "copy"
        && !matches!(ctx.write_mode, Some(WriteMode::Upsert { .. }));

    let (result, method_name) = if use_copy {
        (copy_batch(ctx, &arrow_schema, &batches).await, "COPY")
    } else {
        (insert_batch(ctx, &arrow_schema, &batches).await, "INSERT")
    };

    match result {
        Ok(count) => Ok((WriteResult { rows_written: count, rows_failed: 0 }, decode_nanos)),
        Err(e) => {
            if matches!(ctx.on_data_error, DataErrorPolicy::Fail) {
                return Err(e);
            }
            host_ffi::log(
                1,
                &format!(
                    "dest-postgres: {} failed ({}), falling back to per-row INSERT with skip",
                    method_name, e
                ),
            );
            let wr = write_rows_individually(
                ctx.client, ctx.target_schema, ctx.stream_name, &arrow_schema, &batches, ctx.write_mode,
                ctx.ignored_columns, ctx.type_null_columns,
            ).await?;
            Ok((wr, decode_nanos))
        }
    }
}

/// Internal: write via multi-value INSERT, returning row count.
///
/// Builds batched statements:
///   INSERT INTO t (c1, c2) VALUES (v1, v2), (v3, v4), ...
///
/// Chunks at INSERT_CHUNK_SIZE rows to keep SQL size bounded.
async fn insert_batch(ctx: &mut WriteContext<'_>, arrow_schema: &Arc<Schema>, batches: &[RecordBatch]) -> Result<u64, String> {
    if batches.is_empty() {
        return Ok(0);
    }

    let qualified_table = format!("\"{}\".\"{}\"", ctx.target_schema, ctx.stream_name);

    // Ensure schema + table exist (only on first encounter)
    ensure_table_and_schema(
        ctx.client, ctx.target_schema, ctx.stream_name,
        ctx.created_tables, ctx.write_mode, ctx.schema_policy,
        arrow_schema, ctx.ignored_columns, ctx.type_null_columns,
    ).await?;

    // Validate upsert primary_key columns
    if let Some(WriteMode::Upsert { primary_key }) = ctx.write_mode {
        for pk_col in primary_key {
            validate_pg_identifier(pk_col)
                .map_err(|e| format!("Invalid primary key column name '{}': {}", pk_col, e))?;
        }
    }

    // 3. Build active column indices (excluding ignored columns)
    let active_cols: Vec<usize> = (0..arrow_schema.fields().len())
        .filter(|&i| !ctx.ignored_columns.contains(arrow_schema.field(i).name()))
        .collect();

    if active_cols.is_empty() {
        host_ffi::log(1, "dest-postgres: all columns ignored, skipping batch");
        return Ok(0);
    }

    let col_list = active_cols
        .iter()
        .map(|&i| format!("\"{}\"", arrow_schema.field(i).name()))
        .collect::<Vec<_>>()
        .join(", ");

    // 4. Insert rows using multi-value INSERT
    let mut total_rows: u64 = 0;

    for batch in batches {
        let num_rows = batch.num_rows();

        // Pre-downcast columns once per batch (not per cell)
        let typed_cols = downcast_columns(batch, &active_cols);
        // Track which active columns are type-nulled
        let type_null_flags: Vec<bool> = active_cols
            .iter()
            .map(|&i| ctx.type_null_columns.contains(arrow_schema.field(i).name()))
            .collect();

        // Pre-compute upsert clause (same for every chunk)
        let upsert_clause: Option<String> =
            if let Some(WriteMode::Upsert { primary_key }) = ctx.write_mode {
                let pk_cols = primary_key
                    .iter()
                    .map(|k| format!("\"{}\"", k))
                    .collect::<Vec<_>>()
                    .join(", ");
                let update_cols: Vec<String> = active_cols
                    .iter()
                    .map(|&i| arrow_schema.field(i).name())
                    .filter(|name| !primary_key.contains(name))
                    .map(|name| format!("\"{}\" = EXCLUDED.\"{}\"", name, name))
                    .collect();
                if update_cols.is_empty() {
                    Some(format!(" ON CONFLICT ({}) DO NOTHING", pk_cols))
                } else {
                    Some(format!(
                        " ON CONFLICT ({}) DO UPDATE SET {}",
                        pk_cols,
                        update_cols.join(", ")
                    ))
                }
            } else {
                None
            };

        // Process in chunks
        for chunk_start in (0..num_rows).step_by(INSERT_CHUNK_SIZE) {
            let chunk_end = (chunk_start + INSERT_CHUNK_SIZE).min(num_rows);
            let chunk_size = chunk_end - chunk_start;

            // Pre-allocate: header + estimated row width (15 bytes/col avg) + separators
            let header = format!("INSERT INTO {} ({}) VALUES ", qualified_table, col_list);
            let estimated_row_width = active_cols.len() * 15; // avg value width per col
            let mut sql = String::with_capacity(
                header.len() + chunk_size * (estimated_row_width + 3), // 3 = "()" + ","
            );
            sql.push_str(&header);

            for row_idx in chunk_start..chunk_end {
                if row_idx > chunk_start {
                    sql.push_str(", ");
                }
                sql.push('(');
                for (pos, typed_col) in typed_cols.iter().enumerate() {
                    if pos > 0 {
                        sql.push_str(", ");
                    }
                    if type_null_flags[pos] {
                        sql.push_str("NULL");
                    } else {
                        write_sql_value(&mut sql, typed_col, row_idx);
                    }
                }
                sql.push(')');
            }

            // Append pre-computed ON CONFLICT clause for upsert mode
            if let Some(ref clause) = upsert_clause {
                sql.push_str(clause);
            }

            ctx.client
                .execute(&sql, &[])
                .await
                .map_err(|e| {
                    format!(
                        "Multi-value INSERT failed for {}, rows {}-{}: {}",
                        ctx.stream_name, chunk_start, chunk_end, e
                    )
                })?;

            total_rows += chunk_size as u64;
        }
    }

    host_ffi::log(
        2,
        &format!(
            "dest-postgres: wrote {} rows to {}",
            total_rows, qualified_table
        ),
    );

    Ok(total_rows)
}

/// Internal: write via COPY FROM STDIN, returning row count.
///
/// Streams rows as tab-separated text directly to PostgreSQL's COPY protocol,
/// bypassing SQL parsing for significantly higher throughput.
async fn copy_batch(ctx: &mut WriteContext<'_>, arrow_schema: &Arc<Schema>, batches: &[RecordBatch]) -> Result<u64, String> {
    if batches.is_empty() {
        return Ok(0);
    }

    let qualified_table = format!("\"{}\".\"{}\"", ctx.target_schema, ctx.stream_name);

    // Ensure schema + table exist (only on first encounter)
    ensure_table_and_schema(
        ctx.client, ctx.target_schema, ctx.stream_name,
        ctx.created_tables, ctx.write_mode, ctx.schema_policy,
        arrow_schema, ctx.ignored_columns, ctx.type_null_columns,
    ).await?;

    // Build active column indices (excluding ignored columns)
    let active_cols: Vec<usize> = (0..arrow_schema.fields().len())
        .filter(|&i| !ctx.ignored_columns.contains(arrow_schema.field(i).name()))
        .collect();

    if active_cols.is_empty() {
        host_ffi::log(1, "dest-postgres: all columns ignored, skipping COPY batch");
        return Ok(0);
    }

    let col_list = active_cols
        .iter()
        .map(|&i| format!("\"{}\"", arrow_schema.field(i).name()))
        .collect::<Vec<_>>()
        .join(", ");
    let copy_stmt = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT text)",
        qualified_table, col_list
    );

    // 4. Start COPY and stream data
    let sink = ctx
        .client
        .copy_in(&copy_stmt)
        .await
        .map_err(|e| format!("COPY start failed: {}", e))?;
    let mut sink = Box::pin(sink);

    let mut total_rows: u64 = 0;
    let mut buf = Vec::with_capacity(COPY_FLUSH_BYTES);

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            for (pos, &col_idx) in active_cols.iter().enumerate() {
                if pos > 0 {
                    buf.push(b'\t');
                }
                if ctx.type_null_columns.contains(arrow_schema.field(col_idx).name()) {
                    buf.extend_from_slice(b"\\N");
                } else {
                    format_copy_value(&mut buf, batch.column(col_idx).as_ref(), row_idx);
                }
            }
            buf.push(b'\n');
            total_rows += 1;

            // Flush periodically to avoid unbounded memory growth
            if buf.len() >= COPY_FLUSH_BYTES {
                sink.send(Bytes::from(std::mem::take(&mut buf)))
                    .await
                    .map_err(|e| format!("COPY send failed: {}", e))?;
                buf = Vec::with_capacity(COPY_FLUSH_BYTES);
            }
        }
    }

    // Flush remaining data
    if !buf.is_empty() {
        sink.send(Bytes::from(buf))
            .await
            .map_err(|e| format!("COPY send failed: {}", e))?;
    }

    let _rows = sink
        .as_mut()
        .finish()
        .await
        .map_err(|e| format!("COPY finish failed: {}", e))?;

    host_ffi::log(
        2,
        &format!(
            "dest-postgres: COPY wrote {} rows to {}",
            total_rows, qualified_table
        ),
    );

    Ok(total_rows)
}

/// Insert rows one at a time, skipping any that fail.
async fn write_rows_individually(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    arrow_schema: &Arc<Schema>,
    batches: &[RecordBatch],
    write_mode: Option<&WriteMode>,
    ignored_columns: &HashSet<String>,
    type_null_columns: &HashSet<String>,
) -> Result<WriteResult, String> {
    let qualified_table = format!("\"{}\".\"{}\"", target_schema, stream_name);

    // Build active column indices (excluding ignored columns)
    let active_cols: Vec<usize> = (0..arrow_schema.fields().len())
        .filter(|&i| !ignored_columns.contains(arrow_schema.field(i).name()))
        .collect();

    if active_cols.is_empty() {
        host_ffi::log(1, "dest-postgres: all columns ignored, skipping per-row writes");
        return Ok(WriteResult { rows_written: 0, rows_failed: 0 });
    }

    let col_list = active_cols
        .iter()
        .map(|&i| format!("\"{}\"", arrow_schema.field(i).name()))
        .collect::<Vec<_>>()
        .join(", ");

    let mut rows_written = 0u64;
    let mut rows_failed = 0u64;

    for batch in batches {
        let typed_cols = downcast_columns(batch, &active_cols);
        let type_null_flags: Vec<bool> = active_cols
            .iter()
            .map(|&i| type_null_columns.contains(arrow_schema.field(i).name()))
            .collect();

        for row_idx in 0..batch.num_rows() {
            let mut sql = format!("INSERT INTO {} ({}) VALUES (", qualified_table, col_list);
            for (pos, typed_col) in typed_cols.iter().enumerate() {
                if pos > 0 {
                    sql.push_str(", ");
                }
                if type_null_flags[pos] {
                    sql.push_str("NULL");
                } else {
                    write_sql_value(&mut sql, typed_col, row_idx);
                }
            }
            sql.push(')');

            // Append ON CONFLICT clause for upsert mode
            if let Some(WriteMode::Upsert { primary_key }) = write_mode {
                let pk_cols = primary_key
                    .iter()
                    .map(|k| format!("\"{}\"", k))
                    .collect::<Vec<_>>()
                    .join(", ");
                let update_cols: Vec<String> = active_cols
                    .iter()
                    .map(|&i| arrow_schema.field(i).name())
                    .filter(|name| !primary_key.contains(name))
                    .map(|name| format!("\"{}\" = EXCLUDED.\"{}\"", name, name))
                    .collect();
                if update_cols.is_empty() {
                    sql.push_str(&format!(" ON CONFLICT ({}) DO NOTHING", pk_cols));
                } else {
                    sql.push_str(&format!(
                        " ON CONFLICT ({}) DO UPDATE SET {}",
                        pk_cols,
                        update_cols.join(", ")
                    ));
                }
            }

            match client.execute(&sql, &[]).await {
                Ok(_) => rows_written += 1,
                Err(e) => {
                    rows_failed += 1;
                    host_ffi::log(
                        1,
                        &format!(
                            "dest-postgres: skipping row {} — INSERT failed: {}",
                            row_idx, e
                        ),
                    );
                }
            }
        }
    }

    if rows_failed > 0 {
        host_ffi::log(
            1,
            &format!(
                "dest-postgres: per-row fallback complete — {} written, {} failed/skipped",
                rows_written, rows_failed
            ),
        );
    }

    Ok(WriteResult {
        rows_written,
        rows_failed,
    })
}
