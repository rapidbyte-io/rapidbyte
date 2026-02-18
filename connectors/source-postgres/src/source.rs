use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{ColumnSchema, ReadSummary, StreamContext};
use rapidbyte_sdk::validation::validate_pg_identifier;

/// Maximum number of rows per Arrow RecordBatch.
const BATCH_SIZE: usize = 10_000;

/// Number of rows to fetch per server-side cursor iteration.
const FETCH_CHUNK: usize = 1_000;

/// Server-side cursor name used for streaming reads.
const CURSOR_NAME: &str = "rb_cursor";

/// Estimate byte size of a single row for max_record_bytes checking.
fn estimate_row_bytes(row: &tokio_postgres::Row, columns: &[ColumnSchema]) -> usize {
    let mut total = 0usize;
    for (col_idx, col) in columns.iter().enumerate() {
        total += match col.data_type.as_str() {
            "Int16" => 2,
            "Int32" | "Float32" => 4,
            "Int64" | "Float64" => 8,
            "Boolean" => 1,
            _ => row
                .try_get::<_, String>(col_idx)
                .map(|s| s.len() + 4)
                .unwrap_or(4),
        };
        total += 1; // null bitmap overhead per column
    }
    total
}

/// Read a single stream using server-side cursors.
pub async fn read_stream(client: &Client, ctx: &StreamContext) -> Result<ReadSummary, String> {
    host_ffi::log(2, &format!("Reading stream: {}", ctx.stream_name));

    validate_pg_identifier(&ctx.stream_name).map_err(|e| format!("Invalid stream name: {}", e))?;

    // Discover schema using parameterized query
    let schema_query = "SELECT column_name, data_type, is_nullable \
        FROM information_schema.columns \
        WHERE table_schema = 'public' AND table_name = $1 \
        ORDER BY ordinal_position";

    let schema_rows = client
        .query(schema_query, &[&ctx.stream_name])
        .await
        .map_err(|e| format!("Schema query failed for {}: {}", ctx.stream_name, e))?;

    let columns: Vec<ColumnSchema> = schema_rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let data_type: String = row.get(1);
            let nullable: bool = row.get::<_, String>(2) == "YES";
            ColumnSchema {
                name,
                data_type: pg_type_to_arrow(&data_type).to_string(),
                nullable,
            }
        })
        .collect();

    if columns.is_empty() {
        return Err(format!(
            "Table '{}' not found or has no columns",
            ctx.stream_name
        ));
    }

    let arrow_schema = build_arrow_schema(&columns);

    // Use server-side cursor for bounded memory
    client
        .execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .map_err(|e| format!("BEGIN failed: {}", e))?;

    // Build the query based on sync mode and cursor info
    let base_query = if let (rapidbyte_sdk::protocol::SyncMode::Incremental, Some(ref ci)) =
        (&ctx.sync_mode, &ctx.cursor_info)
    {
        validate_pg_identifier(&ci.cursor_field)
            .map_err(|e| format!("Invalid cursor field: {}", e))?;

        if let Some(ref last_value) = ci.last_value {
            let literal = cursor_value_to_sql_literal(last_value);
            host_ffi::log(
                2,
                &format!(
                    "Incremental read: {} WHERE \"{}\" > {}",
                    ctx.stream_name, ci.cursor_field, literal
                ),
            );
            format!(
                "SELECT * FROM \"{}\" WHERE \"{}\" > {} ORDER BY \"{}\"",
                ctx.stream_name, ci.cursor_field, literal, ci.cursor_field
            )
        } else {
            host_ffi::log(
                2,
                &format!(
                    "Incremental read (no prior cursor): {} ORDER BY \"{}\"",
                    ctx.stream_name, ci.cursor_field
                ),
            );
            format!(
                "SELECT * FROM \"{}\" ORDER BY \"{}\"",
                ctx.stream_name, ci.cursor_field
            )
        }
    } else {
        format!("SELECT * FROM \"{}\"", ctx.stream_name)
    };

    let declare = format!(
        "DECLARE {} NO SCROLL CURSOR FOR {}",
        CURSOR_NAME, base_query
    );
    client
        .execute(&declare, &[])
        .await
        .map_err(|e| format!("DECLARE CURSOR failed: {}", e))?;

    // Byte-based batching
    let max_batch_bytes = if ctx.limits.max_batch_bytes > 0 {
        ctx.limits.max_batch_bytes as usize
    } else {
        64 * 1024 * 1024 // 64MB default
    };

    let max_record_bytes = if ctx.limits.max_record_bytes > 0 {
        ctx.limits.max_record_bytes as usize
    } else {
        16 * 1024 * 1024 // 16MB default
    };
    let mut records_skipped: u64 = 0;

    // For incremental mode, find the cursor column index to track max value
    let cursor_col_idx: Option<usize> = ctx.cursor_info.as_ref().map(|ci| {
        columns
            .iter()
            .position(|c| c.name == ci.cursor_field)
            .unwrap_or_else(|| panic!("Cursor field '{}' not found in schema", ci.cursor_field))
    });
    let mut max_cursor_value: Option<String> = None;

    let fetch_query = format!("FETCH {} FROM {}", FETCH_CHUNK, CURSOR_NAME);
    let mut total_records: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut batches_emitted: u64 = 0;
    let mut accumulated_rows: Vec<tokio_postgres::Row> = Vec::new();
    let mut estimated_bytes: usize = 256; // IPC framing overhead

    let mut loop_error: Option<String> = None;

    loop {
        let rows = match client.query(&fetch_query, &[]).await {
            Ok(r) => r,
            Err(e) => {
                loop_error = Some(format!("FETCH failed for {}: {}", ctx.stream_name, e));
                break;
            }
        };

        let exhausted = rows.is_empty();

        if !exhausted {
            // Per-row max_record_bytes enforcement (spec § Data Exchange Format)
            let mut valid_rows: Vec<tokio_postgres::Row> = Vec::with_capacity(rows.len());
            for row in rows {
                let row_bytes = estimate_row_bytes(&row, &columns);
                if row_bytes > max_record_bytes {
                    match ctx.policies.on_data_error {
                        rapidbyte_sdk::protocol::DataErrorPolicy::Fail => {
                            loop_error = Some(format!(
                                "Record exceeds max_record_bytes ({} > {})",
                                row_bytes, max_record_bytes,
                            ));
                            break;
                        }
                        _ => {
                            // Skip (or DLQ — treated as skip for now)
                            records_skipped += 1;
                            host_ffi::log(
                                1, // warn
                                &format!(
                                    "Skipping oversized record: {} bytes > max_record_bytes {}",
                                    row_bytes, max_record_bytes,
                                ),
                            );
                            continue;
                        }
                    }
                }
                valid_rows.push(row);
            }

            if loop_error.is_some() {
                break;
            }

            // Accumulate valid rows with per-row max_batch_bytes enforcement.
            // Flush before adding any row that would push the batch over the limit
            // (spec: max_batch_bytes is a hard ceiling, not a soft trigger).
            for row in valid_rows {
                let row_bytes = estimate_row_bytes(&row, &columns);

                // Flush if adding this row would exceed max_batch_bytes
                if !accumulated_rows.is_empty() && estimated_bytes + row_bytes >= max_batch_bytes {
                    match rows_to_record_batch(&accumulated_rows, &columns, &arrow_schema)
                        .and_then(|batch| batch_to_ipc(&batch))
                    {
                        Ok(ipc_bytes) => {
                            total_records += accumulated_rows.len() as u64;
                            total_bytes += ipc_bytes.len() as u64;
                            batches_emitted += 1;

                            if let Err(e) = host_ffi::emit_batch(&ipc_bytes) {
                                loop_error = Some(format!("emit_batch failed: {}", e));
                                break;
                            }

                            accumulated_rows.clear();
                            estimated_bytes = 256; // IPC framing overhead
                        }
                        Err(e) => {
                            loop_error = Some(e);
                            break;
                        }
                    }
                }

                if loop_error.is_some() {
                    break;
                }

                estimated_bytes += row_bytes;

                // Track max cursor value for incremental checkpoint
                if let Some(col_idx) = cursor_col_idx {
                    let val: Option<String> = row
                        .try_get::<_, String>(col_idx)
                        .ok()
                        .or_else(|| row.try_get::<_, i64>(col_idx).ok().map(|n| n.to_string()))
                        .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(|n| n.to_string()));

                    if let Some(val) = val {
                        match &max_cursor_value {
                            None => max_cursor_value = Some(val),
                            Some(current) => {
                                let is_greater = match (val.parse::<i64>(), current.parse::<i64>())
                                {
                                    (Ok(a), Ok(b)) => a > b,
                                    _ => val > *current,
                                };
                                if is_greater {
                                    max_cursor_value = Some(val);
                                }
                            }
                        }
                    }
                }

                accumulated_rows.push(row);
            }
        }

        let should_emit = !accumulated_rows.is_empty()
            && (estimated_bytes >= max_batch_bytes
                || accumulated_rows.len() >= BATCH_SIZE
                || exhausted);

        if should_emit {
            match rows_to_record_batch(&accumulated_rows, &columns, &arrow_schema)
                .and_then(|batch| batch_to_ipc(&batch))
            {
                Ok(ipc_bytes) => {
                    total_records += accumulated_rows.len() as u64;
                    total_bytes += ipc_bytes.len() as u64;
                    batches_emitted += 1;

                    if let Err(e) = host_ffi::emit_batch(&ipc_bytes) {
                        loop_error = Some(format!("emit_batch failed: {}", e));
                        break;
                    }

                    accumulated_rows.clear();
                    estimated_bytes = 256;
                }
                Err(e) => {
                    loop_error = Some(e);
                    break;
                }
            }
        }

        if exhausted {
            break;
        }
    }

    // Always clean up cursor and transaction
    let close_query = format!("CLOSE {}", CURSOR_NAME);
    let _ = client.execute(&*close_query, &[]).await;
    if loop_error.is_some() {
        let _ = client.execute("ROLLBACK", &[]).await;
    } else {
        client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("COMMIT failed: {}", e))?;
    }

    if let Some(e) = loop_error {
        return Err(e);
    }

    // Emit source checkpoint with cursor position for incremental state tracking
    let checkpoint_count =
        if let (Some(ref ci), Some(ref max_val)) = (&ctx.cursor_info, &max_cursor_value) {
            let cp = rapidbyte_sdk::protocol::Checkpoint {
                id: 1,
                kind: rapidbyte_sdk::protocol::CheckpointKind::Source,
                stream: ctx.stream_name.clone(),
                cursor_field: Some(ci.cursor_field.clone()),
                cursor_value: Some(rapidbyte_sdk::protocol::CursorValue::Utf8(max_val.clone())),
                records_processed: total_records,
                bytes_processed: total_bytes,
            };
            let _ = host_ffi::checkpoint("source-postgres", &ctx.stream_name, &cp);
            host_ffi::log(
                2,
                &format!(
                    "Source checkpoint: stream={} cursor_field={} cursor_value={}",
                    ctx.stream_name, ci.cursor_field, max_val
                ),
            );
            1u64
        } else {
            0u64
        };

    host_ffi::log(
        2,
        &format!(
            "Stream '{}' complete: {} records, {} bytes, {} batches",
            ctx.stream_name, total_records, total_bytes, batches_emitted
        ),
    );

    Ok(ReadSummary {
        records_read: total_records,
        bytes_read: total_bytes,
        batches_emitted,
        checkpoint_count,
        records_skipped,
        perf: None,
    })
}

fn build_arrow_schema(columns: &[ColumnSchema]) -> Arc<Schema> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let dt = match col.data_type.as_str() {
                "Int16" => DataType::Int16,
                "Int32" => DataType::Int32,
                "Int64" => DataType::Int64,
                "Float32" => DataType::Float32,
                "Float64" => DataType::Float64,
                "Boolean" => DataType::Boolean,
                _ => DataType::Utf8,
            };
            Field::new(&col.name, dt, col.nullable)
        })
        .collect();
    Arc::new(Schema::new(fields))
}

fn rows_to_record_batch(
    rows: &[tokio_postgres::Row],
    columns: &[ColumnSchema],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, String> {
    let arrays: Vec<Arc<dyn arrow::array::Array>> = columns
        .iter()
        .enumerate()
        .map(
            |(col_idx, col)| -> Result<Arc<dyn arrow::array::Array>, String> {
                match col.data_type.as_str() {
                    "Int16" => {
                        let values: Vec<Option<i16>> = rows
                            .iter()
                            .map(|row| row.try_get::<_, i16>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(Int16Array::from(values)))
                    }
                    "Int32" => {
                        let values: Vec<Option<i32>> = rows
                            .iter()
                            .map(|row| row.try_get::<_, i32>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(Int32Array::from(values)))
                    }
                    "Int64" => {
                        let values: Vec<Option<i64>> = rows
                            .iter()
                            .map(|row| row.try_get::<_, i64>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(Int64Array::from(values)))
                    }
                    "Float32" => {
                        let values: Vec<Option<f32>> = rows
                            .iter()
                            .map(|row| row.try_get::<_, f32>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(Float32Array::from(values)))
                    }
                    "Float64" => {
                        let values: Vec<Option<f64>> = rows
                            .iter()
                            .map(|row| row.try_get::<_, f64>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(Float64Array::from(values)))
                    }
                    "Boolean" => {
                        let values: Vec<Option<bool>> = rows
                            .iter()
                            .map(|row| row.try_get::<_, bool>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(BooleanArray::from(values)))
                    }
                    _ => {
                        // Utf8 fallback — try to get as String
                        let values: Vec<Option<String>> = rows
                            .iter()
                            .map(|row| row.try_get::<_, String>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(StringArray::from(
                            values
                                .iter()
                                .map(|v| v.as_deref())
                                .collect::<Vec<Option<&str>>>(),
                        )))
                    }
                }
            },
        )
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))
}

fn batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
        .map_err(|e| format!("IPC writer error: {}", e))?;
    writer
        .write(batch)
        .map_err(|e| format!("IPC write error: {}", e))?;
    writer
        .finish()
        .map_err(|e| format!("IPC finish error: {}", e))?;
    Ok(buf)
}

fn pg_type_to_arrow(pg_type: &str) -> &'static str {
    match pg_type {
        "integer" | "int4" | "serial" => "Int32",
        "bigint" | "int8" | "bigserial" => "Int64",
        "smallint" | "int2" => "Int16",
        "real" | "float4" => "Float32",
        "double precision" | "float8" => "Float64",
        "boolean" | "bool" => "Boolean",
        _ => "Utf8",
    }
}

/// Convert a CursorValue to a SQL literal for use in WHERE clauses.
fn cursor_value_to_sql_literal(value: &rapidbyte_sdk::protocol::CursorValue) -> String {
    use rapidbyte_sdk::protocol::CursorValue;
    match value {
        CursorValue::Null => "NULL".to_string(),
        CursorValue::Int64(n) => n.to_string(),
        CursorValue::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
        CursorValue::TimestampMillis(ms) => {
            let secs = ms / 1000;
            let nsecs = ((ms % 1000) * 1_000_000) as u32;
            let ts = chrono::DateTime::from_timestamp(secs, nsecs)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
                .unwrap_or_else(|| ms.to_string());
            format!("'{}'", ts)
        }
        CursorValue::TimestampMicros(us) => {
            let secs = us / 1_000_000;
            let nsecs = ((us % 1_000_000) * 1_000) as u32;
            let ts = chrono::DateTime::from_timestamp(secs, nsecs)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                .unwrap_or_else(|| us.to_string());
            format!("'{}'", ts)
        }
        CursorValue::Decimal { value, .. } => value.clone(),
        CursorValue::Json(v) => format!("'{}'", v.to_string().replace('\'', "''")),
    }
}
