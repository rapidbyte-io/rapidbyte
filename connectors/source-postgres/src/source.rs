use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{ColumnSchema, ReadRequest, ReadSummary, ReadSummaryV1, StreamContext, StreamSelection};
use rapidbyte_sdk::validation::validate_pg_identifier;

/// Maximum number of rows per Arrow RecordBatch.
const BATCH_SIZE: usize = 10_000;

/// Number of rows to fetch per server-side cursor iteration.
const FETCH_CHUNK: usize = 1_000;

/// Server-side cursor name used for streaming reads.
const CURSOR_NAME: &str = "rb_cursor";

/// Estimate Arrow IPC byte size for a set of rows.
fn estimate_batch_bytes(rows: &[tokio_postgres::Row], columns: &[ColumnSchema]) -> usize {
    let mut total = 0usize;
    for row in rows {
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
            total += 1; // null bitmap overhead per column per row
        }
    }
    total
}

/// Read data from PostgreSQL for each stream in the request,
/// emitting Arrow IPC batches via the host function.
pub async fn read_streams(
    client: &Client,
    request: &ReadRequest,
) -> Result<ReadSummary, String> {
    let mut total_records: u64 = 0;
    let mut total_bytes: u64 = 0;

    for stream_sel in &request.streams {
        let (records, bytes) = read_single_stream(client, stream_sel).await?;
        total_records += records;
        total_bytes += bytes;
    }

    Ok(ReadSummary {
        records_read: total_records,
        bytes_read: total_bytes,
    })
}

async fn read_single_stream(
    client: &Client,
    stream: &StreamSelection,
) -> Result<(u64, u64), String> {
    host_ffi::log(2, &format!("Reading stream: {}", stream.name));

    // Discover schema for this table
    let schema_query = format!(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = '{}' ORDER BY ordinal_position",
        stream.name
    );

    let schema_rows = client
        .query(&schema_query, &[])
        .await
        .map_err(|e| format!("Schema query failed for {}: {}", stream.name, e))?;

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
        return Err(format!("Table '{}' not found or has no columns", stream.name));
    }

    // Build Arrow schema
    let arrow_schema = build_arrow_schema(&columns);

    // Query all rows
    let query = format!("SELECT * FROM \"{}\"", stream.name);
    let rows = client
        .query(&query, &[])
        .await
        .map_err(|e| format!("Data query failed for {}: {}", stream.name, e))?;

    let mut total_records: u64 = 0;
    let mut total_bytes: u64 = 0;

    // Process rows in batches
    for chunk in rows.chunks(BATCH_SIZE) {
        let batch = rows_to_record_batch(chunk, &columns, &arrow_schema)?;
        let ipc_bytes = batch_to_ipc(&batch)?;

        total_records += chunk.len() as u64;
        total_bytes += ipc_bytes.len() as u64;

        // Emit via host function
        host_ffi::emit_record_batch(&stream.name, &ipc_bytes);
        host_ffi::report_progress(chunk.len() as u64, ipc_bytes.len() as u64);
    }

    host_ffi::log(
        2,
        &format!(
            "Stream '{}' complete: {} records, {} bytes",
            stream.name, total_records, total_bytes
        ),
    );

    Ok((total_records, total_bytes))
}

/// Read a single stream using v1 protocol with server-side cursors.
pub async fn read_stream_v1(
    client: &Client,
    ctx: &StreamContext,
) -> Result<ReadSummaryV1, String> {
    host_ffi::log(2, &format!("Reading stream (v1): {}", ctx.stream_name));

    validate_pg_identifier(&ctx.stream_name)
        .map_err(|e| format!("Invalid stream name: {}", e))?;

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

    let declare = format!(
        "DECLARE {} NO SCROLL CURSOR FOR SELECT * FROM \"{}\"",
        CURSOR_NAME, ctx.stream_name
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
            estimated_bytes += estimate_batch_bytes(&rows, &columns);
            accumulated_rows.extend(rows);
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

    host_ffi::log(
        2,
        &format!(
            "Stream '{}' complete (v1): {} records, {} bytes, {} batches",
            ctx.stream_name, total_records, total_bytes, batches_emitted
        ),
    );

    Ok(ReadSummaryV1 {
        records_read: total_records,
        bytes_read: total_bytes,
        batches_emitted,
        checkpoint_count: 0,
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
        .map(|(col_idx, col)| -> Result<Arc<dyn arrow::array::Array>, String> {
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
        })
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
