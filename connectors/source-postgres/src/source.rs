use std::sync::Arc;

use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{ColumnSchema, ReadRequest, ReadSummary, StreamSelection};

/// Maximum number of rows per Arrow RecordBatch.
const BATCH_SIZE: usize = 10_000;

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
