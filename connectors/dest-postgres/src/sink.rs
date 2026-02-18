use std::io::Cursor;

use arrow::array::{
    Array, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
use arrow::ipc::reader::StreamReader;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::validation::validate_pg_identifier;

use std::io::Write;
use bytes::Bytes;
use futures_util::SinkExt;

/// Create the target table if it doesn't exist, based on the Arrow schema.
async fn ensure_table(
    client: &Client,
    qualified_table: &str,
    arrow_schema: &arrow::datatypes::Schema,
) -> Result<(), String> {
    // Validate all column names before interpolating into DDL
    for field in arrow_schema.fields() {
        validate_pg_identifier(field.name())
            .map_err(|e| format!("Invalid column name: {}", e))?;
    }

    let columns_ddl: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let pg_type = arrow_to_pg_type(field.data_type());
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            format!("\"{}\" {}{}", field.name(), pg_type, nullable)
        })
        .collect();

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        qualified_table,
        columns_ddl.join(", ")
    );

    host_ffi::log(3, &format!("dest-postgres: ensuring table: {}", ddl));

    client
        .execute(&ddl, &[])
        .await
        .map_err(|e| format!("Failed to create table {}: {}", qualified_table, e))?;

    Ok(())
}

/// Map Arrow data types back to PostgreSQL column types.
fn arrow_to_pg_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Boolean => "BOOLEAN",
        DataType::Utf8 => "TEXT",
        _ => "TEXT",
    }
}

/// Maximum rows per multi-value INSERT statement.
/// Keeps SQL string size manageable (~350KB for 7 columns).
const INSERT_CHUNK_SIZE: usize = 1000;

/// Buffer size for COPY data before flushing to the sink.
const COPY_FLUSH_BYTES: usize = 4 * 1024 * 1024; // 4MB

/// Write an Arrow IPC batch to PostgreSQL using multi-value INSERT.
///
/// Builds batched statements:
///   INSERT INTO t (c1, c2) VALUES (v1, v2), (v3, v4), ...
///
/// Chunks at INSERT_CHUNK_SIZE rows to keep SQL size bounded.
pub async fn write_batch(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    ipc_bytes: &[u8],
    created_tables: &mut std::collections::HashSet<String>,
) -> Result<u64, String> {
    // Caller must validate stream_name and target_schema before calling.

    // 1. Decode Arrow IPC
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("Failed to read Arrow IPC: {}", e))?;

    let arrow_schema = reader.schema();
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read batches: {}", e))?;

    if batches.is_empty() {
        return Ok(0);
    }

    let qualified_table = format!("\"{}\".\"{}\"", target_schema, stream_name);

    // 2. Ensure schema + table exist (only on first encounter)
    if !created_tables.contains(&qualified_table) {
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", target_schema);
        client
            .execute(&create_schema, &[])
            .await
            .map_err(|e| format!("Failed to create schema '{}': {}", target_schema, e))?;

        ensure_table(client, &qualified_table, &arrow_schema).await?;
        created_tables.insert(qualified_table.clone());
    }

    // 3. Build column list
    let col_list = arrow_schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");

    // 4. Insert rows using multi-value INSERT
    let mut total_rows: u64 = 0;

    for batch in &batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        // Process in chunks
        for chunk_start in (0..num_rows).step_by(INSERT_CHUNK_SIZE) {
            let chunk_end = (chunk_start + INSERT_CHUNK_SIZE).min(num_rows);
            let chunk_size = chunk_end - chunk_start;

            let mut sql = format!("INSERT INTO {} ({}) VALUES ", qualified_table, col_list);

            for row_idx in chunk_start..chunk_end {
                if row_idx > chunk_start {
                    sql.push_str(", ");
                }
                sql.push('(');
                for col_idx in 0..num_cols {
                    if col_idx > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&format_sql_value(batch.column(col_idx).as_ref(), row_idx));
                }
                sql.push(')');
            }

            client
                .execute(&sql, &[])
                .await
                .map_err(|e| {
                    format!(
                        "Multi-value INSERT failed for {}, rows {}-{}: {}",
                        stream_name, chunk_start, chunk_end, e
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

/// Write an Arrow IPC batch to PostgreSQL using COPY FROM STDIN (text format).
///
/// Streams rows as tab-separated text directly to PostgreSQL's COPY protocol,
/// bypassing SQL parsing for significantly higher throughput.
pub async fn write_batch_copy(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    ipc_bytes: &[u8],
    created_tables: &mut std::collections::HashSet<String>,
) -> Result<u64, String> {
    // Caller must validate stream_name and target_schema before calling.

    // 1. Decode Arrow IPC
    let cursor = Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("Failed to read Arrow IPC: {}", e))?;
    let arrow_schema = reader.schema();
    let batches: Vec<_> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read batches: {}", e))?;

    if batches.is_empty() {
        return Ok(0);
    }

    let qualified_table = format!("\"{}\".\"{}\"", target_schema, stream_name);

    // 2. Ensure schema + table exist (only on first encounter)
    if !created_tables.contains(&qualified_table) {
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", target_schema);
        client
            .execute(&create_schema, &[])
            .await
            .map_err(|e| format!("Failed to create schema '{}': {}", target_schema, e))?;
        ensure_table(client, &qualified_table, &arrow_schema).await?;
        created_tables.insert(qualified_table.clone());
    }

    // 3. Build COPY statement
    let col_list = arrow_schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let copy_stmt = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT text)",
        qualified_table, col_list
    );

    // 4. Start COPY and stream data
    let sink = client
        .copy_in(&copy_stmt)
        .await
        .map_err(|e| format!("COPY start failed: {}", e))?;
    let mut sink = Box::pin(sink);

    let mut total_rows: u64 = 0;
    let mut buf = Vec::with_capacity(COPY_FLUSH_BYTES);

    for batch in &batches {
        let num_cols = batch.num_columns();
        for row_idx in 0..batch.num_rows() {
            for col_idx in 0..num_cols {
                if col_idx > 0 {
                    buf.push(b'\t');
                }
                format_copy_value(&mut buf, batch.column(col_idx).as_ref(), row_idx);
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

/// Format an Arrow array value at a given row index as a SQL literal.
///
/// Returns "NULL" for null values, properly escapes strings,
/// and formats numbers/booleans as literals.
///
/// Supported Arrow types: Int16, Int32, Int64, Float32, Float64, Boolean, Utf8.
/// Other types fall back to "NULL" rather than panicking.
fn format_sql_value(col: &dyn Array, row_idx: usize) -> String {
    if col.is_null(row_idx) {
        return "NULL".to_string();
    }

    match col.data_type() {
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = arr.value(row_idx);
            if v.is_nan() {
                "'NaN'::real".to_string()
            } else if v.is_infinite() {
                if v > 0.0 { "'Infinity'::real".to_string() } else { "'-Infinity'::real".to_string() }
            } else {
                v.to_string()
            }
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = arr.value(row_idx);
            if v.is_nan() {
                "'NaN'::double precision".to_string()
            } else if v.is_infinite() {
                if v > 0.0 { "'Infinity'::double precision".to_string() } else { "'-Infinity'::double precision".to_string() }
            } else {
                v.to_string()
            }
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            if arr.value(row_idx) { "TRUE".to_string() } else { "FALSE".to_string() }
        }
        DataType::Utf8 => {
            let arr = col.as_string::<i32>();
            let val = arr.value(row_idx);
            // Strip null bytes (libpq treats them as string terminators) and
            // escape single quotes by doubling them (PG standard_conforming_strings=on)
            let cleaned = val.replace('\0', "");
            format!("'{}'", cleaned.replace('\'', "''"))
        }
        _ => {
            // Unsupported type — insert NULL rather than panic
            "NULL".to_string()
        }
    }
}

/// Format an Arrow array value at a given row index for COPY text format.
///
/// COPY text format rules:
/// - NULL: `\N`
/// - Strings: backslash-escape `\`, tab, newline, carriage return; strip null bytes
/// - Booleans: `t` / `f`
/// - Numbers: decimal representation (NaN, Infinity as literals)
fn format_copy_value(buf: &mut Vec<u8>, col: &dyn Array, row_idx: usize) {
    if col.is_null(row_idx) {
        buf.extend_from_slice(b"\\N");
        return;
    }

    match col.data_type() {
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            let _ = write!(buf, "{}", arr.value(row_idx));
        }
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            let v = arr.value(row_idx);
            if v.is_nan() {
                buf.extend_from_slice(b"NaN");
            } else if v.is_infinite() {
                if v > 0.0 {
                    buf.extend_from_slice(b"Infinity");
                } else {
                    buf.extend_from_slice(b"-Infinity");
                }
            } else {
                let _ = write!(buf, "{}", v);
            }
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let v = arr.value(row_idx);
            if v.is_nan() {
                buf.extend_from_slice(b"NaN");
            } else if v.is_infinite() {
                if v > 0.0 {
                    buf.extend_from_slice(b"Infinity");
                } else {
                    buf.extend_from_slice(b"-Infinity");
                }
            } else {
                let _ = write!(buf, "{}", v);
            }
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            buf.push(if arr.value(row_idx) { b't' } else { b'f' });
        }
        DataType::Utf8 => {
            let arr = col.as_string::<i32>();
            let val = arr.value(row_idx);
            // COPY text format: escape backslash, tab, newline, CR; strip null bytes
            for byte in val.bytes() {
                match byte {
                    b'\\' => buf.extend_from_slice(b"\\\\"),
                    b'\t' => buf.extend_from_slice(b"\\t"),
                    b'\n' => buf.extend_from_slice(b"\\n"),
                    b'\r' => buf.extend_from_slice(b"\\r"),
                    0 => {} // skip null bytes
                    _ => buf.push(byte),
                }
            }
        }
        _ => {
            buf.extend_from_slice(b"\\N");
        }
    }
}
