use std::io::Cursor;

use arrow::array::{
    Array, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
use arrow::ipc::reader::StreamReader;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;

/// Write an Arrow IPC batch to PostgreSQL.
///
/// 1. Deserialize the Arrow IPC bytes into RecordBatches
/// 2. Auto-create the target table if it doesn't exist (from Arrow schema)
/// 3. INSERT rows using parameterized statements
///
/// Returns the number of rows written.
pub async fn write_batch(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    ipc_bytes: &[u8],
) -> Result<u64, String> {
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

    // 2. Ensure schema exists
    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", target_schema);
    client
        .execute(&create_schema, &[])
        .await
        .map_err(|e| format!("Failed to create schema '{}': {}", target_schema, e))?;

    // 3. Auto-create table from Arrow schema
    let qualified_table = format!("\"{}\".\"{}\"", target_schema, stream_name);
    ensure_table(client, &qualified_table, &arrow_schema).await?;

    // 4. Build INSERT statement
    let columns: Vec<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    let col_list = columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");

    let placeholders: Vec<String> = (1..=columns.len())
        .map(|i| format!("${}", i))
        .collect();
    let placeholder_list = placeholders.join(", ");

    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        qualified_table, col_list, placeholder_list
    );

    host_ffi::log(
        3,
        &format!(
            "dest-postgres: inserting into {} ({} columns)",
            qualified_table,
            columns.len()
        ),
    );

    // 5. Insert rows from each batch
    let mut total_rows: u64 = 0;

    for batch in &batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> = Vec::with_capacity(num_cols);

            for col_idx in 0..num_cols {
                let col = batch.column(col_idx);

                if col.is_null(row_idx) {
                    params.push(Box::new(None::<String>));
                    continue;
                }

                match col.data_type() {
                    DataType::Int16 => {
                        let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                        params.push(Box::new(arr.value(row_idx) as i32));
                    }
                    DataType::Int32 => {
                        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        params.push(Box::new(arr.value(row_idx)));
                    }
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        params.push(Box::new(arr.value(row_idx)));
                    }
                    DataType::Float32 => {
                        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                        params.push(Box::new(arr.value(row_idx)));
                    }
                    DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        params.push(Box::new(arr.value(row_idx)));
                    }
                    DataType::Boolean => {
                        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                        params.push(Box::new(arr.value(row_idx)));
                    }
                    _ => {
                        // Utf8 and all other types → String
                        let arr = col.as_string::<i32>();
                        params.push(Box::new(arr.value(row_idx).to_string()));
                    }
                }
            }

            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                params.iter().map(|p| p.as_ref()).collect();

            client
                .execute(&insert_sql, &param_refs)
                .await
                .map_err(|e| {
                    format!(
                        "INSERT failed for {}, row {}: {}",
                        stream_name, row_idx, e
                    )
                })?;
        }

        total_rows += num_rows as u64;
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

/// Create the target table if it doesn't exist, based on the Arrow schema.
async fn ensure_table(
    client: &Client,
    qualified_table: &str,
    arrow_schema: &arrow::datatypes::Schema,
) -> Result<(), String> {
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

/// Write an Arrow IPC batch to PostgreSQL using multi-value INSERT.
///
/// Instead of one INSERT per row, builds:
///   INSERT INTO t (c1, c2) VALUES (v1, v2), (v3, v4), ...
///
/// Chunks at INSERT_CHUNK_SIZE rows to keep SQL size bounded.
pub async fn write_batch_multi_value(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    ipc_bytes: &[u8],
    created_tables: &mut std::collections::HashSet<String>,
) -> Result<u64, String> {
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

/// Format an Arrow array value at a given row index as a SQL literal.
///
/// Returns "NULL" for null values, properly escapes strings,
/// and formats numbers/booleans as literals.
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
        _ => {
            // Utf8 and all other types → escaped string literal
            let arr = col.as_string::<i32>();
            let val = arr.value(row_idx);
            // Escape single quotes by doubling them (PG standard_conforming_strings)
            format!("'{}'", val.replace('\'', "''"))
        }
    }
}
