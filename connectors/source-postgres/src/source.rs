use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context};
use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, SecondsFormat, Utc};
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    ColumnSchema, CursorType, CursorValue, Metric, MetricValue, ReadSummary, StreamContext,
    SyncMode,
};
use rapidbyte_sdk::validation::validate_pg_identifier;

/// Maximum number of rows per Arrow RecordBatch.
const BATCH_SIZE: usize = 10_000;

/// Number of rows to fetch per server-side cursor iteration.
const FETCH_CHUNK: usize = 10_000;

/// Server-side cursor name used for streaming reads.
const CURSOR_NAME: &str = "rb_cursor";

/// Estimate byte size of a single row for max_record_bytes checking.
fn estimate_row_bytes(columns: &[ColumnSchema]) -> usize {
    let mut total = 0usize;
    for col in columns {
        total += match col.data_type.as_str() {
            "Int16" => 2,
            "Int32" | "Float32" => 4,
            "Int64" | "Float64" => 8,
            "Boolean" => 1,
            _ => 64,
        };
        total += 1;
    }
    total
}

enum CursorBindParam {
    Int64(i64),
    Text(String),
    Json(serde_json::Value),
}

impl CursorBindParam {
    fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Int64(v) => v,
            Self::Text(v) => v,
            Self::Json(v) => v,
        }
    }
}

struct CursorQuery {
    sql: String,
    bind: Option<CursorBindParam>,
}

/// Read a single stream using server-side cursors.
pub async fn read_stream(
    client: &Client,
    ctx: &StreamContext,
    connect_secs: f64,
) -> Result<ReadSummary, String> {
    read_stream_inner(client, ctx, connect_secs)
        .await
        .map_err(|e| e.to_string())
}

async fn read_stream_inner(
    client: &Client,
    ctx: &StreamContext,
    connect_secs: f64,
) -> anyhow::Result<ReadSummary> {
    host_ffi::log(2, &format!("Reading stream: {}", ctx.stream_name));

    validate_pg_identifier(&ctx.stream_name)
        .map_err(|e| anyhow!("Invalid stream name '{}': {}", ctx.stream_name, e))?;

    let query_start = Instant::now();

    let schema_query = "SELECT column_name, data_type, is_nullable \
        FROM information_schema.columns \
        WHERE table_schema = 'public' AND table_name = $1 \
        ORDER BY ordinal_position";

    let schema_rows = client
        .query(schema_query, &[&ctx.stream_name])
        .await
        .with_context(|| format!("Schema query failed for {}", ctx.stream_name))?;

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
        bail!("Table '{}' not found or has no columns", ctx.stream_name);
    }

    let arrow_schema = build_arrow_schema(&columns);

    client
        .execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .context("BEGIN failed")?;

    let cursor_query = build_base_query(ctx, &columns)?;

    let declare = format!(
        "DECLARE {} NO SCROLL CURSOR FOR {}",
        CURSOR_NAME, cursor_query.sql
    );
    match cursor_query.bind.as_ref() {
        Some(bind) => {
            let params: [&(dyn ToSql + Sync); 1] = [bind.as_tosql()];
            client
                .execute(&declare, &params)
                .await
                .context("DECLARE CURSOR failed")?;
        }
        None => {
            client
                .execute(&declare, &[])
                .await
                .context("DECLARE CURSOR failed")?;
        }
    }

    let query_secs = query_start.elapsed().as_secs_f64();

    let max_batch_bytes = if ctx.limits.max_batch_bytes > 0 {
        ctx.limits.max_batch_bytes as usize
    } else {
        64 * 1024 * 1024
    };

    let max_record_bytes = if ctx.limits.max_record_bytes > 0 {
        ctx.limits.max_record_bytes as usize
    } else {
        16 * 1024 * 1024
    };
    let mut records_skipped: u64 = 0;

    let cursor_col_idx = if let Some(ci) = ctx.cursor_info.as_ref() {
        Some(
            columns
                .iter()
                .position(|c| c.name == ci.cursor_field)
                .ok_or_else(|| {
                    anyhow!(
                        "Cursor field '{}' not found in schema for table '{}'",
                        ci.cursor_field,
                        ctx.stream_name
                    )
                })?,
        )
    } else {
        None
    };
    let mut max_cursor_value: Option<String> = None;

    let fetch_query = format!("FETCH {} FROM {}", FETCH_CHUNK, CURSOR_NAME);
    let mut total_records: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut batches_emitted: u64 = 0;
    let mut accumulated_rows: Vec<tokio_postgres::Row> = Vec::new();
    let mut estimated_bytes: usize = 256;

    let mut loop_error: Option<anyhow::Error> = None;
    let mut arrow_encode_nanos: u64 = 0;

    let fetch_start = Instant::now();
    loop {
        let rows = match client.query(&fetch_query, &[]).await {
            Ok(r) => r,
            Err(e) => {
                loop_error = Some(anyhow!("FETCH failed for {}: {}", ctx.stream_name, e));
                break;
            }
        };

        let exhausted = rows.is_empty();

        if !exhausted {
            let mut valid_rows: Vec<tokio_postgres::Row> = Vec::with_capacity(rows.len());
            for row in rows {
                let row_bytes = estimate_row_bytes(&columns);
                if row_bytes > max_record_bytes {
                    match ctx.policies.on_data_error {
                        rapidbyte_sdk::protocol::DataErrorPolicy::Fail => {
                            loop_error = Some(anyhow!(
                                "Record exceeds max_record_bytes ({} > {})",
                                row_bytes,
                                max_record_bytes,
                            ));
                            break;
                        }
                        _ => {
                            records_skipped += 1;
                            host_ffi::log(
                                1,
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

            for row in valid_rows {
                let row_bytes = estimate_row_bytes(&columns);

                if !accumulated_rows.is_empty() && estimated_bytes + row_bytes >= max_batch_bytes {
                    let encode_start = Instant::now();
                    let encode_result = rows_to_record_batch(&accumulated_rows, &columns, &arrow_schema)
                        .and_then(|batch| batch_to_ipc(&batch));
                    arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;
                    match encode_result {
                        Ok(ipc_bytes) => {
                            total_records += accumulated_rows.len() as u64;
                            total_bytes += ipc_bytes.len() as u64;
                            batches_emitted += 1;

                            if let Err(e) = host_ffi::emit_batch(&ipc_bytes) {
                                loop_error = Some(anyhow!("emit_batch failed: {}", e));
                                break;
                            }

                            let _ = host_ffi::metric(
                                "source-postgres",
                                &ctx.stream_name,
                                &Metric {
                                    name: "records_read".to_string(),
                                    value: MetricValue::Counter(total_records),
                                    labels: vec![],
                                },
                            );
                            let _ = host_ffi::metric(
                                "source-postgres",
                                &ctx.stream_name,
                                &Metric {
                                    name: "bytes_read".to_string(),
                                    value: MetricValue::Counter(total_bytes),
                                    labels: vec![],
                                },
                            );

                            accumulated_rows.clear();
                            estimated_bytes = 256;
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

                if let Some(col_idx) = cursor_col_idx {
                    let val: Option<String> = match ctx.cursor_info.as_ref().map(|ci| &ci.cursor_type)
                    {
                        Some(CursorType::Int64) => row
                            .try_get::<_, i64>(col_idx)
                            .ok()
                            .map(|n| n.to_string())
                            .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(|n| n.to_string())),
                        _ => row
                            .try_get::<_, String>(col_idx)
                            .ok()
                            .or_else(|| row.try_get::<_, i64>(col_idx).ok().map(|n| n.to_string()))
                            .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(|n| n.to_string())),
                    };

                    if let Some(val) = val {
                        match &max_cursor_value {
                            None => max_cursor_value = Some(val),
                            Some(current) => {
                                let is_greater = match (val.parse::<i64>(), current.parse::<i64>()) {
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
            let encode_start = Instant::now();
            let encode_result = rows_to_record_batch(&accumulated_rows, &columns, &arrow_schema)
                .and_then(|batch| batch_to_ipc(&batch));
            arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;
            match encode_result {
                Ok(ipc_bytes) => {
                    total_records += accumulated_rows.len() as u64;
                    total_bytes += ipc_bytes.len() as u64;
                    batches_emitted += 1;

                    if let Err(e) = host_ffi::emit_batch(&ipc_bytes) {
                        loop_error = Some(anyhow!("emit_batch failed: {}", e));
                        break;
                    }

                    let _ = host_ffi::metric(
                        "source-postgres",
                        &ctx.stream_name,
                        &Metric {
                            name: "records_read".to_string(),
                            value: MetricValue::Counter(total_records),
                            labels: vec![],
                        },
                    );
                    let _ = host_ffi::metric(
                        "source-postgres",
                        &ctx.stream_name,
                        &Metric {
                            name: "bytes_read".to_string(),
                            value: MetricValue::Counter(total_bytes),
                            labels: vec![],
                        },
                    );

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

    let fetch_secs = fetch_start.elapsed().as_secs_f64();

    let close_query = format!("CLOSE {}", CURSOR_NAME);
    let _ = client.execute(&close_query, &[]).await;
    if loop_error.is_some() {
        let _ = client.execute("ROLLBACK", &[]).await;
    } else {
        client.execute("COMMIT", &[]).await.context("COMMIT failed")?;
    }

    if let Some(e) = loop_error {
        return Err(e);
    }

    let checkpoint_count =
        if let (Some(ci), Some(max_val)) = (ctx.cursor_info.as_ref(), max_cursor_value.as_ref()) {
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
        perf: Some(rapidbyte_sdk::protocol::ReadPerf {
            connect_secs,
            query_secs,
            fetch_secs,
            arrow_encode_secs: arrow_encode_nanos as f64 / 1e9,
        }),
    })
}

fn effective_cursor_type(cursor_type: CursorType, cursor_column_arrow_type: &str) -> CursorType {
    match (cursor_type, cursor_column_arrow_type) {
        // Host state currently stores incremental cursor values as UTF-8.
        // If the actual cursor column is numeric, bind as Int64 for a valid
        // typed predicate instead of comparing against text.
        (CursorType::Utf8, "Int16" | "Int32" | "Int64") => CursorType::Int64,
        _ => cursor_type,
    }
}

fn build_base_query(ctx: &StreamContext, columns: &[ColumnSchema]) -> anyhow::Result<CursorQuery> {
    if let (SyncMode::Incremental, Some(ci)) = (&ctx.sync_mode, &ctx.cursor_info) {
        validate_pg_identifier(&ci.cursor_field)
            .map_err(|e| anyhow!("Invalid cursor field '{}': {}", ci.cursor_field, e))?;

        let table_name = format!("\"{}\"", ctx.stream_name);
        let cursor_field = format!("\"{}\"", ci.cursor_field);
        let cursor_column_arrow_type = columns
            .iter()
            .find(|c| c.name == ci.cursor_field)
            .map(|c| c.data_type.as_str())
            .unwrap_or("Utf8");

        if let Some(last_value) = ci.last_value.as_ref() {
            if matches!(last_value, CursorValue::Null) {
                host_ffi::log(
                    2,
                    &format!(
                        "Incremental read (null prior cursor): {} ORDER BY \"{}\"",
                        ctx.stream_name, ci.cursor_field
                    ),
                );
                return Ok(CursorQuery {
                    sql: format!(
                        "SELECT * FROM {} ORDER BY {}",
                        table_name, cursor_field
                    ),
                    bind: None,
                });
            }

            let resolved_cursor_type = effective_cursor_type(ci.cursor_type, cursor_column_arrow_type);
            if resolved_cursor_type != ci.cursor_type {
                host_ffi::log(
                    3,
                    &format!(
                        "Incremental cursor type adjusted: stream={} field={} declared={:?} inferred={} effective={:?}",
                        ctx.stream_name,
                        ci.cursor_field,
                        ci.cursor_type,
                        cursor_column_arrow_type,
                        resolved_cursor_type
                    ),
                );
            }

            let (bind, cast) = cursor_bind_param(&resolved_cursor_type, last_value).with_context(|| {
                format!(
                    "Invalid incremental cursor value for stream '{}' field '{}'",
                    ctx.stream_name, ci.cursor_field
                )
            })?;

            host_ffi::log(
                2,
                &format!(
                    "Incremental read: {} WHERE \"{}\" > $1::{}",
                    ctx.stream_name, ci.cursor_field, cast
                ),
            );

            return Ok(CursorQuery {
                sql: format!(
                    "SELECT * FROM {} WHERE {} > $1::{} ORDER BY {}",
                    table_name, cursor_field, cast, cursor_field
                ),
                bind: Some(bind),
            });
        }

        host_ffi::log(
            2,
            &format!(
                "Incremental read (no prior cursor): {} ORDER BY \"{}\"",
                ctx.stream_name, ci.cursor_field
            ),
        );
        return Ok(CursorQuery {
            sql: format!("SELECT * FROM {} ORDER BY {}", table_name, cursor_field),
            bind: None,
        });
    }

    Ok(CursorQuery {
        sql: format!("SELECT * FROM \"{}\"", ctx.stream_name),
        bind: None,
    })
}

fn cursor_bind_param(
    cursor_type: &CursorType,
    value: &CursorValue,
) -> anyhow::Result<(CursorBindParam, &'static str)> {
    match cursor_type {
        CursorType::Int64 => {
            let n = match value {
                CursorValue::Int64(v) => *v,
                CursorValue::Utf8(v) => v
                    .parse::<i64>()
                    .with_context(|| format!("failed to parse '{}' as i64", v))?,
                CursorValue::Decimal { value, .. } => value
                    .parse::<i64>()
                    .with_context(|| format!("failed to parse decimal '{}' as i64", value))?,
                _ => bail!("cursor value is incompatible with int64 cursor type"),
            };
            Ok((CursorBindParam::Int64(n), "bigint"))
        }
        CursorType::Utf8 => {
            let text = match value {
                CursorValue::Utf8(v) => v.clone(),
                CursorValue::Int64(v) => v.to_string(),
                CursorValue::TimestampMillis(v) => timestamp_millis_to_rfc3339(*v)?,
                CursorValue::TimestampMicros(v) => timestamp_micros_to_rfc3339(*v)?,
                CursorValue::Decimal { value, .. } => value.clone(),
                CursorValue::Json(v) => v.to_string(),
                CursorValue::Null => bail!("null cursor cannot be used as a predicate"),
            };
            Ok((CursorBindParam::Text(text), "text"))
        }
        CursorType::TimestampMillis => {
            let ts = match value {
                CursorValue::TimestampMillis(v) => timestamp_millis_to_rfc3339(*v)?,
                CursorValue::Int64(v) => timestamp_millis_to_rfc3339(*v)?,
                CursorValue::Utf8(v) => v.clone(),
                CursorValue::TimestampMicros(v) => timestamp_micros_to_rfc3339(*v)?,
                _ => bail!("cursor value is incompatible with timestamp_millis cursor type"),
            };
            Ok((CursorBindParam::Text(ts), "timestamptz"))
        }
        CursorType::TimestampMicros => {
            let ts = match value {
                CursorValue::TimestampMicros(v) => timestamp_micros_to_rfc3339(*v)?,
                CursorValue::Int64(v) => timestamp_micros_to_rfc3339(*v)?,
                CursorValue::Utf8(v) => v.clone(),
                CursorValue::TimestampMillis(v) => timestamp_millis_to_rfc3339(*v)?,
                _ => bail!("cursor value is incompatible with timestamp_micros cursor type"),
            };
            Ok((CursorBindParam::Text(ts), "timestamptz"))
        }
        CursorType::Decimal => {
            let decimal = match value {
                CursorValue::Decimal { value, .. } => value.clone(),
                CursorValue::Utf8(v) => v.clone(),
                CursorValue::Int64(v) => v.to_string(),
                _ => bail!("cursor value is incompatible with decimal cursor type"),
            };
            Ok((CursorBindParam::Text(decimal), "numeric"))
        }
        CursorType::Json => {
            let json = match value {
                CursorValue::Json(v) => v.clone(),
                CursorValue::Utf8(v) => serde_json::from_str::<serde_json::Value>(v)
                    .with_context(|| format!("failed to parse '{}' as json", v))?,
                CursorValue::Null => serde_json::Value::Null,
                _ => bail!("cursor value is incompatible with json cursor type"),
            };
            Ok((CursorBindParam::Json(json), "jsonb"))
        }
    }
}

fn timestamp_millis_to_rfc3339(ms: i64) -> anyhow::Result<String> {
    let dt: DateTime<Utc> = DateTime::from_timestamp_millis(ms)
        .ok_or_else(|| anyhow!("invalid timestamp millis value: {}", ms))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

fn timestamp_micros_to_rfc3339(us: i64) -> anyhow::Result<String> {
    let secs = us.div_euclid(1_000_000);
    let micros = us.rem_euclid(1_000_000) as u32;
    let nanos = micros * 1_000;
    let dt: DateTime<Utc> = DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| anyhow!("invalid timestamp micros value: {}", us))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Micros, true))
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
) -> anyhow::Result<RecordBatch> {
    let arrays: Vec<Arc<dyn arrow::array::Array>> = columns
        .iter()
        .enumerate()
        .map(
            |(col_idx, col)| -> anyhow::Result<Arc<dyn arrow::array::Array>> {
                match col.data_type.as_str() {
                    "Int16" => {
                        let arr: Int16Array = rows
                            .iter()
                            .map(|row| row.try_get::<_, i16>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Int32" => {
                        let arr: Int32Array = rows
                            .iter()
                            .map(|row| row.try_get::<_, i32>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Int64" => {
                        let arr: Int64Array = rows
                            .iter()
                            .map(|row| row.try_get::<_, i64>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Float32" => {
                        let arr: Float32Array = rows
                            .iter()
                            .map(|row| row.try_get::<_, f32>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Float64" => {
                        let arr: Float64Array = rows
                            .iter()
                            .map(|row| row.try_get::<_, f64>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    "Boolean" => {
                        let arr: BooleanArray = rows
                            .iter()
                            .map(|row| row.try_get::<_, bool>(col_idx).ok())
                            .collect();
                        Ok(Arc::new(arr))
                    }
                    _ => {
                        let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
                        for row in rows {
                            match row.try_get::<_, String>(col_idx).ok() {
                                Some(s) => builder.append_value(&s),
                                None => builder.append_null(),
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                }
            },
        )
        .collect::<anyhow::Result<Vec<_>>>()?;

    RecordBatch::try_new(schema.clone(), arrays).context("Failed to create RecordBatch")
}

fn batch_to_ipc(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
        .context("IPC writer error")?;
    writer.write(batch).context("IPC write error")?;
    writer.finish().context("IPC finish error")?;
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
