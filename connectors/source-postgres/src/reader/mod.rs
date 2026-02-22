//! Full-refresh and incremental stream reads using server-side cursors.
//!
//! Builds typed cursor predicates, fetches rows in chunks via DECLARE/FETCH,
//! encodes records to Arrow IPC, and emits batches/checkpoints to the host.

mod arrow_encode;
mod query;

use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context};
use rapidbyte_sdk::arrow::datatypes::Schema;
use chrono::NaiveDateTime;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    ColumnSchema, CursorType, DataErrorPolicy, ReadSummary, StreamContext, SyncMode,
    DEFAULT_MAX_BATCH_BYTES, DEFAULT_MAX_RECORD_BYTES,
};

use crate::metrics::emit_read_metrics;
use crate::schema::pg_type_to_arrow;

use rapidbyte_sdk::arrow::build_arrow_schema;
use self::arrow_encode::rows_to_record_batch;
use self::query::build_base_query;

/// Maximum number of rows per Arrow RecordBatch.
const BATCH_SIZE: usize = 10_000;

/// Number of rows to fetch per server-side cursor iteration.
const FETCH_CHUNK: usize = 10_000;

/// Server-side cursor name used for streaming reads.
const CURSOR_NAME: &str = "rb_cursor";

/// Initial fixed overhead estimate for an empty batch payload.
const BATCH_OVERHEAD_BYTES: usize = 256;

/// Estimate byte size of a single row for max_record_bytes checking.
pub(crate) fn estimate_row_bytes(columns: &[ColumnSchema]) -> usize {
    let mut total = 0usize;
    for col in columns {
        total += match col.data_type.as_str() {
            "Int16" => 2,
            "Int32" | "Float32" | "Date32" => 4,
            "Int64" | "Float64" | "TimestampMicros" => 8,
            "Boolean" => 1,
            _ => 64,
        };
        total += 1;
    }
    total
}

struct EmitState {
    total_records: u64,
    total_bytes: u64,
    batches_emitted: u64,
    arrow_encode_nanos: u64,
}

enum MaxCursorValue {
    Int(i64),
    Text(String),
}

impl MaxCursorValue {
    fn update_int(current: &mut Option<Self>, value: i64) {
        match current {
            Some(Self::Int(existing)) => {
                if value > *existing {
                    *existing = value;
                }
            }
            _ => *current = Some(Self::Int(value)),
        }
    }

    fn update_text(current: &mut Option<Self>, value: String) {
        match current {
            Some(Self::Text(existing)) => {
                if value > *existing {
                    *existing = value;
                }
            }
            _ => *current = Some(Self::Text(value)),
        }
    }

    fn as_checkpoint_string(&self) -> String {
        match self {
            Self::Int(v) => v.to_string(),
            Self::Text(v) => v.clone(),
        }
    }
}

/// Encode and emit all currently accumulated rows as one Arrow IPC batch.
fn emit_accumulated_rows(
    rows: &mut Vec<tokio_postgres::Row>,
    columns: &[ColumnSchema],
    pg_types: &[String],
    schema: &Arc<Schema>,
    stream_name: &str,
    state: &mut EmitState,
    estimated_bytes: &mut usize,
) -> anyhow::Result<()> {
    let encode_start = Instant::now();
    let batch = rows_to_record_batch(rows, columns, pg_types, schema)?;
    state.arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;

    state.total_records += rows.len() as u64;
    state.total_bytes += batch.get_array_memory_size() as u64;
    state.batches_emitted += 1;

    host_ffi::emit_batch(&batch).map_err(|e| anyhow!("emit_batch failed: {}", e.message))?;
    emit_read_metrics(stream_name, state.total_records, state.total_bytes);

    rows.clear();
    *estimated_bytes = BATCH_OVERHEAD_BYTES;
    Ok(())
}

/// Read a single stream using server-side cursors.
pub async fn read_stream(
    client: &Client,
    ctx: &StreamContext,
    connect_secs: f64,
) -> Result<ReadSummary, String> {
    read_stream_inner(client, ctx, connect_secs)
        .await
        .map_err(|e| format!("{:#}", e))
}

async fn read_stream_inner(
    client: &Client,
    ctx: &StreamContext,
    connect_secs: f64,
) -> anyhow::Result<ReadSummary> {
    host_ffi::log(2, &format!("Reading stream: {}", ctx.stream_name));

    let query_start = Instant::now();

    let schema_query = "SELECT column_name, data_type, is_nullable \
        FROM information_schema.columns \
        WHERE table_schema = 'public' AND table_name = $1 \
        ORDER BY ordinal_position";

    let schema_rows = client
        .query(schema_query, &[&ctx.stream_name])
        .await
        .with_context(|| format!("Schema query failed for {}", ctx.stream_name))?;

    let all_columns: Vec<ColumnSchema> = schema_rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let data_type: String = row.get(1);
            let nullable: bool = row.get::<_, String>(2) == "YES";
            ColumnSchema {
                name,
                data_type: pg_type_to_arrow(&data_type),
                nullable,
            }
        })
        .collect();

    if all_columns.is_empty() {
        bail!("Table '{}' not found or has no columns", ctx.stream_name);
    }

    // Build parallel pg_types vector for json/jsonb extraction in Arrow encoder.
    let all_pg_types: Vec<String> = schema_rows
        .iter()
        .map(|row| row.get::<_, String>(1))
        .collect();

    // Apply projection pushdown: filter to selected columns if specified.
    // Column order follows table ordinal position, not user-specified order.
    let (columns, pg_types): (Vec<ColumnSchema>, Vec<String>) = match &ctx.selected_columns {
        Some(selected) if !selected.is_empty() => {
            let unknown: Vec<&str> = selected
                .iter()
                .filter(|name| !all_columns.iter().any(|c| c.name == **name))
                .map(|s| s.as_str())
                .collect();
            if !unknown.is_empty() {
                bail!(
                    "Selected columns {:?} not found in table '{}'",
                    unknown,
                    ctx.stream_name
                );
            }

            let (filtered, filtered_pg_types): (Vec<ColumnSchema>, Vec<String>) = all_columns
                .into_iter()
                .zip(all_pg_types.into_iter())
                .filter(|(c, _)| selected.iter().any(|s| s == &c.name))
                .unzip();
            if filtered.is_empty() {
                bail!(
                    "None of the selected columns {:?} found in table '{}'",
                    selected,
                    ctx.stream_name
                );
            }
            if let (SyncMode::Incremental, Some(ci)) = (&ctx.sync_mode, &ctx.cursor_info) {
                if !filtered.iter().any(|c| c.name == ci.cursor_field) {
                    bail!(
                        "Cursor field '{}' must be included in selected columns for incremental stream '{}'",
                        ci.cursor_field,
                        ctx.stream_name
                    );
                }
            }
            (filtered, filtered_pg_types)
        }
        _ => (all_columns, all_pg_types),
    };

    let arrow_schema = build_arrow_schema(&columns);

    client
        .execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .context("BEGIN failed")?;

    let cursor_query = build_base_query(ctx, &columns, &pg_types)?;

    let declare = format!(
        "DECLARE {} NO SCROLL CURSOR FOR {}",
        CURSOR_NAME, cursor_query.sql
    );
    match cursor_query.bind.as_ref() {
        Some(bind) => {
            let params: [&(dyn tokio_postgres::types::ToSql + Sync); 1] = [bind.as_tosql()];
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
        DEFAULT_MAX_BATCH_BYTES as usize
    };

    let max_record_bytes = if ctx.limits.max_record_bytes > 0 {
        ctx.limits.max_record_bytes as usize
    } else {
        DEFAULT_MAX_RECORD_BYTES as usize
    };
    let estimated_row_bytes = estimate_row_bytes(&columns);
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
    let track_cursor_as_int = matches!(
        ctx.cursor_info.as_ref().map(|ci| ci.cursor_type),
        Some(CursorType::Int64)
    );
    let mut max_cursor_value: Option<MaxCursorValue> = None;

    let fetch_query = format!("FETCH {} FROM {}", FETCH_CHUNK, CURSOR_NAME);
    let mut accumulated_rows: Vec<tokio_postgres::Row> = Vec::new();
    let mut estimated_bytes: usize = BATCH_OVERHEAD_BYTES;
    let mut state = EmitState {
        total_records: 0,
        total_bytes: 0,
        batches_emitted: 0,
        arrow_encode_nanos: 0,
    };

    let mut loop_error: Option<anyhow::Error> = None;

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
                if estimated_row_bytes > max_record_bytes {
                    match ctx.policies.on_data_error {
                        DataErrorPolicy::Fail => {
                            loop_error = Some(anyhow!(
                                "Record exceeds max_record_bytes ({} > {})",
                                estimated_row_bytes,
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
                                    estimated_row_bytes, max_record_bytes,
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
                if !accumulated_rows.is_empty() && estimated_bytes + estimated_row_bytes >= max_batch_bytes {
                    if let Err(e) = emit_accumulated_rows(
                        &mut accumulated_rows,
                        &columns,
                        &pg_types,
                        &arrow_schema,
                        &ctx.stream_name,
                        &mut state,
                        &mut estimated_bytes,
                    ) {
                        loop_error = Some(e);
                        break;
                    }
                }

                if loop_error.is_some() {
                    break;
                }

                estimated_bytes += estimated_row_bytes;

                if let Some(col_idx) = cursor_col_idx {
                    // IMPORTANT: PostgreSQL SERIAL is INT4 (i32), not INT8 (i64).
                    // tokio-postgres `try_get()` requires exact type matches, so we
                    // must chain i64 -> i32 fallbacks. The host orchestrator currently
                    // hardcodes CursorType::Utf8 for incremental state, so the catch-all
                    // arm is common. Do not remove the i32 fallback or incremental
                    // tracking can silently stop advancing on SERIAL cursor columns.
                    if track_cursor_as_int {
                        let val = row
                            .try_get::<_, i64>(col_idx)
                            .ok()
                            .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(i64::from));
                        if let Some(val) = val {
                            MaxCursorValue::update_int(&mut max_cursor_value, val);
                        }
                    } else {
                        let val = row
                            .try_get::<_, String>(col_idx)
                            .ok()
                            .or_else(|| row.try_get::<_, i64>(col_idx).ok().map(|n| n.to_string()))
                            .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(|n| n.to_string()))
                            .or_else(|| {
                                row.try_get::<_, NaiveDateTime>(col_idx)
                                    .ok()
                                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                            })
                            .or_else(|| {
                                row.try_get::<_, chrono::DateTime<chrono::Utc>>(col_idx)
                                    .ok()
                                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                            })
                            .or_else(|| {
                                row.try_get::<_, chrono::NaiveDate>(col_idx)
                                    .ok()
                                    .map(|d| d.to_string())
                            })
                            .or_else(|| {
                                row.try_get::<_, serde_json::Value>(col_idx)
                                    .ok()
                                    .map(|v| v.to_string())
                            });
                        if let Some(val) = val {
                            MaxCursorValue::update_text(&mut max_cursor_value, val);
                        }
                    }
                }

                accumulated_rows.push(row);
            }
        }

        let should_emit = !accumulated_rows.is_empty()
            && (estimated_bytes >= max_batch_bytes || accumulated_rows.len() >= BATCH_SIZE || exhausted);

        if should_emit {
            if let Err(e) = emit_accumulated_rows(
                &mut accumulated_rows,
                &columns,
                &pg_types,
                &arrow_schema,
                &ctx.stream_name,
                &mut state,
                &mut estimated_bytes,
            ) {
                loop_error = Some(e);
                break;
            }
        }

        if exhausted {
            break;
        }
    }

    let fetch_secs = fetch_start.elapsed().as_secs_f64();

    let close_query = format!("CLOSE {}", CURSOR_NAME);
    if let Err(e) = client.execute(&close_query, &[]).await {
        host_ffi::log(
            1,
            &format!(
                "Warning: cursor CLOSE failed for stream '{}': {} (non-fatal, transaction cleanup will close it)",
                ctx.stream_name, e
            ),
        );
    }
    if loop_error.is_some() {
        let _ = client.execute("ROLLBACK", &[]).await;
    } else {
        client
            .execute("COMMIT", &[])
            .await
            .context("COMMIT failed")?;
    }

    if let Some(e) = loop_error {
        return Err(e);
    }

    let checkpoint_count =
        if let (Some(ci), Some(max_val)) = (ctx.cursor_info.as_ref(), max_cursor_value.as_ref()) {
            let max_val = max_val.as_checkpoint_string();
            let cp = rapidbyte_sdk::protocol::Checkpoint {
                id: 1,
                kind: rapidbyte_sdk::protocol::CheckpointKind::Source,
                stream: ctx.stream_name.clone(),
                cursor_field: Some(ci.cursor_field.clone()),
                cursor_value: Some(rapidbyte_sdk::protocol::CursorValue::Utf8(max_val.clone())),
                records_processed: state.total_records,
                bytes_processed: state.total_bytes,
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
            ctx.stream_name, state.total_records, state.total_bytes, state.batches_emitted
        ),
    );

    Ok(ReadSummary {
        records_read: state.total_records,
        bytes_read: state.total_bytes,
        batches_emitted: state.batches_emitted,
        checkpoint_count,
        records_skipped,
        perf: Some(rapidbyte_sdk::protocol::ReadPerf {
            connect_secs,
            query_secs,
            fetch_secs,
            arrow_encode_secs: state.arrow_encode_nanos as f64 / 1e9,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::protocol::ArrowDataType;

    #[test]
    fn estimate_row_bytes_matches_expected_mix() {
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: ArrowDataType::Utf8,
                nullable: true,
            },
            ColumnSchema {
                name: "active".to_string(),
                data_type: ArrowDataType::Boolean,
                nullable: false,
            },
        ];

        // Int64(8)+Utf8(64)+Boolean(1) plus 1-byte null bitmap overhead per column.
        assert_eq!(estimate_row_bytes(&columns), 8 + 64 + 1 + 3);
    }
}
