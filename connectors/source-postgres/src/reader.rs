//! Full-refresh and incremental stream reading.
//!
//! Orchestrates: schema resolution -> query building -> server-side cursor ->
//! fetch loop -> Arrow batch emission -> checkpoint.

use std::sync::Arc;
use std::time::Instant;

use chrono::NaiveDateTime;
use tokio_postgres::Client;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::protocol::{DataErrorPolicy, DEFAULT_MAX_BATCH_BYTES, DEFAULT_MAX_RECORD_BYTES};

use crate::cursor::CursorTracker;
use crate::encode;
use crate::metrics::emit_read_metrics;
use crate::query;
use crate::types::Column;

/// Maximum number of rows per Arrow RecordBatch.
const BATCH_SIZE: usize = 10_000;

/// Number of rows to fetch per server-side cursor iteration.
const FETCH_CHUNK: usize = 10_000;

/// Server-side cursor name used for streaming reads.
const CURSOR_NAME: &str = "rb_cursor";

/// Initial fixed overhead estimate for an empty batch payload.
const BATCH_OVERHEAD_BYTES: usize = 256;

/// Estimate byte size of a single row for max_record_bytes checking.
pub(crate) fn estimate_row_bytes(columns: &[Column]) -> usize {
    let mut total = 0usize;
    for col in columns {
        total += match col.arrow_type {
            ArrowDataType::Int16 => 2,
            ArrowDataType::Int32 | ArrowDataType::Float32 | ArrowDataType::Date32 => 4,
            ArrowDataType::Int64 | ArrowDataType::Float64 | ArrowDataType::TimestampMicros => 8,
            ArrowDataType::Boolean => 1,
            _ => 64,
        };
        // 1-byte null bitmap overhead per column.
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

/// Encode and emit all currently accumulated rows as one Arrow IPC batch.
fn emit_accumulated_rows(
    rows: &mut Vec<tokio_postgres::Row>,
    columns: &[Column],
    schema: &Arc<Schema>,
    ctx: &Context,
    state: &mut EmitState,
    estimated_bytes: &mut usize,
) -> Result<(), String> {
    let encode_start = Instant::now();
    let batch = encode::rows_to_record_batch(rows, columns, schema)?;
    state.arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;

    state.total_records += rows.len() as u64;
    state.total_bytes += batch.get_array_memory_size() as u64;
    state.batches_emitted += 1;

    ctx.emit_batch(&batch)
        .map_err(|e| format!("emit_batch failed: {}", e.message))?;
    emit_read_metrics(ctx, state.total_records, state.total_bytes);

    rows.clear();
    *estimated_bytes = BATCH_OVERHEAD_BYTES;
    Ok(())
}

/// Read a single stream using server-side cursors.
pub async fn read_stream(
    client: &Client,
    ctx: &Context,
    stream: &StreamContext,
    connect_secs: f64,
) -> Result<ReadSummary, String> {
    ctx.log(
        LogLevel::Info,
        &format!("Reading stream: {}", stream.stream_name),
    );

    let query_start = Instant::now();

    // ── 1. Schema resolution ──────────────────────────────────────────
    let schema_query = "SELECT column_name, data_type, is_nullable \
        FROM information_schema.columns \
        WHERE table_schema = 'public' AND table_name = $1 \
        ORDER BY ordinal_position";

    let schema_rows = client
        .query(schema_query, &[&stream.stream_name])
        .await
        .map_err(|e| format!("Schema query failed for {}: {e}", stream.stream_name))?;

    let all_columns: Vec<Column> = schema_rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let pg_type: String = row.get(1);
            let nullable: bool = row.get::<_, String>(2) == "YES";
            Column::new(&name, &pg_type, nullable)
        })
        .collect();

    if all_columns.is_empty() {
        return Err(format!(
            "Table '{}' not found or has no columns",
            stream.stream_name
        ));
    }

    // ── 2. Projection pushdown ────────────────────────────────────────
    let columns: Vec<Column> = match &stream.selected_columns {
        Some(selected) if !selected.is_empty() => {
            let unknown: Vec<&str> = selected
                .iter()
                .filter(|name| !all_columns.iter().any(|c| c.name == **name))
                .map(|s| s.as_str())
                .collect();
            if !unknown.is_empty() {
                return Err(format!(
                    "Selected columns {:?} not found in table '{}'",
                    unknown, stream.stream_name
                ));
            }

            let filtered: Vec<Column> = all_columns
                .into_iter()
                .filter(|c| selected.iter().any(|s| s == &c.name))
                .collect();
            if filtered.is_empty() {
                return Err(format!(
                    "None of the selected columns {:?} found in table '{}'",
                    selected, stream.stream_name
                ));
            }
            if let (SyncMode::Incremental, Some(ci)) = (&stream.sync_mode, &stream.cursor_info) {
                if !filtered.iter().any(|c| c.name == ci.cursor_field) {
                    return Err(format!(
                        "Cursor field '{}' must be included in selected columns for incremental stream '{}'",
                        ci.cursor_field,
                        stream.stream_name
                    ));
                }
            }
            filtered
        }
        _ => all_columns,
    };

    // ── 3. Arrow schema ───────────────────────────────────────────────
    let arrow_schema = encode::arrow_schema(&columns);

    // ── 4. Transaction + server-side cursor ───────────────────────────
    client
        .execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .map_err(|e| format!("BEGIN failed: {e}"))?;

    let cursor_query = query::build_base_query(ctx, stream, &columns)?;

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
                .map_err(|e| format!("DECLARE CURSOR failed: {e}"))?;
        }
        None => {
            client
                .execute(&declare, &[])
                .await
                .map_err(|e| format!("DECLARE CURSOR failed: {e}"))?;
        }
    }

    let query_secs = query_start.elapsed().as_secs_f64();

    // ── 5. Limits + cursor tracker setup ──────────────────────────────
    let max_batch_bytes = if stream.limits.max_batch_bytes > 0 {
        stream.limits.max_batch_bytes as usize
    } else {
        DEFAULT_MAX_BATCH_BYTES as usize
    };

    let max_record_bytes = if stream.limits.max_record_bytes > 0 {
        stream.limits.max_record_bytes as usize
    } else {
        DEFAULT_MAX_RECORD_BYTES as usize
    };
    let estimated_row_bytes = estimate_row_bytes(&columns);
    let mut records_skipped: u64 = 0;

    let mut tracker: Option<CursorTracker> = match stream.cursor_info.as_ref() {
        Some(ci) => Some(CursorTracker::new(ci, &columns)?),
        None => None,
    };

    let fetch_query = format!("FETCH {} FROM {}", FETCH_CHUNK, CURSOR_NAME);
    let mut accumulated_rows: Vec<tokio_postgres::Row> = Vec::new();
    let mut estimated_bytes: usize = BATCH_OVERHEAD_BYTES;
    let mut state = EmitState {
        total_records: 0,
        total_bytes: 0,
        batches_emitted: 0,
        arrow_encode_nanos: 0,
    };

    let mut loop_error: Option<String> = None;

    // ── 6. Fetch loop ─────────────────────────────────────────────────
    let fetch_start = Instant::now();
    loop {
        let rows = match client.query(&fetch_query, &[]).await {
            Ok(r) => r,
            Err(e) => {
                loop_error = Some(format!("FETCH failed for {}: {}", stream.stream_name, e));
                break;
            }
        };

        let exhausted = rows.is_empty();

        if !exhausted {
            let mut valid_rows: Vec<tokio_postgres::Row> = Vec::with_capacity(rows.len());
            for row in rows {
                if estimated_row_bytes > max_record_bytes {
                    match stream.policies.on_data_error {
                        DataErrorPolicy::Fail => {
                            loop_error = Some(format!(
                                "Record exceeds max_record_bytes ({} > {})",
                                estimated_row_bytes, max_record_bytes,
                            ));
                            break;
                        }
                        _ => {
                            records_skipped += 1;
                            ctx.log(
                                LogLevel::Warn,
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
                if !accumulated_rows.is_empty()
                    && estimated_bytes + estimated_row_bytes >= max_batch_bytes
                {
                    if let Err(e) = emit_accumulated_rows(
                        &mut accumulated_rows,
                        &columns,
                        &arrow_schema,
                        ctx,
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

                // ── Cursor extraction ─────────────────────────────────
                // IMPORTANT: PostgreSQL SERIAL is INT4 (i32), not INT8 (i64).
                // tokio-postgres `try_get()` requires exact type matches, so we
                // must chain i64 -> i32 fallbacks. The host orchestrator currently
                // hardcodes CursorType::Utf8 for incremental state, so the catch-all
                // arm is common. Do not remove the i32 fallback or incremental
                // tracking can silently stop advancing on SERIAL cursor columns.
                if let Some(ref mut t) = tracker {
                    let col_idx = t.col_idx();
                    if t.is_int_strategy() {
                        let val = row
                            .try_get::<_, i64>(col_idx)
                            .ok()
                            .or_else(|| row.try_get::<_, i32>(col_idx).ok().map(i64::from));
                        if let Some(val) = val {
                            t.observe_int(val);
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
                            t.observe_text(&val);
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
            if let Err(e) = emit_accumulated_rows(
                &mut accumulated_rows,
                &columns,
                &arrow_schema,
                ctx,
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

    // ── 7. Cleanup ────────────────────────────────────────────────────
    let close_query = format!("CLOSE {}", CURSOR_NAME);
    if let Err(e) = client.execute(&close_query, &[]).await {
        ctx.log(
            LogLevel::Warn,
            &format!(
                "Warning: cursor CLOSE failed for stream '{}': {} (non-fatal, transaction cleanup will close it)",
                stream.stream_name, e
            ),
        );
    }
    if loop_error.is_some() {
        let _ = client.execute("ROLLBACK", &[]).await;
    } else {
        client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| format!("COMMIT failed: {e}"))?;
    }

    if let Some(e) = loop_error {
        return Err(e);
    }

    // ── 8. Checkpoint ─────────────────────────────────────────────────
    let checkpoint_count = if let Some(t) = tracker {
        if let Some(cp) =
            t.into_checkpoint(&stream.stream_name, state.total_records, state.total_bytes)
        {
            let cursor_field = cp.cursor_field.as_deref().unwrap_or("");
            let cursor_value = cp
                .cursor_value
                .as_ref()
                .map(|v| match v {
                    CursorValue::Utf8(s) => s.clone(),
                    _ => format!("{v:?}"),
                })
                .unwrap_or_default();
            let _ = ctx.checkpoint(&cp);
            ctx.log(
                LogLevel::Info,
                &format!(
                    "Source checkpoint: stream={} cursor_field={} cursor_value={}",
                    stream.stream_name, cursor_field, cursor_value
                ),
            );
            1u64
        } else {
            0u64
        }
    } else {
        0u64
    };

    // ── 9. Summary ────────────────────────────────────────────────────
    ctx.log(
        LogLevel::Info,
        &format!(
            "Stream '{}' complete: {} records, {} bytes, {} batches",
            stream.stream_name, state.total_records, state.total_bytes, state.batches_emitted
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

    #[test]
    fn estimate_row_bytes_matches_expected_mix() {
        let columns = vec![
            Column::new("id", "bigint", false),
            Column::new("name", "text", true),
            Column::new("active", "boolean", false),
        ];
        // Int64(8)+Utf8(64)+Boolean(1) plus 1-byte null bitmap overhead per column.
        assert_eq!(estimate_row_bytes(&columns), 8 + 64 + 1 + 3);
    }
}
