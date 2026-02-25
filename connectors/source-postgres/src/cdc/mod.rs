//! Change Data Capture via `PostgreSQL` logical replication slots.
//!
//! Reads WAL changes through `pg_logical_slot_get_changes` with the
//! `test_decoding` plugin, parses decoded rows, emits Arrow IPC batches,
//! and checkpoints by LSN.

mod parser;
#[allow(dead_code)] // Integration with the CDC reader happens in a later task.
pub(crate) mod pgoutput;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rapidbyte_sdk::arrow::array::{Array, StringBuilder};
use rapidbyte_sdk::arrow::datatypes::{DataType, Field, Schema};
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;

use crate::config::Config;
use crate::metrics::emit_read_metrics;
use crate::types::Column;

use parser::{lsn_gt, parse_change_line, CdcChange};

/// Maximum number of change rows per Arrow `RecordBatch`.
const BATCH_SIZE: usize = 10_000;

/// Maximum WAL changes consumed per CDC invocation to avoid unbounded memory use.
/// Type is i32 because `pg_logical_slot_get_changes()` expects int4.
const CDC_MAX_CHANGES: i32 = 100_000;

/// Default replication slot prefix. Full slot names are `rapidbyte_{stream_name}`.
const SLOT_PREFIX: &str = "rapidbyte_";

struct CdcEmitState {
    total_records: u64,
    total_bytes: u64,
    batches_emitted: u64,
    arrow_encode_nanos: u64,
}

fn emit_cdc_batch(
    changes: &mut Vec<CdcChange>,
    table_columns: &[Column],
    schema: &Arc<Schema>,
    ctx: &Context,
    state: &mut CdcEmitState,
) -> Result<(), String> {
    let encode_start = Instant::now();
    let batch = changes_to_batch(changes, table_columns, schema)?;
    // Safety: encode timing in nanos will not exceed u64::MAX for any realistic duration.
    #[allow(clippy::cast_possible_truncation)]
    {
        state.arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;
    }

    state.total_records += changes.len() as u64;
    state.total_bytes += batch.get_array_memory_size() as u64;
    state.batches_emitted += 1;

    ctx.emit_batch(&batch)
        .map_err(|e| format!("emit_batch failed: {}", e.message))?;
    emit_read_metrics(ctx, state.total_records, state.total_bytes);

    changes.clear();
    Ok(())
}

/// Read CDC changes from a logical replication slot using `pg_logical_slot_get_changes()`.
#[allow(clippy::too_many_lines)]
pub async fn read_cdc_changes(
    client: &Client,
    ctx: &Context,
    stream: &StreamContext,
    config: &Config,
    connect_secs: f64,
) -> Result<ReadSummary, String> {
    ctx.log(
        LogLevel::Info,
        &format!("CDC reading stream: {}", stream.stream_name),
    );

    let query_start = Instant::now();

    // 1. Derive slot name
    let slot_name = config
        .replication_slot
        .clone()
        .unwrap_or_else(|| format!("{}{}", SLOT_PREFIX, stream.stream_name));

    // 2. Ensure replication slot exists (idempotent)
    ensure_replication_slot(client, ctx, &slot_name).await?;

    // 3. Get table schema for Arrow construction
    let table_columns =
        crate::discovery::query_table_columns(client, &stream.stream_name).await?;

    // Build Arrow schema: table columns + _rb_op metadata column
    let arrow_schema = build_cdc_arrow_schema(&table_columns);

    // 4. Read changes from the slot (this CONSUMES them)
    // Limit WAL changes per invocation to prevent OOM on large backlogs.
    let changes_query = "SELECT lsn::text, data FROM pg_logical_slot_get_changes($1, NULL, $2)";
    let change_rows = client
        .query(changes_query, &[&slot_name, &CDC_MAX_CHANGES])
        .await
        .map_err(|e| {
            format!(
                "pg_logical_slot_get_changes failed for slot {slot_name}: {e}"
            )
        })?;

    let query_secs = query_start.elapsed().as_secs_f64();

    let fetch_start = Instant::now();
    let mut state = CdcEmitState {
        total_records: 0,
        total_bytes: 0,
        batches_emitted: 0,
        arrow_encode_nanos: 0,
    };
    let mut max_lsn: Option<String> = None;

    // 5. Parse changes, filter to our table, accumulate into batches
    let target_table = format!("public.{}", stream.stream_name);
    let mut accumulated_changes: Vec<CdcChange> = Vec::new();

    for row in &change_rows {
        let lsn: String = row.get(0);
        let data: String = row.get(1);

        // Track max LSN
        if max_lsn.as_ref().is_none_or(|current| lsn_gt(&lsn, current)) {
            max_lsn = Some(lsn.clone());
        }

        // Parse change line; skip BEGIN/COMMIT/non-matching tables
        if let Some(change) = parse_change_line(&data) {
            if change.table == target_table || change.table == stream.stream_name {
                accumulated_changes.push(change);
            }
        }

        // Flush batch if accumulated enough
        if accumulated_changes.len() >= BATCH_SIZE {
            emit_cdc_batch(
                &mut accumulated_changes,
                &table_columns,
                &arrow_schema,
                ctx,
                &mut state,
            )?;
        }
    }

    // Flush remaining changes
    if !accumulated_changes.is_empty() {
        emit_cdc_batch(
            &mut accumulated_changes,
            &table_columns,
            &arrow_schema,
            ctx,
            &mut state,
        )?;
    }

    let fetch_secs = fetch_start.elapsed().as_secs_f64();

    // 6. Emit checkpoint with max LSN
    let checkpoint_count = if let Some(ref lsn) = max_lsn {
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: stream.stream_name.clone(),
            cursor_field: Some("lsn".to_string()),
            cursor_value: Some(CursorValue::Lsn { value: lsn.clone() }),
            records_processed: state.total_records,
            bytes_processed: state.total_bytes,
        };
        // CDC uses get_changes (destructive) so checkpoint MUST succeed to avoid data loss.
        ctx.checkpoint(&cp).map_err(|e| {
            format!(
                "CDC checkpoint failed (WAL already consumed): {}",
                e.message
            )
        })?;
        ctx.log(
            LogLevel::Info,
            &format!("CDC checkpoint: stream={} lsn={}", stream.stream_name, lsn),
        );
        1u64
    } else {
        ctx.log(
            LogLevel::Info,
            &format!("CDC: no new changes for stream '{}'", stream.stream_name),
        );
        0u64
    };

    ctx.log(
        LogLevel::Info,
        &format!(
            "CDC stream '{}' complete: {} records, {} bytes, {} batches",
            stream.stream_name, state.total_records, state.total_bytes, state.batches_emitted
        ),
    );

    // Safety: nanosecond timing precision loss beyond 52 bits is acceptable for metrics.
    #[allow(clippy::cast_precision_loss)]
    let arrow_encode_secs = state.arrow_encode_nanos as f64 / 1e9;

    Ok(ReadSummary {
        records_read: state.total_records,
        bytes_read: state.total_bytes,
        batches_emitted: state.batches_emitted,
        checkpoint_count,
        records_skipped: 0,
        perf: Some(ReadPerf {
            connect_secs,
            query_secs,
            fetch_secs,
            arrow_encode_secs,
        }),
    })
}

/// Ensure the logical replication slot exists, creating it if necessary.
/// Uses try-create to avoid TOCTOU race between check and create.
async fn ensure_replication_slot(
    client: &Client,
    ctx: &Context,
    slot_name: &str,
) -> Result<(), String> {
    ctx.log(
        LogLevel::Debug,
        &format!("Ensuring replication slot '{slot_name}' exists"),
    );

    // Try to create; if it already exists, PG raises duplicate_object (42710).
    let result = client
        .query_one(
            "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
            &[&slot_name],
        )
        .await;

    match result {
        Ok(_) => {
            ctx.log(
                LogLevel::Info,
                &format!(
                    "Created replication slot '{slot_name}' with test_decoding"
                ),
            );
        }
        Err(e) => {
            // Check for duplicate_object error (SQLSTATE 42710)
            let is_duplicate = e
                .as_db_error()
                .is_some_and(|db| db.code().code() == "42710");

            if is_duplicate {
                ctx.log(
                    LogLevel::Debug,
                    &format!("Replication slot '{slot_name}' already exists"),
                );
            } else {
                return Err(format!(
                    "Failed to create logical replication slot '{slot_name}'. \
                     Ensure wal_level=logical in postgresql.conf: {e}"
                ));
            }
        }
    }

    Ok(())
}

/// Build Arrow schema for CDC batches: table columns (all as Utf8 for simplicity)
/// plus the `_rb_op` metadata column.
fn build_cdc_arrow_schema(columns: &[Column]) -> Arc<Schema> {
    let mut fields: Vec<Field> = columns
        .iter()
        .map(|col| Field::new(&col.name, DataType::Utf8, true))
        .collect();

    // Add the CDC operation column
    fields.push(Field::new("_rb_op", DataType::Utf8, false));

    Arc::new(Schema::new(fields))
}

/// Convert accumulated CDC changes into an Arrow `RecordBatch`.
fn changes_to_batch(
    changes: &[CdcChange],
    table_columns: &[Column],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, String> {
    let num_cols = table_columns.len() + 1; // +1 for _rb_op
    let mut builders: Vec<StringBuilder> = (0..num_cols)
        .map(|_| StringBuilder::with_capacity(changes.len(), changes.len() * 32))
        .collect();

    for change in changes {
        // Build lookup: column_name -> value
        let values: HashMap<&str, &str> = change
            .columns
            .iter()
            .map(|(name, _type, value)| (name.as_str(), value.as_str()))
            .collect();

        for (col_idx, col) in table_columns.iter().enumerate() {
            match values.get(col.name.as_str()) {
                Some(&"null") | None => builders[col_idx].append_null(),
                Some(v) => builders[col_idx].append_value(v),
            }
        }

        // _rb_op column
        builders[num_cols - 1].append_value(change.op.as_str());
    }

    let arrays: Vec<Arc<dyn Array>> = builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as Arc<dyn Array>)
        .collect();

    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("failed to create CDC RecordBatch: {e}"))
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use super::parser::CdcOp;

    #[test]
    fn test_build_cdc_arrow_schema() {
        let columns = vec![
            Column::new("id", "integer", false),
            Column::new("name", "text", true),
        ];

        let schema = build_cdc_arrow_schema(&columns);
        assert_eq!(schema.fields().len(), 3);
        // All columns are Utf8 in CDC mode (values come as text from test_decoding)
        assert_eq!(*schema.field(0).data_type(), DataType::Utf8);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8);
        assert_eq!(schema.field(2).name(), "_rb_op");
        assert!(!schema.field(2).is_nullable());
    }

    #[test]
    fn test_changes_to_batch() {
        let columns = vec![
            Column::new("id", "integer", false),
            Column::new("name", "text", true),
        ];
        let schema = build_cdc_arrow_schema(&columns);

        let changes = vec![
            CdcChange {
                op: CdcOp::Insert,
                table: "public.users".to_string(),
                columns: vec![
                    ("id".to_string(), "integer".to_string(), "1".to_string()),
                    ("name".to_string(), "text".to_string(), "Alice".to_string()),
                ],
            },
            CdcChange {
                op: CdcOp::Delete,
                table: "public.users".to_string(),
                columns: vec![("id".to_string(), "integer".to_string(), "2".to_string())],
            },
        ];

        let batch = changes_to_batch(&changes, &columns, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3); // id, name, _rb_op
    }
}
