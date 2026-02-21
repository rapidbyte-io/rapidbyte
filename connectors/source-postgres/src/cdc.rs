use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context};
use arrow::array::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use tokio_postgres::Client;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    Checkpoint, CheckpointKind, ColumnSchema, CursorValue, Metric, MetricValue, ReadSummary,
    StreamContext,
};
use rapidbyte_sdk::validation::validate_pg_identifier;

use crate::config::Config;
use crate::schema::pg_type_to_arrow;

/// Maximum number of change rows per Arrow RecordBatch.
const BATCH_SIZE: usize = 10_000;

/// CDC operation type extracted from test_decoding output.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CdcOp {
    Insert,
    Update,
    Delete,
}

impl CdcOp {
    fn as_str(&self) -> &'static str {
        match self {
            CdcOp::Insert => "insert",
            CdcOp::Update => "update",
            CdcOp::Delete => "delete",
        }
    }
}

/// A single parsed change from test_decoding output.
#[derive(Debug, Clone)]
struct CdcChange {
    op: CdcOp,
    table: String,
    columns: Vec<(String, String, String)>, // (name, pg_type, value)
}

/// Read CDC changes from a logical replication slot using `pg_logical_slot_get_changes()`.
pub async fn read_cdc_changes(
    client: &Client,
    ctx: &StreamContext,
    config: &Config,
    connect_secs: f64,
) -> Result<ReadSummary, String> {
    read_cdc_inner(client, ctx, config, connect_secs)
        .await
        .map_err(|e| e.to_string())
}

async fn read_cdc_inner(
    client: &Client,
    ctx: &StreamContext,
    config: &Config,
    connect_secs: f64,
) -> anyhow::Result<ReadSummary> {
    host_ffi::log(2, &format!("CDC reading stream: {}", ctx.stream_name));

    validate_pg_identifier(&ctx.stream_name)
        .map_err(|e| anyhow!("Invalid stream name '{}': {}", ctx.stream_name, e))?;

    let query_start = Instant::now();

    // 1. Derive slot name
    let slot_name = config
        .replication_slot
        .clone()
        .unwrap_or_else(|| format!("rapidbyte_{}", ctx.stream_name));

    validate_pg_identifier(&slot_name)
        .map_err(|e| anyhow!("Invalid replication slot name '{}': {}", slot_name, e))?;

    // 2. Ensure replication slot exists (idempotent)
    ensure_replication_slot(client, &slot_name).await?;

    // 3. Get table schema for Arrow construction
    let schema_query = "SELECT column_name, data_type, is_nullable \
        FROM information_schema.columns \
        WHERE table_schema = 'public' AND table_name = $1 \
        ORDER BY ordinal_position";

    let schema_rows = client
        .query(schema_query, &[&ctx.stream_name])
        .await
        .with_context(|| format!("Schema query failed for {}", ctx.stream_name))?;

    let table_columns: Vec<ColumnSchema> = schema_rows
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

    if table_columns.is_empty() {
        bail!("Table '{}' not found or has no columns", ctx.stream_name);
    }

    // Build Arrow schema: table columns + _rb_op metadata column
    let arrow_schema = build_cdc_arrow_schema(&table_columns);

    // 4. Read changes from the slot (this CONSUMES them)
    // Limit WAL changes per invocation to prevent OOM on large backlogs.
    let max_changes = 100_000i64;
    let changes_query =
        "SELECT lsn::text, data FROM pg_logical_slot_get_changes($1, NULL, $2)";
    let change_rows = client
        .query(changes_query, &[&slot_name, &max_changes])
        .await
        .with_context(|| format!("pg_logical_slot_get_changes failed for slot {}", slot_name))?;

    let query_secs = query_start.elapsed().as_secs_f64();

    let fetch_start = Instant::now();
    let mut arrow_encode_nanos: u64 = 0;

    let mut total_records: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut batches_emitted: u64 = 0;
    let mut max_lsn: Option<String> = None;

    // 5. Parse changes, filter to our table, accumulate into batches
    let target_table = format!("public.{}", ctx.stream_name);
    let mut accumulated_changes: Vec<CdcChange> = Vec::new();

    for row in &change_rows {
        let lsn: String = row.get(0);
        let data: String = row.get(1);

        // Track max LSN
        if max_lsn.as_ref().map_or(true, |current| lsn_gt(&lsn, current)) {
            max_lsn = Some(lsn.clone());
        }

        // Parse change line; skip BEGIN/COMMIT/non-matching tables
        if let Some(change) = parse_change_line(&data) {
            if change.table == target_table
                || change.table == ctx.stream_name
            {
                accumulated_changes.push(change);
            }
        }

        // Flush batch if accumulated enough
        if accumulated_changes.len() >= BATCH_SIZE {
            let encode_start = Instant::now();
            let ipc_bytes =
                changes_to_ipc(&accumulated_changes, &table_columns, &arrow_schema)?;
            arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;

            total_records += accumulated_changes.len() as u64;
            total_bytes += ipc_bytes.len() as u64;
            batches_emitted += 1;

            host_ffi::emit_batch(&ipc_bytes)
                .map_err(|e| anyhow!("emit_batch failed: {}", e.message))?;

            emit_metrics(ctx, total_records, total_bytes);
            accumulated_changes.clear();
        }
    }

    // Flush remaining changes
    if !accumulated_changes.is_empty() {
        let encode_start = Instant::now();
        let ipc_bytes =
            changes_to_ipc(&accumulated_changes, &table_columns, &arrow_schema)?;
        arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;

        total_records += accumulated_changes.len() as u64;
        total_bytes += ipc_bytes.len() as u64;
        batches_emitted += 1;

        host_ffi::emit_batch(&ipc_bytes)
            .map_err(|e| anyhow!("emit_batch failed: {}", e.message))?;

        emit_metrics(ctx, total_records, total_bytes);
    }

    let fetch_secs = fetch_start.elapsed().as_secs_f64();

    // 6. Emit checkpoint with max LSN
    let checkpoint_count = if let Some(ref lsn) = max_lsn {
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: ctx.stream_name.clone(),
            cursor_field: Some("lsn".to_string()),
            cursor_value: Some(CursorValue::Lsn(lsn.clone())),
            records_processed: total_records,
            bytes_processed: total_bytes,
        };
        // CDC uses get_changes (destructive) so checkpoint MUST succeed to avoid data loss.
        host_ffi::checkpoint("source-postgres", &ctx.stream_name, &cp)
            .map_err(|e| anyhow!("CDC checkpoint failed (WAL already consumed): {}", e.message))?;
        host_ffi::log(
            2,
            &format!(
                "CDC checkpoint: stream={} lsn={}",
                ctx.stream_name, lsn
            ),
        );
        1u64
    } else {
        host_ffi::log(
            2,
            &format!(
                "CDC: no new changes for stream '{}'",
                ctx.stream_name
            ),
        );
        0u64
    };

    host_ffi::log(
        2,
        &format!(
            "CDC stream '{}' complete: {} records, {} bytes, {} batches",
            ctx.stream_name, total_records, total_bytes, batches_emitted
        ),
    );

    Ok(ReadSummary {
        records_read: total_records,
        bytes_read: total_bytes,
        batches_emitted,
        checkpoint_count,
        records_skipped: 0,
        perf: Some(rapidbyte_sdk::protocol::ReadPerf {
            connect_secs,
            query_secs,
            fetch_secs,
            arrow_encode_secs: arrow_encode_nanos as f64 / 1e9,
        }),
    })
}

/// Ensure the logical replication slot exists, creating it if necessary.
/// Uses try-create to avoid TOCTOU race between check and create.
async fn ensure_replication_slot(client: &Client, slot_name: &str) -> anyhow::Result<()> {
    host_ffi::log(
        3,
        &format!("Ensuring replication slot '{}' exists", slot_name),
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
            host_ffi::log(
                2,
                &format!("Created replication slot '{}' with test_decoding", slot_name),
            );
        }
        Err(e) => {
            // Check for duplicate_object error (SQLSTATE 42710)
            let is_duplicate = e
                .as_db_error()
                .map(|db| db.code().code() == "42710")
                .unwrap_or(false);

            if is_duplicate {
                host_ffi::log(
                    3,
                    &format!("Replication slot '{}' already exists", slot_name),
                );
            } else {
                return Err(anyhow::anyhow!(
                    "Failed to create logical replication slot '{}'. \
                     Ensure wal_level=logical in postgresql.conf: {}",
                    slot_name,
                    e
                ));
            }
        }
    }

    Ok(())
}

/// Parse a single test_decoding output line into a CdcChange.
///
/// Format examples:
///   BEGIN 12345
///   table public.users: INSERT: id[integer]:1 name[text]:'John' active[boolean]:t
///   table public.users: UPDATE: id[integer]:1 name[text]:'Jane'
///   table public.users: DELETE: id[integer]:1
///   COMMIT 12345
///
/// Returns None for BEGIN/COMMIT lines.
fn parse_change_line(line: &str) -> Option<CdcChange> {
    let line = line.trim();

    if line.starts_with("BEGIN") || line.starts_with("COMMIT") {
        return None;
    }

    // Expected format: "table <schema.table>: <OP>: <columns>"
    let rest = line.strip_prefix("table ")?;

    // Split on ": " to get table and the remainder
    let first_colon = rest.find(": ")?;
    let table = rest[..first_colon].to_string();
    let after_table = &rest[first_colon + 2..];

    // Extract operation
    let op_colon = after_table.find(": ");
    let (op_str, columns_str) = match op_colon {
        Some(pos) => (&after_table[..pos], &after_table[pos + 2..]),
        None => (after_table, ""),
    };

    let op = match op_str {
        "INSERT" => CdcOp::Insert,
        "UPDATE" => CdcOp::Update,
        "DELETE" => CdcOp::Delete,
        _ => return None,
    };

    let columns = if columns_str.is_empty() {
        Vec::new()
    } else {
        parse_columns(columns_str)
    };

    Some(CdcChange { op, table, columns })
}

/// Parse test_decoding column values from a string like:
///   id[integer]:1 name[text]:'John' active[boolean]:t
fn parse_columns(s: &str) -> Vec<(String, String, String)> {
    let mut columns = Vec::new();
    let mut remaining = s.trim();

    while !remaining.is_empty() {
        // Find column name: everything before '['
        let bracket_start = match remaining.find('[') {
            Some(pos) => pos,
            None => break,
        };
        let col_name = remaining[..bracket_start].to_string();

        // Find type: everything between '[' and ']'
        let bracket_end = match remaining[bracket_start..].find(']') {
            Some(pos) => bracket_start + pos,
            None => break,
        };
        let col_type = remaining[bracket_start + 1..bracket_end].to_string();

        // After ']' should be ':'
        let after_bracket = &remaining[bracket_end + 1..];
        if !after_bracket.starts_with(':') {
            break;
        }
        let value_start = &after_bracket[1..];

        // Parse value: could be quoted string or unquoted value
        let (value, rest) = parse_value(value_start);
        columns.push((col_name, col_type, value));
        remaining = rest.trim_start();
    }

    columns
}

/// Parse a single value from test_decoding output.
/// Handles quoted strings (with '' escape for internal quotes) and unquoted values.
fn parse_value(s: &str) -> (String, &str) {
    if s.starts_with('\'') {
        // Quoted string value — iterate by char to handle multi-byte UTF-8 correctly
        let inner = &s[1..]; // skip opening quote
        let mut value = String::new();
        let mut chars = inner.char_indices();
        loop {
            match chars.next() {
                Some((i, '\'')) => {
                    // Check for escaped quote ('')
                    let rest = &inner[i + 1..];
                    if rest.starts_with('\'') {
                        value.push('\'');
                        chars.next(); // skip the second quote
                    } else {
                        // End of quoted string; +1 for opening quote, +i+1 for closing quote
                        return (value, rest);
                    }
                }
                Some((_, ch)) => value.push(ch),
                None => return (value, ""), // unterminated quote
            }
        }
    } else {
        // Unquoted value - ends at space or end of string
        match s.find(' ') {
            Some(pos) => (s[..pos].to_string(), &s[pos..]),
            None => (s.to_string(), ""),
        }
    }
}

/// Build Arrow schema for CDC batches: table columns (all as Utf8 for simplicity)
/// plus the `_rb_op` metadata column.
fn build_cdc_arrow_schema(columns: &[ColumnSchema]) -> Arc<Schema> {
    let mut fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            // CDC values come as text from test_decoding, so all are Utf8
            Field::new(&col.name, DataType::Utf8, true)
        })
        .collect();

    // Add the CDC operation column
    fields.push(Field::new("_rb_op", DataType::Utf8, false));

    Arc::new(Schema::new(fields))
}

/// Convert accumulated CDC changes into an Arrow IPC-encoded RecordBatch.
fn changes_to_ipc(
    changes: &[CdcChange],
    table_columns: &[ColumnSchema],
    schema: &Arc<Schema>,
) -> anyhow::Result<Vec<u8>> {
    let num_rows = changes.len();

    // Build one StringBuilder per table column + 1 for _rb_op
    let mut col_builders: Vec<StringBuilder> = table_columns
        .iter()
        .map(|_| StringBuilder::with_capacity(num_rows, num_rows * 32))
        .collect();

    let mut op_builder = StringBuilder::with_capacity(num_rows, num_rows * 8);

    for change in changes {
        // For each table column, find the matching value in the change
        for (col_idx, col) in table_columns.iter().enumerate() {
            let value = change
                .columns
                .iter()
                .find(|(name, _, _)| name == &col.name)
                .map(|(_, _, val)| val.as_str());

            match value {
                Some(v) if v == "null" => col_builders[col_idx].append_null(),
                Some(v) => col_builders[col_idx].append_value(v),
                None => col_builders[col_idx].append_null(),
            }
        }

        op_builder.append_value(change.op.as_str());
    }

    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = col_builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as Arc<dyn arrow::array::Array>)
        .collect();
    arrays.push(Arc::new(op_builder.finish()));

    let batch =
        RecordBatch::try_new(schema.clone(), arrays).context("Failed to create CDC RecordBatch")?;

    let mut buf = Vec::new();
    let mut writer =
        StreamWriter::try_new(&mut buf, batch.schema().as_ref()).context("IPC writer error")?;
    writer.write(&batch).context("IPC write error")?;
    writer.finish().context("IPC finish error")?;
    Ok(buf)
}

/// Compare two LSN strings (format: "X/YYYYYYYY").
/// Returns true if `a` is greater than `b`.
fn lsn_gt(a: &str, b: &str) -> bool {
    let parse_lsn = |s: &str| -> Option<(u64, u64)> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return None;
        }
        let high = u64::from_str_radix(parts[0], 16).ok()?;
        let low = u64::from_str_radix(parts[1], 16).ok()?;
        Some((high, low))
    };

    match (parse_lsn(a), parse_lsn(b)) {
        (Some(a), Some(b)) => a > b,
        _ => false, // unparseable LSN — don't advance
    }
}

fn emit_metrics(ctx: &StreamContext, total_records: u64, total_bytes: u64) {
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
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_insert_line() {
        let line =
            "table public.users: INSERT: id[integer]:1 name[text]:'John' active[boolean]:t";
        let change = parse_change_line(line).expect("should parse INSERT");
        assert_eq!(change.op, CdcOp::Insert);
        assert_eq!(change.table, "public.users");
        assert_eq!(change.columns.len(), 3);
        assert_eq!(change.columns[0], ("id".into(), "integer".into(), "1".into()));
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "John".into())
        );
        assert_eq!(
            change.columns[2],
            ("active".into(), "boolean".into(), "t".into())
        );
    }

    #[test]
    fn test_parse_update_line() {
        let line = "table public.users: UPDATE: id[integer]:1 name[text]:'Jane'";
        let change = parse_change_line(line).expect("should parse UPDATE");
        assert_eq!(change.op, CdcOp::Update);
        assert_eq!(change.table, "public.users");
        assert_eq!(change.columns.len(), 2);
        assert_eq!(change.columns[0], ("id".into(), "integer".into(), "1".into()));
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "Jane".into())
        );
    }

    #[test]
    fn test_parse_delete_line() {
        let line = "table public.users: DELETE: id[integer]:1";
        let change = parse_change_line(line).expect("should parse DELETE");
        assert_eq!(change.op, CdcOp::Delete);
        assert_eq!(change.table, "public.users");
        assert_eq!(change.columns.len(), 1);
        assert_eq!(change.columns[0], ("id".into(), "integer".into(), "1".into()));
    }

    #[test]
    fn test_parse_begin_commit() {
        assert!(parse_change_line("BEGIN 12345").is_none());
        assert!(parse_change_line("COMMIT 12345").is_none());
    }

    #[test]
    fn test_parse_full_transaction() {
        let lines = vec![
            "BEGIN 12345",
            "table public.users: INSERT: id[integer]:1 name[text]:'John'",
            "table public.users: UPDATE: id[integer]:1 name[text]:'Jane'",
            "table public.users: DELETE: id[integer]:2",
            "COMMIT 12345",
        ];

        let changes: Vec<CdcChange> = lines
            .into_iter()
            .filter_map(parse_change_line)
            .collect();

        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].op, CdcOp::Insert);
        assert_eq!(changes[1].op, CdcOp::Update);
        assert_eq!(changes[2].op, CdcOp::Delete);
    }

    #[test]
    fn test_parse_empty_changes() {
        let lines: Vec<&str> = vec![];
        let changes: Vec<CdcChange> = lines
            .into_iter()
            .filter_map(parse_change_line)
            .collect();
        assert!(changes.is_empty());
    }

    #[test]
    fn test_parse_quoted_value_with_spaces() {
        let line = "table public.users: INSERT: id[integer]:1 name[text]:'John Doe'";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "John Doe".into())
        );
    }

    #[test]
    fn test_parse_quoted_value_with_escaped_quote() {
        let line = "table public.users: INSERT: id[integer]:1 name[text]:'O''Brien'";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "O'Brien".into())
        );
    }

    #[test]
    fn test_parse_null_value() {
        let line = "table public.users: INSERT: id[integer]:1 email[text]:null";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("email".into(), "text".into(), "null".into())
        );
    }

    #[test]
    fn test_lsn_ordering() {
        // Basic ordering
        assert!(lsn_gt("0/16B3748", "0/16B3740"));
        assert!(!lsn_gt("0/16B3740", "0/16B3748"));
        assert!(!lsn_gt("0/16B3748", "0/16B3748"));

        // High part comparison
        assert!(lsn_gt("1/0", "0/FFFFFFFF"));
        assert!(!lsn_gt("0/FFFFFFFF", "1/0"));

        // Same high, different low
        assert!(lsn_gt("0/2", "0/1"));
    }

    #[test]
    fn test_cdc_op_as_str() {
        assert_eq!(CdcOp::Insert.as_str(), "insert");
        assert_eq!(CdcOp::Update.as_str(), "update");
        assert_eq!(CdcOp::Delete.as_str(), "delete");
    }

    #[test]
    fn test_parse_numeric_values() {
        let line = "table public.orders: INSERT: id[integer]:42 amount[numeric]:123.45 qty[bigint]:1000000";
        let change = parse_change_line(line).unwrap();
        assert_eq!(change.columns.len(), 3);
        assert_eq!(change.columns[0], ("id".into(), "integer".into(), "42".into()));
        assert_eq!(
            change.columns[1],
            ("amount".into(), "numeric".into(), "123.45".into())
        );
        assert_eq!(
            change.columns[2],
            ("qty".into(), "bigint".into(), "1000000".into())
        );
    }

    #[test]
    fn test_parse_boolean_values() {
        let line = "table public.flags: INSERT: id[integer]:1 active[boolean]:t archived[boolean]:f";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("active".into(), "boolean".into(), "t".into())
        );
        assert_eq!(
            change.columns[2],
            ("archived".into(), "boolean".into(), "f".into())
        );
    }

    #[test]
    fn test_parse_different_schemas() {
        let line = "table myschema.orders: INSERT: id[integer]:1";
        let change = parse_change_line(line).unwrap();
        assert_eq!(change.table, "myschema.orders");
    }

    #[test]
    fn test_build_cdc_arrow_schema() {
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: "Int32".to_string(),
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
            },
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
    fn test_changes_to_ipc() {
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: "Int32".to_string(),
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
            },
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
                columns: vec![
                    ("id".to_string(), "integer".to_string(), "2".to_string()),
                ],
            },
        ];

        let ipc_bytes = changes_to_ipc(&changes, &columns, &schema).unwrap();
        assert!(!ipc_bytes.is_empty());

        // Decode and verify
        let cursor = std::io::Cursor::new(&ipc_bytes);
        let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<RecordBatch> = reader.into_iter().filter_map(|b| b.ok()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 3); // id, name, _rb_op
    }

    #[test]
    fn test_parse_multibyte_utf8_value() {
        let line = "table public.users: INSERT: id[integer]:1 name[text]:'\u{00e9}l\u{00e8}ve'";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "\u{00e9}l\u{00e8}ve".into())
        );
    }

    #[test]
    fn test_parse_cjk_utf8_value() {
        let line = "table public.users: INSERT: id[integer]:1 name[text]:'\u{4f60}\u{597d}\u{4e16}\u{754c}'";
        let change = parse_change_line(line).unwrap();
        assert_eq!(
            change.columns[1],
            ("name".into(), "text".into(), "\u{4f60}\u{597d}\u{4e16}\u{754c}".into())
        );
    }

    #[test]
    fn test_lsn_gt_unparseable_returns_false() {
        // Malformed LSN should not advance
        assert!(!lsn_gt("invalid", "0/1"));
        assert!(!lsn_gt("0/1", "invalid"));
        assert!(!lsn_gt("bad", "also_bad"));
    }
}
