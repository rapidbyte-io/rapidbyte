//! Multi-value INSERT write path.
//!
//! Writes Arrow RecordBatch data to PostgreSQL via batched multi-value INSERT
//! statements. Supports upsert via ON CONFLICT clause.

use std::fmt::Write as _;
use std::sync::Arc;

use pg_escape::quote_identifier;
use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use tokio_postgres::types::ToSql;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;

use crate::decode::{downcast_columns, sql_param_value, SqlParamValue};

/// Maximum rows per multi-value INSERT statement (PG parameter limit).
const CHUNK_SIZE: usize = 1000;

/// Write batches via multi-value INSERT. Returns rows written.
///
/// Parameters are all pre-computed by the session layer:
/// - `qualified_table`: schema-qualified table name
/// - `active_cols`: indices of non-ignored columns
/// - `upsert_clause`: optional ON CONFLICT clause
/// - `type_null_flags`: per-column flag forcing NULL for type-incompatible columns
pub(crate) async fn write(
    ctx: &Context,
    client: &Client,
    qualified_table: &str,
    active_cols: &[usize],
    arrow_schema: &Arc<Schema>,
    batches: &[RecordBatch],
    upsert_clause: Option<&str>,
    type_null_flags: &[bool],
) -> Result<u64, String> {
    if batches.is_empty() || active_cols.is_empty() {
        return Ok(0);
    }

    let col_list = active_cols
        .iter()
        .map(|&i| quote_identifier(arrow_schema.field(i).name()))
        .collect::<Vec<_>>()
        .join(", ");

    let mut total_rows: u64 = 0;

    for batch in batches {
        let num_rows = batch.num_rows();
        let typed_cols = downcast_columns(batch, active_cols)?;

        for chunk_start in (0..num_rows).step_by(CHUNK_SIZE) {
            let chunk_end = (chunk_start + CHUNK_SIZE).min(num_rows);
            let chunk_size = chunk_end - chunk_start;

            let header = format!("INSERT INTO {} ({}) VALUES ", qualified_table, col_list);
            let mut sql =
                String::with_capacity(header.len() + chunk_size * typed_cols.len() * 6);
            sql.push_str(&header);

            let mut params: Vec<SqlParamValue<'_>> =
                Vec::with_capacity(chunk_size.saturating_mul(typed_cols.len()));

            for row_idx in chunk_start..chunk_end {
                if row_idx > chunk_start {
                    sql.push_str(", ");
                }
                sql.push('(');
                for (pos, typed_col) in typed_cols.iter().enumerate() {
                    if pos > 0 {
                        sql.push_str(", ");
                    }
                    let value = if type_null_flags[pos] {
                        SqlParamValue::Text(None)
                    } else {
                        sql_param_value(typed_col, row_idx)
                    };
                    params.push(value);
                    let _ = write!(sql, "${}", params.len());
                }
                sql.push(')');
            }

            if let Some(clause) = upsert_clause {
                sql.push_str(clause);
            }

            let param_refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(SqlParamValue::as_tosql).collect();

            client.execute(&sql, &param_refs).await.map_err(|e| {
                format!(
                    "INSERT failed for {}, rows {}-{}: {e}",
                    qualified_table, chunk_start, chunk_end
                )
            })?;

            total_rows += chunk_size as u64;
        }
    }

    ctx.log(
        LogLevel::Info,
        &format!("dest-postgres: wrote {} rows to {}", total_rows, qualified_table),
    );

    Ok(total_rows)
}
