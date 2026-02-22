//! Multi-value INSERT write path.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::Arc;

use rapidbyte_sdk::arrow::datatypes::Schema;
use rapidbyte_sdk::arrow::record_batch::RecordBatch;
use pg_escape::quote_identifier;
use tokio_postgres::types::ToSql;

use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::WriteMode;

use crate::batch::typed_col::{downcast_columns, sql_param_value, SqlParamValue};
use crate::batch::{WriteContext, INSERT_CHUNK_SIZE};
use crate::ddl::ensure_table_and_schema;

pub(crate) fn active_column_indices(
    arrow_schema: &Arc<Schema>,
    ignored_columns: &HashSet<String>,
) -> Vec<usize> {
    (0..arrow_schema.fields().len())
        .filter(|&i| !ignored_columns.contains(arrow_schema.field(i).name()))
        .collect()
}

pub(crate) fn build_upsert_clause(
    write_mode: Option<&WriteMode>,
    arrow_schema: &Arc<Schema>,
    active_cols: &[usize],
) -> Result<Option<String>, String> {
    if let Some(WriteMode::Upsert { primary_key }) = write_mode {
        let pk_cols = primary_key
            .iter()
            .map(|k| quote_identifier(k))
            .collect::<Vec<_>>()
            .join(", ");

        let update_cols: Vec<String> = active_cols
            .iter()
            .map(|&i| arrow_schema.field(i).name())
            .filter(|name| !primary_key.contains(name))
            .map(|name| {
                format!(
                    "{} = EXCLUDED.{}",
                    quote_identifier(name),
                    quote_identifier(name)
                )
            })
            .collect();

        if update_cols.is_empty() {
            Ok(Some(format!(" ON CONFLICT ({}) DO NOTHING", pk_cols)))
        } else {
            Ok(Some(format!(
                " ON CONFLICT ({}) DO UPDATE SET {}",
                pk_cols,
                update_cols.join(", ")
            )))
        }
    } else {
        Ok(None)
    }
}

/// Write via multi-value INSERT and return rows written.
pub(crate) async fn insert_batch(
    ctx: &mut WriteContext<'_>,
    arrow_schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<u64, String> {
    if batches.is_empty() {
        return Ok(0);
    }

    let qualified_table = format!(
        "{}.{}",
        quote_identifier(ctx.target_schema),
        quote_identifier(ctx.stream_name)
    );

    ensure_table_and_schema(
        ctx.client,
        ctx.target_schema,
        ctx.stream_name,
        ctx.created_tables,
        ctx.write_mode,
        ctx.schema_policy,
        arrow_schema,
        ctx.ignored_columns,
        ctx.type_null_columns,
    )
    .await
    .map_err(|e| format!("Failed to ensure table/schema for {}: {e}", ctx.stream_name))?;

    let active_cols = active_column_indices(arrow_schema, ctx.ignored_columns);
    if active_cols.is_empty() {
        host_ffi::log(1, "dest-postgres: all columns ignored, skipping batch");
        return Ok(0);
    }
    let upsert_clause = build_upsert_clause(ctx.write_mode, arrow_schema, &active_cols)?;

    let col_list = active_cols
        .iter()
        .map(|&i| quote_identifier(arrow_schema.field(i).name()))
        .collect::<Vec<_>>()
        .join(", ");

    let mut total_rows: u64 = 0;

    for batch in batches {
        let num_rows = batch.num_rows();

        let typed_cols = downcast_columns(batch, &active_cols)?;
        let type_null_flags: Vec<bool> = active_cols
            .iter()
            .map(|&i| ctx.type_null_columns.contains(arrow_schema.field(i).name()))
            .collect();

        for chunk_start in (0..num_rows).step_by(INSERT_CHUNK_SIZE) {
            let chunk_end = (chunk_start + INSERT_CHUNK_SIZE).min(num_rows);
            let chunk_size = chunk_end - chunk_start;

            let header = format!("INSERT INTO {} ({}) VALUES ", qualified_table, col_list);
            let mut sql = String::with_capacity(header.len() + chunk_size * typed_cols.len() * 6);
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

            if let Some(clause) = upsert_clause.as_ref() {
                sql.push_str(clause);
            }

            let param_refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(SqlParamValue::as_tosql).collect();

            ctx.client
                .execute(&sql, &param_refs)
                .await
                .map_err(|e| {
                    format!(
                        "Multi-value INSERT failed for {}, rows {}-{}: {e}",
                        ctx.stream_name, chunk_start, chunk_end
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

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::protocol::ArrowDataType;

    fn schema_with_id_name() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            rapidbyte_sdk::arrow::datatypes::Field::new("id", rapidbyte_sdk::arrow::datatypes::DataType::Int64, false),
            rapidbyte_sdk::arrow::datatypes::Field::new("name", rapidbyte_sdk::arrow::datatypes::DataType::Utf8, true),
        ]))
    }

    #[test]
    fn active_column_indices_respects_ignored_set() {
        let schema = schema_with_id_name();
        let ignored = HashSet::from(["name".to_string()]);
        assert_eq!(active_column_indices(&schema, &ignored), vec![0]);
    }

    #[test]
    fn build_upsert_clause_quotes_identifiers() {
        let schema = schema_with_id_name();
        let mode = WriteMode::Upsert {
            primary_key: vec!["id".to_string()],
        };
        let clause = build_upsert_clause(Some(&mode), &schema, &[0, 1])
            .expect("clause")
            .expect("upsert clause");
        assert_eq!(
            clause,
            " ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
        );
    }

    #[test]
    fn arrow_data_type_marker_is_linked() {
        let marker = ArrowDataType::Int64;
        assert_eq!(marker.as_str(), "Int64");
    }
}
