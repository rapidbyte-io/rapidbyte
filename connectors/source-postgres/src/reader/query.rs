//! SQL and cursor parameter helpers for source incremental reads.

use anyhow::{anyhow, bail, Context};
use chrono::{DateTime, SecondsFormat, Utc};
use pg_escape::quote_identifier;
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{ColumnSchema, CursorType, CursorValue, StreamContext, SyncMode};
use tokio_postgres::types::ToSql;

#[derive(Debug)]
pub(crate) enum CursorBindParam {
    Int64(i64),
    Text(String),
    Json(serde_json::Value),
}

impl CursorBindParam {
    pub(crate) fn as_tosql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Int64(v) => v,
            Self::Text(v) => v,
            Self::Json(v) => v,
        }
    }
}

pub(crate) struct CursorQuery {
    pub(crate) sql: String,
    pub(crate) bind: Option<CursorBindParam>,
}

pub(crate) fn effective_cursor_type(
    cursor_type: CursorType,
    cursor_column_arrow_type: &str,
) -> CursorType {
    match (cursor_type, cursor_column_arrow_type) {
        // Host state currently stores incremental cursor values as UTF-8.
        // If the actual cursor column is numeric, bind as Int64 for a valid
        // typed predicate instead of comparing against text.
        (CursorType::Utf8, "Int16" | "Int32" | "Int64") => CursorType::Int64,
        _ => cursor_type,
    }
}

pub(crate) fn build_base_query(
    ctx: &StreamContext,
    columns: &[ColumnSchema],
) -> anyhow::Result<CursorQuery> {
    let col_list = columns
        .iter()
        .map(|c| quote_identifier(&c.name))
        .collect::<Vec<_>>()
        .join(", ");

    if let (SyncMode::Incremental, Some(ci)) = (&ctx.sync_mode, &ctx.cursor_info) {
        let table_name = quote_identifier(&ctx.stream_name);
        let cursor_field = quote_identifier(&ci.cursor_field);
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
                        "Incremental read (null prior cursor): {} ORDER BY {}",
                        ctx.stream_name, cursor_field
                    ),
                );
                return Ok(CursorQuery {
                    sql: format!(
                        "SELECT {} FROM {} ORDER BY {}",
                        col_list, table_name, cursor_field
                    ),
                    bind: None,
                });
            }

            let resolved_cursor_type =
                effective_cursor_type(ci.cursor_type, cursor_column_arrow_type);
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

            let (bind, cast) =
                cursor_bind_param(&resolved_cursor_type, last_value).with_context(|| {
                    format!(
                        "Invalid incremental cursor value for stream '{}' field '{}'",
                        ctx.stream_name, ci.cursor_field
                    )
                })?;

            host_ffi::log(
                2,
                &format!(
                    "Incremental read: {} WHERE {} > $1::{}",
                    ctx.stream_name, cursor_field, cast
                ),
            );

            return Ok(CursorQuery {
                sql: format!(
                    "SELECT {} FROM {} WHERE {} > $1::{} ORDER BY {}",
                    col_list, table_name, cursor_field, cast, cursor_field
                ),
                bind: Some(bind),
            });
        }

        host_ffi::log(
            2,
            &format!(
                "Incremental read (no prior cursor): {} ORDER BY {}",
                ctx.stream_name, cursor_field
            ),
        );
        return Ok(CursorQuery {
            sql: format!(
                "SELECT {} FROM {} ORDER BY {}",
                col_list, table_name, cursor_field
            ),
            bind: None,
        });
    }

    Ok(CursorQuery {
        sql: format!(
            "SELECT {} FROM {}",
            col_list,
            quote_identifier(&ctx.stream_name)
        ),
        bind: None,
    })
}

pub(crate) fn cursor_bind_param(
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
                CursorValue::Lsn(v) => v.clone(),
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
        CursorType::Lsn => {
            // LSN cursors are used in CDC mode and are not applicable to
            // incremental queries. Treat as text if encountered here.
            let text = match value {
                CursorValue::Lsn(v) => v.clone(),
                CursorValue::Utf8(v) => v.clone(),
                _ => bail!("cursor value is incompatible with lsn cursor type"),
            };
            Ok((CursorBindParam::Text(text), "pg_lsn"))
        }
    }
}

pub(crate) fn timestamp_millis_to_rfc3339(ms: i64) -> anyhow::Result<String> {
    let dt: DateTime<Utc> = DateTime::from_timestamp_millis(ms)
        .ok_or_else(|| anyhow!("invalid timestamp millis value: {}", ms))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

pub(crate) fn timestamp_micros_to_rfc3339(us: i64) -> anyhow::Result<String> {
    let secs = us.div_euclid(1_000_000);
    let micros = us.rem_euclid(1_000_000) as u32;
    let nanos = micros * 1_000;
    let dt: DateTime<Utc> = DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| anyhow!("invalid timestamp micros value: {}", us))?;
    Ok(dt.to_rfc3339_opts(SecondsFormat::Micros, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::protocol::{
        ArrowDataType, CursorInfo, StreamContext, StreamLimits, StreamPolicies, SyncMode,
    };

    fn columns_for_cursor() -> Vec<ColumnSchema> {
        vec![
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
        ]
    }

    fn base_context() -> StreamContext {
        StreamContext {
            stream_name: "users".to_string(),
            schema: rapidbyte_sdk::protocol::SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
        }
    }

    #[test]
    fn effective_cursor_type_promotes_numeric_utf8() {
        assert_eq!(
            effective_cursor_type(CursorType::Utf8, "Int64"),
            CursorType::Int64
        );
        assert_eq!(
            effective_cursor_type(CursorType::Utf8, "Utf8"),
            CursorType::Utf8
        );
    }

    #[test]
    fn cursor_bind_param_parses_int64_from_utf8() {
        let (bind, cast) = cursor_bind_param(&CursorType::Int64, &CursorValue::Utf8("42".into()))
            .expect("bind should parse");
        match bind {
            CursorBindParam::Int64(v) => assert_eq!(v, 42),
            _ => panic!("expected int64 bind"),
        }
        assert_eq!(cast, "bigint");
    }

    #[test]
    fn cursor_bind_param_rejects_bad_int64() {
        let err = cursor_bind_param(
            &CursorType::Int64,
            &CursorValue::Utf8("not_an_int".into()),
        )
        .expect_err("invalid int64 should fail");
        assert!(format!("{:#}", err).contains("failed to parse 'not_an_int' as i64"));
    }

    #[test]
    fn build_base_query_full_refresh_quotes_identifiers() {
        let mut ctx = base_context();
        ctx.stream_name = "User".to_string();
        let columns = vec![ColumnSchema {
            name: "select".to_string(),
            data_type: ArrowDataType::Utf8,
            nullable: true,
        }];
        let query = build_base_query(&ctx, &columns).expect("query should build");
        assert_eq!(query.sql, "SELECT \"select\" FROM \"User\"");
        assert!(query.bind.is_none());
    }

    #[test]
    fn build_base_query_incremental_with_bind() {
        let mut ctx = base_context();
        ctx.sync_mode = SyncMode::Incremental;
        ctx.cursor_info = Some(CursorInfo {
            cursor_field: "id".to_string(),
            cursor_type: CursorType::Int64,
            last_value: Some(CursorValue::Int64(7)),
        });

        let query = build_base_query(&ctx, &columns_for_cursor()).expect("query should build");
        assert_eq!(
            query.sql,
            "SELECT id, name FROM users WHERE id > $1::bigint ORDER BY id"
        );
        assert!(query.bind.is_some());
    }
}
