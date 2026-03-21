//! Change Data Capture via `PostgreSQL` logical replication slots.
//!
//! Reads WAL changes through `pg_logical_slot_get_binary_changes` with the
//! `pgoutput` plugin, decodes binary messages, emits typed Arrow IPC batches,
//! and checkpoints by LSN.

pub(crate) mod encode;
pub(crate) mod pgoutput;

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Instant;

use tokio_postgres::Client;

use crate::config::Config;
use crate::diagnostics::{
    cdc_checkpoint_failure_diagnostic, cdc_publication_mismatch_diagnostic,
    cdc_resume_ambiguity_diagnostic, cdc_slot_mismatch_diagnostic,
};
use crate::metrics::{emit_batch_counters, emit_source_timings, EmitState, BATCH_SIZE};
use rapidbyte_sdk::prelude::*;

use encode::{encode_cdc_batch, CdcRow, RelationInfo};
use pgoutput::{CdcOp, PgOutputMessage, TupleData};

/// Maximum WAL changes consumed per CDC invocation to avoid unbounded memory use.
/// Type is i32 because `pg_logical_slot_get_binary_changes()` expects int4.
const CDC_MAX_CHANGES_DEFAULT: i32 = 10_000;

/// Default replication slot prefix. Full slot names are `rapidbyte_{stream_name}`.
const SLOT_PREFIX: &str = "rapidbyte_";

/// Default publication prefix. Full publication names are `rapidbyte_{stream_name}`.
const PUB_PREFIX: &str = "rapidbyte_";

fn validate_pg_identifier(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if value.len() > 63 {
        return Err(format!(
            "{field} '{value}' exceeds PostgreSQL 63-byte limit"
        ));
    }
    Ok(())
}

fn validate_replication_slot_name(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("replication slot must not be empty".to_string());
    }
    if value.len() > 63 {
        return Err(format!(
            "replication slot '{value}' exceeds PostgreSQL 63-byte limit"
        ));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(format!(
            "replication slot '{value}' may only contain lowercase letters, digits, and underscores"
        ));
    }
    Ok(())
}

fn cdc_stream_token(stream_name: &str) -> String {
    let mut parts = stream_name.splitn(2, '.');
    let first = parts.next().unwrap_or(stream_name);
    let second = parts.next();

    let token = match second {
        Some(table_name) if first == "public" => table_name,
        Some(table_name) => {
            return format!(
                "{}_{}",
                sanitize_identifier_fragment(first),
                sanitize_identifier_fragment(table_name)
            );
        }
        None => first,
    };

    sanitize_identifier_fragment(token)
}

fn sanitize_identifier_fragment(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn resolve_cdc_name(
    prefix: &str,
    configured: Option<&str>,
    stream_name: &str,
    field: &str,
) -> Result<String, String> {
    let name = configured
        .map(std::borrow::ToOwned::to_owned)
        .unwrap_or_else(|| format!("{prefix}{}", cdc_stream_token(stream_name)));
    if field == "replication slot" {
        validate_replication_slot_name(&name)?;
    } else {
        validate_pg_identifier(&name, field)?;
    }
    Ok(name)
}

fn resolve_cdc_names(config: &Config, stream_name: &str) -> Result<(String, String), String> {
    let slot_name = resolve_cdc_name(
        SLOT_PREFIX,
        config.replication_slot.as_deref(),
        stream_name,
        "replication slot",
    )?;
    let publication_name = resolve_cdc_name(
        PUB_PREFIX,
        config.publication.as_deref(),
        stream_name,
        "publication",
    )?;
    Ok((slot_name, publication_name))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CdcSlotInfo {
    database: String,
    slot_type: String,
    active: bool,
}

#[allow(async_fn_in_trait)]
trait CdcPreflightInspector {
    async fn replication_slot_info(&self, slot_name: &str) -> Result<Option<CdcSlotInfo>, String>;

    async fn publication_has_stream(
        &self,
        publication_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<bool, String>;
}

impl CdcPreflightInspector for Client {
    async fn replication_slot_info(&self, slot_name: &str) -> Result<Option<CdcSlotInfo>, String> {
        let row = self
            .query_opt(
                "SELECT database, slot_type, active FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await
            .map_err(|e| format!("error querying replication slot metadata for '{slot_name}': {e}"))?;

        Ok(row.map(|row| CdcSlotInfo {
            database: row.get::<usize, String>(0),
            slot_type: row.get::<usize, String>(1),
            active: row.get::<usize, bool>(2),
        }))
    }

    async fn publication_has_stream(
        &self,
        publication_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<bool, String> {
        self
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3)",
                &[&publication_name, &schema_name, &table_name],
            )
            .await
            .map(|row| row.get::<usize, bool>(0))
            .map_err(|e| format!("error querying publication membership for '{publication_name}': {e}"))
    }
}

fn split_stream_name(stream_name: &str, default_schema: Option<&str>) -> (String, String) {
    let mut parts = stream_name.rsplitn(2, '.');
    let table = parts.next().unwrap_or(stream_name);
    let schema = parts
        .next()
        .unwrap_or_else(|| default_schema.unwrap_or("public"));
    (schema.to_string(), table.to_string())
}

async fn preflight_cdc_checks<I: CdcPreflightInspector>(
    inspector: &I,
    config: &Config,
    stream: &StreamContext,
    slot_name: &str,
    publication_name: &str,
) -> Result<(), String> {
    if let Some(slot_info) = inspector.replication_slot_info(slot_name).await? {
        if slot_info.database != config.database {
            return Err(
                cdc_slot_mismatch_diagnostic(
                    &stream.stream_name,
                    slot_name,
                    &format!(
                        "the slot is attached to database '{}' but this CDC run targets database '{}'",
                        slot_info.database, config.database
                    ),
                )
                .render(),
            );
        }
        if slot_info.slot_type != "logical" {
            return Err(
                cdc_slot_mismatch_diagnostic(
                    &stream.stream_name,
                    slot_name,
                    &format!(
                        "the slot is type '{}' instead of the required 'logical' slot type",
                        slot_info.slot_type
                    ),
                )
                .render(),
            );
        }
        if slot_info.active {
            return Err(
                cdc_slot_mismatch_diagnostic(
                    &stream.stream_name,
                    slot_name,
                    "the slot is already active for another replication session",
                )
                .render(),
            );
        }
    }

    let (schema_name, table_name) =
        split_stream_name(stream.source_stream_or_stream_name(), config.schema.as_deref());
    let publication_has_stream = inspector
        .publication_has_stream(publication_name, &schema_name, &table_name)
        .await?;
    if !publication_has_stream {
        return Err(
            cdc_publication_mismatch_diagnostic(
                &stream.stream_name,
                publication_name,
                &format!(
                    "the publication does not include the target table '{}.{}'",
                    schema_name, table_name
                ),
            )
            .render(),
        );
    }

    Ok(())
}

/// Read max changes per CDC query from env, with sane defaults and validation.
fn cdc_max_changes() -> i32 {
    static CACHED: OnceLock<i32> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("RAPIDBYTE_CDC_MAX_CHANGES")
            .ok()
            .and_then(|raw| raw.trim().parse::<i32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(CDC_MAX_CHANGES_DEFAULT)
    })
}

fn emit_batch(
    rows: &mut Vec<CdcRow>,
    relation: &RelationInfo,
    ctx: &Context,
    state: &mut EmitState,
) -> Result<(), String> {
    let encode_start = Instant::now();
    let batch = encode_cdc_batch(rows, relation)?;
    // Safety: encode timing in nanos will not exceed u64::MAX for any realistic duration.
    #[allow(clippy::cast_possible_truncation)]
    {
        state.arrow_encode_nanos += encode_start.elapsed().as_nanos() as u64;
    }

    state.total_records += rows.len() as u64;
    state.total_bytes += batch.get_array_memory_size() as u64;
    state.batches_emitted += 1;

    ctx.emit_batch(&batch)
        .map_err(|e| format!("emit_batch failed: {}", e.message))?;
    emit_batch_counters(ctx, state);

    rows.clear();
    Ok(())
}

/// Read CDC changes from a logical replication slot using `pg_logical_slot_get_binary_changes()`.
#[allow(clippy::too_many_lines)]
pub async fn read_cdc_changes(
    client: &Client,
    ctx: &Context,
    stream: &StreamContext,
    resume: &CdcResumeToken,
    config: &Config,
    connect_secs: f64,
) -> Result<ReadSummary, String> {
    ctx.log(
        LogLevel::Info,
        &format!("CDC reading stream: {}", stream.stream_name),
    );

    let query_start = Instant::now();

    // 1. Derive and validate CDC identifiers.
    let (slot_name, publication_name) = resolve_cdc_names(config, &stream.stream_name)?;

    // 1b. Surface ambiguous resume states before touching the destructive CDC path.
    if let Some(resume_diagnostic) = cdc_resume_ambiguity_diagnostic(&stream.stream_name, resume) {
        match resume_diagnostic.level {
            crate::diagnostics::DiagnosticLevel::Warning => {
                ctx.log(LogLevel::Warn, &resume_diagnostic.message);
            }
            crate::diagnostics::DiagnosticLevel::Error => {
                return Err(resume_diagnostic.render());
            }
        }
    }

    // 2. Preflight server state that we can verify before touching WAL.
    preflight_cdc_checks(client, config, stream, &slot_name, &publication_name).await?;

    // 3. Ensure replication slot exists (idempotent)
    ensure_replication_slot(client, ctx, &slot_name).await?;

    // 4. Read binary changes from the slot (this CONSUMES them)
    // Uses pgoutput plugin with proto_version 1 and the configured publication.
    let changes_query = "SELECT lsn::text, data \
                         FROM pg_logical_slot_get_binary_changes(\
                             $1, NULL, $2, \
                             'proto_version', '1', \
                             'publication_names', $3\
                         )";
    let max_changes = cdc_max_changes();
    let change_rows = client
        .query(
            changes_query,
            &[&slot_name, &max_changes, &publication_name],
        )
        .await
        .map_err(|e| {
            format!(
                "pg_logical_slot_get_binary_changes failed for stream '{}' with slot '{}' and publication '{}'. Ensure the publication exists and includes the target table before retrying: {e}",
                stream.stream_name, slot_name, publication_name
            )
        })?;

    let query_secs = query_start.elapsed().as_secs_f64();

    let fetch_start = Instant::now();
    let mut state = EmitState {
        total_records: 0,
        total_bytes: 0,
        batches_emitted: 0,
        arrow_encode_nanos: 0,
        last_emitted_records: 0,
        last_emitted_bytes: 0,
    };
    let mut max_lsn: Option<u64> = None;

    // 5. Decode messages, filter to our table, accumulate into batches
    let mut relations: HashMap<u32, RelationInfo> = HashMap::new();
    let mut target_oid: Option<u32> = None;
    let mut accumulated_rows: Vec<CdcRow> = Vec::new();

    for row in &change_rows {
        let lsn_str: String = row.get(0);
        let data: &[u8] = row.get(1);

        // Track max LSN using numeric comparison
        if let Some(lsn) = pgoutput::parse_lsn(&lsn_str) {
            if max_lsn.is_none_or(|current| lsn > current) {
                max_lsn = Some(lsn);
            }
        }

        // Decode binary pgoutput message
        let msg = match pgoutput::decode(data) {
            Ok(m) => m,
            Err(e) => {
                ctx.log(
                    LogLevel::Warn,
                    &format!("pgoutput decode error (skipping): {e}"),
                );
                continue;
            }
        };

        match msg {
            PgOutputMessage::Relation {
                oid,
                namespace,
                name,
                columns,
                ..
            } => {
                // Track whether this relation matches the target stream.
                // Compare both unqualified name and schema-qualified name to
                // handle publications that span multiple schemas.
                if name == stream.stream_name || format!("{namespace}.{name}") == stream.stream_name
                {
                    target_oid = Some(oid);
                }
                let info = RelationInfo::new(oid, namespace, name, columns);
                relations.insert(oid, info);
            }
            PgOutputMessage::Insert {
                relation_oid,
                new_tuple,
            } if target_oid == Some(relation_oid) => {
                accumulated_rows.push(CdcRow {
                    op: CdcOp::Insert,
                    tuple: new_tuple,
                });
            }
            PgOutputMessage::Update {
                relation_oid,
                new_tuple,
                ..
            } if target_oid == Some(relation_oid) => {
                accumulated_rows.push(CdcRow {
                    op: CdcOp::Update,
                    tuple: new_tuple,
                });
            }
            PgOutputMessage::Delete {
                relation_oid,
                key_tuple,
                old_tuple,
            } if target_oid == Some(relation_oid) => {
                let tuple = old_tuple
                    .or(key_tuple)
                    .unwrap_or_else(|| TupleData { columns: vec![] });
                accumulated_rows.push(CdcRow {
                    op: CdcOp::Delete,
                    tuple,
                });
            }
            // Begin, Commit, Truncate, Origin, Type, Message, and non-matching DML — skip
            _ => {}
        }

        // Flush batch if accumulated enough
        if accumulated_rows.len() >= BATCH_SIZE {
            let rel = target_oid
                .and_then(|oid| relations.get(&oid))
                .ok_or_else(|| {
                    "CDC batch ready but no Relation message received for target stream".to_string()
                })?;
            emit_batch(&mut accumulated_rows, rel, ctx, &mut state)?;
        }
    }

    // Flush remaining rows
    if !accumulated_rows.is_empty() {
        let rel = target_oid
            .and_then(|oid| relations.get(&oid))
            .ok_or_else(|| {
                "CDC rows accumulated but no Relation message received for target stream"
                    .to_string()
            })?;
        emit_batch(&mut accumulated_rows, rel, ctx, &mut state)?;
    }

    let fetch_secs = fetch_start.elapsed().as_secs_f64();

    // 6. Emit checkpoint with max LSN
    let checkpoint_count = if let Some(lsn) = max_lsn {
        let lsn_string = pgoutput::lsn_to_string(lsn);
        let cp = Checkpoint {
            id: 1,
            kind: CheckpointKind::Source,
            stream: stream.stream_name.clone(),
            cursor_field: Some("lsn".to_string()),
            cursor_value: Some(CursorValue::Lsn {
                value: lsn_string.clone(),
            }),
            records_processed: state.total_records,
            bytes_processed: state.total_bytes,
        };
        // CDC uses get_binary_changes (destructive) so checkpoint MUST succeed to avoid data loss.
        ctx.checkpoint(&cp).map_err(|e| {
            cdc_checkpoint_failure_diagnostic(&stream.stream_name, &lsn_string, &e.message).render()
        })?;
        ctx.log(
            LogLevel::Info,
            &format!(
                "CDC checkpoint: stream={} lsn={}",
                stream.stream_name, lsn_string
            ),
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

    let perf = ReadPerf {
        connect_secs,
        query_secs,
        fetch_secs,
        arrow_encode_secs,
    };
    emit_source_timings(ctx, &perf);

    Ok(ReadSummary {
        records_read: state.total_records,
        bytes_read: state.total_bytes,
        batches_emitted: state.batches_emitted,
        checkpoint_count,
        records_skipped: 0,
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
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot_name],
        )
        .await;

    match result {
        Ok(_) => {
            ctx.log(
                LogLevel::Info,
                &format!("Created replication slot '{slot_name}' with pgoutput"),
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

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::encode::CdcRow;
    use super::encode::RelationInfo;
    use super::pgoutput::{CdcOp, ColumnDef, ColumnValue, TupleData};
    use super::{CdcPreflightInspector, CdcSlotInfo};
    use rapidbyte_sdk::arrow::datatypes::DataType;
    use rapidbyte_sdk::cursor::CursorType;
    use rapidbyte_sdk::stream::CdcResumeToken;
    use rapidbyte_sdk::wire::SyncMode;

    use crate::diagnostics::{
        cdc_checkpoint_failure_diagnostic, cdc_publication_mismatch_diagnostic,
        cdc_resume_ambiguity_diagnostic, cdc_slot_mismatch_diagnostic, DiagnosticLevel,
    };
    use crate::config::Config;

    struct FakeInspector {
        slot_info: Option<CdcSlotInfo>,
        publication_has_stream: bool,
    }

    impl CdcPreflightInspector for FakeInspector {
        async fn replication_slot_info(&self, _slot_name: &str) -> Result<Option<CdcSlotInfo>, String> {
            Ok(self.slot_info.clone())
        }

        async fn publication_has_stream(
            &self,
            _publication_name: &str,
            _schema_name: &str,
            _table_name: &str,
        ) -> Result<bool, String> {
            Ok(self.publication_has_stream)
        }
    }

    fn base_config() -> Config {
        Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "test_db".to_string(),
            schema: Some("public".to_string()),
            replication_slot: Some("slot_a".to_string()),
            publication: Some("pub_a".to_string()),
        }
    }

    fn cdc_stream_context() -> rapidbyte_sdk::stream::StreamContext {
        let mut stream = rapidbyte_sdk::stream::StreamContext::test_default("public.orders");
        stream.sync_mode = SyncMode::Cdc;
        stream
    }

    #[test]
    fn relation_info_schema_includes_rb_op() {
        let rel = RelationInfo::new(
            16385,
            "public".to_string(),
            "users".to_string(),
            vec![
                ColumnDef {
                    flags: 1,
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnDef {
                    flags: 0,
                    name: "name".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        );

        let schema = &rel.arrow_schema;
        // 2 data columns + _rb_op
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
        assert_eq!(schema.field(2).name(), "_rb_op");
        assert_eq!(*schema.field(2).data_type(), DataType::Utf8);
        assert!(!schema.field(2).is_nullable());
    }

    #[test]
    fn cdc_row_with_insert_op() {
        let row = CdcRow {
            op: CdcOp::Insert,
            tuple: TupleData {
                columns: vec![
                    ColumnValue::Text("42".to_string()),
                    ColumnValue::Text("Alice".to_string()),
                ],
            },
        };
        assert_eq!(row.op.as_str(), "insert");
        assert_eq!(row.tuple.columns.len(), 2);
    }

    #[test]
    fn delete_row_with_empty_tuple() {
        // Simulates a DELETE where neither old_tuple nor key_tuple is available.
        let tuple = TupleData { columns: vec![] };
        let row = CdcRow {
            op: CdcOp::Delete,
            tuple,
        };
        assert_eq!(row.op.as_str(), "delete");
        assert!(row.tuple.columns.is_empty());
    }

    #[test]
    fn publication_name_default() {
        let prefix = super::PUB_PREFIX;
        let pub_name = format!("{prefix}{}", "users");
        assert_eq!(pub_name, "rapidbyte_users");
    }

    #[test]
    fn slot_name_default() {
        let prefix = super::SLOT_PREFIX;
        let slot_name = format!("{prefix}{}", "orders");
        assert_eq!(slot_name, "rapidbyte_orders");
    }

    #[test]
    fn resolve_cdc_names_uses_sanitized_schema_qualified_default() {
        let config = crate::config::Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "test_db".to_string(),
            schema: Some("analytics".to_string()),
            replication_slot: None,
            publication: None,
        };

        let (slot_name, publication_name) = super::resolve_cdc_names(&config, "analytics.users")
            .expect("schema-qualified names should derive valid defaults");
        assert_eq!(slot_name, "rapidbyte_analytics_users");
        assert_eq!(publication_name, "rapidbyte_analytics_users");
    }

    #[test]
    fn resolve_cdc_names_rejects_overlong_derived_defaults() {
        let config = crate::config::Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "test_db".to_string(),
            schema: None,
            replication_slot: None,
            publication: None,
        };
        let stream_name = "a".repeat(64);

        let err = super::resolve_cdc_names(&config, &stream_name).unwrap_err();
        assert!(err.contains("replication slot"));
        assert!(err.contains("63-byte limit"));
    }

    #[test]
    fn resolve_cdc_names_rejects_overlong_derived_publication_name() {
        let config = crate::config::Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "test_db".to_string(),
            schema: None,
            replication_slot: Some("slot_a".to_string()),
            publication: None,
        };
        let stream_name = "b".repeat(64);

        let err = super::resolve_cdc_names(&config, &stream_name).unwrap_err();
        assert!(err.contains("publication"));
        assert!(err.contains("63-byte limit"));
    }

    #[test]
    fn resolve_cdc_names_sanitizes_invalid_derived_slot_characters() {
        let config = crate::config::Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "test_db".to_string(),
            schema: None,
            replication_slot: None,
            publication: Some("pub_a".to_string()),
        };
        let stream_name = "stream-name";

        let (slot_name, publication_name) =
            super::resolve_cdc_names(&config, stream_name).unwrap();
        assert_eq!(slot_name, "rapidbyte_stream_name");
        assert_eq!(publication_name, "pub_a");
    }

    #[test]
    fn cdc_slot_mismatch_diagnostic_is_operator_facing() {
        let diagnostic = cdc_slot_mismatch_diagnostic(
            "public.orders",
            "rapidbyte_orders",
            "the slot is attached to database 'other_db' but this CDC run targets database 'test_db'",
        );

        assert_eq!(diagnostic.level, DiagnosticLevel::Error);
        assert!(diagnostic.message.contains("public.orders"));
        assert!(diagnostic.message.contains("rapidbyte_orders"));
        assert!(diagnostic.message.contains("other_db"));
        assert!(diagnostic.fix_hint.as_deref().unwrap_or("").contains("drop"));
    }

    #[test]
    fn cdc_publication_mismatch_diagnostic_is_operator_facing() {
        let diagnostic = cdc_publication_mismatch_diagnostic(
            "public.orders",
            "rapidbyte_orders",
            "the publication does not include the target table 'public.orders'",
        );

        assert_eq!(diagnostic.level, DiagnosticLevel::Error);
        assert!(diagnostic.message.contains("public.orders"));
        assert!(diagnostic.message.contains("rapidbyte_orders"));
        assert!(diagnostic.message.contains("target table"));
        assert!(diagnostic.fix_hint.as_deref().unwrap_or("").contains("CREATE PUBLICATION"));
    }

    #[test]
    fn cdc_checkpoint_failure_diagnostic_mentions_destructive_read() {
        let diagnostic =
            cdc_checkpoint_failure_diagnostic("public.orders", "0/16B6C50", "checkpoint lost");

        assert_eq!(diagnostic.level, DiagnosticLevel::Error);
        assert!(diagnostic.message.contains("WAL already consumed"));
        assert!(diagnostic.message.contains("0/16B6C50"));
        assert!(diagnostic.message.contains("checkpoint lost"));
        assert!(diagnostic.fix_hint.as_deref().unwrap_or("").contains("backfill"));
    }

    #[test]
    fn cdc_resume_ambiguity_diagnostic_warns_on_missing_resume_and_errors_on_non_lsn() {
        let warning = cdc_resume_ambiguity_diagnostic(
            "public.orders",
            &CdcResumeToken {
                value: None,
                cursor_type: CursorType::Lsn,
            },
        )
        .expect("missing resume value should be ambiguous");
        assert_eq!(warning.level, DiagnosticLevel::Warning);
        assert!(warning.message.contains("resume token"));
        assert!(warning.message.contains("public.orders"));

        let error = cdc_resume_ambiguity_diagnostic(
            "public.orders",
            &CdcResumeToken {
                value: Some("abc".to_string()),
                cursor_type: CursorType::Utf8,
            },
        )
        .expect("non-LSN resume token should be rejected");
        assert_eq!(error.level, DiagnosticLevel::Error);
        assert!(error.message.contains("LSN"));
        assert!(error.message.contains("Utf8"));
    }

    #[tokio::test]
    async fn preflight_cdc_checks_rejects_slot_database_mismatch() {
        let err = super::preflight_cdc_checks(
            &FakeInspector {
                slot_info: Some(CdcSlotInfo {
                    database: "other_db".to_string(),
                    slot_type: "logical".to_string(),
                    active: false,
                }),
                publication_has_stream: true,
            },
            &base_config(),
            &cdc_stream_context(),
            "slot_a",
            "pub_a",
        )
        .await
        .expect_err("slot database mismatch should fail preflight");

        assert!(err.contains("CDC slot mismatch"));
        assert!(err.contains("other_db"));
        assert!(err.contains("test_db"));
    }

    #[tokio::test]
    async fn preflight_cdc_checks_rejects_missing_publication_membership() {
        let err = super::preflight_cdc_checks(
            &FakeInspector {
                slot_info: Some(CdcSlotInfo {
                    database: "test_db".to_string(),
                    slot_type: "logical".to_string(),
                    active: false,
                }),
                publication_has_stream: false,
            },
            &base_config(),
            &cdc_stream_context(),
            "slot_a",
            "pub_a",
        )
        .await
        .expect_err("publication membership mismatch should fail preflight");

        assert!(err.contains("CDC publication mismatch"));
        assert!(err.contains("public.orders"));
        assert!(err.contains("publication does not include the target table"));
    }

    #[tokio::test]
    async fn preflight_cdc_checks_allows_compatible_slot_and_publication() {
        super::preflight_cdc_checks(
            &FakeInspector {
                slot_info: Some(CdcSlotInfo {
                    database: "test_db".to_string(),
                    slot_type: "logical".to_string(),
                    active: false,
                }),
                publication_has_stream: true,
            },
            &base_config(),
            &cdc_stream_context(),
            "slot_a",
            "pub_a",
        )
        .await
        .expect("compatible slot and publication should pass preflight");
    }
}
