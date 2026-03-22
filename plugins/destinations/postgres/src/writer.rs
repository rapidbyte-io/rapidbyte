//! Stream write orchestration for destination `PostgreSQL` plugin.

use std::collections::HashSet;
use std::time::Instant;

use rapidbyte_sdk::prelude::*;

use crate::apply::prepare_stream_contract;
use crate::contract::{mark_contract_prepared, prepare_stream_once, stream_schema_signature, CheckpointConfig};
use crate::ddl::{read_contract_handoff, ContractHandoff};
use crate::metrics::emit_dest_timings;
use crate::session::{clamp_copy_flush_bytes, CommitError, WriteSession};

fn commit_error_to_plugin_error(e: CommitError) -> PluginError {
    PluginError::transient_db("COMMIT_FAILED", e.message).with_commit_state(e.commit_state)
}

fn resolve_copy_flush_bytes(
    stream_override: Option<u64>,
    configured: Option<usize>,
) -> Option<usize> {
    if let Some(bytes) = stream_override {
        if bytes == 0 {
            return configured.map(clamp_copy_flush_bytes);
        }
        let override_bytes = usize::try_from(bytes).unwrap_or(usize::MAX);
        return Some(clamp_copy_flush_bytes(override_bytes));
    }

    configured.map(clamp_copy_flush_bytes)
}

fn staging_table_name(stream_name: &str) -> String {
    format!("{stream_name}__rb_staging")
}

fn replace_staging_qualified_table(target_schema: &str, stream_name: &str) -> String {
    crate::decode::qualified_name(target_schema, &staging_table_name(stream_name))
}

async fn live_destination_state_is_fresh(
    client: &tokio_postgres::Client,
    target_schema: &str,
    table_name: &str,
    stream_schema: &rapidbyte_sdk::schema::StreamSchema,
    handoff: &ContractHandoff,
) -> Result<bool, PluginError> {
    let Some(arrow_schema) = crate::contract::preflight_schema_from_stream_schema(stream_schema)
        .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
    else {
        return Ok(true);
    };

    let drift = crate::ddl::detect_schema_drift(client, target_schema, table_name, &arrow_schema)
        .await
        .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;

    let Some(drift) = drift else {
        return Ok(handoff.ignored_columns.is_empty() && handoff.type_null_columns.is_empty());
    };

    let drift_ignored_columns: HashSet<&str> =
        drift.new_columns.iter().map(|(col_name, _)| col_name.as_str()).collect();
    let drift_type_null_columns: HashSet<&str> = drift
        .type_changes
        .iter()
        .map(|(col_name, _, _)| col_name.as_str())
        .collect();

    let handoff_ignored_columns: HashSet<&str> =
        handoff.ignored_columns.iter().map(|col| col.as_str()).collect();
    let handoff_type_null_columns: HashSet<&str> = handoff
        .type_null_columns
        .iter()
        .map(|col| col.as_str())
        .collect();

    Ok(drift.removed_columns.is_empty()
        && drift.nullability_changes.is_empty()
        && drift_ignored_columns == handoff_ignored_columns
        && drift_type_null_columns == handoff_type_null_columns)
}

async fn replace_staging_reuse_is_safe(
    client: &tokio_postgres::Client,
    target_schema: &str,
    stream_name: &str,
    stream_schema: &rapidbyte_sdk::schema::StreamSchema,
    handoff: &ContractHandoff,
) -> Result<bool, PluginError> {
    if !live_destination_state_is_fresh(
        client,
        target_schema,
        &staging_table_name(stream_name),
        stream_schema,
        handoff,
    )
    .await?
    {
        return Ok(false);
    }

    let staging_table = replace_staging_qualified_table(target_schema, stream_name);
    let row = match client
        .query_opt(&format!("SELECT 1 FROM {staging_table} LIMIT 1"), &[])
        .await
    {
        Ok(row) => row,
        Err(_) => return Ok(false),
    };
    Ok(row.is_none())
}

fn existing_replace_contract(mut setup: crate::contract::WriteContract) -> crate::contract::WriteContract {
    setup.effective_stream = staging_table_name(&setup.stream_name);
    setup.qualified_table =
        crate::decode::qualified_name(&setup.target_schema, &setup.effective_stream);
    mark_contract_prepared(setup)
}

fn contract_from_handoff(
    mut setup: crate::contract::WriteContract,
    handoff: &ContractHandoff,
) -> crate::contract::WriteContract {
    setup.ignored_columns = handoff.ignored_columns.iter().cloned().collect();
    setup.type_null_columns = handoff.type_null_columns.iter().cloned().collect();
    mark_contract_prepared(setup)
}

pub(crate) fn reuse_contract_from_handoff(
    setup: &crate::contract::WriteContract,
    current_signature: Option<&str>,
    handoff: Option<&ContractHandoff>,
    destination_state_is_fresh: bool,
) -> Option<crate::contract::WriteContract> {
    let handoff = handoff?;
    if current_signature != Some(handoff.schema_signature.as_str()) {
        return None;
    }
    if !destination_state_is_fresh {
        return None;
    }

    let setup = setup.clone();

    Some(if setup.is_replace {
        contract_from_handoff(existing_replace_contract(setup), handoff)
    } else {
        contract_from_handoff(setup, handoff)
    })
}

/// Entry point for writing a single stream.
pub async fn write_stream(
    config: &crate::config::Config,
    ctx: &Context,
    stream: &StreamContext,
) -> Result<WriteSummary, PluginError> {
    let connect_start = Instant::now();
    let client = crate::client::connect(config)
        .await
        .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
    let connect_secs = connect_start.elapsed().as_secs_f64();

    let setup = prepare_stream_once(
        &config.schema,
        &stream.stream_name,
        stream.write_mode.clone(),
        &stream.schema,
        stream.partition_count.unwrap_or(1) <= 1,
        stream.policies.schema_evolution,
        CheckpointConfig {
            bytes: stream.limits.checkpoint_interval_bytes,
            rows: stream.limits.checkpoint_interval_rows,
            seconds: stream.limits.checkpoint_interval_seconds,
        },
        resolve_copy_flush_bytes(stream.copy_flush_bytes_override, config.copy_flush_bytes),
        config.load_method,
    )
    .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;

    let current_signature = stream_schema_signature(&stream.schema)
        .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;

    let setup = if setup.is_replace {
        let staging_table =
            replace_staging_qualified_table(&setup.target_schema, &setup.stream_name);
        let handoff = read_contract_handoff(&client, &staging_table)
            .await
            .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;
        let destination_state_is_fresh = match handoff.as_ref() {
            Some(handoff)
                if current_signature.as_deref() == Some(handoff.schema_signature.as_str()) =>
            {
                replace_staging_reuse_is_safe(
                    &client,
                    &setup.target_schema,
                    &setup.stream_name,
                    &stream.schema,
                    handoff,
                )
                .await?
            }
            _ => false,
        };

        if let Some(reused) = reuse_contract_from_handoff(
            &setup,
            current_signature.as_deref(),
            handoff.as_ref(),
            destination_state_is_fresh,
        ) {
            reused
        } else {
            prepare_stream_contract(ctx, &client, stream, setup)
                .await
                .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
        }
    } else {
        let handoff = read_contract_handoff(&client, &setup.qualified_table)
            .await
            .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;
        let destination_state_is_fresh = match handoff.as_ref() {
            Some(handoff)
                if current_signature.as_deref() == Some(handoff.schema_signature.as_str()) =>
            {
                live_destination_state_is_fresh(
                    &client,
                    &setup.target_schema,
                    &setup.stream_name,
                    &stream.schema,
                    handoff,
                )
                .await?
            }
            _ => false,
        };

        if let Some(reused) = reuse_contract_from_handoff(
            &setup,
            current_signature.as_deref(),
            handoff.as_ref(),
            destination_state_is_fresh,
        ) {
            reused
        } else {
            prepare_stream_contract(ctx, &client, stream, setup)
                .await
                .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
        }
    };

    let mut session = WriteSession::begin(ctx, &client, &config.schema, setup)
        .await
        .map_err(|e| PluginError::transient_db("SESSION_BEGIN_FAILED", e))?;

    let mut loop_error: Option<String> = None;
    let mut arrow_decode_secs = 0.0;

    loop {
        match rapidbyte_sdk::host_ffi::next_batch_with_decode_timing(stream.limits.max_batch_bytes)
        {
            Ok(None) => break,
            Ok(Some(decoded)) => {
                arrow_decode_secs += decoded.decode_secs;
                let _ = ctx.histogram("dest_arrow_decode_secs", decoded.decode_secs);

                if let Err(e) = session.process_batch(&decoded.schema, &decoded.batches).await {
                    loop_error = Some(e);
                    break;
                }
            }
            Err(e) => {
                loop_error = Some(format!("next_batch failed: {e}"));
                break;
            }
        }
    }

    if let Some(err) = loop_error {
        let commit_state = session.loop_error_commit_state();
        ctx.log(
            LogLevel::Warn,
            crate::diagnostics::checkpoint_safety_message(
                crate::diagnostics::CheckpointSafetyPhase::LoopFailureBeforeNewDurableCommit,
                commit_state,
            ),
        );
        session.rollback().await;
        return Err(PluginError::transient_db("WRITE_FAILED", err).with_commit_state(commit_state));
    }

    let result = match session.commit().await {
        Ok(result) => result,
        Err(e) => {
            ctx.log(
                LogLevel::Warn,
                crate::diagnostics::checkpoint_safety_message(e.safety_phase, e.commit_state),
            );
            return Err(commit_error_to_plugin_error(e));
        }
    };

    let perf = WritePerf {
        connect_secs,
        flush_secs: result.flush_secs,
        commit_secs: result.commit_secs,
        arrow_decode_secs,
    };
    emit_dest_timings(ctx, &perf);

    Ok(WriteSummary {
        records_written: result.total_rows,
        bytes_written: result.total_bytes,
        batches_written: result.batches_written,
        checkpoint_count: result.checkpoint_count,
        records_failed: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        commit_error_to_plugin_error, contract_from_handoff, existing_replace_contract,
        live_destination_state_is_fresh, replace_staging_qualified_table,
        replace_staging_reuse_is_safe, resolve_copy_flush_bytes, reuse_contract_from_handoff,
        staging_table_name,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::session::COPY_FLUSH_MAX;
    use crate::session::CommitError;
    use crate::WriteMode;
    use rapidbyte_sdk::prelude::CommitState;
    use rapidbyte_sdk::schema::{SchemaField, StreamSchema};
    use tokio_postgres::NoTls;

    static NEXT_SUFFIX: AtomicU64 = AtomicU64::new(1);

    fn fresh_name(prefix: &str) -> String {
        format!(
            "{}_{}_{}",
            prefix,
            std::process::id(),
            NEXT_SUFFIX.fetch_add(1, Ordering::Relaxed)
        )
    }

    async fn connect() -> tokio_postgres::Client {
        let dsn = std::env::var("RAPIDBYTE_POSTGRES_TEST_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| {
                "host=127.0.0.1 port=33603 user=postgres password=postgres dbname=postgres"
                    .to_string()
            });
        let (client, connection) = tokio_postgres::connect(
            &dsn,
            NoTls,
        )
        .await
        .expect("connect to test postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    fn live_stream_schema() -> StreamSchema {
        StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false).with_primary_key(true),
                SchemaField::new("name", "utf8", true),
            ],
            primary_key: vec!["id".to_string()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        }
    }

    async fn create_table(client: &tokio_postgres::Client, schema: &str, table: &str) {
        let qualified = crate::decode::qualified_name(schema, table);
        client
            .execute(
                &format!("CREATE SCHEMA IF NOT EXISTS {}", pg_escape::quote_identifier(schema)),
                &[],
            )
            .await
            .expect("create schema");
        client
            .execute(
                &format!("CREATE TABLE {} (\"id\" bigint not null, \"name\" text)", qualified),
                &[],
            )
            .await
            .expect("create table");
    }

    #[test]
    fn runtime_override_takes_precedence_over_configured_flush_bytes() {
        let resolved = resolve_copy_flush_bytes(Some(16 * 1024 * 1024), Some(2 * 1024 * 1024));
        assert_eq!(resolved, Some(16 * 1024 * 1024));
    }

    #[test]
    fn zero_runtime_override_falls_back_to_configured_flush_bytes() {
        let resolved = resolve_copy_flush_bytes(Some(0), Some(2 * 1024 * 1024));
        assert_eq!(resolved, Some(2 * 1024 * 1024));
    }

    #[test]
    fn configured_flush_bytes_used_when_no_runtime_override() {
        let resolved = resolve_copy_flush_bytes(None, Some(2 * 1024 * 1024));
        assert_eq!(resolved, Some(2 * 1024 * 1024));
    }

    #[test]
    fn resolved_flush_bytes_are_clamped_to_guardrails() {
        let clamped_override = resolve_copy_flush_bytes(Some(u64::MAX), None);
        assert_eq!(clamped_override, Some(COPY_FLUSH_MAX));

        let clamped_config = resolve_copy_flush_bytes(None, Some(256 * 1024));
        assert_eq!(clamped_config, Some(1024 * 1024));
    }

    #[test]
    fn commit_error_to_plugin_error_preserves_unknown_commit_state() {
        let err = CommitError::pre_commit_failure(CommitState::AfterCommitUnknown, "commit failed");

        let plugin_err = commit_error_to_plugin_error(err);

        assert_eq!(plugin_err.code, "COMMIT_FAILED");
        assert_eq!(plugin_err.commit_state, Some(CommitState::AfterCommitUnknown));
        assert!(plugin_err.message.contains("commit failed"));
    }

    #[test]
    fn staging_table_names_are_deterministic() {
        assert_eq!(staging_table_name("users"), "users__rb_staging");
    }

    #[test]
    fn replace_staging_table_identity_is_schema_qualified() {
        assert_eq!(
            replace_staging_qualified_table("my schema", "users"),
            r#""my schema".users__rb_staging"#
        );
    }

    #[test]
    fn existing_replace_contract_targets_staging_table() {
        let contract = crate::contract::prepare_stream_once(
            "public",
            "users",
            Some(WriteMode::Replace),
            &rapidbyte_sdk::schema::StreamSchema::default(),
            true,
            rapidbyte_sdk::stream::SchemaEvolutionPolicy::default(),
            crate::contract::CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Copy,
        )
        .expect("contract");

        let resolved = existing_replace_contract(contract);
        assert_eq!(resolved.effective_stream, "users__rb_staging");
        assert_eq!(resolved.qualified_table, "public.users__rb_staging");
        assert!(!resolved.needs_schema_ensure);
    }

    #[test]
    fn contract_from_handoff_applies_state_without_structural_work() {
        let contract = crate::contract::prepare_stream_once(
            "public",
            "users",
            None,
            &StreamSchema::default(),
            true,
            rapidbyte_sdk::stream::SchemaEvolutionPolicy::default(),
            crate::contract::CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Copy,
        )
        .expect("contract");
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: "sig".to_string(),
            ignored_columns: vec!["legacy".to_string()],
            type_null_columns: vec!["coerce_me".to_string()],
        };

        let prepared = contract_from_handoff(contract, &handoff);
        assert!(prepared.ignored_columns.contains("legacy"));
        assert!(prepared.type_null_columns.contains("coerce_me"));
        assert!(!prepared.needs_schema_ensure);
    }

    #[test]
    fn reuse_contract_from_handoff_reuses_append_contract() {
        let contract = crate::contract::prepare_stream_once(
            "public",
            "users",
            Some(WriteMode::Append),
            &StreamSchema::default(),
            true,
            rapidbyte_sdk::stream::SchemaEvolutionPolicy::default(),
            crate::contract::CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Copy,
        )
        .expect("contract");
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: "sig".to_string(),
            ignored_columns: vec!["legacy".to_string()],
            type_null_columns: vec!["coerce_me".to_string()],
        };

        let reused = reuse_contract_from_handoff(&contract, Some("sig"), Some(&handoff), true)
            .expect("handoff should be reused");
        assert_eq!(reused.effective_write_mode, Some(WriteMode::Append));
        assert!(reused.ignored_columns.contains("legacy"));
        assert!(reused.type_null_columns.contains("coerce_me"));
        assert!(!reused.needs_schema_ensure);
    }

    #[tokio::test]
    async fn live_freshness_allows_ignored_new_columns() {
        let schema = fresh_name("rb_writer_ignore");
        let table = "users";
        let client = connect().await;
        create_table(&client, &schema, table).await;

        let stream_schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false).with_primary_key(true),
                SchemaField::new("name", "utf8", true),
                SchemaField::new("age", "int64", true),
            ],
            primary_key: vec!["id".to_string()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: crate::contract::stream_schema_signature(&stream_schema)
                .expect("signature")
                .expect("schema signature"),
            ignored_columns: vec!["age".to_string()],
            type_null_columns: vec![],
        };

        let fresh = live_destination_state_is_fresh(&client, &schema, table, &stream_schema, &handoff)
            .await
            .expect("freshness");
        assert!(fresh);

        let contract = crate::contract::prepare_stream_once(
            &schema,
            table,
            Some(WriteMode::Append),
            &stream_schema,
            true,
            rapidbyte_sdk::stream::SchemaEvolutionPolicy::default(),
            crate::contract::CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Copy,
        )
        .expect("contract");

        let reused = reuse_contract_from_handoff(
            &contract,
            Some(handoff.schema_signature.as_str()),
            Some(&handoff),
            fresh,
        );
        assert!(reused.is_some());

        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS {} CASCADE", pg_escape::quote_identifier(&schema)),
                &[],
            )
            .await;
    }

    #[tokio::test]
    async fn live_freshness_allows_type_null_columns() {
        let schema = fresh_name("rb_writer_null");
        let table = "users";
        let client = connect().await;
        create_table(&client, &schema, table).await;
        client
            .execute(
                &format!(
                    "ALTER TABLE {} ALTER COLUMN \"id\" TYPE integer USING \"id\"::integer",
                    crate::decode::qualified_name(&schema, table)
                ),
                &[],
            )
            .await
            .expect("set table type for compatibility test");

        let stream_schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "utf8", false).with_primary_key(true),
                SchemaField::new("name", "utf8", true),
            ],
            primary_key: vec!["id".to_string()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: crate::contract::stream_schema_signature(&stream_schema)
                .expect("signature")
                .expect("schema signature"),
            ignored_columns: vec![],
            type_null_columns: vec!["id".to_string()],
        };

        let fresh = live_destination_state_is_fresh(&client, &schema, table, &stream_schema, &handoff)
            .await
            .expect("freshness");
        assert!(fresh);

        let contract = crate::contract::prepare_stream_once(
            &schema,
            table,
            Some(WriteMode::Append),
            &stream_schema,
            true,
            rapidbyte_sdk::stream::SchemaEvolutionPolicy::default(),
            crate::contract::CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Copy,
        )
        .expect("contract");

        let reused = reuse_contract_from_handoff(
            &contract,
            Some(handoff.schema_signature.as_str()),
            Some(&handoff),
            fresh,
        );
        assert!(reused.is_some());

        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS {} CASCADE", pg_escape::quote_identifier(&schema)),
                &[],
            )
            .await;
    }

    #[tokio::test]
    async fn live_freshness_rejects_stale_ignored_columns_after_repair() {
        let schema = fresh_name("rb_writer_ignore_stale");
        let table = "users";
        let client = connect().await;
        create_table(&client, &schema, table).await;

        let stream_schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false).with_primary_key(true),
                SchemaField::new("name", "utf8", true),
                SchemaField::new("age", "int64", true),
            ],
            primary_key: vec!["id".to_string()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: crate::contract::stream_schema_signature(&stream_schema)
                .expect("signature")
                .expect("schema signature"),
            ignored_columns: vec!["age".to_string()],
            type_null_columns: vec![],
        };

        assert!(
            live_destination_state_is_fresh(&client, &schema, table, &stream_schema, &handoff)
                .await
                .expect("freshness while ignored column is still absent")
        );

        client
            .execute(
                &format!(
                    "ALTER TABLE {} ADD COLUMN age bigint",
                    crate::decode::qualified_name(&schema, table)
                ),
                &[],
            )
            .await
            .expect("repair ignored column");

        assert!(
            !live_destination_state_is_fresh(&client, &schema, table, &stream_schema, &handoff)
                .await
                .expect("freshness after repair")
        );

        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS {} CASCADE", pg_escape::quote_identifier(&schema)),
                &[],
            )
            .await;
    }

    #[tokio::test]
    async fn live_freshness_rejects_stale_type_null_columns_after_repair() {
        let schema = fresh_name("rb_writer_null_stale");
        let table = "users";
        let client = connect().await;
        create_table(&client, &schema, table).await;
        client
            .execute(
                &format!(
                    "ALTER TABLE {} ALTER COLUMN \"id\" TYPE integer USING \"id\"::integer",
                    crate::decode::qualified_name(&schema, table)
                ),
                &[],
            )
            .await
            .expect("introduce type drift");

        let stream_schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "utf8", false).with_primary_key(true),
                SchemaField::new("name", "utf8", true),
            ],
            primary_key: vec!["id".to_string()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: crate::contract::stream_schema_signature(&stream_schema)
                .expect("signature")
                .expect("schema signature"),
            ignored_columns: vec![],
            type_null_columns: vec!["id".to_string()],
        };

        assert!(
            live_destination_state_is_fresh(&client, &schema, table, &stream_schema, &handoff)
                .await
                .expect("freshness while type-null workaround is still needed")
        );

        client
            .execute(
                &format!(
                    "ALTER TABLE {} ALTER COLUMN \"id\" TYPE text USING \"id\"::text",
                    crate::decode::qualified_name(&schema, table)
                ),
                &[],
            )
            .await
            .expect("repair type-null column");

        assert!(
            !live_destination_state_is_fresh(&client, &schema, table, &stream_schema, &handoff)
                .await
                .expect("freshness after repair")
        );

        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS {} CASCADE", pg_escape::quote_identifier(&schema)),
                &[],
            )
            .await;
    }

    #[tokio::test]
    async fn replace_mode_reuses_empty_staging_table_when_policy_compatible() {
        let schema = fresh_name("rb_replace_fresh");
        let table = "users";
        let client = connect().await;
        create_table(&client, &schema, &staging_table_name(table)).await;

        let stream_schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false).with_primary_key(true),
                SchemaField::new("name", "utf8", true),
                SchemaField::new("age", "int64", true),
            ],
            primary_key: vec!["id".to_string()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: crate::contract::stream_schema_signature(&stream_schema)
                .expect("signature")
                .expect("schema signature"),
            ignored_columns: vec!["age".to_string()],
            type_null_columns: vec![],
        };

        let safe = replace_staging_reuse_is_safe(
            &client,
            &schema,
            table,
            &stream_schema,
            &handoff,
        )
        .await
        .expect("replace freshness");
        assert!(safe);

        let contract = crate::contract::prepare_stream_once(
            &schema,
            table,
            Some(WriteMode::Replace),
            &stream_schema,
            true,
            rapidbyte_sdk::stream::SchemaEvolutionPolicy::default(),
            crate::contract::CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Copy,
        )
        .expect("contract");

        let reused = reuse_contract_from_handoff(
            &contract,
            Some(handoff.schema_signature.as_str()),
            Some(&handoff),
            safe,
        );
        assert!(reused.is_some());

        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS {} CASCADE", pg_escape::quote_identifier(&schema)),
                &[],
            )
            .await;
    }

    #[test]
    fn reuse_contract_from_handoff_reuses_upsert_contract() {
        let contract = crate::contract::prepare_stream_once(
            "public",
            "users",
            Some(WriteMode::Upsert {
                primary_key: vec!["id".to_string()],
            }),
            &StreamSchema::default(),
            true,
            rapidbyte_sdk::stream::SchemaEvolutionPolicy::default(),
            crate::contract::CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Insert,
        )
        .expect("contract");
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: "sig".to_string(),
            ignored_columns: vec![],
            type_null_columns: vec![],
        };

        let reused = reuse_contract_from_handoff(&contract, Some("sig"), Some(&handoff), true)
            .expect("handoff should be reused");
        assert_eq!(
            reused.effective_write_mode,
            Some(WriteMode::Upsert {
                primary_key: vec!["id".to_string()],
            })
        );
        assert!(!reused.needs_schema_ensure);
    }

    #[test]
    fn reuse_contract_from_handoff_rejects_schema_signature_drift() {
        let contract = crate::contract::prepare_stream_once(
            "public",
            "users",
            Some(WriteMode::Append),
            &StreamSchema::default(),
            true,
            rapidbyte_sdk::stream::SchemaEvolutionPolicy::default(),
            crate::contract::CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Copy,
        )
        .expect("contract");
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: "sig".to_string(),
            ignored_columns: vec![],
            type_null_columns: vec![],
        };

        assert!(reuse_contract_from_handoff(&contract, Some("different"), Some(&handoff), true).is_none());
    }

    #[test]
    fn reuse_contract_from_handoff_rejects_stale_destination_state() {
        let contract = crate::contract::prepare_stream_once(
            "public",
            "users",
            Some(WriteMode::Append),
            &StreamSchema::default(),
            true,
            rapidbyte_sdk::stream::SchemaEvolutionPolicy::default(),
            crate::contract::CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Copy,
        )
        .expect("contract");
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: "sig".to_string(),
            ignored_columns: vec![],
            type_null_columns: vec![],
        };

        assert!(reuse_contract_from_handoff(&contract, Some("sig"), Some(&handoff), false).is_none());
    }

    #[tokio::test]
    async fn live_drift_detection_forces_safe_reprepare_path() {
        let schema = fresh_name("rb_writer_drift");
        let table = "users";
        let client = connect().await;
        create_table(&client, &schema, table).await;

        let stream_schema = live_stream_schema();
        let contract = crate::contract::prepare_stream_once(
            &schema,
            table,
            Some(WriteMode::Append),
            &stream_schema,
            true,
            rapidbyte_sdk::stream::SchemaEvolutionPolicy::default(),
            crate::contract::CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Copy,
        )
        .expect("contract");
        let current_signature = crate::contract::stream_schema_signature(&stream_schema)
            .expect("signature");
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: current_signature.clone().expect("schema signature"),
            ignored_columns: vec![],
            type_null_columns: vec![],
        };

        client
            .execute(
                &format!(
                    "ALTER TABLE {} ADD COLUMN out_of_band text",
                    crate::decode::qualified_name(&schema, table)
                ),
                &[],
            )
            .await
            .expect("out-of-band drift");

        let fresh = live_destination_state_is_fresh(&client, &schema, table, &stream_schema, &handoff)
            .await
            .expect("drift detection");
        assert!(!fresh);

        let reused = reuse_contract_from_handoff(
            &contract,
            current_signature.as_deref(),
            Some(&handoff),
            fresh,
        );
        assert!(reused.is_none());

        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS {} CASCADE", pg_escape::quote_identifier(&schema)),
                &[],
            )
            .await;
    }

    #[tokio::test]
    async fn stale_replace_staging_contents_block_contract_reuse() {
        let schema = fresh_name("rb_replace_stale");
        let table = "users";
        let client = connect().await;
        create_table(&client, &schema, &staging_table_name(table)).await;
        client
            .execute(
                &format!(
                    "INSERT INTO {} (\"id\", \"name\") VALUES (1, 'stale')",
                    crate::decode::qualified_name(&schema, &staging_table_name(table))
                ),
                &[],
            )
            .await
            .expect("seed stale staging row");

        let stream_schema = live_stream_schema();
        let handoff = crate::ddl::ContractHandoff {
            schema_signature: crate::contract::stream_schema_signature(&stream_schema)
                .expect("signature")
                .expect("schema signature"),
            ignored_columns: vec![],
            type_null_columns: vec![],
        };

        crate::ddl::write_contract_handoff(
            &client,
            &crate::decode::qualified_name(&schema, &staging_table_name(table)),
            &handoff,
        )
        .await
        .expect("write staging handoff");

        let safe = replace_staging_reuse_is_safe(&client, &schema, table, &stream_schema, &handoff)
            .await
            .expect("staging safety check");
        assert!(!safe);

        let _ = client
            .execute(
                &format!("DROP SCHEMA IF EXISTS {} CASCADE", pg_escape::quote_identifier(&schema)),
                &[],
            )
            .await;
    }
}
