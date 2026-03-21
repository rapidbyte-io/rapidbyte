//! Stream write orchestration for destination `PostgreSQL` plugin.

use std::time::Instant;

use rapidbyte_sdk::prelude::*;

use crate::apply::prepare_stream_contract;
use crate::contract::{mark_contract_prepared, prepare_stream_once, CheckpointConfig};
use crate::ddl::SchemaState;
use crate::metrics::emit_dest_timings;
use crate::session::{clamp_copy_flush_bytes, WriteSession};

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

fn should_prepare_fallback(table_exists: bool) -> bool {
    !table_exists
}

fn existing_replace_contract(mut setup: crate::contract::WriteContract) -> crate::contract::WriteContract {
    setup.effective_stream = staging_table_name(&setup.stream_name);
    setup.qualified_table =
        crate::decode::qualified_name(&setup.target_schema, &setup.effective_stream);
    mark_contract_prepared(setup)
}

async fn existing_table_runtime_contract(
    client: &tokio_postgres::Client,
    setup: crate::contract::WriteContract,
    stream_schema: &StreamSchema,
) -> Result<crate::contract::WriteContract, String> {
    let mut schema_state = SchemaState::new();
    schema_state
        .load_existing_table_state(
            client,
            &setup.target_schema,
            &setup.effective_stream,
            &setup.schema_policy,
            stream_schema,
        )
        .await?;

    let mut setup = setup;
    setup.ignored_columns = schema_state.ignored_columns;
    setup.type_null_columns = schema_state.type_null_columns;
    Ok(mark_contract_prepared(setup))
}

fn replace_staging_signature_matches(
    marker_signature: Option<&str>,
    stream_schema: &StreamSchema,
) -> Result<bool, String> {
    Ok(marker_signature == crate::contract::stream_schema_signature(stream_schema)?.as_deref())
}

async fn target_table_exists(
    client: &tokio_postgres::Client,
    target_schema: &str,
    table_name: &str,
) -> Result<bool, String> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 AND table_type = 'BASE TABLE')",
            &[&target_schema, &table_name],
        )
        .await
        .map_err(|e| format!("failed to query target table existence: {e}"))?
        .get(0);
    Ok(exists)
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

    let setup = if setup.is_replace {
        let staging_name = staging_table_name(&setup.stream_name);
        let staging_exists = target_table_exists(&client, &setup.target_schema, &staging_name)
            .await
            .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;
        let staging_signature = crate::ddl::staging_prepared_signature(
            &client,
            &setup.target_schema,
            &setup.stream_name,
        )
            .await
            .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;
        if staging_exists
            && replace_staging_signature_matches(staging_signature.as_deref(), &stream.schema)
            .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
        {
            existing_table_runtime_contract(
                &client,
                existing_replace_contract(setup),
                &stream.schema,
            )
                .await
                .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
        } else {
            prepare_stream_contract(ctx, &client, stream, setup)
                .await
                .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
        }
    } else {
        let table_exists = target_table_exists(&client, &setup.target_schema, &setup.effective_stream)
            .await
            .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;
        if should_prepare_fallback(table_exists) {
            prepare_stream_contract(ctx, &client, stream, setup)
                .await
                .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
        } else {
            existing_table_runtime_contract(&client, setup, &stream.schema)
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
        session.rollback().await;
        return Err(PluginError::transient_db("WRITE_FAILED", err).with_commit_state(commit_state));
    }

    let result = session.commit().await.map_err(|e| {
        PluginError::transient_db("COMMIT_FAILED", e)
            .with_commit_state(CommitState::AfterCommitUnknown)
    })?;

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
        existing_replace_contract, replace_staging_signature_matches, resolve_copy_flush_bytes,
        should_prepare_fallback, staging_table_name,
    };
    use crate::session::COPY_FLUSH_MAX;
    use crate::WriteMode;
    use rapidbyte_sdk::schema::{SchemaField, StreamSchema};

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
    fn existing_tables_skip_fallback_setup_for_standard_writes() {
        assert!(!should_prepare_fallback(true));
    }

    #[test]
    fn missing_tables_trigger_fallback_setup() {
        assert!(should_prepare_fallback(false));
    }

    #[test]
    fn staging_table_names_are_deterministic() {
        assert_eq!(staging_table_name("users"), "users__rb_staging");
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
    fn replace_staging_exists_skips_fallback() {
        assert!(!should_prepare_fallback(true));
    }

    #[test]
    fn replace_staging_missing_triggers_fallback() {
        assert!(should_prepare_fallback(false));
    }

    #[test]
    fn replace_staging_signature_must_match_current_schema() {
        let schema = StreamSchema {
            fields: vec![SchemaField::new("id", "int64", false)],
            primary_key: vec!["id".to_string()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };
        let marker = crate::contract::stream_schema_signature(&schema)
            .expect("signature")
            .expect("non-empty schema");
        assert!(replace_staging_signature_matches(Some(&marker), &schema).expect("match"));

        let changed = StreamSchema {
            fields: vec![SchemaField::new("id", "utf8", false)],
            primary_key: vec!["id".to_string()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };
        assert!(!replace_staging_signature_matches(Some(&marker), &changed).expect("mismatch"));
        assert!(!replace_staging_signature_matches(None, &schema).expect("missing marker"));
    }
}
