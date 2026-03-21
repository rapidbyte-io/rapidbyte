//! Stream write orchestration for destination `PostgreSQL` plugin.

use std::time::Instant;

use rapidbyte_sdk::prelude::*;

use crate::apply::prepare_stream_contract;
use crate::contract::{mark_contract_prepared, prepare_stream_once, stream_schema_signature, CheckpointConfig};
use crate::ddl::{read_contract_handoff, ContractHandoff};
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
        let staging_table = staging_table_name(&setup.stream_name);
        let handoff = read_contract_handoff(&client, &staging_table)
            .await
            .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;
        if let Some(handoff) = handoff {
            if current_signature.as_deref() == Some(handoff.schema_signature.as_str()) {
                contract_from_handoff(existing_replace_contract(setup), &handoff)
            } else {
                prepare_stream_contract(ctx, &client, stream, setup)
                    .await
                    .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
            }
        } else {
            prepare_stream_contract(ctx, &client, stream, setup)
                .await
                .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
        }
    } else {
        let handoff = read_contract_handoff(&client, &setup.qualified_table)
            .await
            .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?;
        if let Some(handoff) = handoff {
            if current_signature.as_deref() == Some(handoff.schema_signature.as_str()) {
                contract_from_handoff(setup, &handoff)
            } else {
                prepare_stream_contract(ctx, &client, stream, setup)
                    .await
                    .map_err(|e| PluginError::config("INVALID_STREAM_SETUP", e))?
            }
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
        contract_from_handoff, existing_replace_contract, resolve_copy_flush_bytes,
        staging_table_name,
    };
    use crate::session::COPY_FLUSH_MAX;
    use crate::WriteMode;
    use rapidbyte_sdk::schema::StreamSchema;

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
}
