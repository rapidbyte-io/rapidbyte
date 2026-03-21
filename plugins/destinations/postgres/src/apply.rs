//! Destination `PostgreSQL` apply lifecycle.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio_postgres::Client;

use rapidbyte_sdk::lifecycle::{ApplyAction, ApplyReport, ApplyRequest};
use rapidbyte_sdk::prelude::*;

use crate::config::Config;
use crate::contract::{mark_contract_prepared, preflight_schema_from_stream_schema, CheckpointConfig};
use crate::ddl::{prepare_staging, SchemaState};
use crate::decode;

fn apply_action_description(config: &Config, stream_name: &str, dry_run: bool) -> String {
    if dry_run {
        format!(
            "Would prepare schema/table for stream '{stream_name}' in schema '{}'",
            config.target_schema()
        )
    } else {
        format!(
            "Prepared schema/table for stream '{stream_name}' in schema '{}'",
            config.target_schema()
        )
    }
}

fn plan_stream_contract(config: &Config, stream: &StreamContext) -> Result<crate::contract::WriteContract, String> {
    crate::contract::prepare_stream_once(
        config.target_schema(),
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
}

fn resolve_copy_flush_bytes(
    stream_override: Option<u64>,
    configured: Option<usize>,
) -> Option<usize> {
    if let Some(bytes) = stream_override {
        if bytes == 0 {
            return configured.map(crate::session::clamp_copy_flush_bytes);
        }
        let override_bytes = usize::try_from(bytes).unwrap_or(usize::MAX);
        return Some(crate::session::clamp_copy_flush_bytes(override_bytes));
    }

    configured.map(crate::session::clamp_copy_flush_bytes)
}

pub(crate) fn plan_apply_request(
    config: &Config,
    request: &ApplyRequest,
) -> Result<ApplyReport, String> {
    let (report, _contracts) = plan_apply_request_with_contracts(config, request)?;
    Ok(report)
}

pub(crate) fn plan_apply_request_with_contracts(
    config: &Config,
    request: &ApplyRequest,
) -> Result<(ApplyReport, HashMap<String, crate::contract::WriteContract>), String> {
    let mut actions = Vec::with_capacity(request.streams.len());
    let mut prepared_contracts = HashMap::with_capacity(request.streams.len());

    for stream in &request.streams {
        let contract = plan_stream_contract(config, stream)?;
        prepared_contracts.insert(stream.stream_name.clone(), contract);
        actions.push(ApplyAction {
            stream_name: stream.stream_name.clone(),
            description: apply_action_description(config, &stream.stream_name, request.dry_run),
            ddl_executed: None,
        });
    }

    Ok((ApplyReport { actions }, prepared_contracts))
}

pub(crate) async fn prepare_stream_contract(
    ctx: &Context,
    client: &Client,
    stream: &StreamContext,
    mut contract: crate::contract::WriteContract,
) -> Result<crate::contract::WriteContract, String> {
    if contract.use_watermarks {
        crate::watermark::ensure_table(client, &contract.target_schema)
            .await
            .map_err(|e| format!("dest-postgres: watermarks table creation failed: {e}"))?;
    }

    if contract.is_replace {
        let staging_name =
            prepare_staging(ctx, client, &contract.target_schema, &contract.stream_name)
                .await
                .map_err(|e| format!("{e:#}"))?;
        ctx.log(
            LogLevel::Info,
            &format!("dest-postgres: Replace mode - writing to staging table '{staging_name}'"),
        );
        contract.effective_stream = staging_name;
        contract.effective_write_mode = Some(WriteMode::Append);
    }

    let mut schema_state = SchemaState::new();
    if let Some(schema) = preflight_schema_from_stream_schema(&stream.schema)? {
        schema_state
            .ensure_table(
                ctx,
                client,
                &contract.target_schema,
                &contract.effective_stream,
                contract.effective_write_mode.as_ref(),
                Some(&contract.schema_policy),
                &schema,
            )
            .await?;
        contract = mark_contract_prepared(contract);
    }

    contract.qualified_table =
        decode::qualified_name(&contract.target_schema, &contract.effective_stream);
    contract.ignored_columns = schema_state.ignored_columns;
    contract.type_null_columns = schema_state.type_null_columns;

    contract.watermark_records = if contract.is_replace || !contract.use_watermarks {
        0
    } else {
        match crate::watermark::get(client, &contract.target_schema, &contract.stream_name).await {
            Ok(w) => {
                if w > 0 {
                    ctx.log(
                        LogLevel::Warn,
                        &format!(
                            "dest-postgres: ignoring stale watermark for stream '{}' ({w} committed records); row-count resume is disabled until checkpoint-safe recovery lands",
                            contract.stream_name
                        ),
                    );
                    let _ = crate::watermark::clear(
                        client,
                        &contract.target_schema,
                        &contract.stream_name,
                    )
                    .await;
                }
                0
            }
            Err(e) => {
                ctx.log(
                    LogLevel::Warn,
                    &format!("dest-postgres: watermark query failed (starting fresh): {e}"),
                );
                0
            }
        }
    };

    Ok(contract)
}

pub(crate) async fn apply(
    config: &Config,
    ctx: &Context,
    request: ApplyRequest,
    prepared_contracts: &Mutex<HashMap<String, crate::contract::WriteContract>>,
) -> Result<ApplyReport, PluginError> {
    if request.dry_run {
        return plan_apply_request(config, &request).map_err(|e| PluginError::config("INVALID_APPLY_REQUEST", e));
    }

    let client = crate::client::connect(config)
        .await
        .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;

    let (mut report, contracts) = plan_apply_request_with_contracts(config, &request)
        .map_err(|e| PluginError::config("INVALID_APPLY_REQUEST", e))?;

    for action in &mut report.actions {
        let stream = request
            .streams
            .iter()
            .find(|stream| stream.stream_name == action.stream_name)
            .ok_or_else(|| {
                PluginError::config(
                    "INVALID_APPLY_REQUEST",
                    format!("missing stream context for '{}'", action.stream_name),
                )
            })?;

        let contract = contracts
            .get(&action.stream_name)
            .cloned()
            .ok_or_else(|| {
                PluginError::config(
                    "INVALID_APPLY_REQUEST",
                    format!("missing prepared contract for '{}'", action.stream_name),
                )
            })?;
        let prepared = prepare_stream_contract(ctx, &client, stream, contract)
            .await
            .map_err(|e| PluginError::config("INVALID_APPLY_REQUEST", e))?;
        action.ddl_executed = Some(format!(
            "prepared {}",
            prepared.qualified_table
        ));
        prepared_contracts
            .lock()
            .expect("prepared contract cache poisoned")
            .insert(action.stream_name.clone(), prepared);
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use rapidbyte_sdk::lifecycle::ApplyRequest;
    use rapidbyte_sdk::schema::{SchemaField, StreamSchema};
    use rapidbyte_sdk::stream::{StreamLimits, StreamPolicies};
    use rapidbyte_sdk::wire::SyncMode;

    use super::*;

    fn base_config() -> Config {
        Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
            schema: "public".to_string(),
            load_method: crate::config::LoadMethod::Copy,
            copy_flush_bytes: None,
        }
    }

    fn stream_schema() -> StreamSchema {
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

    fn stream_context(name: &str) -> StreamContext {
        StreamContext {
            stream_name: name.to_string(),
            source_stream_name: None,
            stream_index: 0,
            schema: stream_schema(),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
            partition_key: None,
            partition_count: None,
            partition_index: None,
            effective_parallelism: None,
            partition_strategy: None,
            copy_flush_bytes_override: None,
        }
    }

    #[test]
    fn apply_plan_builds_structured_actions() {
        let request = ApplyRequest {
            streams: vec![stream_context("users")],
            dry_run: true,
        };

        let report = plan_apply_request(&base_config(), &request).expect("plan");

        assert_eq!(report.actions.len(), 1);
        assert!(report.actions[0].description.contains("Would prepare"));
    }

    #[test]
    fn apply_plan_returns_contracts_for_each_stream() {
        let request = ApplyRequest {
            streams: vec![stream_context("users")],
            dry_run: false,
        };

        let (report, contracts) =
            plan_apply_request_with_contracts(&base_config(), &request).expect("plan");

        assert_eq!(report.actions.len(), 1);
        assert!(contracts.contains_key("users"));
        assert!(contracts["users"].needs_schema_ensure);
    }

    #[test]
    fn mark_contract_prepared_disables_schema_ensure() {
        let contract = crate::contract::prepare_stream_once(
            "public",
            "users",
            None,
            &StreamSchema::default(),
            true,
            StreamPolicies::default().schema_evolution,
            CheckpointConfig {
                bytes: 0,
                rows: 0,
                seconds: 0,
            },
            None,
            crate::config::LoadMethod::Copy,
        )
        .expect("contract");

        let prepared = mark_contract_prepared(contract);
        assert!(!prepared.needs_schema_ensure);
    }

}
