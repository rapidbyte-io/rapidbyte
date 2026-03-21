//! WASM plugin runner adapter — self-contained implementation.
//!
//! Wraps `rapidbyte-runtime` to implement the [`PluginRunner`] port trait.
//! All WASM execution logic (source, destination, transform, validate,
//! discover) is implemented directly in this module — no delegation to
//! legacy runner functions.

use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::Context;
use async_trait::async_trait;
use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{
    create_component_linker, dest_bindings, dest_error_to_sdk, source_bindings,
    source_error_to_sdk, source_validation_to_sdk, transform_bindings, transform_error_to_sdk,
    ComponentHostState, CompressionCodec, Frame, HostTimings, LoadedComponent, SandboxOverrides,
    WasmRuntime,
};
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::discovery::{DiscoveredStream, PluginSpec};
use rapidbyte_types::lifecycle::{ApplyAction, ApplyReport, TeardownReport};
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::metric::{ReadSummary, TransformSummary, WriteSummary};
use rapidbyte_types::schema::{SchemaField, StreamSchema};
use rapidbyte_types::state::RunStats;
use rapidbyte_types::state_backend::StateBackend;
use rapidbyte_types::stream::StreamContext;
use rapidbyte_types::validation::{
    PrerequisiteCheck, PrerequisiteSeverity, PrerequisitesReport, ValidationReport,
};
use rapidbyte_types::wire::{PluginKind, SyncMode, WriteMode};

use crate::adapter::engine_factory::noop_state_backend;
use crate::domain::error::PipelineError;
use crate::domain::ports::runner::{
    ApplyParams, CheckComponentStatus, DestinationOutcome, DestinationRunParams, DiscoverParams,
    PluginRunner, PrerequisitesParams, SourceOutcome, SourceRunParams, SpecParams, TeardownParams,
    TransformOutcome, TransformRunParams, ValidateParams,
};

// ── Public adapter ──────────────────────────────────────────────────

/// Adapter that implements [`PluginRunner`] by calling the WASM runtime
/// directly.
pub struct WasmPluginRunner {
    runtime: WasmRuntime,
    state_backend: Arc<dyn StateBackend>,
}

impl WasmPluginRunner {
    /// Create a new `WasmPluginRunner`.
    pub fn new(runtime: WasmRuntime, state_backend: Arc<dyn StateBackend>) -> Self {
        Self {
            runtime,
            state_backend,
        }
    }
}

// ── PluginRunner implementation ─────────────────────────────────────

#[async_trait]
impl PluginRunner for WasmPluginRunner {
    async fn run_source(&self, params: SourceRunParams) -> Result<SourceOutcome, PipelineError> {
        let compression = parse_compression(params.compression.as_deref())?;
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let state_backend = Arc::clone(&self.state_backend);

        tokio::task::spawn_blocking(move || {
            run_source_stream(
                &component,
                state_backend,
                &params.pipeline_name,
                &params.metric_run_label,
                &params.plugin_id,
                &params.plugin_version,
                &params.stream_ctx,
                params.permissions.as_ref(),
                params.sandbox_overrides.as_ref(),
                compression,
                params.frame_sender,
                &params.config,
                params.stats,
                params.on_batch_emitted,
                params.cancel_flag,
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("source task panicked: {e}")))?
    }

    async fn run_transform(
        &self,
        params: TransformRunParams,
    ) -> Result<TransformOutcome, PipelineError> {
        let compression = parse_compression(params.compression.as_deref())?;
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let state_backend = Arc::clone(&self.state_backend);

        tokio::task::spawn_blocking(move || {
            run_transform_stream(
                &component,
                state_backend,
                &params.pipeline_name,
                &params.metric_run_label,
                &params.plugin_id,
                &params.plugin_version,
                &params.stream_ctx,
                params.permissions.as_ref(),
                params.sandbox_overrides.as_ref(),
                compression,
                params.frame_receiver,
                params.frame_sender,
                params.dlq_records,
                params.transform_index,
                &params.config,
                params.cancel_flag,
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("transform task panicked: {e}")))?
    }

    async fn run_destination(
        &self,
        params: DestinationRunParams,
    ) -> Result<DestinationOutcome, PipelineError> {
        let compression = parse_compression(params.compression.as_deref())?;
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let state_backend = Arc::clone(&self.state_backend);

        tokio::task::spawn_blocking(move || {
            run_destination_stream(
                &component,
                state_backend,
                &params.pipeline_name,
                &params.metric_run_label,
                &params.plugin_id,
                &params.plugin_version,
                &params.stream_ctx,
                params.permissions.as_ref(),
                params.sandbox_overrides.as_ref(),
                compression,
                params.frame_receiver,
                params.dlq_records,
                &params.config,
                params.stats,
                params.cancel_flag,
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("destination task panicked: {e}")))?
    }

    async fn validate_plugin(
        &self,
        params: &ValidateParams,
    ) -> Result<CheckComponentStatus, PipelineError> {
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let kind = params.kind;
        let plugin_id = params.plugin_id.clone();
        let plugin_version = params.plugin_version.clone();
        let config = params.config.clone();
        let stream_name = params.stream_name.clone();
        let permissions = params.permissions.clone();
        let sandbox_overrides = params.sandbox_overrides.clone();
        let upstream_schema = params.upstream_schema.clone();

        let validation = tokio::task::spawn_blocking(move || {
            validate_plugin_impl(
                &component,
                kind,
                &plugin_id,
                &plugin_version,
                &config,
                &stream_name,
                permissions.as_ref(),
                sandbox_overrides.as_ref(),
                upstream_schema.as_ref(),
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("validate task panicked: {e}")))?
        .map_err(PipelineError::Infrastructure)?;

        Ok(CheckComponentStatus { validation })
    }

    async fn discover(
        &self,
        params: &DiscoverParams,
    ) -> Result<Vec<DiscoveredStream>, PipelineError> {
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let plugin_id = params.plugin_id.clone();
        let plugin_version = params.plugin_version.clone();
        let config = params.config.clone();
        let permissions = params.permissions.clone();
        let sandbox_overrides = params.sandbox_overrides.clone();

        let streams = tokio::task::spawn_blocking(move || {
            run_discover_impl(
                &component,
                &plugin_id,
                &plugin_version,
                &config,
                permissions.as_ref(),
                sandbox_overrides.as_ref(),
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("discover task panicked: {e}")))?
        .map_err(PipelineError::Infrastructure)?;

        Ok(streams)
    }

    async fn spec(&self, params: &SpecParams) -> Result<PluginSpec, PipelineError> {
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let kind = params.kind;
        let plugin_id = params.plugin_id.clone();
        let plugin_version = params.plugin_version.clone();

        tokio::task::spawn_blocking(move || {
            run_spec_impl(&component, kind, &plugin_id, &plugin_version)
        })
        .await
        .map_err(|e| PipelineError::infra(format!("spec task panicked: {e}")))?
        .map_err(PipelineError::Infrastructure)
    }

    async fn prerequisites(
        &self,
        params: &PrerequisitesParams,
    ) -> Result<PrerequisitesReport, PipelineError> {
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let kind = params.kind;
        let plugin_id = params.plugin_id.clone();
        let plugin_version = params.plugin_version.clone();
        let config = params.config.clone();
        let permissions = params.permissions.clone();
        let sandbox_overrides = params.sandbox_overrides.clone();

        tokio::task::spawn_blocking(move || {
            run_prerequisites_impl(
                &component,
                kind,
                &plugin_id,
                &plugin_version,
                &config,
                permissions.as_ref(),
                sandbox_overrides.as_ref(),
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("prerequisites task panicked: {e}")))?
        .map_err(PipelineError::Infrastructure)
    }

    async fn apply(&self, params: &ApplyParams) -> Result<ApplyReport, PipelineError> {
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let kind = params.kind;
        let plugin_id = params.plugin_id.clone();
        let plugin_version = params.plugin_version.clone();
        let config = params.config.clone();
        let streams = params.streams.clone();
        let dry_run = params.dry_run;
        let permissions = params.permissions.clone();
        let sandbox_overrides = params.sandbox_overrides.clone();

        tokio::task::spawn_blocking(move || {
            run_apply_impl(
                &component,
                kind,
                &plugin_id,
                &plugin_version,
                &config,
                &streams,
                dry_run,
                permissions.as_ref(),
                sandbox_overrides.as_ref(),
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("apply task panicked: {e}")))?
        .map_err(PipelineError::Infrastructure)
    }

    async fn teardown(&self, params: &TeardownParams) -> Result<TeardownReport, PipelineError> {
        let component = self
            .runtime
            .load_module(&params.wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        let kind = params.kind;
        let plugin_id = params.plugin_id.clone();
        let plugin_version = params.plugin_version.clone();
        let config = params.config.clone();
        let streams = params.streams.clone();
        let reason = params.reason.clone();
        let permissions = params.permissions.clone();
        let sandbox_overrides = params.sandbox_overrides.clone();

        tokio::task::spawn_blocking(move || {
            run_teardown_impl(
                &component,
                kind,
                &plugin_id,
                &plugin_version,
                &config,
                &streams,
                &reason,
                permissions.as_ref(),
                sandbox_overrides.as_ref(),
            )
        })
        .await
        .map_err(|e| PipelineError::infra(format!("teardown task panicked: {e}")))?
        .map_err(PipelineError::Infrastructure)
    }
}

// ── Helper: compression parsing ─────────────────────────────────────

/// Parse an optional compression string into a [`CompressionCodec`].
fn parse_compression(s: Option<&str>) -> Result<Option<CompressionCodec>, PipelineError> {
    match s {
        None | Some("") => Ok(None),
        Some("lz4") => Ok(Some(CompressionCodec::Lz4)),
        Some("zstd") => Ok(Some(CompressionCodec::Zstd)),
        Some(other) => Err(PipelineError::infra(format!(
            "unknown compression codec: {other}"
        ))),
    }
}

// ── Host-state builder helpers ──────────────────────────────────────

/// Build a host state instance key for WASM store identification.
fn plugin_instance_key(
    stage: &str,
    plugin_id: &str,
    stream_ctx: &StreamContext,
    instance_ordinal: Option<usize>,
) -> String {
    let partition_index = stream_ctx.partition_index.unwrap_or(0);
    match instance_ordinal {
        Some(ordinal) => format!(
            "{stage}:{plugin_id}:{}:p{partition_index}:i{ordinal}",
            stream_ctx.stream_name
        ),
        None => format!(
            "{stage}:{plugin_id}:{}:p{partition_index}",
            stream_ctx.stream_name
        ),
    }
}

/// Build a [`ComponentHostState`] pre-populated with fields common to all
/// runner kinds. Callers chain kind-specific setters then call `.build()`.
#[allow(clippy::too_many_arguments)]
fn build_base_host_state(
    component: &LoadedComponent,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    metric_run_label: &str,
    plugin_id: &str,
    stream_ctx: &StreamContext,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
    stage: &str,
    config: &serde_json::Value,
    instance_ordinal: Option<usize>,
) -> rapidbyte_runtime::HostStateBuilder {
    let _ = component; // used implicitly for type context
    let shard_index = stream_ctx.partition_index.unwrap_or(0) as usize;
    let host_timings = HostTimings::new(pipeline_name, &stream_ctx.stream_name, shard_index)
        .with_run_label(metric_run_label);

    let mut builder = ComponentHostState::builder()
        .pipeline(pipeline_name)
        .plugin_id(plugin_id)
        .plugin_instance_key(plugin_instance_key(
            stage,
            plugin_id,
            stream_ctx,
            instance_ordinal,
        ))
        .stream(stream_ctx.stream_name.clone())
        .metric_run_label(metric_run_label)
        .state_backend(state_backend)
        .timings(host_timings)
        .config(config)
        .compression(compression);

    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    builder
}

/// Serialize a plugin config value to JSON.
fn serialize_plugin_config(
    config: &serde_json::Value,
    role: &str,
) -> Result<String, PipelineError> {
    serde_json::to_string(config)
        .with_context(|| format!("Failed to serialize {role} config"))
        .map_err(PipelineError::Infrastructure)
}

// ── StreamContext → WIT converters ──────────────────────────────────
//
// Each WIT world (source, destination, transform) has its own copy of
// the StreamContext type. These helpers convert from the shared
// `rapidbyte_types::stream::StreamContext` to the per-world WIT type.

macro_rules! stream_context_to_wit {
    ($fn_name:ident, $mod:ident) => {
        #[allow(clippy::too_many_lines)]
        fn $fn_name(
            ctx: &StreamContext,
        ) -> Result<$mod::rapidbyte::plugin::types::StreamContext, PipelineError> {
            use rapidbyte_types::stream::PartitionStrategy;
            use rapidbyte_types::wire::{SyncMode, WriteMode};

            let sync_mode = match ctx.sync_mode {
                SyncMode::FullRefresh => $mod::rapidbyte::plugin::types::SyncMode::FullRefresh,
                SyncMode::Incremental => $mod::rapidbyte::plugin::types::SyncMode::Incremental,
                SyncMode::Cdc => $mod::rapidbyte::plugin::types::SyncMode::Cdc,
            };

            let write_mode = ctx.write_mode.as_ref().map(|wm| match wm {
                WriteMode::Append => $mod::rapidbyte::plugin::types::WriteMode::Append,
                WriteMode::Replace => $mod::rapidbyte::plugin::types::WriteMode::Replace,
                WriteMode::Upsert { .. } => $mod::rapidbyte::plugin::types::WriteMode::Upsert,
            });

            let cursor_info = ctx.cursor_info.as_ref().map(|ci| {
                let last_value_json = ci
                    .last_value
                    .as_ref()
                    .map(|v| serde_json::to_string(v).unwrap_or_default());
                $mod::rapidbyte::plugin::types::CursorInfo {
                    cursor_field: ci.cursor_field.clone(),
                    tie_breaker_field: ci.tie_breaker_field.clone(),
                    cursor_type: serde_json::to_value(&ci.cursor_type)
                        .ok()
                        .and_then(|v| v.as_str().map(String::from))
                        .unwrap_or_else(|| "utf8".to_string()),
                    last_value_json,
                }
            });

            let schema = $mod::rapidbyte::plugin::types::StreamSchema {
                fields: ctx
                    .schema
                    .fields
                    .iter()
                    .map(|f| $mod::rapidbyte::plugin::types::SchemaField {
                        name: f.name.clone(),
                        arrow_type: f.arrow_type.clone(),
                        nullable: f.nullable,
                        is_primary_key: f.is_primary_key,
                        is_generated: f.is_generated,
                        is_partition_key: f.is_partition_key,
                        default_value: f.default_value.clone(),
                    })
                    .collect(),
                primary_key: ctx.schema.primary_key.clone(),
                partition_keys: ctx.schema.partition_keys.clone(),
                source_defined_cursor: ctx.schema.source_defined_cursor.clone(),
                schema_id: ctx.schema.schema_id.clone(),
            };

            let on_data_error = match ctx.policies.on_data_error {
                rapidbyte_types::stream::DataErrorPolicy::Fail => {
                    $mod::rapidbyte::plugin::types::DataErrorPolicy::Fail
                }
                rapidbyte_types::stream::DataErrorPolicy::Skip => {
                    $mod::rapidbyte::plugin::types::DataErrorPolicy::Skip
                }
                rapidbyte_types::stream::DataErrorPolicy::Dlq => {
                    $mod::rapidbyte::plugin::types::DataErrorPolicy::Dlq
                }
            };

            let se = &ctx.policies.schema_evolution;
            let schema_evolution = $mod::rapidbyte::plugin::types::SchemaEvolutionPolicy {
                new_column: match se.new_column {
                    rapidbyte_types::stream::ColumnPolicy::Add => {
                        $mod::rapidbyte::plugin::types::ColumnPolicy::Add
                    }
                    rapidbyte_types::stream::ColumnPolicy::Ignore => {
                        $mod::rapidbyte::plugin::types::ColumnPolicy::Ignore
                    }
                    rapidbyte_types::stream::ColumnPolicy::Fail => {
                        $mod::rapidbyte::plugin::types::ColumnPolicy::Fail
                    }
                },
                removed_column: match se.removed_column {
                    rapidbyte_types::stream::ColumnPolicy::Add => {
                        $mod::rapidbyte::plugin::types::ColumnPolicy::Add
                    }
                    rapidbyte_types::stream::ColumnPolicy::Ignore => {
                        $mod::rapidbyte::plugin::types::ColumnPolicy::Ignore
                    }
                    rapidbyte_types::stream::ColumnPolicy::Fail => {
                        $mod::rapidbyte::plugin::types::ColumnPolicy::Fail
                    }
                },
                type_change: match se.type_change {
                    rapidbyte_types::stream::TypeChangePolicy::Coerce => {
                        $mod::rapidbyte::plugin::types::TypeChangePolicy::Coerce
                    }
                    rapidbyte_types::stream::TypeChangePolicy::Fail => {
                        $mod::rapidbyte::plugin::types::TypeChangePolicy::Fail
                    }
                    rapidbyte_types::stream::TypeChangePolicy::Null => {
                        $mod::rapidbyte::plugin::types::TypeChangePolicy::NullOut
                    }
                },
                nullability_change: match se.nullability_change {
                    rapidbyte_types::stream::NullabilityPolicy::Allow => {
                        $mod::rapidbyte::plugin::types::NullabilityPolicy::Allow
                    }
                    rapidbyte_types::stream::NullabilityPolicy::Fail => {
                        $mod::rapidbyte::plugin::types::NullabilityPolicy::Fail
                    }
                },
            };

            let partition_strategy = ctx.partition_strategy.as_ref().map(|ps| match ps {
                PartitionStrategy::ModHash => {
                    $mod::rapidbyte::plugin::types::PartitionStrategy::ModHash
                }
                PartitionStrategy::Range => {
                    $mod::rapidbyte::plugin::types::PartitionStrategy::Range
                }
            });

            Ok($mod::rapidbyte::plugin::types::StreamContext {
                stream_index: ctx.stream_index,
                stream_name: ctx.stream_name.clone(),
                source_stream_name: ctx.source_stream_name.clone(),
                schema,
                sync_mode,
                cursor_info,
                limits: $mod::rapidbyte::plugin::types::StreamLimits {
                    max_batch_bytes: ctx.limits.max_batch_bytes,
                    max_record_bytes: ctx.limits.max_record_bytes,
                    max_inflight_batches: ctx.limits.max_inflight_batches,
                    max_parallel_requests: ctx.limits.max_parallel_requests,
                    checkpoint_interval_bytes: ctx.limits.checkpoint_interval_bytes,
                    checkpoint_interval_rows: ctx.limits.checkpoint_interval_rows,
                    checkpoint_interval_seconds: ctx.limits.checkpoint_interval_seconds,
                    max_records: ctx.limits.max_records,
                },
                policies: $mod::rapidbyte::plugin::types::StreamPolicies {
                    on_data_error,
                    schema_evolution,
                },
                write_mode,
                selected_columns: ctx.selected_columns.clone(),
                partition_key: ctx.partition_key.clone(),
                partition_count: ctx.partition_count,
                partition_index: ctx.partition_index,
                effective_parallelism: ctx.effective_parallelism,
                partition_strategy,
            })
        }
    };
}

stream_context_to_wit!(stream_context_to_wit_source, source_bindings);
stream_context_to_wit!(stream_context_to_wit_dest, dest_bindings);
stream_context_to_wit!(stream_context_to_wit_transform, transform_bindings);

/// Drain checkpoints from a shared mutex, returning an owned `Vec`.
fn extract_checkpoints(
    checkpoints: &Arc<Mutex<Vec<Checkpoint>>>,
    role: &str,
) -> Result<Vec<Checkpoint>, PipelineError> {
    checkpoints
        .lock()
        .map_err(|_| PipelineError::infra(format!("{role} checkpoint mutex poisoned")))
        .map(|mut guard| guard.drain(..).collect())
}

/// Handle the result of a plugin `close` call — log warnings but don't
/// propagate errors since the main work is already done.
fn handle_close_result<E, F>(
    result: std::result::Result<
        std::result::Result<(), E>,
        rapidbyte_runtime::wasmtime_reexport::Error,
    >,
    role: &str,
    stream_name: &str,
    convert: F,
) where
    F: Fn(E) -> String,
{
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            tracing::warn!(
                stream = stream_name,
                "{} close failed: {}",
                role,
                convert(err)
            );
        }
        Err(err) => {
            tracing::warn!(stream = stream_name, "{} close trap: {}", role, err);
        }
    }
}

// ── Source runner ────────────────────────────────────────────────────

#[allow(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]
fn run_source_stream(
    component: &LoadedComponent,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    metric_run_label: &str,
    plugin_id: &str,
    plugin_version: &str,
    stream_ctx: &StreamContext,
    permissions: Option<&Permissions>,
    overrides: Option<&SandboxOverrides>,
    compression: Option<CompressionCodec>,
    frame_sender: mpsc::SyncSender<Frame>,
    source_config: &serde_json::Value,
    stats: Arc<Mutex<RunStats>>,
    on_batch_emitted: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    cancel_flag: Arc<AtomicBool>,
) -> Result<SourceOutcome, PipelineError> {
    let phase_start = Instant::now();

    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));

    let mut builder = build_base_host_state(
        component,
        state_backend,
        pipeline_name,
        metric_run_label,
        plugin_id,
        stream_ctx,
        permissions,
        compression,
        "source",
        source_config,
        None,
    );
    if let Some(ovr) = overrides {
        builder = builder.overrides(ovr);
    }
    builder = builder
        .sender(frame_sender.clone())
        .source_checkpoints(source_checkpoints.clone())
        .cancel_flag(cancel_flag);
    if let Some(cb) = on_batch_emitted {
        builder = builder.on_emit(cb);
    }
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = overrides.and_then(|o| o.timeout_seconds);
    let mut store = component.new_store(host_state, timeout);
    let linker = create_component_linker(&component.engine, "source", |linker| {
        source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
            .context("Failed to add rapidbyte source host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings =
        source_bindings::RapidbyteSource::instantiate(&mut store, &component.component, &linker)
            .map_err(|e| {
                PipelineError::infra(format!("Failed to instantiate source bindings: {e}"))
            })?;

    let iface = bindings.rapidbyte_plugin_source();
    let source_config_json = serialize_plugin_config(source_config, "source")?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Opening source plugin for stream"
    );
    let session = iface
        .call_open(&mut store, &source_config_json)
        .map_err(|e| PipelineError::infra(format!("Failed to call source open: {e}")))?
        .map_err(|err| PipelineError::Plugin(source_error_to_sdk(err)))?;

    tracing::info!(stream = stream_ctx.stream_name, "Starting source read");
    let wit_stream_ctx = stream_context_to_wit_source(stream_ctx)?;
    let run_request = source_bindings::rapidbyte::plugin::types::RunRequest {
        streams: vec![wit_stream_ctx],
        dry_run: false,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::infra(format!("Failed to call source run: {e}")))?;

    let summary = match run_result {
        Ok(run_summary) => {
            let result = run_summary.results.into_iter().next().ok_or_else(|| {
                let _ = iface.call_close(&mut store, session);
                PipelineError::infra("source run summary returned no stream results")
            })?;
            if !result.succeeded {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::infra(format!(
                    "source stream '{}' failed: {}",
                    result.stream_name, result.outcome_json
                )));
            }
            serde_json::from_str::<ReadSummary>(&result.outcome_json).map_err(|e| {
                let _ = iface.call_close(&mut store, session);
                PipelineError::infra(format!("failed to parse source outcome_json: {e}"))
            })?
        }
        Err(err) => {
            let _ = iface.call_close(&mut store, session);
            return Err(PipelineError::Plugin(source_error_to_sdk(err)));
        }
    };

    tracing::info!(
        stream = stream_ctx.stream_name,
        records = summary.records_read,
        bytes = summary.bytes_read,
        "Source read complete for stream"
    );

    {
        let mut s = stats
            .lock()
            .map_err(|_| PipelineError::infra("run stats mutex poisoned"))?;
        s.records_read += summary.records_read;
        s.bytes_read += summary.bytes_read;
    }

    let _ = frame_sender.send(Frame::EndStream);

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Closing source plugin for stream"
    );
    handle_close_result(
        iface.call_close(&mut store, session),
        "Source",
        &stream_ctx.stream_name,
        |err| source_error_to_sdk(err).to_string(),
    );

    let checkpoints = extract_checkpoints(&source_checkpoints, "source")?;

    Ok(SourceOutcome {
        duration_secs: phase_start.elapsed().as_secs_f64(),
        summary,
        checkpoints,
    })
}

// ── Destination runner ──────────────────────────────────────────────

#[allow(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]
fn run_destination_stream(
    component: &LoadedComponent,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    metric_run_label: &str,
    plugin_id: &str,
    plugin_version: &str,
    stream_ctx: &StreamContext,
    permissions: Option<&Permissions>,
    overrides: Option<&SandboxOverrides>,
    compression: Option<CompressionCodec>,
    frame_receiver: mpsc::Receiver<Frame>,
    dlq_records: Arc<Mutex<Vec<rapidbyte_types::envelope::DlqRecord>>>,
    dest_config: &serde_json::Value,
    stats: Arc<Mutex<RunStats>>,
    cancel_flag: Arc<AtomicBool>,
) -> Result<DestinationOutcome, PipelineError> {
    let phase_start = Instant::now();
    let vm_setup_start = Instant::now();

    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));

    let mut builder = build_base_host_state(
        component,
        state_backend,
        pipeline_name,
        metric_run_label,
        plugin_id,
        stream_ctx,
        permissions,
        compression,
        "destination",
        dest_config,
        None,
    );
    if let Some(ovr) = overrides {
        builder = builder.overrides(ovr);
    }
    let builder = builder
        .receiver(frame_receiver)
        .dest_checkpoints(dest_checkpoints.clone())
        .dlq_records(dlq_records.clone())
        .cancel_flag(cancel_flag);
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = overrides.and_then(|o| o.timeout_seconds);
    let mut store = component.new_store(host_state, timeout);
    let linker = create_component_linker(&component.engine, "destination", |linker| {
        dest_bindings::RapidbyteDestination::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
            .context("Failed to add rapidbyte destination host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings =
        dest_bindings::RapidbyteDestination::instantiate(&mut store, &component.component, &linker)
            .map_err(|e| {
                PipelineError::infra(format!("Failed to instantiate destination bindings: {e}"))
            })?;

    let iface = bindings.rapidbyte_plugin_destination();
    let wasm_instantiation_secs = vm_setup_start.elapsed().as_secs_f64();

    let dest_config_json = serialize_plugin_config(dest_config, "destination")?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Opening destination plugin for stream"
    );
    let session = iface
        .call_open(&mut store, &dest_config_json)
        .map_err(|e| PipelineError::infra(format!("Failed to call destination open: {e}")))?
        .map_err(|err| PipelineError::Plugin(dest_error_to_sdk(err)))?;

    let recv_start = Instant::now();

    tracing::info!(
        stream = stream_ctx.stream_name,
        "Starting destination write"
    );
    let wit_stream_ctx = stream_context_to_wit_dest(stream_ctx)?;
    let run_request = dest_bindings::rapidbyte::plugin::types::RunRequest {
        streams: vec![wit_stream_ctx],
        dry_run: false,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::infra(format!("Failed to call destination run: {e}")))?;

    let summary = match run_result {
        Ok(run_summary) => {
            let result = run_summary.results.into_iter().next().ok_or_else(|| {
                let _ = iface.call_close(&mut store, session);
                PipelineError::infra("destination run summary returned no stream results")
            })?;
            if !result.succeeded {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::infra(format!(
                    "destination stream '{}' failed: {}",
                    result.stream_name, result.outcome_json
                )));
            }
            serde_json::from_str::<WriteSummary>(&result.outcome_json).map_err(|e| {
                let _ = iface.call_close(&mut store, session);
                PipelineError::infra(format!("failed to parse destination outcome_json: {e}"))
            })?
        }
        Err(err) => {
            let _ = iface.call_close(&mut store, session);
            return Err(PipelineError::Plugin(dest_error_to_sdk(err)));
        }
    };

    tracing::info!(
        stream = stream_ctx.stream_name,
        records = summary.records_written,
        bytes = summary.bytes_written,
        "Destination write complete for stream"
    );

    {
        let mut s = stats
            .lock()
            .map_err(|_| PipelineError::infra("run stats mutex poisoned"))?;
        s.records_written += summary.records_written;
        s.bytes_written += summary.bytes_written;
    }

    let frame_receive_secs = recv_start.elapsed().as_secs_f64();

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Closing destination plugin for stream"
    );
    handle_close_result(
        iface.call_close(&mut store, session),
        "Destination",
        &stream_ctx.stream_name,
        |err| dest_error_to_sdk(err).to_string(),
    );

    let checkpoints = extract_checkpoints(&dest_checkpoints, "destination")?;

    Ok(DestinationOutcome {
        duration_secs: phase_start.elapsed().as_secs_f64(),
        summary,
        wasm_instantiation_secs,
        frame_receive_secs,
        checkpoints,
    })
}

// ── Transform runner ────────────────────────────────────────────────

#[allow(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]
fn run_transform_stream(
    component: &LoadedComponent,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    metric_run_label: &str,
    plugin_id: &str,
    plugin_version: &str,
    stream_ctx: &StreamContext,
    permissions: Option<&Permissions>,
    overrides: Option<&SandboxOverrides>,
    compression: Option<CompressionCodec>,
    frame_receiver: mpsc::Receiver<Frame>,
    frame_sender: mpsc::SyncSender<Frame>,
    dlq_records: Arc<Mutex<Vec<rapidbyte_types::envelope::DlqRecord>>>,
    transform_index: usize,
    transform_config: &serde_json::Value,
    cancel_flag: Arc<AtomicBool>,
) -> Result<TransformOutcome, PipelineError> {
    let phase_start = Instant::now();

    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));

    let mut builder = build_base_host_state(
        component,
        state_backend,
        pipeline_name,
        metric_run_label,
        plugin_id,
        stream_ctx,
        permissions,
        compression,
        "transform",
        transform_config,
        Some(transform_index),
    );
    if let Some(ovr) = overrides {
        builder = builder.overrides(ovr);
    }
    let builder = builder
        .sender(frame_sender.clone())
        .receiver(frame_receiver)
        .dlq_records(dlq_records)
        .source_checkpoints(source_checkpoints)
        .dest_checkpoints(dest_checkpoints)
        .cancel_flag(cancel_flag);
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = overrides.and_then(|o| o.timeout_seconds);
    let mut store = component.new_store(host_state, timeout);
    let linker = create_component_linker(&component.engine, "transform", |linker| {
        transform_bindings::RapidbyteTransform::add_to_linker::<_, HasSelf<_>>(linker, |state| {
            state
        })
        .context("Failed to add rapidbyte transform host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings = transform_bindings::RapidbyteTransform::instantiate(
        &mut store,
        &component.component,
        &linker,
    )
    .map_err(|e| PipelineError::infra(format!("Failed to instantiate transform bindings: {e}")))?;

    let iface = bindings.rapidbyte_plugin_transform();
    let transform_config_json = serialize_plugin_config(transform_config, "transform")?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Opening transform plugin for stream"
    );
    let session = iface
        .call_open(&mut store, &transform_config_json)
        .map_err(|e| PipelineError::infra(format!("Failed to call transform open: {e}")))?
        .map_err(|err| PipelineError::Plugin(transform_error_to_sdk(err)))?;

    tracing::info!(stream = stream_ctx.stream_name, "Starting transform");
    let wit_stream_ctx = stream_context_to_wit_transform(stream_ctx)?;
    let run_request = transform_bindings::rapidbyte::plugin::types::RunRequest {
        streams: vec![wit_stream_ctx],
        dry_run: false,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::infra(format!("Failed to call transform run: {e}")))?;

    let summary = match run_result {
        Ok(run_summary) => {
            let result = run_summary.results.into_iter().next().ok_or_else(|| {
                let _ = iface.call_close(&mut store, session);
                PipelineError::infra("transform run summary returned no stream results")
            })?;
            if !result.succeeded {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::infra(format!(
                    "transform stream '{}' failed: {}",
                    result.stream_name, result.outcome_json
                )));
            }
            serde_json::from_str::<TransformSummary>(&result.outcome_json).map_err(|e| {
                let _ = iface.call_close(&mut store, session);
                PipelineError::infra(format!("failed to parse transform outcome_json: {e}"))
            })?
        }
        Err(err) => {
            let _ = iface.call_close(&mut store, session);
            return Err(PipelineError::Plugin(transform_error_to_sdk(err)));
        }
    };

    let _ = frame_sender.send(Frame::EndStream);

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        stream = stream_ctx.stream_name,
        "Closing transform plugin for stream"
    );
    handle_close_result(
        iface.call_close(&mut store, session),
        "Transform",
        &stream_ctx.stream_name,
        |err| transform_error_to_sdk(err).to_string(),
    );

    Ok(TransformOutcome {
        duration_secs: phase_start.elapsed().as_secs_f64(),
        summary,
    })
}

// ── Validation runner ───────────────────────────────────────────────

/// Validate a plugin configuration by invoking its lifecycle validation
/// entrypoint in-process.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn validate_plugin_impl(
    module: &LoadedComponent,
    kind: PluginKind,
    plugin_id: &str,
    plugin_version: &str,
    config: &serde_json::Value,
    stream_name: &str,
    permissions: Option<&Permissions>,
    sandbox_overrides: Option<&SandboxOverrides>,
    upstream_schema: Option<&StreamSchema>,
) -> anyhow::Result<ValidationReport> {
    use rapidbyte_runtime::{dest_validation_to_sdk, transform_validation_to_sdk};

    tracing::info!(plugin = plugin_id, version = plugin_version, kind = ?kind, "Validating plugin");

    let state = noop_state_backend();

    let mut builder = ComponentHostState::builder()
        .pipeline("check")
        .plugin_id(plugin_id)
        .plugin_instance_key(format!("validate:{kind:?}:{plugin_id}"))
        .stream(stream_name)
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(ovr) = sandbox_overrides {
        builder = builder.overrides(ovr);
    }
    let host_state = builder.build()?;

    let timeout = sandbox_overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let config_json = serde_json::to_string(config)?;

    match kind {
        PluginKind::Source => {
            let linker = create_component_linker(&module.engine, "source", |linker| {
                source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = source_bindings::RapidbyteSource::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_source();

            let session = match iface
                .call_open(&mut store, &config_json)?
                .map_err(source_error_to_sdk)
            {
                Ok(session) => session,
                Err(err) => return config_error_as_validation_failure("Source", &err),
            };

            let wit_schema = upstream_schema.map(schema_to_wit_source);
            let result = iface
                .call_validate(&mut store, session, wit_schema.as_ref())?
                .map(source_validation_to_sdk)
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Destination => {
            let linker = create_component_linker(&module.engine, "destination", |linker| {
                dest_bindings::RapidbyteDestination::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = dest_bindings::RapidbyteDestination::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_destination();

            let session = match iface
                .call_open(&mut store, &config_json)?
                .map_err(dest_error_to_sdk)
            {
                Ok(session) => session,
                Err(err) => return config_error_as_validation_failure("Destination", &err),
            };

            let wit_schema = upstream_schema.map(schema_to_wit_dest);
            let result = iface
                .call_validate(&mut store, session, wit_schema.as_ref())?
                .map(dest_validation_to_sdk)
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Transform => {
            let linker = create_component_linker(&module.engine, "transform", |linker| {
                transform_bindings::RapidbyteTransform::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = transform_bindings::RapidbyteTransform::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_transform();

            let session = match iface
                .call_open(&mut store, &config_json)?
                .map_err(transform_error_to_sdk)
            {
                Ok(session) => session,
                Err(err) => return config_error_as_validation_failure("Transform", &err),
            };

            let wit_schema = upstream_schema.map(schema_to_wit_transform);
            let result = iface
                .call_validate(&mut store, session, wit_schema.as_ref())?
                .map(transform_validation_to_sdk)
                .map_err(transform_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Unknown => {
            anyhow::bail!("cannot validate plugin with unknown kind")
        }
    }
}

fn config_error_as_validation_failure(
    role: &str,
    err: &rapidbyte_types::error::PluginError,
) -> anyhow::Result<ValidationReport> {
    if err.category == rapidbyte_types::error::ErrorCategory::Config {
        Ok(ValidationReport::failed(&err.message))
    } else {
        Err(anyhow::anyhow!("{role} open failed: {err}"))
    }
}

// ── Discover runner ─────────────────────────────────────────────────

/// Discover available streams from a source plugin.
fn run_discover_impl(
    module: &LoadedComponent,
    plugin_id: &str,
    plugin_version: &str,
    config: &serde_json::Value,
    permissions: Option<&Permissions>,
    sandbox_overrides: Option<&SandboxOverrides>,
) -> anyhow::Result<Vec<DiscoveredStream>> {
    let state = noop_state_backend();
    let mut builder = ComponentHostState::builder()
        .pipeline("discover")
        .plugin_id(plugin_id)
        .plugin_instance_key(format!("discover:source:{plugin_id}"))
        .stream("discover")
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(ovr) = sandbox_overrides {
        builder = builder.overrides(ovr);
    }
    let host_state = builder.build()?;

    let timeout = sandbox_overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let linker = create_component_linker(&module.engine, "source", |linker| {
        source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(linker, |state| state)?;
        Ok(())
    })?;
    let bindings =
        source_bindings::RapidbyteSource::instantiate(&mut store, &module.component, &linker)?;
    let iface = bindings.rapidbyte_plugin_source();
    let config_json = serde_json::to_string(config)?;

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        "Opening source plugin for discover"
    );
    let session = iface
        .call_open(&mut store, &config_json)?
        .map_err(source_error_to_sdk)
        .map_err(|e| anyhow::anyhow!("Source open failed for discover: {e}"))?;

    let wit_streams = iface
        .call_discover(&mut store, session)?
        .map_err(source_error_to_sdk)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let streams = wit_streams
        .into_iter()
        .map(source_discovered_stream_to_sdk)
        .collect();

    tracing::info!(
        plugin = plugin_id,
        version = plugin_version,
        "Closing source plugin after discover"
    );
    if let Err(err) = iface.call_close(&mut store, session)? {
        tracing::warn!(
            "Source close failed after discover: {}",
            source_error_to_sdk(err)
        );
    }

    Ok(streams)
}

// ── Schema → WIT converters ─────────────────────────────────────────

macro_rules! schema_to_wit {
    ($fn_name:ident, $mod:ident) => {
        fn $fn_name(schema: &StreamSchema) -> $mod::rapidbyte::plugin::types::StreamSchema {
            $mod::rapidbyte::plugin::types::StreamSchema {
                fields: schema
                    .fields
                    .iter()
                    .map(|f| $mod::rapidbyte::plugin::types::SchemaField {
                        name: f.name.clone(),
                        arrow_type: f.arrow_type.clone(),
                        nullable: f.nullable,
                        is_primary_key: f.is_primary_key,
                        is_generated: f.is_generated,
                        is_partition_key: f.is_partition_key,
                        default_value: f.default_value.clone(),
                    })
                    .collect(),
                primary_key: schema.primary_key.clone(),
                partition_keys: schema.partition_keys.clone(),
                source_defined_cursor: schema.source_defined_cursor.clone(),
                schema_id: schema.schema_id.clone(),
            }
        }
    };
}

schema_to_wit!(schema_to_wit_source, source_bindings);
schema_to_wit!(schema_to_wit_dest, dest_bindings);
schema_to_wit!(schema_to_wit_transform, transform_bindings);

fn source_discovered_stream_to_sdk(
    stream: source_bindings::rapidbyte::plugin::types::DiscoveredStream,
) -> DiscoveredStream {
    let source_bindings::rapidbyte::plugin::types::DiscoveredStream {
        name,
        schema,
        supported_sync_modes,
        default_cursor_field,
        estimated_row_count,
        metadata_json,
    } = stream;
    let source_bindings::rapidbyte::plugin::types::StreamSchema {
        fields,
        primary_key,
        partition_keys,
        source_defined_cursor,
        schema_id,
    } = schema;

    DiscoveredStream {
        name,
        schema: StreamSchema {
            fields: fields
                .into_iter()
                .map(|field| SchemaField {
                    name: field.name,
                    arrow_type: field.arrow_type,
                    nullable: field.nullable,
                    is_primary_key: field.is_primary_key,
                    is_generated: field.is_generated,
                    is_partition_key: field.is_partition_key,
                    default_value: field.default_value,
                })
                .collect(),
            primary_key,
            partition_keys,
            source_defined_cursor,
            schema_id,
        },
        supported_sync_modes: supported_sync_modes
            .into_iter()
            .map(|mode| match mode {
                source_bindings::rapidbyte::plugin::types::SyncMode::FullRefresh => {
                    SyncMode::FullRefresh
                }
                source_bindings::rapidbyte::plugin::types::SyncMode::Incremental => {
                    SyncMode::Incremental
                }
                source_bindings::rapidbyte::plugin::types::SyncMode::Cdc => SyncMode::Cdc,
            })
            .collect(),
        default_cursor_field,
        estimated_row_count,
        metadata_json,
    }
}

// ── WIT → SDK converters for lifecycle types ────────────────────────

macro_rules! wit_spec_to_sdk {
    ($fn_name:ident, $mod:ident) => {
        fn $fn_name(spec: $mod::rapidbyte::plugin::types::PluginSpec) -> PluginSpec {
            PluginSpec {
                protocol_version: spec.protocol_version,
                config_schema_json: spec.config_schema_json,
                resource_schema_json: spec.resource_schema_json,
                documentation_url: spec.documentation_url,
                features: spec.features,
                supported_sync_modes: spec
                    .supported_sync_modes
                    .into_iter()
                    .map(|m| match m {
                        $mod::rapidbyte::plugin::types::SyncMode::FullRefresh => {
                            SyncMode::FullRefresh
                        }
                        $mod::rapidbyte::plugin::types::SyncMode::Incremental => {
                            SyncMode::Incremental
                        }
                        $mod::rapidbyte::plugin::types::SyncMode::Cdc => SyncMode::Cdc,
                    })
                    .collect(),
                supported_write_modes: spec.supported_write_modes.map(|modes| {
                    modes
                        .into_iter()
                        .map(|m| match m {
                            $mod::rapidbyte::plugin::types::WriteMode::Append => WriteMode::Append,
                            $mod::rapidbyte::plugin::types::WriteMode::Replace => {
                                WriteMode::Replace
                            }
                            $mod::rapidbyte::plugin::types::WriteMode::Upsert => {
                                WriteMode::Upsert {
                                    primary_key: vec![],
                                }
                            }
                        })
                        .collect()
                }),
            }
        }
    };
}

wit_spec_to_sdk!(source_spec_to_sdk, source_bindings);
wit_spec_to_sdk!(dest_spec_to_sdk, dest_bindings);
wit_spec_to_sdk!(transform_spec_to_sdk, transform_bindings);

macro_rules! wit_prerequisites_to_sdk {
    ($fn_name:ident, $mod:ident) => {
        fn $fn_name(
            report: $mod::rapidbyte::plugin::types::PrerequisitesReport,
        ) -> PrerequisitesReport {
            PrerequisitesReport {
                passed: report.passed,
                checks: report
                    .checks
                    .into_iter()
                    .map(|c| PrerequisiteCheck {
                        name: c.name,
                        passed: c.passed,
                        severity: match c.severity {
                            $mod::rapidbyte::plugin::types::PrerequisiteSeverity::Error => {
                                PrerequisiteSeverity::Error
                            }
                            $mod::rapidbyte::plugin::types::PrerequisiteSeverity::Warning => {
                                PrerequisiteSeverity::Warning
                            }
                            $mod::rapidbyte::plugin::types::PrerequisiteSeverity::Info => {
                                PrerequisiteSeverity::Info
                            }
                        },
                        message: c.message,
                        fix_hint: c.fix_hint,
                    })
                    .collect(),
            }
        }
    };
}

wit_prerequisites_to_sdk!(source_prerequisites_to_sdk, source_bindings);
wit_prerequisites_to_sdk!(dest_prerequisites_to_sdk, dest_bindings);

macro_rules! wit_apply_report_to_sdk {
    ($fn_name:ident, $mod:ident) => {
        fn $fn_name(report: $mod::rapidbyte::plugin::types::ApplyReport) -> ApplyReport {
            ApplyReport {
                actions: report
                    .actions
                    .into_iter()
                    .map(|a| ApplyAction {
                        stream_name: a.stream_name,
                        description: a.description,
                        ddl_executed: a.ddl_executed,
                    })
                    .collect(),
            }
        }
    };
}

wit_apply_report_to_sdk!(source_apply_report_to_sdk, source_bindings);
wit_apply_report_to_sdk!(dest_apply_report_to_sdk, dest_bindings);

macro_rules! wit_teardown_report_to_sdk {
    ($fn_name:ident, $mod:ident) => {
        fn $fn_name(report: $mod::rapidbyte::plugin::types::TeardownReport) -> TeardownReport {
            TeardownReport {
                actions: report.actions,
            }
        }
    };
}

wit_teardown_report_to_sdk!(source_teardown_report_to_sdk, source_bindings);
wit_teardown_report_to_sdk!(dest_teardown_report_to_sdk, dest_bindings);

// ── Spec runner ─────────────────────────────────────────────────────

/// Retrieve the plugin spec by calling the sessionless `spec()` export.
fn run_spec_impl(
    module: &LoadedComponent,
    kind: PluginKind,
    plugin_id: &str,
    plugin_version: &str,
) -> anyhow::Result<PluginSpec> {
    tracing::info!(plugin = plugin_id, version = plugin_version, kind = ?kind, "Retrieving plugin spec");

    let state = noop_state_backend();
    let host_state = ComponentHostState::builder()
        .pipeline("spec")
        .plugin_id(plugin_id)
        .plugin_instance_key(format!("spec:{kind:?}:{plugin_id}"))
        .stream("spec")
        .state_backend(state)
        .config(&serde_json::Value::Null)
        .compression(None)
        .build()?;

    let mut store = module.new_store(host_state, None);

    match kind {
        PluginKind::Source => {
            let linker = create_component_linker(&module.engine, "source", |linker| {
                source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = source_bindings::RapidbyteSource::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_source();
            let wit_spec = iface
                .call_spec(&mut store)?
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Source spec failed: {e}"))?;
            Ok(source_spec_to_sdk(wit_spec))
        }
        PluginKind::Destination => {
            let linker = create_component_linker(&module.engine, "destination", |linker| {
                dest_bindings::RapidbyteDestination::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = dest_bindings::RapidbyteDestination::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_destination();
            let wit_spec = iface
                .call_spec(&mut store)?
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Destination spec failed: {e}"))?;
            Ok(dest_spec_to_sdk(wit_spec))
        }
        PluginKind::Transform => {
            let linker = create_component_linker(&module.engine, "transform", |linker| {
                transform_bindings::RapidbyteTransform::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = transform_bindings::RapidbyteTransform::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_transform();
            let wit_spec = iface
                .call_spec(&mut store)?
                .map_err(transform_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Transform spec failed: {e}"))?;
            Ok(transform_spec_to_sdk(wit_spec))
        }
        PluginKind::Unknown => {
            anyhow::bail!("cannot retrieve spec for plugin with unknown kind")
        }
    }
}

// ── Prerequisites runner ────────────────────────────────────────────

/// Run prerequisite checks against the plugin.
#[allow(clippy::too_many_arguments)]
fn run_prerequisites_impl(
    module: &LoadedComponent,
    kind: PluginKind,
    plugin_id: &str,
    plugin_version: &str,
    config: &serde_json::Value,
    permissions: Option<&Permissions>,
    sandbox_overrides: Option<&SandboxOverrides>,
) -> anyhow::Result<PrerequisitesReport> {
    tracing::info!(plugin = plugin_id, version = plugin_version, kind = ?kind, "Running prerequisites check");

    let state = noop_state_backend();
    let mut builder = ComponentHostState::builder()
        .pipeline("prerequisites")
        .plugin_id(plugin_id)
        .plugin_instance_key(format!("prerequisites:{kind:?}:{plugin_id}"))
        .stream("prerequisites")
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(ovr) = sandbox_overrides {
        builder = builder.overrides(ovr);
    }
    let host_state = builder.build()?;

    let timeout = sandbox_overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let config_json = serde_json::to_string(config)?;

    match kind {
        PluginKind::Source => {
            let linker = create_component_linker(&module.engine, "source", |linker| {
                source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = source_bindings::RapidbyteSource::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_source();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Source open failed: {e}"))?;

            let result = iface
                .call_prerequisites(&mut store, session)?
                .map(source_prerequisites_to_sdk)
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Destination => {
            let linker = create_component_linker(&module.engine, "destination", |linker| {
                dest_bindings::RapidbyteDestination::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = dest_bindings::RapidbyteDestination::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_destination();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Destination open failed: {e}"))?;

            let result = iface
                .call_prerequisites(&mut store, session)?
                .map(dest_prerequisites_to_sdk)
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Transform => {
            // Transform interface does not have prerequisites — return passed.
            Ok(PrerequisitesReport::passed())
        }
        PluginKind::Unknown => {
            anyhow::bail!("cannot run prerequisites for plugin with unknown kind")
        }
    }
}

// ── Apply runner ────────────────────────────────────────────────────

/// Apply schema changes for streams.
#[allow(clippy::too_many_arguments)]
fn run_apply_impl(
    module: &LoadedComponent,
    kind: PluginKind,
    plugin_id: &str,
    plugin_version: &str,
    config: &serde_json::Value,
    streams: &[StreamContext],
    dry_run: bool,
    permissions: Option<&Permissions>,
    sandbox_overrides: Option<&SandboxOverrides>,
) -> anyhow::Result<ApplyReport> {
    tracing::info!(plugin = plugin_id, version = plugin_version, kind = ?kind, dry_run, "Running apply");

    let state = noop_state_backend();
    let mut builder = ComponentHostState::builder()
        .pipeline("apply")
        .plugin_id(plugin_id)
        .plugin_instance_key(format!("apply:{kind:?}:{plugin_id}"))
        .stream("apply")
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(ovr) = sandbox_overrides {
        builder = builder.overrides(ovr);
    }
    let host_state = builder.build()?;

    let timeout = sandbox_overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let config_json = serde_json::to_string(config)?;

    match kind {
        PluginKind::Source => {
            let linker = create_component_linker(&module.engine, "source", |linker| {
                source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = source_bindings::RapidbyteSource::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_source();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Source open failed: {e}"))?;

            let wit_streams: Vec<_> = streams
                .iter()
                .map(stream_context_to_wit_source)
                .collect::<Result<_, _>>()?;
            let request = source_bindings::rapidbyte::plugin::types::ApplyRequest {
                streams: wit_streams,
                dry_run,
            };

            let result = iface
                .call_apply(&mut store, session, &request)?
                .map(source_apply_report_to_sdk)
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Destination => {
            let linker = create_component_linker(&module.engine, "destination", |linker| {
                dest_bindings::RapidbyteDestination::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = dest_bindings::RapidbyteDestination::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_destination();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Destination open failed: {e}"))?;

            let wit_streams: Vec<_> = streams
                .iter()
                .map(stream_context_to_wit_dest)
                .collect::<Result<_, _>>()?;
            let request = dest_bindings::rapidbyte::plugin::types::ApplyRequest {
                streams: wit_streams,
                dry_run,
            };

            let result = iface
                .call_apply(&mut store, session, &request)?
                .map(dest_apply_report_to_sdk)
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Transform => {
            // Transform interface does not have apply — return noop.
            Ok(ApplyReport::noop())
        }
        PluginKind::Unknown => {
            anyhow::bail!("cannot run apply for plugin with unknown kind")
        }
    }
}

// ── Teardown runner ─────────────────────────────────────────────────

/// Tear down resources for streams.
#[allow(clippy::too_many_arguments)]
fn run_teardown_impl(
    module: &LoadedComponent,
    kind: PluginKind,
    plugin_id: &str,
    plugin_version: &str,
    config: &serde_json::Value,
    streams: &[String],
    reason: &str,
    permissions: Option<&Permissions>,
    sandbox_overrides: Option<&SandboxOverrides>,
) -> anyhow::Result<TeardownReport> {
    tracing::info!(plugin = plugin_id, version = plugin_version, kind = ?kind, reason, "Running teardown");

    let state = noop_state_backend();
    let mut builder = ComponentHostState::builder()
        .pipeline("teardown")
        .plugin_id(plugin_id)
        .plugin_instance_key(format!("teardown:{kind:?}:{plugin_id}"))
        .stream("teardown")
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(ovr) = sandbox_overrides {
        builder = builder.overrides(ovr);
    }
    let host_state = builder.build()?;

    let timeout = sandbox_overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let config_json = serde_json::to_string(config)?;

    match kind {
        PluginKind::Source => {
            let linker = create_component_linker(&module.engine, "source", |linker| {
                source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = source_bindings::RapidbyteSource::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_source();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Source open failed: {e}"))?;

            let request = source_bindings::rapidbyte::plugin::types::TeardownRequest {
                streams: streams.to_vec(),
                reason: reason.to_string(),
            };

            let result = iface
                .call_teardown(&mut store, session, &request)?
                .map(source_teardown_report_to_sdk)
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Destination => {
            let linker = create_component_linker(&module.engine, "destination", |linker| {
                dest_bindings::RapidbyteDestination::add_to_linker::<_, HasSelf<_>>(
                    linker,
                    |state| state,
                )?;
                Ok(())
            })?;
            let bindings = dest_bindings::RapidbyteDestination::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_plugin_destination();

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Destination open failed: {e}"))?;

            let request = dest_bindings::rapidbyte::plugin::types::TeardownRequest {
                streams: streams.to_vec(),
                reason: reason.to_string(),
            };

            let result = iface
                .call_teardown(&mut store, session, &request)?
                .map(dest_teardown_report_to_sdk)
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store, session);
            result
        }
        PluginKind::Transform => {
            // Transform interface does not have teardown — return noop.
            Ok(TeardownReport::noop())
        }
        PluginKind::Unknown => {
            anyhow::bail!("cannot run teardown for plugin with unknown kind")
        }
    }
}

// ── Public API for benchmarks crate ──────────────────────────────────
//
// The benchmarks crate needs direct access to the low-level runner
// functions. These wrappers expose the internal runner logic through
// the `StreamRunContext` facade that benchmarks expect.

/// Shared context for running a plugin stream (source, destination, or
/// transform). Used by the benchmarks crate for direct WASM invocations.
pub struct StreamRunContext<'a> {
    pub component: &'a LoadedComponent,
    pub state_backend: Arc<dyn StateBackend>,
    pub pipeline_name: &'a str,
    pub metric_run_label: &'a str,
    pub plugin_id: &'a str,
    pub plugin_version: &'a str,
    pub stream_ctx: &'a StreamContext,
    pub permissions: Option<&'a Permissions>,
    pub compression: Option<CompressionCodec>,
}

/// Result of running a source plugin for a single stream (benchmark API).
pub struct SourceStreamOutcome {
    pub duration_secs: f64,
    pub summary: ReadSummary,
    pub checkpoints: Vec<Checkpoint>,
}

/// Result of running a destination plugin for a single stream (benchmark API).
pub struct DestinationStreamOutcome {
    pub duration_secs: f64,
    pub summary: WriteSummary,
    pub wasm_instantiation_secs: f64,
    pub frame_receive_secs: f64,
    pub checkpoints: Vec<Checkpoint>,
}

/// Result of running a transform plugin for a single stream (benchmark API).
pub struct TransformStreamOutcome {
    pub duration_secs: f64,
    pub summary: TransformSummary,
}

/// Run a source plugin for a single stream (benchmark API).
///
/// # Errors
///
/// Returns an error if the component cannot be instantiated, opened, run, or
/// closed cleanly for the given stream.
#[allow(clippy::needless_pass_by_value)]
pub fn run_source_stream_bench(
    ctx: &StreamRunContext<'_>,
    frame_sender: mpsc::SyncSender<Frame>,
    source_config: &serde_json::Value,
    stats: Arc<Mutex<RunStats>>,
    on_batch_emitted: Option<Arc<dyn Fn(u64) + Send + Sync>>,
) -> Result<SourceStreamOutcome, PipelineError> {
    let outcome = run_source_stream(
        ctx.component,
        ctx.state_backend.clone(),
        ctx.pipeline_name,
        ctx.metric_run_label,
        ctx.plugin_id,
        ctx.plugin_version,
        ctx.stream_ctx,
        ctx.permissions,
        None, // No sandbox overrides for benchmarks
        ctx.compression,
        frame_sender,
        source_config,
        stats,
        on_batch_emitted,
        Arc::new(AtomicBool::new(false)),
    )?;
    Ok(SourceStreamOutcome {
        duration_secs: outcome.duration_secs,
        summary: outcome.summary,
        checkpoints: outcome.checkpoints,
    })
}

/// Run a destination plugin for a single stream (benchmark API).
///
/// # Errors
///
/// Returns an error if the component cannot be instantiated, opened, consume
/// all input frames, or close cleanly for the given stream.
#[allow(clippy::needless_pass_by_value)]
pub fn run_destination_stream_bench(
    ctx: &StreamRunContext<'_>,
    frame_receiver: mpsc::Receiver<Frame>,
    dlq_records: Arc<Mutex<Vec<rapidbyte_types::envelope::DlqRecord>>>,
    dest_config: &serde_json::Value,
    stats: Arc<Mutex<RunStats>>,
) -> Result<DestinationStreamOutcome, PipelineError> {
    let outcome = run_destination_stream(
        ctx.component,
        ctx.state_backend.clone(),
        ctx.pipeline_name,
        ctx.metric_run_label,
        ctx.plugin_id,
        ctx.plugin_version,
        ctx.stream_ctx,
        ctx.permissions,
        None, // No sandbox overrides for benchmarks
        ctx.compression,
        frame_receiver,
        dlq_records,
        dest_config,
        stats,
        Arc::new(AtomicBool::new(false)),
    )?;
    Ok(DestinationStreamOutcome {
        duration_secs: outcome.duration_secs,
        summary: outcome.summary,
        wasm_instantiation_secs: outcome.wasm_instantiation_secs,
        frame_receive_secs: outcome.frame_receive_secs,
        checkpoints: outcome.checkpoints,
    })
}

/// Run a transform plugin for a single stream (benchmark API).
///
/// # Errors
///
/// Returns an error if the component cannot be instantiated, opened, consume
/// input frames, emit output frames, or close cleanly for the given stream.
#[allow(clippy::needless_pass_by_value)]
pub fn run_transform_stream_bench(
    ctx: &StreamRunContext<'_>,
    frame_receiver: mpsc::Receiver<Frame>,
    frame_sender: mpsc::SyncSender<Frame>,
    dlq_records: Arc<Mutex<Vec<rapidbyte_types::envelope::DlqRecord>>>,
    transform_index: usize,
    transform_config: &serde_json::Value,
) -> Result<TransformStreamOutcome, PipelineError> {
    let outcome = run_transform_stream(
        ctx.component,
        ctx.state_backend.clone(),
        ctx.pipeline_name,
        ctx.metric_run_label,
        ctx.plugin_id,
        ctx.plugin_version,
        ctx.stream_ctx,
        ctx.permissions,
        None, // No sandbox overrides for benchmarks
        ctx.compression,
        frame_receiver,
        frame_sender,
        dlq_records,
        transform_index,
        transform_config,
        Arc::new(AtomicBool::new(false)),
    )?;
    Ok(TransformStreamOutcome {
        duration_secs: outcome.duration_secs,
        summary: outcome.summary,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        config_error_as_validation_failure, handle_close_result, parse_compression,
        plugin_instance_key,
    };
    use rapidbyte_runtime::CompressionCodec;
    use rapidbyte_types::stream::StreamContext;
    use rapidbyte_types::validation::ValidationStatus;

    #[test]
    fn test_handle_close_result_ok() {
        handle_close_result::<String, _>(Ok(Ok(())), "Source", "users", |e| e);
    }

    #[test]
    fn parse_compression_lz4() {
        let result = parse_compression(Some("lz4")).unwrap();
        assert!(matches!(result, Some(CompressionCodec::Lz4)));
    }

    #[test]
    fn parse_compression_zstd() {
        let result = parse_compression(Some("zstd")).unwrap();
        assert!(matches!(result, Some(CompressionCodec::Zstd)));
    }

    #[test]
    fn parse_compression_none_input() {
        let result = parse_compression(None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn parse_compression_empty_string_returns_none() {
        let result = parse_compression(Some("")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn parse_compression_invalid_returns_error() {
        let result = parse_compression(Some("brotli"));
        assert!(result.is_err());
    }

    #[test]
    fn config_error_as_validation_failure_maps_config_category() {
        let err = rapidbyte_types::error::PluginError::config("BAD_CONFIG", "invalid config");

        let report = config_error_as_validation_failure("Transform", &err)
            .expect("config errors should map to failed validation");
        assert_eq!(report.status, ValidationStatus::Failed);
        assert!(report.message.contains("invalid config"));
    }

    fn test_stream_ctx(stream_name: &str, partition_index: Option<u32>) -> StreamContext {
        let mut ctx = StreamContext::test_default(stream_name);
        ctx.partition_index = partition_index;
        ctx
    }

    #[test]
    fn plugin_instance_key_without_ordinal() {
        let ctx = test_stream_ctx("public.users", Some(2));
        let key = plugin_instance_key("source", "rapidbyte/source-pg", &ctx, None);
        assert_eq!(key, "source:rapidbyte/source-pg:public.users:p2");
    }

    #[test]
    fn plugin_instance_key_with_ordinal() {
        let ctx = test_stream_ctx("public.orders", None);
        let key = plugin_instance_key("transform", "rapidbyte/transform-sql", &ctx, Some(3));
        assert_eq!(key, "transform:rapidbyte/transform-sql:public.orders:p0:i3");
    }
}
