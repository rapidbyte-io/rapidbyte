//! WASM plugin runner adapter — self-contained implementation.
//!
//! Wraps `rapidbyte-runtime` to implement the [`PluginRunner`] port trait.
//! All WASM execution logic (source, destination, transform, validate,
//! discover) is implemented directly in this module — no delegation to
//! legacy runner functions.

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
use rapidbyte_types::catalog::Catalog;
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::error::ValidationResult;
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::metric::{ReadSummary, TransformSummary, WriteSummary};
use rapidbyte_types::state::RunStats;
use rapidbyte_types::state_backend::StateBackend;
use rapidbyte_types::stream::StreamContext;
use rapidbyte_types::wire::PluginKind;

use crate::adapter::engine_factory::noop_state_backend;
use crate::domain::error::PipelineError;
use crate::domain::ports::runner::{
    CheckComponentStatus, DestinationOutcome, DestinationRunParams, DiscoverParams,
    DiscoveredStream, PluginRunner, SourceOutcome, SourceRunParams, TransformOutcome,
    TransformRunParams, ValidateParams,
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

        let catalog = tokio::task::spawn_blocking(move || {
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

        let streams = catalog
            .streams
            .into_iter()
            .map(|s| {
                let catalog_json = serde_json::to_string(&s)
                    .map_err(|e| PipelineError::infra(format!("failed to serialize catalog: {e}")));
                match catalog_json {
                    Ok(json) => Ok(DiscoveredStream {
                        name: s.name,
                        catalog_json: json,
                    }),
                    Err(e) => Err(e),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(streams)
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

/// Serialize a [`StreamContext`] to JSON for the `RunRequest`.
fn serialize_stream_context(stream_ctx: &StreamContext) -> Result<String, PipelineError> {
    serde_json::to_string(stream_ctx)
        .context("Failed to serialize StreamContext")
        .map_err(PipelineError::Infrastructure)
}

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
        .source_checkpoints(source_checkpoints.clone());
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

    let ctx_json = serialize_stream_context(stream_ctx)?;

    tracing::info!(stream = stream_ctx.stream_name, "Starting source read");
    let run_request = source_bindings::rapidbyte::plugin::types::RunRequest {
        phase: source_bindings::rapidbyte::plugin::types::RunPhase::Read,
        stream_context_json: ctx_json,
        dry_run: false,
        max_records: None,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::infra(format!("Failed to call source run: {e}")))?;

    let summary = match run_result {
        Ok(summary) => {
            let Some(summary) = summary.read else {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::infra(
                    "source run summary missing read section",
                ));
            };
            ReadSummary {
                records_read: summary.records_read,
                bytes_read: summary.bytes_read,
                batches_emitted: summary.batches_emitted,
                checkpoint_count: summary.checkpoint_count,
                records_skipped: summary.records_skipped,
            }
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
        .dlq_records(dlq_records.clone());
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
    let ctx_json = serialize_stream_context(stream_ctx)?;

    tracing::info!(
        stream = stream_ctx.stream_name,
        "Starting destination write"
    );
    let run_request = dest_bindings::rapidbyte::plugin::types::RunRequest {
        phase: dest_bindings::rapidbyte::plugin::types::RunPhase::Write,
        stream_context_json: ctx_json,
        dry_run: false,
        max_records: None,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::infra(format!("Failed to call destination run: {e}")))?;

    let summary = match run_result {
        Ok(summary) => {
            let Some(summary) = summary.write else {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::infra(
                    "destination run summary missing write section",
                ));
            };
            WriteSummary {
                records_written: summary.records_written,
                bytes_written: summary.bytes_written,
                batches_written: summary.batches_written,
                checkpoint_count: summary.checkpoint_count,
                records_failed: summary.records_failed,
            }
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
        .dest_checkpoints(dest_checkpoints);
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

    let ctx_json = serialize_stream_context(stream_ctx)?;

    tracing::info!(stream = stream_ctx.stream_name, "Starting transform");
    let run_request = transform_bindings::rapidbyte::plugin::types::RunRequest {
        phase: transform_bindings::rapidbyte::plugin::types::RunPhase::Transform,
        stream_context_json: ctx_json,
        dry_run: false,
        max_records: None,
    };
    let run_result = iface
        .call_run(&mut store, session, &run_request)
        .map_err(|e| PipelineError::infra(format!("Failed to call transform run: {e}")))?;

    let summary = match run_result {
        Ok(summary) => {
            let Some(summary) = summary.transform else {
                let _ = iface.call_close(&mut store, session);
                return Err(PipelineError::infra(
                    "transform run summary missing transform section",
                ));
            };
            TransformSummary {
                records_in: summary.records_in,
                records_out: summary.records_out,
                bytes_in: summary.bytes_in,
                bytes_out: summary.bytes_out,
                batches_processed: summary.batches_processed,
            }
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
) -> anyhow::Result<ValidationResult> {
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

    let mut store = module.new_store(host_state, None);
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
                .call_validate(&mut store, session)?
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

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Destination open failed: {e}"))?;

            let result = iface
                .call_validate(&mut store, session)?
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

            let session = iface
                .call_open(&mut store, &config_json)?
                .map_err(transform_error_to_sdk)
                .map_err(|e| anyhow::anyhow!("Transform open failed: {e}"))?;

            let result = iface
                .call_validate(&mut store, session)?
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

// ── Discover runner ─────────────────────────────────────────────────

/// Discover available streams from a source plugin.
fn run_discover_impl(
    module: &LoadedComponent,
    plugin_id: &str,
    plugin_version: &str,
    config: &serde_json::Value,
    permissions: Option<&Permissions>,
    sandbox_overrides: Option<&SandboxOverrides>,
) -> anyhow::Result<Catalog> {
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

    let mut store = module.new_store(host_state, None);
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

    let discover_json = iface
        .call_discover(&mut store, session)?
        .map_err(source_error_to_sdk)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let catalog = serde_json::from_str::<Catalog>(&discover_json)
        .context("Failed to parse discover catalog JSON")?;

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

    Ok(catalog)
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
    )?;
    Ok(TransformStreamOutcome {
        duration_secs: outcome.duration_secs,
        summary: outcome.summary,
    })
}

#[cfg(test)]
mod tests {
    use super::{handle_close_result, parse_compression, plugin_instance_key};
    use rapidbyte_runtime::CompressionCodec;
    use rapidbyte_types::catalog::SchemaHint;
    use rapidbyte_types::stream::{StreamContext, StreamLimits, StreamPolicies};
    use rapidbyte_types::wire::SyncMode;

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

    fn test_stream_ctx(stream_name: &str, partition_index: Option<u32>) -> StreamContext {
        StreamContext {
            stream_name: stream_name.to_string(),
            source_stream_name: None,
            schema: SchemaHint::Columns(vec![]),
            sync_mode: SyncMode::FullRefresh,
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
            selected_columns: None,
            partition_key: None,
            partition_count: None,
            partition_index,
            effective_parallelism: None,
            partition_strategy: None,
            copy_flush_bytes_override: None,
        }
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
