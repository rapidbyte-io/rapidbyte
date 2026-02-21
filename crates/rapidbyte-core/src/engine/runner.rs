use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use wasmtime::component::Linker;
use wasmtime::Store;

use rapidbyte_sdk::errors::ValidationResult;
use rapidbyte_sdk::manifest::Permissions;
use rapidbyte_sdk::protocol::{
    Catalog, Checkpoint, ConnectorRole, ReadSummary, StreamContext, TransformSummary, WriteSummary,
};

use super::errors::PipelineError;
use crate::runtime::component_runtime::{
    self, dest_bindings, dest_error_to_sdk, source_bindings, source_error_to_sdk,
    transform_bindings, transform_error_to_sdk, ComponentHostState, Frame, HostTimings,
    LoadedComponent, WasmRuntime,
};
use super::compression::CompressionCodec;
use crate::state::backend::{RunStats, StateBackend};
use crate::state::sqlite::SqliteStateBackend;

/// Result of a pipeline run.
#[derive(Debug)]
pub struct PipelineResult {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub duration_secs: f64,
    pub source_duration_secs: f64,
    pub dest_duration_secs: f64,
    pub source_module_load_ms: u64,
    pub dest_module_load_ms: u64,
    // Dest sub-phase timing (from connector)
    pub dest_connect_secs: f64,
    pub dest_flush_secs: f64,
    pub dest_commit_secs: f64,
    // Source sub-phase timing (from connector)
    pub source_connect_secs: f64,
    pub source_query_secs: f64,
    pub source_fetch_secs: f64,
    pub source_arrow_encode_secs: f64,
    pub dest_arrow_decode_secs: f64,
    // Host overhead breakdown
    pub dest_vm_setup_secs: f64,
    pub dest_recv_secs: f64,
    pub wasm_overhead_secs: f64,
    // Host function timing
    pub source_emit_nanos: u64,
    pub source_compress_nanos: u64,
    pub source_emit_count: u64,
    pub dest_recv_nanos: u64,
    pub dest_decompress_nanos: u64,
    pub dest_recv_count: u64,
    // Transform timing
    pub transform_count: usize,
    pub transform_duration_secs: f64,
    pub transform_module_load_ms: Vec<u64>,
    pub retry_count: u32,
}

/// Result of a pipeline check.
#[derive(Debug)]
pub struct CheckResult {
    pub source_validation: ValidationResult,
    pub destination_validation: ValidationResult,
    pub transform_validations: Vec<ValidationResult>,
    pub state_ok: bool,
}

fn create_source_linker(engine: &wasmtime::Engine) -> Result<Linker<ComponentHostState>> {
    let mut linker = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
        .context("Failed to add WASI imports for source")?;
    source_bindings::RapidbyteSource::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
        &mut linker,
        |state| state,
    )
    .context("Failed to add rapidbyte source host imports")?;
    Ok(linker)
}

fn create_dest_linker(engine: &wasmtime::Engine) -> Result<Linker<ComponentHostState>> {
    let mut linker = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
        .context("Failed to add WASI imports for destination")?;
    dest_bindings::RapidbyteDestination::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
        &mut linker,
        |state| state,
    )
    .context("Failed to add rapidbyte destination host imports")?;
    Ok(linker)
}

fn create_transform_linker(engine: &wasmtime::Engine) -> Result<Linker<ComponentHostState>> {
    let mut linker = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
        .context("Failed to add WASI imports for transform")?;
    transform_bindings::RapidbyteTransform::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
        &mut linker,
        |state| state,
    )
    .context("Failed to add rapidbyte transform host imports")?;
    Ok(linker)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_source(
    module: LoadedComponent,
    sender: mpsc::Sender<Frame>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    connector_id: &str,
    connector_version: &str,
    source_config: &serde_json::Value,
    stream_ctxs: &[StreamContext],
    stats: Arc<Mutex<RunStats>>,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
) -> Result<(f64, ReadSummary, Vec<Checkpoint>, HostTimings), PipelineError> {
    let phase_start = Instant::now();

    let current_stream = Arc::new(Mutex::new(String::new()));
    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let source_timings = Arc::new(Mutex::new(HostTimings::default()));

    let stats_ref = stats.clone();
    let host_state = ComponentHostState::new(
        pipeline_name.to_string(),
        connector_id.to_string(),
        current_stream.clone(),
        state_backend,
        stats,
        Some(sender.clone()),
        None,
        compression,
        source_checkpoints.clone(),
        Arc::new(Mutex::new(Vec::new())),
        source_timings.clone(),
        permissions,
        source_config,
    )
    .map_err(PipelineError::Infrastructure)?;

    let mut store = Store::new(&module.engine, host_state);
    let linker = create_source_linker(&module.engine).map_err(PipelineError::Infrastructure)?;
    let bindings =
        source_bindings::RapidbyteSource::instantiate(&mut store, &module.component, &linker)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let iface = bindings.rapidbyte_connector_source_connector();

    let source_config_json = serde_json::to_string(source_config)
        .context("Failed to serialize source config")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(connector = connector_id, version = connector_version, "Opening source connector");
    let open_result = iface
        .call_open(&mut store, &source_config_json)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;
    if let Err(err) = open_result {
        return Err(PipelineError::Connector(source_error_to_sdk(err)));
    }

    let mut total_summary = ReadSummary {
        records_read: 0,
        bytes_read: 0,
        batches_emitted: 0,
        checkpoint_count: 0,
        records_skipped: 0,
        perf: None,
    };

    let stream_result: Result<(), PipelineError> = (|| {
        for stream_ctx in stream_ctxs {
            *current_stream.lock().unwrap() = stream_ctx.stream_name.clone();

            let ctx_json = serde_json::to_string(stream_ctx)
                .context("Failed to serialize StreamContext")
                .map_err(PipelineError::Infrastructure)?;

            tracing::info!(stream = stream_ctx.stream_name, "Starting source read");
            let run_result = iface
                .call_run_read(&mut store, &ctx_json)
                .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

            let summary = match run_result {
                Ok(summary) => ReadSummary {
                    records_read: summary.records_read,
                    bytes_read: summary.bytes_read,
                    batches_emitted: summary.batches_emitted,
                    checkpoint_count: summary.checkpoint_count,
                    records_skipped: summary.records_skipped,
                    perf: None,
                },
                Err(err) => return Err(PipelineError::Connector(source_error_to_sdk(err))),
            };

            tracing::info!(
                stream = stream_ctx.stream_name,
                records = summary.records_read,
                bytes = summary.bytes_read,
                "Source read complete for stream"
            );

            total_summary.records_read += summary.records_read;
            total_summary.bytes_read += summary.bytes_read;
            total_summary.batches_emitted += summary.batches_emitted;
            total_summary.checkpoint_count += summary.checkpoint_count;
            total_summary.records_skipped += summary.records_skipped;

            {
                let mut s = stats_ref.lock().unwrap();
                s.records_read = total_summary.records_read;
                s.bytes_read = total_summary.bytes_read;
            }

            let _ = sender.blocking_send(Frame::EndStream);
        }
        Ok(())
    })();

    tracing::info!(connector = connector_id, version = connector_version, "Closing source connector");
    match iface.call_close(&mut store) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            tracing::warn!("Source close failed: {}", source_error_to_sdk(err));
        }
        Err(err) => {
            tracing::warn!("Source close trap: {}", err);
        }
    }

    stream_result?;

    let checkpoints = source_checkpoints
        .lock()
        .unwrap()
        .drain(..)
        .collect::<Vec<_>>();
    let source_host_timings = source_timings.lock().unwrap().clone();

    Ok((
        phase_start.elapsed().as_secs_f64(),
        total_summary,
        checkpoints,
        source_host_timings,
    ))
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub(crate) fn run_destination(
    module: LoadedComponent,
    receiver: mpsc::Receiver<Frame>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    connector_id: &str,
    connector_version: &str,
    dest_config: &serde_json::Value,
    stream_ctxs: &[StreamContext],
    stats: Arc<Mutex<RunStats>>,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
) -> Result<(f64, WriteSummary, f64, f64, Vec<Checkpoint>, HostTimings), PipelineError> {
    let phase_start = Instant::now();
    let vm_setup_start = Instant::now();

    let current_stream = Arc::new(Mutex::new(String::new()));
    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let dest_timings = Arc::new(Mutex::new(HostTimings::default()));

    let stats_ref = stats.clone();
    let host_state = ComponentHostState::new(
        pipeline_name.to_string(),
        connector_id.to_string(),
        current_stream.clone(),
        state_backend,
        stats,
        None,
        Some(receiver),
        compression,
        Arc::new(Mutex::new(Vec::new())),
        dest_checkpoints.clone(),
        dest_timings.clone(),
        permissions,
        dest_config,
    )
    .map_err(PipelineError::Infrastructure)?;

    let mut store = Store::new(&module.engine, host_state);
    let linker = create_dest_linker(&module.engine).map_err(PipelineError::Infrastructure)?;
    let bindings =
        dest_bindings::RapidbyteDestination::instantiate(&mut store, &module.component, &linker)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let iface = bindings.rapidbyte_connector_dest_connector();

    let vm_setup_secs = vm_setup_start.elapsed().as_secs_f64();

    let dest_config_json = serde_json::to_string(dest_config)
        .context("Failed to serialize destination config")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(connector = connector_id, version = connector_version, "Opening destination connector");
    let open_result = iface
        .call_open(&mut store, &dest_config_json)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;
    if let Err(err) = open_result {
        return Err(PipelineError::Connector(dest_error_to_sdk(err)));
    }

    let recv_start = Instant::now();
    let mut total_summary = WriteSummary {
        records_written: 0,
        bytes_written: 0,
        batches_written: 0,
        checkpoint_count: 0,
        records_failed: 0,
        perf: None,
    };

    let stream_result: Result<(), PipelineError> = (|| {
        for stream_ctx in stream_ctxs {
            *current_stream.lock().unwrap() = stream_ctx.stream_name.clone();

            let ctx_json = serde_json::to_string(stream_ctx)
                .context("Failed to serialize StreamContext")
                .map_err(PipelineError::Infrastructure)?;

            tracing::info!(
                stream = stream_ctx.stream_name,
                "Starting destination write"
            );
            let run_result = iface
                .call_run_write(&mut store, &ctx_json)
                .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

            let summary = match run_result {
                Ok(summary) => WriteSummary {
                    records_written: summary.records_written,
                    bytes_written: summary.bytes_written,
                    batches_written: summary.batches_written,
                    checkpoint_count: summary.checkpoint_count,
                    records_failed: summary.records_failed,
                    perf: None,
                },
                Err(err) => return Err(PipelineError::Connector(dest_error_to_sdk(err))),
            };

            tracing::info!(
                stream = stream_ctx.stream_name,
                records = summary.records_written,
                bytes = summary.bytes_written,
                "Destination write complete for stream"
            );

            total_summary.records_written += summary.records_written;
            total_summary.bytes_written += summary.bytes_written;
            total_summary.batches_written += summary.batches_written;
            total_summary.checkpoint_count += summary.checkpoint_count;
            total_summary.records_failed += summary.records_failed;

            {
                let mut s = stats_ref.lock().unwrap();
                s.records_written = total_summary.records_written;
            }
        }
        Ok(())
    })();

    let recv_secs = recv_start.elapsed().as_secs_f64();

    tracing::info!(connector = connector_id, version = connector_version, "Closing destination connector");
    match iface.call_close(&mut store) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            tracing::warn!("Destination close failed: {}", dest_error_to_sdk(err));
        }
        Err(err) => {
            tracing::warn!("Destination close trap: {}", err);
        }
    }

    stream_result?;

    let checkpoints = dest_checkpoints
        .lock()
        .unwrap()
        .drain(..)
        .collect::<Vec<_>>();
    let dest_host_timings = dest_timings.lock().unwrap().clone();

    Ok((
        phase_start.elapsed().as_secs_f64(),
        total_summary,
        vm_setup_secs,
        recv_secs,
        checkpoints,
        dest_host_timings,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_transform(
    module: LoadedComponent,
    receiver: mpsc::Receiver<Frame>,
    sender: mpsc::Sender<Frame>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    connector_id: &str,
    connector_version: &str,
    transform_config: &serde_json::Value,
    stream_ctxs: &[StreamContext],
    stats: Arc<Mutex<RunStats>>,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
) -> Result<(f64, TransformSummary), PipelineError> {
    let phase_start = Instant::now();

    let current_stream = Arc::new(Mutex::new(String::new()));
    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let timings = Arc::new(Mutex::new(HostTimings::default()));

    let host_state = ComponentHostState::new(
        pipeline_name.to_string(),
        connector_id.to_string(),
        current_stream.clone(),
        state_backend,
        stats,
        Some(sender.clone()),
        Some(receiver),
        compression,
        source_checkpoints,
        dest_checkpoints,
        timings,
        permissions,
        transform_config,
    )
    .map_err(PipelineError::Infrastructure)?;

    let mut store = Store::new(&module.engine, host_state);
    let linker = create_transform_linker(&module.engine).map_err(PipelineError::Infrastructure)?;
    let bindings =
        transform_bindings::RapidbyteTransform::instantiate(&mut store, &module.component, &linker)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let iface = bindings.rapidbyte_connector_transform_connector();

    let transform_config_json = serde_json::to_string(transform_config)
        .context("Failed to serialize transform config")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(connector = connector_id, version = connector_version, "Opening transform connector");
    let open_result = iface
        .call_open(&mut store, &transform_config_json)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;
    if let Err(err) = open_result {
        return Err(PipelineError::Connector(transform_error_to_sdk(err)));
    }

    let mut total_summary = TransformSummary {
        records_in: 0,
        records_out: 0,
        bytes_in: 0,
        bytes_out: 0,
        batches_processed: 0,
    };

    let stream_result: Result<(), PipelineError> = (|| {
        for stream_ctx in stream_ctxs {
            *current_stream.lock().unwrap() = stream_ctx.stream_name.clone();

            let ctx_json = serde_json::to_string(stream_ctx)
                .context("Failed to serialize StreamContext")
                .map_err(PipelineError::Infrastructure)?;

            tracing::info!(stream = stream_ctx.stream_name, "Starting transform");
            let run_result = iface
                .call_run_transform(&mut store, &ctx_json)
                .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

            let summary = match run_result {
                Ok(summary) => TransformSummary {
                    records_in: summary.records_in,
                    records_out: summary.records_out,
                    bytes_in: summary.bytes_in,
                    bytes_out: summary.bytes_out,
                    batches_processed: summary.batches_processed,
                },
                Err(err) => return Err(PipelineError::Connector(transform_error_to_sdk(err))),
            };

            total_summary.records_in += summary.records_in;
            total_summary.records_out += summary.records_out;
            total_summary.bytes_in += summary.bytes_in;
            total_summary.bytes_out += summary.bytes_out;
            total_summary.batches_processed += summary.batches_processed;

            let _ = sender.blocking_send(Frame::EndStream);
        }
        Ok(())
    })();

    tracing::info!(connector = connector_id, version = connector_version, "Closing transform connector");
    match iface.call_close(&mut store) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            tracing::warn!("Transform close failed: {}", transform_error_to_sdk(err));
        }
        Err(err) => {
            tracing::warn!("Transform close trap: {}", err);
        }
    }

    stream_result?;

    Ok((phase_start.elapsed().as_secs_f64(), total_summary))
}

pub(crate) fn validate_connector(
    wasm_path: &std::path::Path,
    role: ConnectorRole,
    connector_id: &str,
    connector_version: &str,
    config: &serde_json::Value,
    permissions: Option<&Permissions>,
) -> Result<ValidationResult> {
    tracing::info!(connector = connector_id, version = connector_version, role = ?role, "Validating connector");

    let runtime = WasmRuntime::new()?;
    let module = runtime.load_module(wasm_path)?;

    let state = Arc::new(SqliteStateBackend::in_memory()?);
    let current_stream = Arc::new(Mutex::new("check".to_string()));

    let host_state = ComponentHostState::new(
        "check".to_string(),
        connector_id.to_string(),
        current_stream,
        state,
        Arc::new(Mutex::new(RunStats::default())),
        None,
        None,
        None,
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(HostTimings::default())),
        permissions,
        config,
    )?;

    let mut store = Store::new(&module.engine, host_state);
    let config_json = serde_json::to_string(config)?;

    match role {
        ConnectorRole::Source => {
            let linker = create_source_linker(&module.engine)?;
            let bindings = source_bindings::RapidbyteSource::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_connector_source_connector();

            if let Err(err) = iface.call_open(&mut store, &config_json)? {
                anyhow::bail!("Source open failed: {}", source_error_to_sdk(err));
            }

            let result = iface
                .call_validate(&mut store)?
                .map(component_runtime::source_validation_to_sdk)
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));

            let _ = iface.call_close(&mut store);
            result
        }
        ConnectorRole::Destination => {
            let linker = create_dest_linker(&module.engine)?;
            let bindings = dest_bindings::RapidbyteDestination::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_connector_dest_connector();

            if let Err(err) = iface.call_open(&mut store, &config_json)? {
                anyhow::bail!("Destination open failed: {}", dest_error_to_sdk(err));
            }

            let result = iface
                .call_validate(&mut store)?
                .map(component_runtime::dest_validation_to_sdk)
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));

            let _ = iface.call_close(&mut store);
            result
        }
        ConnectorRole::Transform => {
            let linker = create_transform_linker(&module.engine)?;
            let bindings = transform_bindings::RapidbyteTransform::instantiate(
                &mut store,
                &module.component,
                &linker,
            )?;
            let iface = bindings.rapidbyte_connector_transform_connector();

            if let Err(err) = iface.call_open(&mut store, &config_json)? {
                anyhow::bail!("Transform open failed: {}", transform_error_to_sdk(err));
            }

            let result = iface
                .call_validate(&mut store)?
                .map(component_runtime::transform_validation_to_sdk)
                .map_err(transform_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));

            let _ = iface.call_close(&mut store);
            result
        }
        ConnectorRole::Utility => {
            anyhow::bail!("Utility connector validation is not implemented")
        }
    }
}

/// Discover available streams from a source connector.
/// Follows open -> discover -> close.
pub(crate) fn run_discover(
    wasm_path: &std::path::Path,
    connector_id: &str,
    connector_version: &str,
    config: &serde_json::Value,
    permissions: Option<&Permissions>,
) -> Result<Catalog> {
    let runtime = WasmRuntime::new()?;
    let module = runtime.load_module(wasm_path)?;

    let state = Arc::new(SqliteStateBackend::in_memory()?);
    let current_stream = Arc::new(Mutex::new("discover".to_string()));

    let host_state = ComponentHostState::new(
        "discover".to_string(),
        connector_id.to_string(),
        current_stream,
        state,
        Arc::new(Mutex::new(RunStats::default())),
        None,
        None,
        None,
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(HostTimings::default())),
        permissions,
        config,
    )?;

    let mut store = Store::new(&module.engine, host_state);
    let linker = create_source_linker(&module.engine)?;
    let bindings =
        source_bindings::RapidbyteSource::instantiate(&mut store, &module.component, &linker)?;
    let iface = bindings.rapidbyte_connector_source_connector();

    let config_json = serde_json::to_string(config)?;

    tracing::info!(
        connector = connector_id,
        version = connector_version,
        "Opening source connector for discover"
    );
    if let Err(err) = iface.call_open(&mut store, &config_json)? {
        anyhow::bail!(
            "Source open failed for discover: {}",
            source_error_to_sdk(err)
        );
    }

    let discover_json = iface
        .call_discover(&mut store)?
        .map_err(source_error_to_sdk)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let catalog = serde_json::from_str::<Catalog>(&discover_json)
        .context("Failed to parse discover catalog JSON")?;

    tracing::info!(
        connector = connector_id,
        version = connector_version,
        "Closing source connector after discover"
    );
    if let Err(err) = iface.call_close(&mut store)? {
        tracing::warn!(
            "Source close failed after discover: {}",
            source_error_to_sdk(err)
        );
    }

    Ok(catalog)
}
