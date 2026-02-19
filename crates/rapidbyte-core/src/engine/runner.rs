use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use wasmedge_sdk::vm::SyncInst;
use wasmedge_sdk::{Module, Store, Vm};

use rapidbyte_sdk::errors::ValidationResult;
use rapidbyte_sdk::manifest::Permissions;
use rapidbyte_sdk::protocol::{
    Catalog, Checkpoint, ConfigBlob, OpenContext, ReadSummary, StreamContext, TransformSummary,
    WriteSummary,
};

use super::errors::PipelineError;
use super::vm_factory::create_secure_wasi_module;
use crate::runtime::compression::CompressionCodec;
use crate::runtime::connector_handle::ConnectorHandle;
use crate::runtime::host_functions::{Frame, HostState};
use crate::runtime::wasm_runtime::{self, WasmRuntime};
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
    // Host overhead breakdown
    pub dest_vm_setup_secs: f64,
    pub dest_recv_secs: f64,
    pub wasm_overhead_secs: f64,
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

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_source(
    module: Module,
    sender: mpsc::SyncSender<Frame>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    connector_id: &str,
    connector_version: &str,
    source_config: &serde_json::Value,
    stream_ctxs: &[StreamContext],
    stats: Arc<Mutex<RunStats>>,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
) -> Result<(f64, ReadSummary, Vec<Checkpoint>), PipelineError> {
    let phase_start = Instant::now();

    // Keep a clone for sending stream-boundary sentinels after each run_read
    let sentinel_sender = sender.clone();

    // Shared handle so we can update current_stream before each run_read
    let current_stream = Arc::new(Mutex::new(String::new()));

    // Shared checkpoint store — clone the Arc so we can read checkpoints after the VM runs
    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));

    let host_state = HostState {
        pipeline_name: pipeline_name.to_string(),
        current_stream: current_stream.clone(),
        state_backend,
        stats,
        batch_sender: Some(sender),
        next_batch_id: 1,
        batch_receiver: None,
        pending_batch: None,
        last_error: None,
        compression,
        source_checkpoints: source_checkpoints.clone(),
        dest_checkpoints: Arc::new(Mutex::new(Vec::new())),
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = create_secure_wasi_module(permissions)?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);

    // Lifecycle: open
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(source_config.clone()),
        connector_id: connector_id.to_string(),
        connector_version: connector_version.to_string(),
    };

    tracing::info!("Opening source connector");
    let _open_info = handle.open(&open_ctx)?;

    // Lifecycle: run_read per stream (sequential)
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
            // Update current_stream so host functions use the correct scope
            *current_stream.lock().unwrap() = stream_ctx.stream_name.clone();

            tracing::info!(stream = stream_ctx.stream_name, "Starting source read");
            let summary = handle.run_read(stream_ctx)?;
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
            if summary.perf.is_some() {
                total_summary.perf = summary.perf;
            }

            // Signal end-of-stream to dest (typed sentinel, not magic empty vec)
            let _ = sentinel_sender.send(Frame::EndStream);
        }
        Ok(())
    })();

    // Lifecycle: close (always, even if stream loop failed)
    tracing::info!("Closing source connector");
    if let Err(e) = handle.close() {
        tracing::warn!("Source close failed: {}", e);
    }

    stream_result?;

    // Extract source checkpoints collected during the run
    let checkpoints = source_checkpoints.lock().unwrap().drain(..).collect();

    // sender is dropped here -> dest sees final EOF on host_next_batch
    Ok((
        phase_start.elapsed().as_secs_f64(),
        total_summary,
        checkpoints,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_destination(
    module: Module,
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
) -> Result<(f64, WriteSummary, f64, f64, Vec<Checkpoint>), PipelineError> {
    let phase_start = Instant::now();
    let vm_setup_start = Instant::now();

    // Shared handle so we can update current_stream before each run_write
    let current_stream = Arc::new(Mutex::new(String::new()));

    // Shared checkpoint store — clone the Arc so we can read checkpoints after the VM runs
    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));

    let host_state = HostState {
        pipeline_name: pipeline_name.to_string(),
        current_stream: current_stream.clone(),
        state_backend,
        stats,
        batch_sender: None,
        next_batch_id: 1,
        batch_receiver: Some(receiver),
        pending_batch: None,
        last_error: None,
        compression,
        source_checkpoints: Arc::new(Mutex::new(Vec::new())),
        dest_checkpoints: dest_checkpoints.clone(),
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = create_secure_wasi_module(permissions)?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);

    // Lifecycle: open
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(dest_config.clone()),
        connector_id: connector_id.to_string(),
        connector_version: connector_version.to_string(),
    };

    let vm_setup_secs = vm_setup_start.elapsed().as_secs_f64();

    tracing::info!("Opening destination connector");
    let _open_info = handle.open(&open_ctx)?;

    // Lifecycle: run_write per stream (sequential)
    // The dest connector calls host_next_batch() internally to pull batches
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
            // Update current_stream so host functions use the correct scope
            *current_stream.lock().unwrap() = stream_ctx.stream_name.clone();

            tracing::info!(stream = stream_ctx.stream_name, "Starting dest write");
            let summary = handle.run_write(stream_ctx)?;
            tracing::info!(
                stream = stream_ctx.stream_name,
                records = summary.records_written,
                bytes = summary.bytes_written,
                "Dest write complete for stream"
            );
            total_summary.records_written += summary.records_written;
            total_summary.bytes_written += summary.bytes_written;
            total_summary.batches_written += summary.batches_written;
            total_summary.checkpoint_count += summary.checkpoint_count;
            total_summary.records_failed += summary.records_failed;
            if summary.perf.is_some() {
                total_summary.perf = summary.perf;
            }
        }
        Ok(())
    })();

    let recv_secs = recv_start.elapsed().as_secs_f64();

    // Lifecycle: close (always, even if stream loop failed)
    tracing::info!("Closing destination connector");
    if let Err(e) = handle.close() {
        tracing::warn!("Destination close failed: {}", e);
    }

    stream_result?;

    // Extract dest checkpoints collected during the run
    let checkpoints = dest_checkpoints.lock().unwrap().drain(..).collect();

    Ok((
        phase_start.elapsed().as_secs_f64(),
        total_summary,
        vm_setup_secs,
        recv_secs,
        checkpoints,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_transform(
    module: Module,
    receiver: mpsc::Receiver<Frame>,
    sender: mpsc::SyncSender<Frame>,
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

    // Clone sender for sentinel forwarding after each stream
    let sentinel_sender = sender.clone();

    let host_state = HostState {
        pipeline_name: pipeline_name.to_string(),
        current_stream: current_stream.clone(),
        state_backend,
        stats,
        batch_sender: Some(sender),
        next_batch_id: 1,
        batch_receiver: Some(receiver),
        pending_batch: None,
        last_error: None,
        compression,
        source_checkpoints: Arc::new(Mutex::new(Vec::new())),
        dest_checkpoints: Arc::new(Mutex::new(Vec::new())),
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = create_secure_wasi_module(permissions)?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);

    // Lifecycle: open
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(transform_config.clone()),
        connector_id: connector_id.to_string(),
        connector_version: connector_version.to_string(),
    };

    tracing::info!("Opening transform connector");
    let _open_info = handle.open(&open_ctx)?;

    // Lifecycle: run_transform per stream (sequential)
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

            tracing::info!(stream = stream_ctx.stream_name, "Starting transform");
            let summary = handle.run_transform(stream_ctx)?;
            tracing::info!(
                stream = stream_ctx.stream_name,
                records_in = summary.records_in,
                records_out = summary.records_out,
                "Transform complete for stream"
            );
            total_summary.records_in += summary.records_in;
            total_summary.records_out += summary.records_out;
            total_summary.bytes_in += summary.bytes_in;
            total_summary.bytes_out += summary.bytes_out;
            total_summary.batches_processed += summary.batches_processed;

            // Forward end-of-stream sentinel downstream
            let _ = sentinel_sender.send(Frame::EndStream);
        }
        Ok(())
    })();

    // Lifecycle: close (always, even if stream loop failed)
    tracing::info!("Closing transform connector");
    if let Err(e) = handle.close() {
        tracing::warn!("Transform close failed: {}", e);
    }

    stream_result?;

    // sender (via sentinel_sender) is dropped here -> downstream sees final EOF
    Ok((phase_start.elapsed().as_secs_f64(), total_summary))
}

pub(crate) fn validate_connector(
    wasm_path: &std::path::Path,
    connector_id: &str,
    connector_version: &str,
    config: &serde_json::Value,
    permissions: Option<&Permissions>,
) -> Result<ValidationResult> {
    let runtime = WasmRuntime::new()?;
    let module = runtime.load_module(wasm_path)?;

    let state = Arc::new(SqliteStateBackend::in_memory()?);

    let host_state = HostState {
        pipeline_name: "check".to_string(),
        current_stream: Arc::new(Mutex::new("check".to_string())),
        state_backend: state,
        stats: Arc::new(Mutex::new(RunStats::default())),
        batch_sender: None,
        next_batch_id: 1,
        batch_receiver: None,
        pending_batch: None,
        last_error: None,
        compression: None,
        source_checkpoints: Arc::new(Mutex::new(Vec::new())),
        dest_checkpoints: Arc::new(Mutex::new(Vec::new())),
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = create_secure_wasi_module(permissions)?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);

    // Wrap config in OpenContext for rb_validate
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(config.clone()),
        connector_id: connector_id.to_string(),
        connector_version: connector_version.to_string(),
    };
    handle.validate(&serde_json::to_value(&open_ctx).unwrap())
}

/// Discover available streams from a source connector.
/// Follows the same VM setup pattern as `validate_connector`, then calls
/// open -> discover -> close to retrieve the catalog.
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

    let host_state = HostState {
        pipeline_name: "discover".to_string(),
        current_stream: Arc::new(Mutex::new("discover".to_string())),
        state_backend: state,
        stats: Arc::new(Mutex::new(RunStats::default())),
        batch_sender: None,
        next_batch_id: 1,
        batch_receiver: None,
        pending_batch: None,
        last_error: None,
        compression: None,
        source_checkpoints: Arc::new(Mutex::new(Vec::new())),
        dest_checkpoints: Arc::new(Mutex::new(Vec::new())),
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = create_secure_wasi_module(permissions)?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);

    // Lifecycle: open
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(config.clone()),
        connector_id: connector_id.to_string(),
        connector_version: connector_version.to_string(),
    };

    tracing::info!("Opening connector for discover");
    handle.open(&open_ctx)?;

    // Lifecycle: discover
    let catalog = handle.discover()?;

    // Lifecycle: close
    tracing::info!("Closing connector after discover");
    if let Err(e) = handle.close() {
        tracing::warn!("Connector close failed after discover: {}", e);
    }

    Ok(catalog)
}
