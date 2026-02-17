use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use wasmedge_sdk::vm::SyncInst;
use wasmedge_sdk::wasi::WasiModule;
use wasmedge_sdk::{Module, Store, Vm};

use rapidbyte_sdk::errors::ValidationResult;
use rapidbyte_sdk::protocol::{
    ConfigBlob, OpenContext, ReadRequest, ReadSummaryV1, SchemaHint, StreamContext, StreamLimits,
    StreamPolicies, StreamSelection, SyncMode, WriteSummary, WriteSummaryV1,
};

use crate::pipeline::types::PipelineConfig;
use crate::runtime::connector_handle::ConnectorHandle;
use crate::runtime::host_functions::HostState;
use crate::runtime::wasm_runtime::{self, WasmRuntime};
use crate::state::backend::{RunStats, RunStatus, StateBackend};
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
    // Host overhead breakdown
    pub dest_vm_setup_secs: f64,
    pub dest_recv_secs: f64,
    pub wasm_overhead_secs: f64,
}

/// Result of a pipeline check.
#[derive(Debug)]
pub struct CheckResult {
    pub source_validation: ValidationResult,
    pub destination_validation: ValidationResult,
    pub state_ok: bool,
}

/// Run a full pipeline: dispatches to v0 or v1 based on connector protocol.
pub async fn run_pipeline(config: &PipelineConfig) -> Result<PipelineResult> {
    // TODO: dispatch v1 when connectors are ready
    run_pipeline_v0(config).await
}

/// Run a full pipeline using the v0 connector protocol: source -> destination with state tracking.
async fn run_pipeline_v0(config: &PipelineConfig) -> Result<PipelineResult> {
    let start = Instant::now();

    tracing::info!(pipeline = config.pipeline, "Starting pipeline run");

    // 1. Resolve connector .wasm file paths
    let source_wasm = wasm_runtime::resolve_connector_path(&config.source.use_ref)?;
    let dest_wasm = wasm_runtime::resolve_connector_path(&config.destination.use_ref)?;

    tracing::info!(
        source = %source_wasm.display(),
        dest = %dest_wasm.display(),
        "Resolved connector paths"
    );

    // 2. Initialize state backend
    let state = create_state_backend(config)?;
    let state = Arc::new(state);

    // 3. Start run tracking
    let run_id = state.start_run(&config.pipeline, "all")?;

    // 4. Load modules
    let runtime = WasmRuntime::new()?;
    let source_load_start = Instant::now();
    let source_module = runtime.load_module(&source_wasm)?;
    let source_module_load_ms = source_load_start.elapsed().as_millis() as u64;
    tracing::info!(
        connector = "source",
        path = %source_wasm.display(),
        load_ms = source_module_load_ms,
        "Loaded source connector module"
    );

    let dest_load_start = Instant::now();
    let dest_module = runtime.load_module(&dest_wasm)?;
    let dest_module_load_ms = dest_load_start.elapsed().as_millis() as u64;
    tracing::info!(
        connector = "destination",
        path = %dest_wasm.display(),
        load_ms = dest_module_load_ms,
        "Loaded destination connector module"
    );

    // 5. Create channel for RecordBatch flow (Arrow IPC bytes)
    let (sender, receiver) = mpsc::sync_channel::<(String, Vec<u8>)>(16);

    // 6. Build ReadRequest from config
    let read_request = build_read_request(config);

    let source_config = config.source.config.clone();
    let dest_config = config.destination.config.clone();
    let pipeline_name = config.pipeline.clone();
    let state_clone = state.clone();
    let stats = Arc::new(Mutex::new(RunStats::default()));
    let stats_source = stats.clone();
    let stats_dest = stats.clone();

    // 7. Spawn source read on a blocking thread
    let source_handle = tokio::task::spawn_blocking(move || -> Result<f64> {
        run_source(
            source_module,
            sender,
            state_clone,
            &pipeline_name,
            &source_config,
            &read_request,
            stats_source,
        )
    });

    // 8. Spawn destination write on a blocking thread
    let dest_pipeline = config.pipeline.clone();
    let dest_handle = tokio::task::spawn_blocking(move || -> Result<(WriteSummary, f64, f64, f64)> {
        run_destination(
            dest_module,
            receiver,
            state.clone(),
            &dest_pipeline,
            &dest_config,
            stats_dest,
        )
    });

    // 9. Wait for both tasks
    let source_result = source_handle.await?;
    let dest_result = dest_handle.await?;

    // 10. Check results
    let final_stats = stats.lock().unwrap().clone();

    match (&source_result, &dest_result) {
        (Ok(source_duration), Ok((summary, dest_duration, vm_setup_secs, recv_secs))) => {
            let connector_internal_secs =
                summary.connect_secs + summary.flush_secs + summary.commit_secs;
            let wasm_overhead_secs =
                (dest_duration - vm_setup_secs - recv_secs - connector_internal_secs).max(0.0);

            tracing::debug!(
                pipeline = config.pipeline,
                run_id,
                "Persisting run state to backend"
            );
            let state_backend = create_state_backend(config)?;
            state_backend.complete_run(
                run_id,
                RunStatus::Completed,
                &RunStats {
                    records_read: final_stats.records_read,
                    records_written: summary.records_written,
                    bytes_read: final_stats.bytes_read,
                    error_message: None,
                },
            )?;

            let duration = start.elapsed();
            tracing::info!(
                pipeline = config.pipeline,
                records_read = final_stats.records_read,
                records_written = summary.records_written,
                duration_secs = duration.as_secs_f64(),
                "Pipeline run completed successfully"
            );

            Ok(PipelineResult {
                records_read: final_stats.records_read,
                records_written: summary.records_written,
                bytes_read: final_stats.bytes_read,
                bytes_written: summary.bytes_written,
                duration_secs: duration.as_secs_f64(),
                source_duration_secs: *source_duration,
                dest_duration_secs: *dest_duration,
                source_module_load_ms,
                dest_module_load_ms,
                dest_connect_secs: summary.connect_secs,
                dest_flush_secs: summary.flush_secs,
                dest_commit_secs: summary.commit_secs,
                dest_vm_setup_secs: *vm_setup_secs,
                dest_recv_secs: *recv_secs,
                wasm_overhead_secs,
            })
        }
        _ => {
            let error_msg = match (&source_result, &dest_result) {
                (Err(e), _) => format!("Source error: {}", e),
                (_, Err(e)) => format!("Destination error: {}", e),
                _ => unreachable!(),
            };

            let state_backend = create_state_backend(config)?;
            state_backend.complete_run(
                run_id,
                RunStatus::Failed,
                &RunStats {
                    records_read: final_stats.records_read,
                    records_written: final_stats.records_written,
                    bytes_read: final_stats.bytes_read,
                    error_message: Some(error_msg.clone()),
                },
            )?;

            // Propagate the first error
            source_result?;
            dest_result?;
            unreachable!()
        }
    }
}

/// Check a pipeline: validate configuration and connectivity without running.
pub async fn check_pipeline(config: &PipelineConfig) -> Result<CheckResult> {
    tracing::info!(pipeline = config.pipeline, "Checking pipeline configuration");

    // 1. Resolve connector paths
    let source_wasm = wasm_runtime::resolve_connector_path(&config.source.use_ref)?;
    let dest_wasm = wasm_runtime::resolve_connector_path(&config.destination.use_ref)?;

    // 2. Check state backend
    let state_ok = check_state_backend(config);

    // 3. Validate source connector
    let source_config = config.source.config.clone();
    let source_validation = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
        validate_connector(&source_wasm, &source_config)
    })
    .await??;

    // 4. Validate destination connector
    let dest_config = config.destination.config.clone();
    let dest_validation = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
        validate_connector(&dest_wasm, &dest_config)
    })
    .await??;

    Ok(CheckResult {
        source_validation,
        destination_validation: dest_validation,
        state_ok,
    })
}

// --- Internal helpers ---

fn run_source(
    module: Module,
    sender: mpsc::SyncSender<(String, Vec<u8>)>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    source_config: &serde_json::Value,
    read_request: &ReadRequest,
    stats: Arc<Mutex<RunStats>>,
) -> Result<f64> {
    let phase_start = Instant::now();

    let host_state = HostState {
        pipeline_name: pipeline_name.to_string(),
        current_stream: "source".to_string(),
        state_backend,
        stats,
        batch_sender: None,
        next_batch_id: 1,
        batch_receiver: None,
        pending_batch: None,
        last_error: None,
        source_checkpoints: Vec::new(),
        dest_checkpoint_count: 0,
        batch_sender_v0: Some(sender),
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = WasiModule::create(None, None, None)
        .map_err(|e| anyhow::anyhow!("Failed to create WASI module: {:?}", e))?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);

    tracing::info!("Initializing source connector");
    handle.init(source_config)?;

    tracing::info!("Starting source read");
    let summary = handle.read(read_request)?;

    tracing::info!(
        records_read = summary.records_read,
        bytes_read = summary.bytes_read,
        "Source read complete"
    );

    // Sender is dropped here, signaling the destination to finish
    Ok(phase_start.elapsed().as_secs_f64())
}

fn run_destination(
    module: Module,
    receiver: mpsc::Receiver<(String, Vec<u8>)>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    dest_config: &serde_json::Value,
    stats: Arc<Mutex<RunStats>>,
) -> Result<(WriteSummary, f64, f64, f64)> {
    let phase_start = Instant::now();

    // Phase A: VM setup + init
    let vm_setup_start = Instant::now();

    let (dummy_sender, _) = mpsc::sync_channel::<(String, Vec<u8>)>(1);

    let host_state = HostState {
        pipeline_name: pipeline_name.to_string(),
        current_stream: "destination".to_string(),
        state_backend,
        stats,
        batch_sender: None,
        next_batch_id: 1,
        batch_receiver: None,
        pending_batch: None,
        last_error: None,
        source_checkpoints: Vec::new(),
        dest_checkpoint_count: 0,
        batch_sender_v0: Some(dummy_sender),
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = WasiModule::create(None, None, None)
        .map_err(|e| anyhow::anyhow!("Failed to create WASI module: {:?}", e))?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);

    tracing::info!("Initializing destination connector");
    handle.init(dest_config)?;

    let vm_setup_secs = vm_setup_start.elapsed().as_secs_f64();

    // Phase B: Receive batches from source
    let recv_start = Instant::now();
    while let Ok((stream_name, ipc_bytes)) = receiver.recv() {
        tracing::debug!(
            stream = stream_name,
            batch_bytes = ipc_bytes.len(),
            "Writing batch to destination"
        );
        handle.write_batch(&stream_name, &ipc_bytes)?;
    }
    let recv_secs = recv_start.elapsed().as_secs_f64();

    // Phase C: Finalize
    tracing::info!("Finalizing destination writes");
    let summary = handle.write_finalize()?;

    tracing::info!(
        records_written = summary.records_written,
        "Destination write complete"
    );

    Ok((summary, phase_start.elapsed().as_secs_f64(), vm_setup_secs, recv_secs))
}

fn validate_connector(
    wasm_path: &std::path::Path,
    config: &serde_json::Value,
) -> Result<ValidationResult> {
    let runtime = WasmRuntime::new()?;
    let module = runtime.load_module(wasm_path)?;

    // Create a dummy sender — validation doesn't emit batches
    let (dummy_sender, _) = mpsc::sync_channel::<(String, Vec<u8>)>(1);
    let state = Arc::new(SqliteStateBackend::in_memory()?);

    let host_state = HostState {
        pipeline_name: "check".to_string(),
        current_stream: "check".to_string(),
        state_backend: state,
        stats: Arc::new(Mutex::new(RunStats::default())),
        batch_sender: None,
        next_batch_id: 1,
        batch_receiver: None,
        pending_batch: None,
        last_error: None,
        source_checkpoints: Vec::new(),
        dest_checkpoint_count: 0,
        batch_sender_v0: Some(dummy_sender),
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = WasiModule::create(None, None, None)
        .map_err(|e| anyhow::anyhow!("Failed to create WASI module: {:?}", e))?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);
    handle.validate(config)
}

fn build_read_request(config: &PipelineConfig) -> ReadRequest {
    let streams = config
        .source
        .streams
        .iter()
        .map(|s| StreamSelection {
            name: s.name.clone(),
            sync_mode: match s.sync_mode.as_str() {
                "incremental" => SyncMode::Incremental,
                _ => SyncMode::FullRefresh,
            },
            cursor_field: s.cursor_field.clone(),
            max_batch_bytes: 64 * 1024 * 1024, // 64MB default
        })
        .collect();

    ReadRequest {
        streams,
        state: None,
    }
}

fn create_state_backend(config: &PipelineConfig) -> Result<SqliteStateBackend> {
    match &config.state.connection {
        Some(path) => {
            SqliteStateBackend::new(std::path::Path::new(path)).context("Failed to open state DB")
        }
        None => {
            // Default: ~/.rapidbyte/state.db
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
            let state_path = PathBuf::from(home)
                .join(".rapidbyte")
                .join("state.db");
            SqliteStateBackend::new(&state_path).context("Failed to open default state DB")
        }
    }
}

fn check_state_backend(config: &PipelineConfig) -> bool {
    match create_state_backend(config) {
        Ok(_) => {
            tracing::info!("State backend: OK");
            true
        }
        Err(e) => {
            tracing::error!("State backend: FAILED — {}", e);
            false
        }
    }
}

// === v1 pipeline ===

/// Run a pipeline using the v1 connector protocol.
///
/// Key differences from v0:
/// - Lifecycle: open -> run_read/run_write -> close
/// - Per-stream channels (source emits via host_emit_batch, dest pulls via host_next_batch)
/// - Typed summaries with batch counts and checkpoint tracking
#[allow(dead_code)]
pub(crate) async fn run_pipeline_v1(config: &PipelineConfig) -> Result<PipelineResult> {
    let start = Instant::now();

    tracing::info!(pipeline = config.pipeline, "Starting v1 pipeline run");

    // 1. Resolve connector paths
    let source_wasm = wasm_runtime::resolve_connector_path(&config.source.use_ref)?;
    let dest_wasm = wasm_runtime::resolve_connector_path(&config.destination.use_ref)?;

    // 2. Initialize state backend
    let state = create_state_backend(config)?;
    let state = Arc::new(state);
    let run_id = state.start_run(&config.pipeline, "all")?;

    // 3. Load modules
    let runtime = WasmRuntime::new()?;

    let source_load_start = Instant::now();
    let source_module = runtime.load_module(&source_wasm)?;
    let source_module_load_ms = source_load_start.elapsed().as_millis() as u64;

    let dest_load_start = Instant::now();
    let dest_module = runtime.load_module(&dest_wasm)?;
    let dest_module_load_ms = dest_load_start.elapsed().as_millis() as u64;

    // 4. Build stream contexts from config
    let stream_ctxs: Vec<StreamContext> = config
        .source
        .streams
        .iter()
        .map(|s| StreamContext {
            stream_name: s.name.clone(),
            schema: SchemaHint::Columns(vec![]), // discovered at runtime
            sync_mode: match s.sync_mode.as_str() {
                "incremental" => SyncMode::Incremental,
                _ => SyncMode::FullRefresh,
            },
            cursor_info: None,
            limits: StreamLimits::default(),
            policies: StreamPolicies::default(),
            write_mode: None,
        })
        .collect();

    let source_config = config.source.config.clone();
    let dest_config = config.destination.config.clone();
    let pipeline_name = config.pipeline.clone();
    let stats = Arc::new(Mutex::new(RunStats::default()));

    // 5. Run source and dest on blocking threads with per-stream channels
    let state_src = state.clone();
    let state_dst = state.clone();
    let stats_src = stats.clone();
    let stats_dst = stats.clone();
    let stream_ctxs_clone = stream_ctxs.clone();
    let pipeline_name_src = pipeline_name.clone();
    let pipeline_name_dst = pipeline_name.clone();

    // Per-stream: create channel, source writes into tx, dest reads from rx
    // v1 processes streams sequentially (one at a time per instance)
    // We spawn source and dest on separate blocking threads with a shared channel
    let (batch_tx, batch_rx) = mpsc::sync_channel::<Vec<u8>>(16);

    let source_handle = tokio::task::spawn_blocking(move || -> Result<(f64, ReadSummaryV1)> {
        run_source_v1(
            source_module,
            batch_tx,
            state_src,
            &pipeline_name_src,
            &source_config,
            &stream_ctxs,
            stats_src,
        )
    });

    let dest_handle =
        tokio::task::spawn_blocking(move || -> Result<(f64, WriteSummaryV1, f64, f64)> {
            run_destination_v1(
                dest_module,
                batch_rx,
                state_dst,
                &pipeline_name_dst,
                &dest_config,
                &stream_ctxs_clone,
                stats_dst,
            )
        });

    // 6. Wait for both
    let source_result = source_handle.await?;
    let dest_result = dest_handle.await?;

    let final_stats = stats.lock().unwrap().clone();

    match (&source_result, &dest_result) {
        (Ok((src_dur, read_summary)), Ok((dst_dur, write_summary, vm_setup_secs, recv_secs))) => {
            let perf = write_summary.perf.as_ref();
            let connector_internal_secs = perf
                .map(|p| p.connect_secs + p.flush_secs + p.commit_secs)
                .unwrap_or(0.0);
            let wasm_overhead_secs =
                (dst_dur - vm_setup_secs - recv_secs - connector_internal_secs).max(0.0);

            let state_backend = create_state_backend(config)?;
            state_backend.complete_run(
                run_id,
                RunStatus::Completed,
                &RunStats {
                    records_read: read_summary.records_read,
                    records_written: write_summary.records_written,
                    bytes_read: read_summary.bytes_read,
                    error_message: None,
                },
            )?;

            let duration = start.elapsed();
            tracing::info!(
                pipeline = config.pipeline,
                records_read = read_summary.records_read,
                records_written = write_summary.records_written,
                duration_secs = duration.as_secs_f64(),
                "v1 pipeline run completed"
            );

            Ok(PipelineResult {
                records_read: read_summary.records_read,
                records_written: write_summary.records_written,
                bytes_read: read_summary.bytes_read,
                bytes_written: write_summary.bytes_written,
                duration_secs: duration.as_secs_f64(),
                source_duration_secs: *src_dur,
                dest_duration_secs: *dst_dur,
                source_module_load_ms,
                dest_module_load_ms,
                dest_connect_secs: perf.map(|p| p.connect_secs).unwrap_or(0.0),
                dest_flush_secs: perf.map(|p| p.flush_secs).unwrap_or(0.0),
                dest_commit_secs: perf.map(|p| p.commit_secs).unwrap_or(0.0),
                dest_vm_setup_secs: *vm_setup_secs,
                dest_recv_secs: *recv_secs,
                wasm_overhead_secs,
            })
        }
        _ => {
            let error_msg = match (&source_result, &dest_result) {
                (Err(e), _) => format!("Source error: {}", e),
                (_, Err(e)) => format!("Destination error: {}", e),
                _ => unreachable!(),
            };

            let state_backend = create_state_backend(config)?;
            state_backend.complete_run(
                run_id,
                RunStatus::Failed,
                &RunStats {
                    records_read: final_stats.records_read,
                    records_written: final_stats.records_written,
                    bytes_read: final_stats.bytes_read,
                    error_message: Some(error_msg.clone()),
                },
            )?;

            source_result.map(|_| ())?;
            dest_result.map(|_| ())?;
            unreachable!()
        }
    }
}

#[allow(dead_code)]
fn run_source_v1(
    module: Module,
    sender: mpsc::SyncSender<Vec<u8>>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    source_config: &serde_json::Value,
    stream_ctxs: &[StreamContext],
    stats: Arc<Mutex<RunStats>>,
) -> Result<(f64, ReadSummaryV1)> {
    let phase_start = Instant::now();

    let host_state = HostState {
        pipeline_name: pipeline_name.to_string(),
        current_stream: String::new(),
        state_backend,
        stats,
        batch_sender: Some(sender),
        next_batch_id: 1,
        batch_receiver: None,
        pending_batch: None,
        last_error: None,
        source_checkpoints: Vec::new(),
        dest_checkpoint_count: 0,
        batch_sender_v0: None,
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = WasiModule::create(None, None, None)
        .map_err(|e| anyhow::anyhow!("Failed to create WASI module: {:?}", e))?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);

    // v1 lifecycle: open
    // TODO: derive connector_id from wasm path or module metadata
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(source_config.clone()),
        connector_id: "source-postgres".to_string(),
        connector_version: "0.1.0".to_string(),
    };

    tracing::info!("Opening source connector (v1)");
    let _open_info = handle.open(&open_ctx)?;

    // v1 lifecycle: run_read per stream (sequential)
    let mut total_summary = ReadSummaryV1 {
        records_read: 0,
        bytes_read: 0,
        batches_emitted: 0,
        checkpoint_count: 0,
    };

    let stream_result: Result<()> = (|| {
        for stream_ctx in stream_ctxs {
            tracing::info!(stream = stream_ctx.stream_name, "Starting v1 source read");
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
        }
        Ok(())
    })();

    // v1 lifecycle: close (always, even if stream loop failed)
    tracing::info!("Closing source connector (v1)");
    if let Err(e) = handle.close() {
        tracing::warn!("Source close failed: {}", e);
    }

    stream_result?;

    // sender is dropped here -> dest sees EOF on host_next_batch
    Ok((phase_start.elapsed().as_secs_f64(), total_summary))
}

#[allow(dead_code)]
fn run_destination_v1(
    module: Module,
    receiver: mpsc::Receiver<Vec<u8>>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    dest_config: &serde_json::Value,
    stream_ctxs: &[StreamContext],
    stats: Arc<Mutex<RunStats>>,
) -> Result<(f64, WriteSummaryV1, f64, f64)> {
    let phase_start = Instant::now();
    let vm_setup_start = Instant::now();

    let host_state = HostState {
        pipeline_name: pipeline_name.to_string(),
        current_stream: String::new(),
        state_backend,
        stats,
        batch_sender: None,
        next_batch_id: 1,
        batch_receiver: Some(receiver),
        pending_batch: None,
        last_error: None,
        source_checkpoints: Vec::new(),
        dest_checkpoint_count: 0,
        batch_sender_v0: None,
    };

    let mut import = wasm_runtime::create_host_imports(host_state)?;
    let mut wasi = WasiModule::create(None, None, None)
        .map_err(|e| anyhow::anyhow!("Failed to create WASI module: {:?}", e))?;

    let mut instances: HashMap<String, &mut dyn SyncInst> = HashMap::new();
    instances.insert("rapidbyte".to_string(), &mut import);
    instances.insert(wasi.name().to_string(), wasi.as_mut());

    let mut vm = Vm::new(
        Store::new(None, instances).map_err(|e| anyhow::anyhow!("Store::new failed: {:?}", e))?,
    );

    vm.register_module(None, module)
        .map_err(|e| anyhow::anyhow!("register_module failed: {:?}", e))?;

    let mut handle = ConnectorHandle::new(vm);

    // v1 lifecycle: open
    // TODO: derive connector_id from wasm path or module metadata
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(dest_config.clone()),
        connector_id: "dest-postgres".to_string(),
        connector_version: "0.1.0".to_string(),
    };

    let vm_setup_secs = vm_setup_start.elapsed().as_secs_f64();

    tracing::info!("Opening destination connector (v1)");
    let _open_info = handle.open(&open_ctx)?;

    // v1 lifecycle: run_write per stream (sequential)
    // The dest connector calls host_next_batch() internally to pull batches
    let recv_start = Instant::now();
    let mut total_summary = WriteSummaryV1 {
        records_written: 0,
        bytes_written: 0,
        batches_written: 0,
        checkpoint_count: 0,
        perf: None,
    };

    debug_assert!(
        stream_ctxs.len() <= 1,
        "v1 multi-stream perf accumulation not implemented"
    );

    let stream_result: Result<()> = (|| {
        for stream_ctx in stream_ctxs {
            tracing::info!(stream = stream_ctx.stream_name, "Starting v1 dest write");
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
            if summary.perf.is_some() {
                total_summary.perf = summary.perf;
            }
        }
        Ok(())
    })();

    let recv_secs = recv_start.elapsed().as_secs_f64();

    // v1 lifecycle: close (always, even if stream loop failed)
    tracing::info!("Closing destination connector (v1)");
    if let Err(e) = handle.close() {
        tracing::warn!("Destination close failed: {}", e);
    }

    stream_result?;

    Ok((
        phase_start.elapsed().as_secs_f64(),
        total_summary,
        vm_setup_secs,
        recv_secs,
    ))
}
