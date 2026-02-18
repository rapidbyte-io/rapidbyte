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
    ConfigBlob, OpenContext, ReadSummary, SchemaHint, StreamContext, StreamLimits, StreamPolicies,
    SyncMode, WriteSummary,
};

use crate::pipeline::types::PipelineConfig;
use crate::runtime::connector_handle::ConnectorHandle;
use crate::runtime::host_functions::{Frame, HostState};
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

/// Run a full pipeline: source -> destination with state tracking.
pub async fn run_pipeline(config: &PipelineConfig) -> Result<PipelineResult> {
    let start = Instant::now();

    tracing::info!(pipeline = config.pipeline, "Starting pipeline run");

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
    let (batch_tx, batch_rx) = mpsc::sync_channel::<Frame>(16);

    let source_handle = tokio::task::spawn_blocking(move || -> Result<(f64, ReadSummary)> {
        run_source(
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
        tokio::task::spawn_blocking(move || -> Result<(f64, WriteSummary, f64, f64)> {
            run_destination(
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
                "Pipeline run completed"
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
    sender: mpsc::SyncSender<Frame>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    source_config: &serde_json::Value,
    stream_ctxs: &[StreamContext],
    stats: Arc<Mutex<RunStats>>,
) -> Result<(f64, ReadSummary)> {
    let phase_start = Instant::now();

    // Keep a clone for sending stream-boundary sentinels after each run_read
    let sentinel_sender = sender.clone();

    // Shared handle so we can update current_stream before each run_read
    let current_stream = Arc::new(Mutex::new(String::new()));

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
        source_checkpoints: Vec::new(),
        dest_checkpoint_count: 0,
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

    // Lifecycle: open
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(source_config.clone()),
        connector_id: "source-postgres".to_string(),
        connector_version: "0.1.0".to_string(),
    };

    tracing::info!("Opening source connector");
    let _open_info = handle.open(&open_ctx)?;

    // Lifecycle: run_read per stream (sequential)
    let mut total_summary = ReadSummary {
        records_read: 0,
        bytes_read: 0,
        batches_emitted: 0,
        checkpoint_count: 0,
    };

    let stream_result: Result<()> = (|| {
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

    // sender is dropped here -> dest sees final EOF on host_next_batch
    Ok((phase_start.elapsed().as_secs_f64(), total_summary))
}

fn run_destination(
    module: Module,
    receiver: mpsc::Receiver<Frame>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    dest_config: &serde_json::Value,
    stream_ctxs: &[StreamContext],
    stats: Arc<Mutex<RunStats>>,
) -> Result<(f64, WriteSummary, f64, f64)> {
    let phase_start = Instant::now();
    let vm_setup_start = Instant::now();

    // Shared handle so we can update current_stream before each run_write
    let current_stream = Arc::new(Mutex::new(String::new()));

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
        source_checkpoints: Vec::new(),
        dest_checkpoint_count: 0,
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

    // Lifecycle: open
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(dest_config.clone()),
        connector_id: "dest-postgres".to_string(),
        connector_version: "0.1.0".to_string(),
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
        perf: None,
    };

    let stream_result: Result<()> = (|| {
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

    Ok((
        phase_start.elapsed().as_secs_f64(),
        total_summary,
        vm_setup_secs,
        recv_secs,
    ))
}

fn validate_connector(
    wasm_path: &std::path::Path,
    config: &serde_json::Value,
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
        source_checkpoints: Vec::new(),
        dest_checkpoint_count: 0,
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

    // Use v1 lifecycle: wrap config in OpenContext for rb_validate
    let open_ctx = OpenContext {
        config: ConfigBlob::Json(config.clone()),
        connector_id: "check".to_string(),
        connector_version: "0.0.0".to_string(),
    };
    handle.validate(&serde_json::to_value(&open_ctx).unwrap())
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
