use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};

use rapidbyte_sdk::errors::ValidationResult;
use rapidbyte_sdk::protocol::{
    CursorInfo, CursorType, CursorValue,
    SchemaHint, StreamContext, StreamLimits, StreamPolicies, SyncMode, WriteMode,
};

use super::checkpoint::correlate_and_persist_cursors;
use super::errors::{compute_backoff, PipelineError};
pub use super::runner::{CheckResult, PipelineResult};
use super::runner::{run_destination, run_source, validate_connector};
use crate::pipeline::types::{parse_byte_size, PipelineConfig};
use crate::runtime::host_functions::Frame;
use crate::runtime::wasm_runtime::{self, parse_connector_ref, WasmRuntime};
use crate::state::backend::{RunStats, RunStatus, StateBackend};
use crate::state::sqlite::SqliteStateBackend;

/// Run a full pipeline: source -> destination with state tracking.
/// Retries on retryable connector errors up to `config.resources.max_retries` times.
pub async fn run_pipeline(config: &PipelineConfig) -> Result<PipelineResult, PipelineError> {
    let max_retries = config.resources.max_retries;
    let mut attempt = 0u32;

    loop {
        attempt += 1;

        let result = execute_pipeline_once(config).await;

        match result {
            Ok(pipeline_result) => return Ok(pipeline_result),
            Err(ref err) if err.is_retryable() && attempt <= max_retries => {
                if let Some(connector_err) = err.as_connector_error() {
                    let delay = compute_backoff(connector_err, attempt);
                    tracing::warn!(
                        attempt,
                        max_retries,
                        delay_ms = delay.as_millis() as u64,
                        category = %connector_err.category,
                        code = %connector_err.code,
                        "Retryable error, will retry"
                    );
                    std::thread::sleep(delay);
                }
                continue;
            }
            Err(err) => {
                if err.is_retryable() {
                    tracing::error!(
                        attempt,
                        max_retries,
                        "Max retries exhausted, failing pipeline"
                    );
                }
                return Err(err);
            }
        }
    }
}

/// Execute a single pipeline attempt: source -> destination with state tracking.
async fn execute_pipeline_once(config: &PipelineConfig) -> Result<PipelineResult, PipelineError> {
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
    let max_batch = parse_byte_size(&config.resources.max_batch_bytes);
    let checkpoint_interval = parse_byte_size(&config.resources.checkpoint_interval_bytes);

    let limits = StreamLimits {
        max_batch_bytes: if max_batch > 0 { max_batch } else { StreamLimits::default().max_batch_bytes },
        checkpoint_interval_bytes: checkpoint_interval,
        ..StreamLimits::default()
    };

    let stream_ctxs: Vec<StreamContext> = config
        .source
        .streams
        .iter()
        .map(|s| {
            let sync_mode = match s.sync_mode.as_str() {
                "incremental" => SyncMode::Incremental,
                _ => SyncMode::FullRefresh,
            };

            // For incremental streams, load cursor from state backend
            let cursor_info = if sync_mode == SyncMode::Incremental {
                if let Some(cursor_field) = &s.cursor_field {
                    let last_value = state
                        .get_cursor(&config.pipeline, &s.name)
                        .ok()
                        .flatten()
                        .and_then(|cs| cs.cursor_value)
                        .map(|v| CursorValue::Utf8(v));

                    Some(CursorInfo {
                        cursor_field: cursor_field.clone(),
                        cursor_type: CursorType::Utf8,
                        last_value,
                    })
                } else {
                    None
                }
            } else {
                None
            };

            let write_mode = match config.destination.write_mode.as_str() {
                "replace" => WriteMode::Replace,
                "upsert" => WriteMode::Upsert {
                    primary_key: config.destination.primary_key.clone(),
                },
                _ => WriteMode::Append,
            };

            StreamContext {
                stream_name: s.name.clone(),
                schema: SchemaHint::Columns(vec![]),
                sync_mode,
                cursor_info,
                limits: limits.clone(),
                policies: StreamPolicies::default(),
                write_mode: Some(write_mode),
            }
        })
        .collect();

    let source_config = config.source.config.clone();
    let dest_config = config.destination.config.clone();
    let pipeline_name = config.pipeline.clone();
    let (source_connector_id, source_connector_version) = parse_connector_ref(&config.source.use_ref);
    let (dest_connector_id, dest_connector_version) = parse_connector_ref(&config.destination.use_ref);
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

    let source_handle = tokio::task::spawn_blocking(move || {
        run_source(
            source_module,
            batch_tx,
            state_src,
            &pipeline_name_src,
            &source_connector_id,
            &source_connector_version,
            &source_config,
            &stream_ctxs,
            stats_src,
        )
    });

    let dest_handle = tokio::task::spawn_blocking(move || {
        run_destination(
            dest_module,
            batch_rx,
            state_dst,
            &pipeline_name_dst,
            &dest_connector_id,
            &dest_connector_version,
            &dest_config,
            &stream_ctxs_clone,
            stats_dst,
        )
    });

    // 6. Wait for both
    let source_result = source_handle
        .await
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!("Source task panicked: {}", e)))?;
    let dest_result = dest_handle
        .await
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!("Dest task panicked: {}", e)))?;

    let final_stats = stats.lock().unwrap().clone();

    match (&source_result, &dest_result) {
        (Ok((src_dur, read_summary, source_checkpoints)), Ok((dst_dur, write_summary, vm_setup_secs, recv_secs, dest_checkpoints))) => {
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

            // Checkpoint coordination: persist cursor only when both source and
            // dest confirm the stream data (per spec § State + Checkpointing)
            let cursors_advanced = correlate_and_persist_cursors(
                &state_backend,
                &config.pipeline,
                &source_checkpoints,
                &dest_checkpoints,
            )?;
            if cursors_advanced > 0 {
                tracing::info!(
                    pipeline = config.pipeline,
                    cursors_advanced,
                    "Checkpoint coordination complete"
                );
            }

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
    let (src_id, src_ver) = parse_connector_ref(&config.source.use_ref);
    let source_validation = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
        validate_connector(&source_wasm, &src_id, &src_ver, &source_config)
    })
    .await??;

    // 4. Validate destination connector
    let dest_config = config.destination.config.clone();
    let (dst_id, dst_ver) = parse_connector_ref(&config.destination.use_ref);
    let dest_validation = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
        validate_connector(&dest_wasm, &dst_id, &dst_ver, &dest_config)
    })
    .await??;

    Ok(CheckResult {
        source_validation,
        destination_validation: dest_validation,
        state_ok,
    })
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
