//! Pipeline orchestrator: resolves connectors, loads modules, executes streams, and finalizes state.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use rapidbyte_types::catalog::{Catalog, SchemaHint};
use rapidbyte_types::cursor::{CursorInfo, CursorType, CursorValue};
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::error::ValidationResult;
use rapidbyte_types::manifest::{Permissions, ResourceLimits};
use rapidbyte_types::metric::{ReadSummary, WriteSummary};
use rapidbyte_types::stream::{StreamContext, StreamLimits, StreamPolicies};
use rapidbyte_types::wire::{ConnectorRole, SyncMode};
use tokio::sync::mpsc;

use crate::checkpoint::correlate_and_persist_cursors;
use crate::config::types::{parse_byte_size, PipelineConfig};
use crate::error::{compute_backoff, PipelineError};
use crate::resolve::{
    build_sandbox_overrides, check_state_backend, create_state_backend,
    load_and_validate_manifest, resolve_connectors, validate_config_against_schema,
    ResolvedConnectors,
};
use crate::result::{CheckResult, DestTiming, PipelineCounts, PipelineResult, SourceTiming};
use crate::runner::{
    run_destination_stream, run_discover, run_source_stream, run_transform_stream,
    validate_connector,
};
use rapidbyte_runtime::{
    parse_connector_ref, Frame, HostTimings, LoadedComponent, SandboxOverrides, WasmRuntime,
};
use rapidbyte_state::StateBackend;
use rapidbyte_types::state::{PipelineId, RunStats, RunStatus, StreamName};

struct StreamResult {
    read_summary: ReadSummary,
    write_summary: WriteSummary,
    source_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    dest_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    src_host_timings: HostTimings,
    dst_host_timings: HostTimings,
    src_duration: f64,
    dst_duration: f64,
    vm_setup_secs: f64,
    recv_secs: f64,
    transform_durations: Vec<f64>,
}

#[derive(Clone)]
struct LoadedTransformModule {
    module: LoadedComponent,
    connector_id: String,
    connector_version: String,
    config: serde_json::Value,
    load_ms: u64,
    permissions: Option<Permissions>,
    manifest_limits: ResourceLimits,
}

struct LoadedModules {
    source_module: LoadedComponent,
    dest_module: LoadedComponent,
    source_module_load_ms: u64,
    dest_module_load_ms: u64,
    transform_modules: Vec<LoadedTransformModule>,
}

struct StreamBuild {
    limits: StreamLimits,
    compression: Option<rapidbyte_runtime::CompressionCodec>,
    stream_ctxs: Vec<StreamContext>,
}

struct AggregatedStreamResults {
    total_read_summary: ReadSummary,
    total_write_summary: WriteSummary,
    source_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    dest_checkpoints: Vec<rapidbyte_types::checkpoint::Checkpoint>,
    src_timings: HostTimings,
    dst_timings: HostTimings,
    max_src_duration: f64,
    max_dst_duration: f64,
    max_dst_vm_setup_secs: f64,
    max_dst_recv_secs: f64,
    transform_durations: Vec<f64>,
    dlq_records: Vec<DlqRecord>,
    final_stats: RunStats,
    first_error: Option<PipelineError>,
}

/// Run a full pipeline: source -> destination with state tracking.
/// Retries on retryable connector errors up to `config.resources.max_retries` times.
///
/// # Errors
///
/// Returns a `PipelineError` if the pipeline fails after exhausting retries
/// or encounters a non-retryable error.
pub async fn run_pipeline(config: &PipelineConfig) -> Result<PipelineResult, PipelineError> {
    let max_retries = config.resources.max_retries;
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        let result = execute_pipeline_once(config, attempt).await;

        match result {
            Ok(pipeline_result) => return Ok(pipeline_result),
            Err(ref err) if err.is_retryable() && attempt <= max_retries => {
                if let Some(connector_err) = err.as_connector_error() {
                    let delay = compute_backoff(connector_err, attempt);
                    let commit_state_str = connector_err
                        .commit_state
                        .as_ref()
                        .map(|cs| format!("{cs:?}"));
                    #[allow(clippy::cast_possible_truncation)]
                    // Safety: delay.as_millis() is always well under u64::MAX
                    let delay_ms = delay.as_millis() as u64;
                    tracing::warn!(
                        attempt,
                        max_retries,
                        delay_ms,
                        category = %connector_err.category,
                        code = %connector_err.code,
                        commit_state = commit_state_str.as_deref(),
                        safe_to_retry = connector_err.safe_to_retry,
                        "Retryable error, will retry"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
            Err(err) => {
                if let Some(connector_err) = err.as_connector_error() {
                    let commit_state_str = connector_err
                        .commit_state
                        .as_ref()
                        .map(|cs| format!("{cs:?}"));
                    if err.is_retryable() {
                        tracing::error!(
                            attempt,
                            max_retries,
                            category = %connector_err.category,
                            code = %connector_err.code,
                            commit_state = commit_state_str.as_deref(),
                            safe_to_retry = connector_err.safe_to_retry,
                            "Max retries exhausted, failing pipeline"
                        );
                    } else {
                        tracing::error!(
                            category = %connector_err.category,
                            code = %connector_err.code,
                            commit_state = commit_state_str.as_deref(),
                            "Non-retryable connector error, failing pipeline"
                        );
                    }
                } else {
                    tracing::error!("Infrastructure error, failing pipeline: {}", err);
                }
                return Err(err);
            }
        }
    }
}

async fn execute_pipeline_once(
    config: &PipelineConfig,
    attempt: u32,
) -> Result<PipelineResult, PipelineError> {
    let start = Instant::now();
    let pipeline_id = PipelineId::new(config.pipeline.clone());
    tracing::info!(pipeline = config.pipeline, "Starting pipeline run");

    let connectors = resolve_connectors(config)?;
    let state = create_state_backend(config).map_err(PipelineError::Infrastructure)?;
    let run_id = state
        .start_run(&pipeline_id, &StreamName::new("all"))
        .map_err(|e| PipelineError::Infrastructure(e.into()))?;

    let modules = load_modules(config, &connectors).await?;
    let stream_build = build_stream_contexts(config, state.as_ref())?;
    let aggregated =
        execute_streams(config, &connectors, &modules, &stream_build, state.clone()).await?;

    finalize_run(
        config,
        &pipeline_id,
        state.as_ref(),
        run_id,
        attempt,
        start,
        &modules,
        aggregated,
    )
}

async fn load_modules(
    config: &PipelineConfig,
    connectors: &ResolvedConnectors,
) -> Result<LoadedModules, PipelineError> {
    let runtime = Arc::new(WasmRuntime::new().map_err(PipelineError::Infrastructure)?);
    tracing::info!(
        source = %connectors.source_wasm.display(),
        destination = %connectors.dest_wasm.display(),
        "Loading connector modules"
    );

    let source_wasm_for_load = connectors.source_wasm.clone();
    let runtime_for_source = runtime.clone();
    #[allow(clippy::cast_possible_truncation)]
    let source_load_task = tokio::task::spawn_blocking(move || {
        let load_start = Instant::now();
        let module = runtime_for_source
            .load_module(&source_wasm_for_load)
            .map_err(PipelineError::Infrastructure)?;
        // Safety: module load time is always well under u64::MAX milliseconds
        let load_ms = load_start.elapsed().as_millis() as u64;
        Ok::<_, PipelineError>((module, load_ms))
    });

    let dest_wasm_for_load = connectors.dest_wasm.clone();
    let runtime_for_dest = runtime.clone();
    #[allow(clippy::cast_possible_truncation)]
    let dest_load_task = tokio::task::spawn_blocking(move || {
        let load_start = Instant::now();
        let module = runtime_for_dest
            .load_module(&dest_wasm_for_load)
            .map_err(PipelineError::Infrastructure)?;
        // Safety: module load time is always well under u64::MAX milliseconds
        let load_ms = load_start.elapsed().as_millis() as u64;
        Ok::<_, PipelineError>((module, load_ms))
    });

    let (source_module, source_module_load_ms) = source_load_task.await.map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!("Source module load task panicked: {e}"))
    })??;
    let (dest_module, dest_module_load_ms) = dest_load_task.await.map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!(
            "Destination module load task panicked: {e}"
        ))
    })??;

    tracing::info!(
        source_ms = source_module_load_ms,
        dest_ms = dest_module_load_ms,
        "Connector modules loaded"
    );

    let mut transform_modules = Vec::with_capacity(config.transforms.len());
    for tc in &config.transforms {
        let wasm_path = rapidbyte_runtime::resolve_connector_path(&tc.use_ref)
            .map_err(PipelineError::Infrastructure)?;
        let manifest =
            load_and_validate_manifest(&wasm_path, &tc.use_ref, ConnectorRole::Transform)
                .map_err(PipelineError::Infrastructure)?;
        if let Some(ref m) = manifest {
            validate_config_against_schema(&tc.use_ref, &tc.config, m)
                .map_err(PipelineError::Infrastructure)?;
        }
        let transform_perms = manifest.as_ref().map(|m| m.permissions.clone());
        let transform_manifest_limits = manifest
            .as_ref()
            .map(|m| m.limits.clone())
            .unwrap_or_default();
        let load_start = Instant::now();
        let module = runtime
            .load_module(&wasm_path)
            .map_err(PipelineError::Infrastructure)?;
        #[allow(clippy::cast_possible_truncation)]
        // Safety: module load time is always well under u64::MAX milliseconds
        let load_ms = load_start.elapsed().as_millis() as u64;
        let (id, ver) = parse_connector_ref(&tc.use_ref);
        transform_modules.push(LoadedTransformModule {
            module,
            connector_id: id,
            connector_version: ver,
            config: tc.config.clone(),
            load_ms,
            permissions: transform_perms,
            manifest_limits: transform_manifest_limits,
        });
    }

    Ok(LoadedModules {
        source_module,
        dest_module,
        source_module_load_ms,
        dest_module_load_ms,
        transform_modules,
    })
}

fn build_stream_contexts(
    config: &PipelineConfig,
    state: &dyn StateBackend,
) -> Result<StreamBuild, PipelineError> {
    let max_batch = parse_byte_size(&config.resources.max_batch_bytes).map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!(
            "Invalid max_batch_bytes '{}': {}",
            config.resources.max_batch_bytes,
            e
        ))
    })?;
    let checkpoint_interval = parse_byte_size(&config.resources.checkpoint_interval_bytes)
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!(
                "Invalid checkpoint_interval_bytes '{}': {}",
                config.resources.checkpoint_interval_bytes,
                e
            ))
        })?;

    let limits = StreamLimits {
        max_batch_bytes: if max_batch > 0 {
            max_batch
        } else {
            StreamLimits::default().max_batch_bytes
        },
        checkpoint_interval_bytes: checkpoint_interval,
        checkpoint_interval_rows: config.resources.checkpoint_interval_rows,
        checkpoint_interval_seconds: config.resources.checkpoint_interval_seconds,
        max_inflight_batches: config.resources.max_inflight_batches,
        ..StreamLimits::default()
    };

    let pipeline_id = PipelineId::new(config.pipeline.clone());
    let stream_ctxs = config
        .source
        .streams
        .iter()
        .map(|s| {
            let cursor_info = match s.sync_mode {
                SyncMode::Incremental => {
                    if let Some(cursor_field) = &s.cursor_field {
                        let last_value = state
                            .get_cursor(&pipeline_id, &StreamName::new(s.name.clone()))
                            .map_err(|e| PipelineError::Infrastructure(e.into()))?
                            .and_then(|cs| cs.cursor_value)
                            .map(|v| CursorValue::Utf8 { value: v });
                        Some(CursorInfo {
                            cursor_field: cursor_field.clone(),
                            cursor_type: CursorType::Utf8,
                            last_value,
                        })
                    } else {
                        None
                    }
                }
                SyncMode::Cdc => {
                    let last_value = state
                        .get_cursor(&pipeline_id, &StreamName::new(s.name.clone()))
                        .map_err(|e| PipelineError::Infrastructure(e.into()))?
                        .and_then(|cs| cs.cursor_value)
                        .map(|v| CursorValue::Lsn { value: v });
                    Some(CursorInfo {
                        cursor_field: "lsn".to_string(),
                        cursor_type: CursorType::Lsn,
                        last_value,
                    })
                }
                SyncMode::FullRefresh => None,
            };

            Ok::<_, PipelineError>(StreamContext {
                stream_name: s.name.clone(),
                schema: SchemaHint::Columns(vec![]),
                sync_mode: s.sync_mode,
                cursor_info,
                limits: limits.clone(),
                policies: StreamPolicies {
                    on_data_error: config.destination.on_data_error,
                    schema_evolution: config.destination.schema_evolution.unwrap_or_default(),
                },
                write_mode: Some(
                    config
                        .destination
                        .write_mode
                        .to_protocol(config.destination.primary_key.clone()),
                ),
                selected_columns: s.columns.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(StreamBuild {
        limits,
        compression: config.resources.compression,
        stream_ctxs,
    })
}

#[allow(clippy::too_many_lines, clippy::similar_names)]
async fn execute_streams(
    config: &PipelineConfig,
    connectors: &ResolvedConnectors,
    modules: &LoadedModules,
    stream_build: &StreamBuild,
    state: Arc<dyn StateBackend>,
) -> Result<AggregatedStreamResults, PipelineError> {
    let source_config = config.source.config.clone();
    let dest_config = config.destination.config.clone();
    let pipeline_name = config.pipeline.clone();
    let (source_connector_id, source_connector_version) =
        parse_connector_ref(&config.source.use_ref);
    let (dest_connector_id, dest_connector_version) =
        parse_connector_ref(&config.destination.use_ref);
    let stats = Arc::new(Mutex::new(RunStats::default()));
    let channel_capacity = usize::max(1, stream_build.limits.max_inflight_batches as usize);
    let num_transforms = config.transforms.len();
    let parallelism = config.resources.parallelism.max(1) as usize;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(parallelism));

    tracing::info!(
        pipeline = config.pipeline,
        parallelism,
        num_streams = stream_build.stream_ctxs.len(),
        num_transforms,
        "Starting per-stream pipeline execution"
    );

    let source_manifest_limits = connectors
        .source_manifest
        .as_ref()
        .map(|m| &m.limits)
        .cloned()
        .unwrap_or_default();
    let source_overrides = build_sandbox_overrides(
        config.source.permissions.as_ref(),
        config.source.limits.as_ref(),
        &source_manifest_limits,
    );

    let dest_manifest_limits = connectors
        .dest_manifest
        .as_ref()
        .map(|m| &m.limits)
        .cloned()
        .unwrap_or_default();
    let dest_overrides = build_sandbox_overrides(
        config.destination.permissions.as_ref(),
        config.destination.limits.as_ref(),
        &dest_manifest_limits,
    );

    let transform_overrides: Vec<Option<SandboxOverrides>> = config
        .transforms
        .iter()
        .zip(modules.transform_modules.iter())
        .map(|(tc, tm)| {
            build_sandbox_overrides(
                tc.permissions.as_ref(),
                tc.limits.as_ref(),
                &tm.manifest_limits,
            )
        })
        .collect();

    let mut stream_join_handles: Vec<tokio::task::JoinHandle<Result<StreamResult, PipelineError>>> =
        Vec::with_capacity(stream_build.stream_ctxs.len());
    let run_dlq_records: Arc<Mutex<Vec<DlqRecord>>> = Arc::new(Mutex::new(Vec::new()));

    for stream_ctx in &stream_build.stream_ctxs {
        let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("Semaphore closed: {e}"))
        })?;

        let source_module = modules.source_module.clone();
        let dest_module = modules.dest_module.clone();
        let state_src = state.clone();
        let state_dst = state.clone();
        let stats_src = stats.clone();
        let stats_dst = stats.clone();
        let stream_ctx = stream_ctx.clone();
        let pipeline_name = pipeline_name.clone();
        let source_config = source_config.clone();
        let dest_config = dest_config.clone();
        let source_connector_id = source_connector_id.clone();
        let source_connector_version = source_connector_version.clone();
        let dest_connector_id = dest_connector_id.clone();
        let dest_connector_version = dest_connector_version.clone();
        let source_permissions = connectors.source_permissions.clone();
        let dest_permissions = connectors.dest_permissions.clone();
        let source_overrides = source_overrides.clone();
        let dest_overrides = dest_overrides.clone();
        let transform_overrides = transform_overrides.clone();
        let run_dlq_records = run_dlq_records.clone();
        let compression = stream_build.compression;
        let transforms = modules.transform_modules.clone();

        let handle = tokio::spawn(async move {
            let num_t = transforms.len();
            let mut channels = Vec::with_capacity(num_t + 1);
            for _ in 0..=num_t {
                channels.push(mpsc::channel::<Frame>(channel_capacity));
            }

            let (mut senders, mut receivers): (
                Vec<mpsc::Sender<Frame>>,
                Vec<mpsc::Receiver<Frame>>,
            ) = channels.into_iter().unzip();

            let source_tx = senders.remove(0);
            let dest_rx = receivers.pop().ok_or_else(|| {
                PipelineError::Infrastructure(anyhow::anyhow!("Missing destination receiver"))
            })?;

            let stream_ctx_for_src = stream_ctx.clone();
            let stream_ctx_for_dst = stream_ctx.clone();
            let pipeline_name_src = pipeline_name.clone();
            let pipeline_name_dst = pipeline_name.clone();

            let src_handle = tokio::task::spawn_blocking(move || {
                run_source_stream(
                    &source_module,
                    source_tx,
                    state_src,
                    &pipeline_name_src,
                    &source_connector_id,
                    &source_connector_version,
                    &source_config,
                    &stream_ctx_for_src,
                    stats_src,
                    source_permissions.as_ref(),
                    compression,
                    source_overrides.as_ref(),
                )
            });

            let mut transform_handles = Vec::with_capacity(num_t);
            for (i, t) in transforms.into_iter().enumerate() {
                let rx = receivers.remove(0);
                let tx = senders.remove(0);
                let state_t = state_dst.clone();
                let stream_ctx_t = stream_ctx.clone();
                let pipeline_name_t = pipeline_name.clone();
                let t_overrides = transform_overrides.get(i).cloned().flatten();
                let t_handle = tokio::task::spawn_blocking(move || {
                    run_transform_stream(
                        &t.module,
                        rx,
                        tx,
                        state_t,
                        &pipeline_name_t,
                        &t.connector_id,
                        &t.connector_version,
                        &t.config,
                        &stream_ctx_t,
                        t.permissions.as_ref(),
                        compression,
                        t_overrides.as_ref(),
                    )
                });
                transform_handles.push((i, t_handle));
            }

            let dst_handle = tokio::task::spawn_blocking(move || {
                run_destination_stream(
                    &dest_module,
                    dest_rx,
                    run_dlq_records,
                    state_dst,
                    &pipeline_name_dst,
                    &dest_connector_id,
                    &dest_connector_version,
                    &dest_config,
                    &stream_ctx_for_dst,
                    stats_dst,
                    dest_permissions.as_ref(),
                    compression,
                    dest_overrides.as_ref(),
                )
            });

            let src_result = src_handle.await.map_err(|e| {
                PipelineError::Infrastructure(anyhow::anyhow!(
                    "Source task panicked for stream '{}': {}",
                    stream_ctx.stream_name,
                    e
                ))
            })?;

            let mut transform_durations = Vec::new();
            let mut first_transform_error: Option<PipelineError> = None;
            for (i, t_handle) in transform_handles {
                let result = t_handle.await.map_err(|e| {
                    PipelineError::Infrastructure(anyhow::anyhow!(
                        "Transform {} task panicked for stream '{}': {}",
                        i,
                        stream_ctx.stream_name,
                        e
                    ))
                })?;
                match result {
                    Ok(result) => {
                        tracing::info!(
                            transform_index = i,
                            stream = stream_ctx.stream_name,
                            duration_secs = result.duration_secs,
                            records_in = result.summary.records_in,
                            records_out = result.summary.records_out,
                            "Transform stage completed for stream"
                        );
                        transform_durations.push(result.duration_secs);
                    }
                    Err(e) => {
                        tracing::error!(
                            transform_index = i,
                            stream = stream_ctx.stream_name,
                            "Transform failed: {}",
                            e
                        );
                        if first_transform_error.is_none() {
                            first_transform_error = Some(e);
                        }
                    }
                }
            }

            let dst_result = dst_handle.await.map_err(|e| {
                PipelineError::Infrastructure(anyhow::anyhow!(
                    "Destination task panicked for stream '{}': {}",
                    stream_ctx.stream_name,
                    e
                ))
            })?;

            drop(permit);

            if let Some(transform_err) = first_transform_error {
                return Err(transform_err);
            }

            let src = src_result?;

            let dst = dst_result?;

            Ok(StreamResult {
                read_summary: src.summary,
                write_summary: dst.summary,
                source_checkpoints: src.checkpoints,
                dest_checkpoints: dst.checkpoints,
                src_host_timings: src.host_timings,
                dst_host_timings: dst.host_timings,
                src_duration: src.duration_secs,
                dst_duration: dst.duration_secs,
                vm_setup_secs: dst.vm_setup_secs,
                recv_secs: dst.recv_secs,
                transform_durations,
            })
        });

        stream_join_handles.push(handle);
    }

    let mut source_checkpoints = Vec::new();
    let mut dest_checkpoints = Vec::new();
    let mut total_read_summary = ReadSummary {
        records_read: 0,
        bytes_read: 0,
        batches_emitted: 0,
        checkpoint_count: 0,
        records_skipped: 0,
        perf: None,
    };
    let mut total_write_summary = WriteSummary {
        records_written: 0,
        bytes_written: 0,
        batches_written: 0,
        checkpoint_count: 0,
        records_failed: 0,
        perf: None,
    };
    let mut src_timings = HostTimings::default();
    let mut dst_timings = HostTimings::default();
    let mut max_src_duration = 0.0;
    let mut max_dst_duration = 0.0;
    let mut max_dst_vm_setup_secs = 0.0;
    let mut max_dst_recv_secs = 0.0;
    let mut transform_durations = Vec::new();
    let mut first_error: Option<PipelineError> = None;

    for handle in stream_join_handles {
        let result = handle.await.map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("Stream task panicked: {e}"))
        })?;

        match result {
            Ok(sr) => {
                total_read_summary.records_read += sr.read_summary.records_read;
                total_read_summary.bytes_read += sr.read_summary.bytes_read;
                total_read_summary.batches_emitted += sr.read_summary.batches_emitted;
                total_read_summary.checkpoint_count += sr.read_summary.checkpoint_count;
                total_read_summary.records_skipped += sr.read_summary.records_skipped;

                total_write_summary.records_written += sr.write_summary.records_written;
                total_write_summary.bytes_written += sr.write_summary.bytes_written;
                total_write_summary.batches_written += sr.write_summary.batches_written;
                total_write_summary.checkpoint_count += sr.write_summary.checkpoint_count;
                total_write_summary.records_failed += sr.write_summary.records_failed;

                source_checkpoints.extend(sr.source_checkpoints);
                dest_checkpoints.extend(sr.dest_checkpoints);

                src_timings.emit_batch_nanos += sr.src_host_timings.emit_batch_nanos;
                src_timings.compress_nanos += sr.src_host_timings.compress_nanos;
                src_timings.emit_batch_count += sr.src_host_timings.emit_batch_count;
                dst_timings.next_batch_nanos += sr.dst_host_timings.next_batch_nanos;
                dst_timings.decompress_nanos += sr.dst_host_timings.decompress_nanos;
                dst_timings.next_batch_count += sr.dst_host_timings.next_batch_count;

                if sr.src_duration > max_src_duration {
                    max_src_duration = sr.src_duration;
                }
                if sr.dst_duration > max_dst_duration {
                    max_dst_duration = sr.dst_duration;
                    max_dst_vm_setup_secs = sr.vm_setup_secs;
                    max_dst_recv_secs = sr.recv_secs;
                }
                transform_durations.extend(sr.transform_durations);
            }
            Err(error) => {
                tracing::error!("Stream failed: {}", error);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }

    let dlq_records = run_dlq_records
        .lock()
        .map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("DLQ collection mutex poisoned"))
        })?
        .drain(..)
        .collect();

    let final_stats = stats
        .lock()
        .map_err(|_| PipelineError::Infrastructure(anyhow::anyhow!("run stats mutex poisoned")))?
        .clone();

    Ok(AggregatedStreamResults {
        total_read_summary,
        total_write_summary,
        source_checkpoints,
        dest_checkpoints,
        src_timings,
        dst_timings,
        max_src_duration,
        max_dst_duration,
        max_dst_vm_setup_secs,
        max_dst_recv_secs,
        transform_durations,
        dlq_records,
        final_stats,
        first_error,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn finalize_run(
    config: &PipelineConfig,
    pipeline_id: &PipelineId,
    state: &dyn StateBackend,
    run_id: i64,
    attempt: u32,
    start: Instant,
    modules: &LoadedModules,
    aggregated: AggregatedStreamResults,
) -> Result<PipelineResult, PipelineError> {
    if let Some(err) = aggregated.first_error {
        state
            .complete_run(
                run_id,
                RunStatus::Failed,
                &RunStats {
                    records_read: aggregated.final_stats.records_read,
                    records_written: aggregated.final_stats.records_written,
                    bytes_read: aggregated.final_stats.bytes_read,
                    error_message: Some(format!("Stream error: {err}")),
                },
            )
            .map_err(|e| PipelineError::Infrastructure(e.into()))?;
        crate::dlq::persist_dlq_records(state, pipeline_id, run_id, &aggregated.dlq_records);
        return Err(err);
    }

    let connector_internal_secs = aggregated
        .total_write_summary
        .perf
        .as_ref()
        .map_or(0.0, |p| p.connect_secs + p.flush_secs + p.commit_secs);
    let wasm_overhead_secs = (aggregated.max_dst_duration
        - aggregated.max_dst_vm_setup_secs
        - aggregated.max_dst_recv_secs
        - connector_internal_secs)
        .max(0.0);

    state
        .complete_run(
            run_id,
            RunStatus::Completed,
            &RunStats {
                records_read: aggregated.total_read_summary.records_read,
                records_written: aggregated.total_write_summary.records_written,
                bytes_read: aggregated.total_read_summary.bytes_read,
                error_message: None,
            },
        )
        .map_err(|e| PipelineError::Infrastructure(e.into()))?;

    tracing::debug!(
        pipeline = config.pipeline,
        source_checkpoint_count = aggregated.source_checkpoints.len(),
        dest_checkpoint_count = aggregated.dest_checkpoints.len(),
        "About to correlate checkpoints"
    );

    let cursors_advanced = correlate_and_persist_cursors(
        state,
        pipeline_id,
        &aggregated.source_checkpoints,
        &aggregated.dest_checkpoints,
    )
    .map_err(PipelineError::Infrastructure)?;
    if cursors_advanced > 0 {
        tracing::info!(
            pipeline = config.pipeline,
            cursors_advanced,
            "Checkpoint coordination complete"
        );
    }

    crate::dlq::persist_dlq_records(state, pipeline_id, run_id, &aggregated.dlq_records);

    let duration = start.elapsed();
    let src_perf = aggregated.total_read_summary.perf.as_ref();
    let perf = aggregated.total_write_summary.perf.as_ref();
    let transform_module_load_ms = modules
        .transform_modules
        .iter()
        .map(|m| m.load_ms)
        .collect::<Vec<_>>();

    tracing::info!(
        pipeline = config.pipeline,
        records_read = aggregated.total_read_summary.records_read,
        records_written = aggregated.total_write_summary.records_written,
        duration_secs = duration.as_secs_f64(),
        "Pipeline run completed"
    );

    Ok(PipelineResult {
        counts: PipelineCounts {
            records_read: aggregated.total_read_summary.records_read,
            records_written: aggregated.total_write_summary.records_written,
            bytes_read: aggregated.total_read_summary.bytes_read,
            bytes_written: aggregated.total_write_summary.bytes_written,
        },
        source: SourceTiming {
            duration_secs: aggregated.max_src_duration,
            module_load_ms: modules.source_module_load_ms,
            connect_secs: src_perf.map_or(0.0, |p| p.connect_secs),
            query_secs: src_perf.map_or(0.0, |p| p.query_secs),
            fetch_secs: src_perf.map_or(0.0, |p| p.fetch_secs),
            arrow_encode_secs: src_perf.map_or(0.0, |p| p.arrow_encode_secs),
            emit_nanos: aggregated.src_timings.emit_batch_nanos,
            compress_nanos: aggregated.src_timings.compress_nanos,
            emit_count: aggregated.src_timings.emit_batch_count,
        },
        dest: DestTiming {
            duration_secs: aggregated.max_dst_duration,
            module_load_ms: modules.dest_module_load_ms,
            connect_secs: perf.map_or(0.0, |p| p.connect_secs),
            flush_secs: perf.map_or(0.0, |p| p.flush_secs),
            commit_secs: perf.map_or(0.0, |p| p.commit_secs),
            arrow_decode_secs: perf.map_or(0.0, |p| p.arrow_decode_secs),
            vm_setup_secs: aggregated.max_dst_vm_setup_secs,
            recv_secs: aggregated.max_dst_recv_secs,
            recv_nanos: aggregated.dst_timings.next_batch_nanos,
            decompress_nanos: aggregated.dst_timings.decompress_nanos,
            recv_count: aggregated.dst_timings.next_batch_count,
        },
        transform_count: aggregated.transform_durations.len(),
        transform_duration_secs: aggregated.transform_durations.iter().sum(),
        transform_module_load_ms,
        duration_secs: duration.as_secs_f64(),
        wasm_overhead_secs,
        retry_count: attempt.saturating_sub(1),
    })
}

/// Check a pipeline: validate configuration and connectivity without running.
///
/// # Errors
///
/// Returns an error if connector resolution, module loading, or validation fails.
#[allow(clippy::too_many_lines)]
pub async fn check_pipeline(config: &PipelineConfig) -> Result<CheckResult> {
    tracing::info!(
        pipeline = config.pipeline,
        "Checking pipeline configuration"
    );

    let connectors = resolve_connectors(config).map_err(|e| anyhow::anyhow!(e.to_string()))?;
    if connectors.source_manifest.is_some() {
        println!("Source manifest:    OK");
    }
    if connectors.dest_manifest.is_some() {
        println!("Dest manifest:      OK");
    }

    if let Some(ref sm) = connectors.source_manifest {
        match validate_config_against_schema(&config.source.use_ref, &config.source.config, sm) {
            Ok(()) => println!("Source config:      OK"),
            Err(e) => println!("Source config:      FAILED\n  {e}"),
        }
    }
    if let Some(ref dm) = connectors.dest_manifest {
        match validate_config_against_schema(
            &config.destination.use_ref,
            &config.destination.config,
            dm,
        ) {
            Ok(()) => println!("Dest config:        OK"),
            Err(e) => println!("Dest config:        FAILED\n  {e}"),
        }
    }

    let state_ok = check_state_backend(config);

    let source_config = config.source.config.clone();
    let source_permissions = connectors.source_permissions.clone();
    let (src_id, src_ver) = parse_connector_ref(&config.source.use_ref);
    let source_wasm = connectors.source_wasm.clone();
    let source_validation_handle =
        tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
            validate_connector(
                &source_wasm,
                ConnectorRole::Source,
                &src_id,
                &src_ver,
                &source_config,
                source_permissions.as_ref(),
            )
        });

    let dest_config = config.destination.config.clone();
    let dest_permissions = connectors.dest_permissions.clone();
    let (dst_id, dst_ver) = parse_connector_ref(&config.destination.use_ref);
    let dest_wasm = connectors.dest_wasm.clone();
    let dest_validation_handle =
        tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
            validate_connector(
                &dest_wasm,
                ConnectorRole::Destination,
                &dst_id,
                &dst_ver,
                &dest_config,
                dest_permissions.as_ref(),
            )
        });

    let source_validation = source_validation_handle
        .await
        .map_err(|e| anyhow::anyhow!("Source validation task panicked: {e}"))??;
    let dest_validation = dest_validation_handle
        .await
        .map_err(|e| anyhow::anyhow!("Destination validation task panicked: {e}"))??;

    let mut transform_tasks = Vec::with_capacity(config.transforms.len());
    for (index, tc) in config.transforms.iter().enumerate() {
        let wasm_path = rapidbyte_runtime::resolve_connector_path(&tc.use_ref)?;
        let manifest =
            load_and_validate_manifest(&wasm_path, &tc.use_ref, ConnectorRole::Transform)?;
        if let Some(ref m) = manifest {
            match validate_config_against_schema(&tc.use_ref, &tc.config, m) {
                Ok(()) => println!("Transform config ({}): OK", tc.use_ref),
                Err(e) => println!("Transform config ({}): FAILED\n  {}", tc.use_ref, e),
            }
        }
        let transform_perms = manifest.as_ref().map(|m| m.permissions.clone());
        let config_val = tc.config.clone();
        let connector_ref = tc.use_ref.clone();
        let (tc_id, tc_ver) = parse_connector_ref(&tc.use_ref);
        let handle = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
            validate_connector(
                &wasm_path,
                ConnectorRole::Transform,
                &tc_id,
                &tc_ver,
                &config_val,
                transform_perms.as_ref(),
            )
        });
        transform_tasks.push((index, connector_ref, handle));
    }

    let mut transform_validations = Vec::with_capacity(transform_tasks.len());
    for (index, connector_ref, handle) in transform_tasks {
        let result = handle.await.map_err(|e| {
            anyhow::anyhow!(
                "Transform validation task panicked (index {index}, {connector_ref}): {e}"
            )
        })??;
        transform_validations.push(result);
    }

    Ok(CheckResult {
        source_validation,
        destination_validation: dest_validation,
        transform_validations,
        state_ok,
    })
}

/// Discover available streams from a source connector.
///
/// # Errors
///
/// Returns an error if the connector cannot be loaded, opened, or discovery fails.
pub async fn discover_connector(
    connector_ref: &str,
    config: &serde_json::Value,
) -> Result<Catalog> {
    let wasm_path = rapidbyte_runtime::resolve_connector_path(connector_ref)?;
    let manifest = load_and_validate_manifest(&wasm_path, connector_ref, ConnectorRole::Source)?;
    let permissions = manifest.as_ref().map(|m| m.permissions.clone());
    let (connector_id, connector_version) = parse_connector_ref(connector_ref);
    let config = config.clone();

    tokio::task::spawn_blocking(move || {
        run_discover(
            &wasm_path,
            &connector_id,
            &connector_version,
            &config,
            permissions.as_ref(),
        )
    })
    .await
    .map_err(|e| anyhow::anyhow!("Discover task panicked: {e}"))?
}

