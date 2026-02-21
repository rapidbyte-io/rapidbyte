use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use rapidbyte_sdk::manifest::Permissions;
use tokio::sync::mpsc;

use rapidbyte_sdk::errors::ValidationResult;
use rapidbyte_sdk::protocol::{
    Catalog, Checkpoint, ColumnPolicy, ConnectorRole, CursorInfo, CursorType, CursorValue,
    DataErrorPolicy, DlqRecord, NullabilityPolicy, ReadSummary, SchemaEvolutionPolicy, SchemaHint,
    StreamContext, StreamLimits, StreamPolicies, SyncMode, TypeChangePolicy,
    WriteMode, WriteSummary,
};

use super::checkpoint::correlate_and_persist_cursors;
use super::errors::{compute_backoff, PipelineError};
use super::runner::{
    run_destination_stream, run_discover, run_source_stream, run_transform_stream,
    validate_connector,
};
pub use super::runner::{CheckResult, PipelineResult};
use crate::pipeline::types::{parse_byte_size, PipelineConfig};
use crate::runtime::component_runtime::{self, parse_connector_ref, Frame, HostTimings, LoadedComponent, WasmRuntime};
use super::compression::CompressionCodec;
use crate::state::backend::{RunStats, RunStatus, StateBackend};
use crate::state::sqlite::SqliteStateBackend;

/// Run a full pipeline: source -> destination with state tracking.
/// Retries on retryable connector errors up to `config.resources.max_retries` times.
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
                        .map(|cs| format!("{:?}", cs));
                    tracing::warn!(
                        attempt,
                        max_retries,
                        delay_ms = delay.as_millis() as u64,
                        category = %connector_err.category,
                        code = %connector_err.code,
                        commit_state = commit_state_str.as_deref(),
                        safe_to_retry = connector_err.safe_to_retry,
                        "Retryable error, will retry"
                    );
                    tokio::time::sleep(delay).await;
                }
                continue;
            }
            Err(err) => {
                if let Some(connector_err) = err.as_connector_error() {
                    let commit_state_str = connector_err
                        .commit_state
                        .as_ref()
                        .map(|cs| format!("{:?}", cs));
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

/// Aggregated results from a single stream's source+dest pipeline.
struct StreamResult {
    read_summary: ReadSummary,
    write_summary: WriteSummary,
    source_checkpoints: Vec<Checkpoint>,
    dest_checkpoints: Vec<Checkpoint>,
    dlq_records: Vec<DlqRecord>,
    src_host_timings: HostTimings,
    dst_host_timings: HostTimings,
    src_duration: f64,
    dst_duration: f64,
    vm_setup_secs: f64,
    recv_secs: f64,
    transform_durations: Vec<f64>,
}

/// Error from a per-stream task, carrying any DLQ records collected before the error.
struct StreamError {
    error: PipelineError,
    dlq_records: Vec<DlqRecord>,
}

impl From<PipelineError> for StreamError {
    fn from(error: PipelineError) -> Self {
        StreamError {
            error,
            dlq_records: Vec::new(),
        }
    }
}

/// Execute a single pipeline attempt: source -> destination with state tracking.
/// Each stream gets its own WASM instance pair (source+dest), connected by its own
/// channel, bounded by a semaphore for concurrency control.
async fn execute_pipeline_once(
    config: &PipelineConfig,
    attempt: u32,
) -> Result<PipelineResult, PipelineError> {
    let start = Instant::now();

    tracing::info!(pipeline = config.pipeline, "Starting pipeline run");

    // 1. Resolve connector paths
    let source_wasm = component_runtime::resolve_connector_path(&config.source.use_ref)?;
    let dest_wasm = component_runtime::resolve_connector_path(&config.destination.use_ref)?;

    // 1b. Load and validate manifests (pre-flight)
    let source_manifest =
        load_and_validate_manifest(&source_wasm, &config.source.use_ref, ConnectorRole::Source)?;
    let dest_manifest = load_and_validate_manifest(
        &dest_wasm,
        &config.destination.use_ref,
        ConnectorRole::Destination,
    )?;

    // 1c. Validate connector config against manifest schemas (zero-boot)
    if let Some(ref sm) = source_manifest {
        validate_config_against_schema(&config.source.use_ref, &config.source.config, sm)?;
    }
    if let Some(ref dm) = dest_manifest {
        validate_config_against_schema(
            &config.destination.use_ref,
            &config.destination.config,
            dm,
        )?;
    }

    // 1d. Extract permissions from manifests (for WASI sandbox configuration)
    let source_permissions = source_manifest.as_ref().map(|m| m.permissions.clone());
    let dest_permissions = dest_manifest.as_ref().map(|m| m.permissions.clone());

    // 2. Initialize state backend
    let state = create_state_backend(config)?;
    let state = Arc::new(state);
    let run_id = state.start_run(&config.pipeline, "all")?;

    // 3. Load modules
    let runtime = Arc::new(WasmRuntime::new()?);
    tracing::info!(
        source = %source_wasm.display(),
        destination = %dest_wasm.display(),
        "Loading connector modules"
    );

    let source_wasm_for_load = source_wasm.clone();
    let runtime_for_source = runtime.clone();
    let source_load_task = tokio::task::spawn_blocking(move || {
        let load_start = Instant::now();
        let module = runtime_for_source
            .load_module(&source_wasm_for_load)
            .map_err(PipelineError::Infrastructure)?;
        let load_ms = load_start.elapsed().as_millis() as u64;
        Ok::<_, PipelineError>((module, load_ms))
    });

    let dest_wasm_for_load = dest_wasm.clone();
    let runtime_for_dest = runtime.clone();
    let dest_load_task = tokio::task::spawn_blocking(move || {
        let load_start = Instant::now();
        let module = runtime_for_dest
            .load_module(&dest_wasm_for_load)
            .map_err(PipelineError::Infrastructure)?;
        let load_ms = load_start.elapsed().as_millis() as u64;
        Ok::<_, PipelineError>((module, load_ms))
    });

    let (source_module, source_module_load_ms) = source_load_task
        .await
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!(
                "Source module load task panicked: {}",
                e
            ))
        })??;
    let (dest_module, dest_module_load_ms) = dest_load_task
        .await
        .map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!(
                "Destination module load task panicked: {}",
                e
            ))
        })??;

    tracing::info!(
        source_ms = source_module_load_ms,
        dest_ms = dest_module_load_ms,
        "Connector modules loaded"
    );

    // 3b. Load transform modules (in order)
    let transform_modules: Vec<(LoadedComponent, String, String, serde_json::Value, u64, Option<Permissions>)> = config
        .transforms
        .iter()
        .map(|tc| {
            let wasm_path = component_runtime::resolve_connector_path(&tc.use_ref)?;
            let manifest =
                load_and_validate_manifest(&wasm_path, &tc.use_ref, ConnectorRole::Transform)?;
            if let Some(ref m) = manifest {
                validate_config_against_schema(&tc.use_ref, &tc.config, m)?;
            }
            let transform_perms = manifest.as_ref().map(|m| m.permissions.clone());
            let load_start = Instant::now();
            let module = runtime.load_module(&wasm_path)?;
            let load_ms = load_start.elapsed().as_millis() as u64;
            let (id, ver) = parse_connector_ref(&tc.use_ref);
            Ok((module, id, ver, tc.config.clone(), load_ms, transform_perms))
        })
        .collect::<Result<Vec<_>>>()?;

    // 4. Build stream contexts from config
    let max_batch = parse_byte_size(&config.resources.max_batch_bytes);
    let checkpoint_interval = parse_byte_size(&config.resources.checkpoint_interval_bytes);

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

    let compression = CompressionCodec::from_str_opt(config.resources.compression.as_deref());

    let on_data_error = match config.destination.on_data_error.as_str() {
        "skip" => DataErrorPolicy::Skip,
        "dlq" => DataErrorPolicy::Dlq,
        _ => DataErrorPolicy::Fail,
    };

    let schema_evolution = match &config.destination.schema_evolution {
        Some(se) => SchemaEvolutionPolicy {
            new_column: match se.new_column.as_str() {
                "ignore" => ColumnPolicy::Ignore,
                "fail" => ColumnPolicy::Fail,
                _ => ColumnPolicy::Add,
            },
            removed_column: match se.removed_column.as_str() {
                "fail" => ColumnPolicy::Fail,
                "add" => ColumnPolicy::Add,
                _ => ColumnPolicy::Ignore,
            },
            type_change: match se.type_change.as_str() {
                "coerce" => TypeChangePolicy::Coerce,
                "null" => TypeChangePolicy::Null,
                _ => TypeChangePolicy::Fail,
            },
            nullability_change: match se.nullability_change.as_str() {
                "fail" => NullabilityPolicy::Fail,
                _ => NullabilityPolicy::Allow,
            },
        },
        None => SchemaEvolutionPolicy::default(),
    };

    let stream_ctxs: Vec<StreamContext> = config
        .source
        .streams
        .iter()
        .map(|s| {
            let sync_mode = match s.sync_mode.as_str() {
                "incremental" => SyncMode::Incremental,
                "cdc" => SyncMode::Cdc,
                _ => SyncMode::FullRefresh,
            };

            // For incremental/CDC streams, load cursor from state backend
            let cursor_info = match sync_mode {
                SyncMode::Incremental => {
                    if let Some(cursor_field) = &s.cursor_field {
                        let last_value = state
                            .get_cursor(&config.pipeline, &s.name)
                            .ok()
                            .flatten()
                            .and_then(|cs| cs.cursor_value)
                            .map(CursorValue::Utf8);

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
                        .get_cursor(&config.pipeline, &s.name)
                        .ok()
                        .flatten()
                        .and_then(|cs| cs.cursor_value)
                        .map(CursorValue::Lsn);

                    Some(CursorInfo {
                        cursor_field: "lsn".to_string(),
                        cursor_type: CursorType::Lsn,
                        last_value,
                    })
                }
                _ => None,
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
                policies: StreamPolicies {
                    on_data_error,
                    schema_evolution,
                },
                write_mode: Some(write_mode),
                selected_columns: s.columns.clone(),
            }
        })
        .collect();

    let source_config = config.source.config.clone();
    let dest_config = config.destination.config.clone();
    let pipeline_name = config.pipeline.clone();
    let (source_connector_id, source_connector_version) =
        parse_connector_ref(&config.source.use_ref);
    let (dest_connector_id, dest_connector_version) =
        parse_connector_ref(&config.destination.use_ref);
    let stats = Arc::new(Mutex::new(RunStats::default()));

    let channel_capacity = usize::max(1, limits.max_inflight_batches as usize);
    let num_transforms = config.transforms.len();
    let transform_module_load_ms: Vec<u64> = transform_modules
        .iter()
        .map(|(_, _, _, _, ms, _)| *ms)
        .collect();

    // 5. Spawn per-stream tasks bounded by semaphore
    let parallelism = config.resources.parallelism.max(1) as usize;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(parallelism));

    tracing::info!(
        pipeline = config.pipeline,
        parallelism,
        num_streams = stream_ctxs.len(),
        num_transforms,
        "Starting per-stream pipeline execution"
    );

    let mut stream_join_handles: Vec<tokio::task::JoinHandle<Result<StreamResult, StreamError>>> =
        Vec::with_capacity(stream_ctxs.len());

    for stream_ctx in stream_ctxs.iter() {
        let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("Semaphore closed: {}", e))
        })?;

        // Clone all data needed for this stream's tasks
        let source_module = source_module.clone();
        let dest_module = dest_module.clone();
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
        let source_permissions = source_permissions.clone();
        let dest_permissions = dest_permissions.clone();
        let transform_modules_for_stream: Vec<_> = transform_modules
            .iter()
            .map(|(m, id, ver, cfg, _ms, perms)| {
                (m.clone(), id.clone(), ver.clone(), cfg.clone(), perms.clone())
            })
            .collect();

        let handle = tokio::spawn(async move {
            // Build per-stream channel chain: source -> [transforms] -> dest
            let num_t = transform_modules_for_stream.len();
            let mut channels = Vec::with_capacity(num_t + 1);
            for _ in 0..=num_t {
                channels.push(mpsc::channel::<Frame>(channel_capacity));
            }

            // channels[0] = (source_tx, transform_0_rx or dest_rx)
            // channels[N] = (transform_N-1_tx or source_tx, dest_rx)
            // Source writes to channels[0].0, Dest reads from channels[num_t].1

            // Extract sender/receiver pairs
            let (mut senders, mut receivers): (Vec<mpsc::Sender<Frame>>, Vec<mpsc::Receiver<Frame>>) = channels.into_iter().unzip();

            let source_tx = senders.remove(0);
            let dest_rx = receivers.pop().unwrap();

            // senders now has num_t elements (one per transform output)
            // receivers now has num_t elements (one per transform input)

            let stream_ctx_for_src = stream_ctx.clone();
            let stream_ctx_for_dst = stream_ctx.clone();
            let pipeline_name_src = pipeline_name.clone();
            let pipeline_name_dst = pipeline_name.clone();

            // Spawn source for this stream
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
                )
            });

            // Spawn transforms for this stream (in pipeline order)
            let mut transform_handles = Vec::with_capacity(num_t);
            for (i, (module, id, ver, tconfig, transform_perms)) in
                transform_modules_for_stream.into_iter().enumerate()
            {
                let rx = receivers.remove(0);
                let tx = senders.remove(0);
                let state_t = state_dst.clone();
                let stream_ctx_t = stream_ctx.clone();
                let pipeline_name_t = pipeline_name.clone();

                let t_handle = tokio::task::spawn_blocking(move || {
                    run_transform_stream(
                        &module,
                        rx,
                        tx,
                        state_t,
                        &pipeline_name_t,
                        &id,
                        &ver,
                        &tconfig,
                        &stream_ctx_t,
                        transform_perms.as_ref(),
                        compression,
                    )
                });
                transform_handles.push((i, t_handle));
            }

            // Spawn destination for this stream
            let dst_handle = tokio::task::spawn_blocking(move || {
                run_destination_stream(
                    &dest_module,
                    dest_rx,
                    state_dst,
                    &pipeline_name_dst,
                    &dest_connector_id,
                    &dest_connector_version,
                    &dest_config,
                    &stream_ctx_for_dst,
                    stats_dst,
                    dest_permissions.as_ref(),
                    compression,
                )
            });

            // Wait for source
            let src_result = src_handle.await.map_err(|e| {
                PipelineError::Infrastructure(anyhow::anyhow!(
                    "Source task panicked for stream '{}': {}",
                    stream_ctx.stream_name,
                    e
                ))
            })?;

            // Wait for transforms (in order)
            let mut transform_durations: Vec<f64> = Vec::new();
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
                    Ok((duration, summary)) => {
                        tracing::info!(
                            transform_index = i,
                            stream = stream_ctx.stream_name,
                            duration_secs = duration,
                            records_in = summary.records_in,
                            records_out = summary.records_out,
                            "Transform stage completed for stream"
                        );
                        transform_durations.push(duration);
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

            // Wait for destination
            let dst_result = dst_handle.await.map_err(|e| {
                PipelineError::Infrastructure(anyhow::anyhow!(
                    "Dest task panicked for stream '{}': {}",
                    stream_ctx.stream_name,
                    e
                ))
            })?;

            // Release semaphore permit
            drop(permit);

            // If a transform failed, propagate the error (but still collect DLQ records)
            if let Some(transform_err) = first_transform_error {
                let dlq_records = match dst_result {
                    Ok((_, _, _, _, _, _, dlq)) => dlq,
                    Err(derr) => derr.dlq_records,
                };
                return Err(StreamError { error: transform_err, dlq_records });
            }

            // Process source and dest results
            let (src_dur, read_summary, source_checkpoints, src_host_timings) = match src_result {
                Ok(ok) => ok,
                Err(src_err) => {
                    // Collect DLQ records from destination before returning
                    let dlq_records = match dst_result {
                        Ok((_, _, _, _, _, _, dlq)) => dlq,
                        Err(derr) => derr.dlq_records,
                    };
                    return Err(StreamError { error: src_err, dlq_records });
                }
            };

            let (dst_dur, write_summary, vm_setup_secs, recv_secs, dest_checkpoints, dst_host_timings, dlq_records) =
                match dst_result {
                    Ok(ok) => ok,
                    Err(derr) => {
                        return Err(StreamError { error: derr.error, dlq_records: derr.dlq_records });
                    }
                };

            Ok(StreamResult {
                read_summary,
                write_summary,
                source_checkpoints,
                dest_checkpoints,
                dlq_records,
                src_host_timings,
                dst_host_timings,
                src_duration: src_dur,
                dst_duration: dst_dur,
                vm_setup_secs,
                recv_secs,
                transform_durations,
            })
        });

        stream_join_handles.push(handle);
    }

    // 6. Wait for all streams and aggregate results
    let mut all_source_checkpoints: Vec<Checkpoint> = Vec::new();
    let mut all_dest_checkpoints: Vec<Checkpoint> = Vec::new();
    let mut all_dlq_records: Vec<DlqRecord> = Vec::new();
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
    let mut total_src_host_timings = HostTimings::default();
    let mut total_dst_host_timings = HostTimings::default();
    let mut max_src_duration: f64 = 0.0;
    let mut max_dst_duration: f64 = 0.0;
    let mut max_dst_vm_setup_secs: f64 = 0.0;
    let mut max_dst_recv_secs: f64 = 0.0;
    let mut all_transform_durations: Vec<f64> = Vec::new();
    let mut first_error: Option<PipelineError> = None;

    for handle in stream_join_handles {
        let result = handle.await.map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("Stream task panicked: {}", e))
        })?;

        match result {
            Ok(sr) => {
                // Aggregate read summary
                total_read_summary.records_read += sr.read_summary.records_read;
                total_read_summary.bytes_read += sr.read_summary.bytes_read;
                total_read_summary.batches_emitted += sr.read_summary.batches_emitted;
                total_read_summary.checkpoint_count += sr.read_summary.checkpoint_count;
                total_read_summary.records_skipped += sr.read_summary.records_skipped;

                // Aggregate write summary
                total_write_summary.records_written += sr.write_summary.records_written;
                total_write_summary.bytes_written += sr.write_summary.bytes_written;
                total_write_summary.batches_written += sr.write_summary.batches_written;
                total_write_summary.checkpoint_count += sr.write_summary.checkpoint_count;
                total_write_summary.records_failed += sr.write_summary.records_failed;

                // Collect checkpoints
                all_source_checkpoints.extend(sr.source_checkpoints);
                all_dest_checkpoints.extend(sr.dest_checkpoints);

                // Collect DLQ records
                all_dlq_records.extend(sr.dlq_records);

                // Aggregate host timings
                total_src_host_timings.emit_batch_nanos += sr.src_host_timings.emit_batch_nanos;
                total_src_host_timings.compress_nanos += sr.src_host_timings.compress_nanos;
                total_src_host_timings.emit_batch_count += sr.src_host_timings.emit_batch_count;
                total_dst_host_timings.next_batch_nanos += sr.dst_host_timings.next_batch_nanos;
                total_dst_host_timings.decompress_nanos += sr.dst_host_timings.decompress_nanos;
                total_dst_host_timings.next_batch_count += sr.dst_host_timings.next_batch_count;

                // Track max durations (parallel streams overlap, so max is more meaningful)
                if sr.src_duration > max_src_duration {
                    max_src_duration = sr.src_duration;
                }
                if sr.dst_duration > max_dst_duration {
                    max_dst_duration = sr.dst_duration;
                    // Keep sub-phase timings from the slowest stream for accurate overhead calculation
                    max_dst_vm_setup_secs = sr.vm_setup_secs;
                    max_dst_recv_secs = sr.recv_secs;
                }
                all_transform_durations.extend(sr.transform_durations);
            }
            Err(StreamError { error, dlq_records }) => {
                tracing::error!("Stream failed: {}", error);
                all_dlq_records.extend(dlq_records);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }

    let final_stats = stats.lock().unwrap().clone();

    // If any stream failed, record failure and return error
    if let Some(err) = first_error {
        state.complete_run(
            run_id,
            RunStatus::Failed,
            &RunStats {
                records_read: final_stats.records_read,
                records_written: final_stats.records_written,
                bytes_read: final_stats.bytes_read,
                error_message: Some(format!("Stream error: {}", err)),
            },
        )?;
        super::dlq::persist_dlq_records(
            state.as_ref(),
            &config.pipeline,
            run_id,
            &all_dlq_records,
        );
        return Err(err);
    }

    // All streams succeeded — finalize

    // For PipelineResult timing, we use max (wall-clock) duration since streams run in parallel
    let connector_internal_secs = total_write_summary
        .perf
        .as_ref()
        .map(|p| p.connect_secs + p.flush_secs + p.commit_secs)
        .unwrap_or(0.0);
    let wasm_overhead_secs =
        (max_dst_duration - max_dst_vm_setup_secs - max_dst_recv_secs - connector_internal_secs)
            .max(0.0);

    state.complete_run(
        run_id,
        RunStatus::Completed,
        &RunStats {
            records_read: total_read_summary.records_read,
            records_written: total_write_summary.records_written,
            bytes_read: total_read_summary.bytes_read,
            error_message: None,
        },
    )?;

    // Checkpoint coordination: persist cursor only when both source and
    // dest confirm the stream data (per spec: State + Checkpointing)
    tracing::debug!(
        pipeline = config.pipeline,
        source_checkpoint_count = all_source_checkpoints.len(),
        dest_checkpoint_count = all_dest_checkpoints.len(),
        "About to correlate checkpoints"
    );
    let cursors_advanced = correlate_and_persist_cursors(
        state.as_ref(),
        &config.pipeline,
        &all_source_checkpoints,
        &all_dest_checkpoints,
    )?;
    if cursors_advanced > 0 {
        tracing::info!(
            pipeline = config.pipeline,
            cursors_advanced,
            "Checkpoint coordination complete"
        );
    }

    super::dlq::persist_dlq_records(
        state.as_ref(),
        &config.pipeline,
        run_id,
        &all_dlq_records,
    );

    let duration = start.elapsed();
    let src_perf = total_read_summary.perf.as_ref();
    let perf = total_write_summary.perf.as_ref();

    tracing::info!(
        pipeline = config.pipeline,
        records_read = total_read_summary.records_read,
        records_written = total_write_summary.records_written,
        duration_secs = duration.as_secs_f64(),
        "Pipeline run completed"
    );

    Ok(PipelineResult {
        records_read: total_read_summary.records_read,
        records_written: total_write_summary.records_written,
        bytes_read: total_read_summary.bytes_read,
        bytes_written: total_write_summary.bytes_written,
        duration_secs: duration.as_secs_f64(),
        source_duration_secs: max_src_duration,
        dest_duration_secs: max_dst_duration,
        source_module_load_ms,
        dest_module_load_ms,
        source_connect_secs: src_perf.map(|p| p.connect_secs).unwrap_or(0.0),
        source_query_secs: src_perf.map(|p| p.query_secs).unwrap_or(0.0),
        source_fetch_secs: src_perf.map(|p| p.fetch_secs).unwrap_or(0.0),
        source_arrow_encode_secs: src_perf.map(|p| p.arrow_encode_secs).unwrap_or(0.0),
        dest_arrow_decode_secs: perf.map(|p| p.arrow_decode_secs).unwrap_or(0.0),
        dest_connect_secs: perf.map(|p| p.connect_secs).unwrap_or(0.0),
        dest_flush_secs: perf.map(|p| p.flush_secs).unwrap_or(0.0),
        dest_commit_secs: perf.map(|p| p.commit_secs).unwrap_or(0.0),
        dest_vm_setup_secs: max_dst_vm_setup_secs,
        dest_recv_secs: max_dst_recv_secs,
        wasm_overhead_secs,
        source_emit_nanos: total_src_host_timings.emit_batch_nanos,
        source_compress_nanos: total_src_host_timings.compress_nanos,
        source_emit_count: total_src_host_timings.emit_batch_count,
        dest_recv_nanos: total_dst_host_timings.next_batch_nanos,
        dest_decompress_nanos: total_dst_host_timings.decompress_nanos,
        dest_recv_count: total_dst_host_timings.next_batch_count,
        transform_count: all_transform_durations.len(),
        transform_duration_secs: all_transform_durations.iter().sum(),
        transform_module_load_ms,
        retry_count: attempt - 1,
    })
}

/// Check a pipeline: validate configuration and connectivity without running.
pub async fn check_pipeline(config: &PipelineConfig) -> Result<CheckResult> {
    tracing::info!(
        pipeline = config.pipeline,
        "Checking pipeline configuration"
    );

    // 1. Resolve connector paths
    let source_wasm = component_runtime::resolve_connector_path(&config.source.use_ref)?;
    let dest_wasm = component_runtime::resolve_connector_path(&config.destination.use_ref)?;

    // 1b. Validate manifests
    let source_manifest =
        load_and_validate_manifest(&source_wasm, &config.source.use_ref, ConnectorRole::Source)?;
    let dest_manifest = load_and_validate_manifest(
        &dest_wasm,
        &config.destination.use_ref,
        ConnectorRole::Destination,
    )?;

    if source_manifest.is_some() {
        println!("Source manifest:    OK");
    }
    if dest_manifest.is_some() {
        println!("Dest manifest:      OK");
    }

    // 1c. Validate connector config against manifest schemas (zero-boot)
    if let Some(ref sm) = source_manifest {
        match validate_config_against_schema(&config.source.use_ref, &config.source.config, sm) {
            Ok(()) => println!("Source config:      OK"),
            Err(e) => println!("Source config:      FAILED\n  {}", e),
        }
    }
    if let Some(ref dm) = dest_manifest {
        match validate_config_against_schema(
            &config.destination.use_ref,
            &config.destination.config,
            dm,
        ) {
            Ok(()) => println!("Dest config:        OK"),
            Err(e) => println!("Dest config:        FAILED\n  {}", e),
        }
    }

    // 2. Check state backend
    let state_ok = check_state_backend(config);

    // 3. Validate source connector
    let source_config = config.source.config.clone();
    let source_permissions = source_manifest.as_ref().map(|m| m.permissions.clone());
    let (src_id, src_ver) = parse_connector_ref(&config.source.use_ref);
    let source_validation_handle = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
        validate_connector(
            &source_wasm,
            ConnectorRole::Source,
            &src_id,
            &src_ver,
            &source_config,
            source_permissions.as_ref(),
        )
    });

    // 4. Validate destination connector
    let dest_config = config.destination.config.clone();
    let dest_permissions = dest_manifest.as_ref().map(|m| m.permissions.clone());
    let (dst_id, dst_ver) = parse_connector_ref(&config.destination.use_ref);
    let dest_validation_handle = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
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
        .map_err(|e| anyhow::anyhow!("Source validation task panicked: {}", e))??;
    let dest_validation = dest_validation_handle
        .await
        .map_err(|e| anyhow::anyhow!("Destination validation task panicked: {}", e))??;

    // 5. Validate transform connectors
    let mut transform_tasks = Vec::with_capacity(config.transforms.len());
    for (index, tc) in config.transforms.iter().enumerate() {
        let wasm_path = component_runtime::resolve_connector_path(&tc.use_ref)?;
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
                "Transform validation task panicked (index {}, {}): {}",
                index,
                connector_ref,
                e
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
/// Resolves the connector path, loads its manifest for permissions,
/// then calls `run_discover` on a blocking thread.
pub async fn discover_connector(
    connector_ref: &str,
    config: &serde_json::Value,
) -> Result<Catalog> {
    let wasm_path = component_runtime::resolve_connector_path(connector_ref)?;

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
    .map_err(|e| anyhow::anyhow!("Discover task panicked: {}", e))?
}

fn create_state_backend(config: &PipelineConfig) -> Result<SqliteStateBackend> {
    match &config.state.connection {
        Some(path) => {
            SqliteStateBackend::new(std::path::Path::new(path)).context("Failed to open state DB")
        }
        None => {
            // Default: ~/.rapidbyte/state.db
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
            let state_path = PathBuf::from(home).join(".rapidbyte").join("state.db");
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

/// Load and validate a connector manifest against the expected role.
/// Returns the manifest if found, or None if no manifest exists (backwards compat).
/// Fails if manifest exists but declares incompatible role or protocol version.
fn load_and_validate_manifest(
    wasm_path: &std::path::Path,
    connector_ref: &str,
    expected_role: ConnectorRole,
) -> Result<Option<rapidbyte_sdk::manifest::ConnectorManifest>> {
    let manifest = component_runtime::load_connector_manifest(wasm_path)?;

    if let Some(ref m) = manifest {
        if !m.supports_role(expected_role) {
            anyhow::bail!(
                "Connector '{}' does not support {:?} role",
                connector_ref,
                expected_role,
            );
        }

        if m.protocol_version != "2" {
            tracing::warn!(
                connector = connector_ref,
                manifest_protocol = m.protocol_version,
                host_protocol = "2",
                "Protocol version mismatch"
            );
        }

        tracing::info!(
            connector = m.id,
            version = m.version,
            "Loaded connector manifest"
        );
    } else {
        tracing::debug!(
            connector = connector_ref,
            "No manifest found, skipping pre-flight validation"
        );
    }

    Ok(manifest)
}

/// Validate connector config against the manifest's config_schema.
/// Returns Ok(()) if no schema is defined or if validation passes.
fn validate_config_against_schema(
    connector_ref: &str,
    config: &serde_json::Value,
    manifest: &rapidbyte_sdk::manifest::ConnectorManifest,
) -> Result<()> {
    let schema_value = match &manifest.config_schema {
        Some(s) => s,
        None => return Ok(()),
    };

    let validator = jsonschema::validator_for(schema_value).with_context(|| {
        format!(
            "Invalid JSON Schema in manifest for connector '{}'",
            connector_ref,
        )
    })?;

    let errors: Vec<String> = validator
        .iter_errors(config)
        .map(|e| format!("  - {}", e))
        .collect();

    if !errors.is_empty() {
        anyhow::bail!(
            "Configuration validation failed for connector '{}':\n{}",
            connector_ref,
            errors.join("\n"),
        );
    }

    tracing::debug!(connector = connector_ref, "Config schema validation passed");
    Ok(())
}
