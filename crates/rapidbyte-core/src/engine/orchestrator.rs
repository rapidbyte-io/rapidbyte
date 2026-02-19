use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};

use rapidbyte_sdk::errors::ValidationResult;
use rapidbyte_sdk::protocol::{
    Catalog, ConnectorRole, CursorInfo, CursorType, CursorValue, DataErrorPolicy, SchemaHint,
    StreamContext, StreamLimits, StreamPolicies, SyncMode, WriteMode,
};

use super::checkpoint::correlate_and_persist_cursors;
use super::errors::{compute_backoff, PipelineError};
use super::runner::{run_destination, run_discover, run_source, run_transform, validate_connector};
pub use super::runner::{CheckResult, PipelineResult};
use crate::pipeline::types::{parse_byte_size, PipelineConfig};
use crate::runtime::compression::CompressionCodec;
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
                    std::thread::sleep(delay);
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

/// Execute a single pipeline attempt: source -> destination with state tracking.
async fn execute_pipeline_once(config: &PipelineConfig, attempt: u32) -> Result<PipelineResult, PipelineError> {
    let start = Instant::now();

    tracing::info!(pipeline = config.pipeline, "Starting pipeline run");

    // 1. Resolve connector paths
    let source_wasm = wasm_runtime::resolve_connector_path(&config.source.use_ref)?;
    let dest_wasm = wasm_runtime::resolve_connector_path(&config.destination.use_ref)?;

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
    let runtime = WasmRuntime::new()?;

    let source_load_start = Instant::now();
    let source_module = runtime.load_module(&source_wasm)?;
    let source_module_load_ms = source_load_start.elapsed().as_millis() as u64;

    let dest_load_start = Instant::now();
    let dest_module = runtime.load_module(&dest_wasm)?;
    let dest_module_load_ms = dest_load_start.elapsed().as_millis() as u64;

    // 3b. Load transform modules (in order)
    let transform_modules = config
        .transforms
        .iter()
        .map(|tc| {
            let wasm_path = wasm_runtime::resolve_connector_path(&tc.use_ref)?;
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

            // For incremental streams, load cursor from state backend
            let cursor_info = if sync_mode == SyncMode::Incremental {
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
                policies: StreamPolicies {
                    on_data_error,
                    ..StreamPolicies::default()
                },
                write_mode: Some(write_mode),
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

    // 5. Build channel chain: source -> [transforms] -> dest
    // For N transforms we need N+1 channels.
    // Channel i connects stage i to stage i+1.
    let num_transforms = config.transforms.len();
    let mut senders: Vec<mpsc::SyncSender<Frame>> = Vec::with_capacity(num_transforms + 1);
    let mut receivers: Vec<mpsc::Receiver<Frame>> = Vec::with_capacity(num_transforms + 1);

    for _ in 0..=num_transforms {
        let (tx, rx) = mpsc::sync_channel::<Frame>(limits.max_inflight_batches as usize);
        senders.push(tx);
        receivers.push(rx);
    }

    // Source writes to senders[0], dest reads from receivers[num_transforms].
    // After removal:
    //   senders  = [s1, s2, ..., s_n]   (n elements for n transforms)
    //   receivers = [r0, r1, ..., r_{n-1}] (n elements for n transforms)
    // Transform[i] gets (receivers[i], senders[i]) = (r_i, s_{i+1}).
    let batch_tx = senders.remove(0);
    let batch_rx = receivers.pop().unwrap(); // last receiver

    // 5a. Spawn source
    let state_src = state.clone();
    let stats_src = stats.clone();
    let stream_ctxs_clone = stream_ctxs.clone();
    let pipeline_name_src = pipeline_name.clone();

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
            source_permissions.as_ref(),
            compression,
        )
    });

    // 5b. Spawn transform threads (in pipeline order)
    let transform_module_load_ms: Vec<u64> = transform_modules
        .iter()
        .map(|(_, _, _, _, ms, _)| *ms)
        .collect();
    let mut transform_handles = Vec::new();
    for (module, id, ver, tconfig, _load_ms, transform_perms) in transform_modules.into_iter() {
        let rx = receivers.remove(0);
        let tx = senders.remove(0);
        let state_t = state.clone();
        let stats_t = stats.clone();
        let stream_ctxs_t = stream_ctxs_clone.clone();
        let pipeline_name_t = pipeline_name.clone();

        let handle = tokio::task::spawn_blocking(move || {
            run_transform(
                module,
                rx,
                tx,
                state_t,
                &pipeline_name_t,
                &id,
                &ver,
                &tconfig,
                &stream_ctxs_t,
                stats_t,
                transform_perms.as_ref(),
                compression,
            )
        });
        transform_handles.push(handle);
    }

    // 5c. Spawn destination
    let state_dst = state.clone();
    let stats_dst = stats.clone();
    let pipeline_name_dst = pipeline_name.clone();

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
            dest_permissions.as_ref(),
            compression,
        )
    });

    // 6. Wait for all stages
    let source_result = source_handle.await.map_err(|e| {
        PipelineError::Infrastructure(anyhow::anyhow!("Source task panicked: {}", e))
    })?;

    // 6a. Wait for transforms (in order)
    let mut transform_durations: Vec<f64> = Vec::new();
    for (i, handle) in transform_handles.into_iter().enumerate() {
        let result = handle.await.map_err(|e| {
            PipelineError::Infrastructure(anyhow::anyhow!("Transform {} task panicked: {}", i, e))
        })?;
        match result {
            Ok((duration, summary)) => {
                tracing::info!(
                    transform_index = i,
                    duration_secs = duration,
                    records_in = summary.records_in,
                    records_out = summary.records_out,
                    "Transform stage completed"
                );
                transform_durations.push(duration);
            }
            Err(e) => {
                tracing::error!(transform_index = i, "Transform failed: {}", e);
                // Record failure and propagate
                let final_stats = stats.lock().unwrap().clone();
                let state_backend = create_state_backend(config)?;
                state_backend.complete_run(
                    run_id,
                    RunStatus::Failed,
                    &RunStats {
                        records_read: final_stats.records_read,
                        records_written: final_stats.records_written,
                        bytes_read: final_stats.bytes_read,
                        error_message: Some(format!("Transform {} error: {}", i, e)),
                    },
                )?;
                return Err(e);
            }
        }
    }

    // 6b. Wait for destination
    let dest_result = dest_handle
        .await
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!("Dest task panicked: {}", e)))?;

    let final_stats = stats.lock().unwrap().clone();

    match (&source_result, &dest_result) {
        (
            Ok((src_dur, read_summary, source_checkpoints)),
            Ok((dst_dur, write_summary, vm_setup_secs, recv_secs, dest_checkpoints)),
        ) => {
            let perf = write_summary.perf.as_ref();
            let src_perf = read_summary.perf.as_ref();
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
                source_checkpoints,
                dest_checkpoints,
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
                source_connect_secs: src_perf.map(|p| p.connect_secs).unwrap_or(0.0),
                source_query_secs: src_perf.map(|p| p.query_secs).unwrap_or(0.0),
                source_fetch_secs: src_perf.map(|p| p.fetch_secs).unwrap_or(0.0),
                dest_connect_secs: perf.map(|p| p.connect_secs).unwrap_or(0.0),
                dest_flush_secs: perf.map(|p| p.flush_secs).unwrap_or(0.0),
                dest_commit_secs: perf.map(|p| p.commit_secs).unwrap_or(0.0),
                dest_vm_setup_secs: *vm_setup_secs,
                dest_recv_secs: *recv_secs,
                wasm_overhead_secs,
                transform_count: transform_durations.len(),
                transform_duration_secs: transform_durations.iter().sum(),
                transform_module_load_ms,
                retry_count: attempt - 1,
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
    tracing::info!(
        pipeline = config.pipeline,
        "Checking pipeline configuration"
    );

    // 1. Resolve connector paths
    let source_wasm = wasm_runtime::resolve_connector_path(&config.source.use_ref)?;
    let dest_wasm = wasm_runtime::resolve_connector_path(&config.destination.use_ref)?;

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
    let source_validation = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
        validate_connector(
            &source_wasm,
            &src_id,
            &src_ver,
            &source_config,
            source_permissions.as_ref(),
        )
    })
    .await??;

    // 4. Validate destination connector
    let dest_config = config.destination.config.clone();
    let dest_permissions = dest_manifest.as_ref().map(|m| m.permissions.clone());
    let (dst_id, dst_ver) = parse_connector_ref(&config.destination.use_ref);
    let dest_validation = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
        validate_connector(
            &dest_wasm,
            &dst_id,
            &dst_ver,
            &dest_config,
            dest_permissions.as_ref(),
        )
    })
    .await??;

    // 5. Validate transform connectors
    let mut transform_validations = Vec::new();
    for tc in &config.transforms {
        let wasm_path = wasm_runtime::resolve_connector_path(&tc.use_ref)?;
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
        let (tc_id, tc_ver) = parse_connector_ref(&tc.use_ref);
        let result = tokio::task::spawn_blocking(move || -> Result<ValidationResult> {
            validate_connector(
                &wasm_path,
                &tc_id,
                &tc_ver,
                &config_val,
                transform_perms.as_ref(),
            )
        })
        .await??;
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
    let wasm_path = wasm_runtime::resolve_connector_path(connector_ref)?;

    let manifest =
        load_and_validate_manifest(&wasm_path, connector_ref, ConnectorRole::Source)?;

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
    .await?
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
    let manifest = wasm_runtime::load_connector_manifest(wasm_path)?;

    if let Some(ref m) = manifest {
        if !m.supports_role(expected_role) {
            anyhow::bail!(
                "Connector '{}' does not support {:?} role",
                connector_ref,
                expected_role,
            );
        }

        if m.protocol_version != "1" {
            tracing::warn!(
                connector = connector_ref,
                manifest_protocol = m.protocol_version,
                host_protocol = "1",
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
