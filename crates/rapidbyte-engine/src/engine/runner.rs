//! Connector runner utilities for source, destination, transform, validate, and discover flows.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

use rapidbyte_types::catalog::Catalog;
use rapidbyte_types::checkpoint::Checkpoint;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::error::ValidationResult;
use rapidbyte_types::manifest::Permissions;
use rapidbyte_types::metric::{ReadSummary, TransformSummary, WriteSummary};
use rapidbyte_types::stream::StreamContext;
use rapidbyte_types::wire::ConnectorRole;

use super::errors::PipelineError;
use rapidbyte_runtime::wasmtime_reexport::HasSelf;
use rapidbyte_runtime::{
    create_component_linker, dest_bindings, dest_error_to_sdk, dest_validation_to_sdk,
    source_bindings, source_error_to_sdk, source_validation_to_sdk, transform_bindings,
    transform_error_to_sdk, transform_validation_to_sdk, ComponentHostState, CompressionCodec,
    Frame, HostTimings, LoadedComponent, SandboxOverrides, WasmRuntime,
};
use rapidbyte_state::{SqliteStateBackend, StateBackend};
use rapidbyte_types::state::RunStats;

#[derive(Debug, Clone, Default)]
pub struct PipelineCounts {
    pub records_read: u64,
    pub records_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Default)]
pub struct SourceTiming {
    pub duration_secs: f64,
    pub module_load_ms: u64,
    pub connect_secs: f64,
    pub query_secs: f64,
    pub fetch_secs: f64,
    pub arrow_encode_secs: f64,
    pub emit_nanos: u64,
    pub compress_nanos: u64,
    pub emit_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DestTiming {
    pub duration_secs: f64,
    pub module_load_ms: u64,
    pub connect_secs: f64,
    pub flush_secs: f64,
    pub commit_secs: f64,
    pub arrow_decode_secs: f64,
    pub vm_setup_secs: f64,
    pub recv_secs: f64,
    pub recv_nanos: u64,
    pub decompress_nanos: u64,
    pub recv_count: u64,
}

/// Result of a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    pub counts: PipelineCounts,
    pub source: SourceTiming,
    pub dest: DestTiming,
    pub transform_count: usize,
    pub transform_duration_secs: f64,
    pub transform_module_load_ms: Vec<u64>,
    pub duration_secs: f64,
    pub wasm_overhead_secs: f64,
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

fn handle_close_result<E, F>(
    result: std::result::Result<std::result::Result<(), E>, rapidbyte_runtime::wasmtime_reexport::Error>,
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

/// Run a source connector for a single stream.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_source_stream(
    module: &LoadedComponent,
    sender: mpsc::Sender<Frame>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    connector_id: &str,
    connector_version: &str,
    source_config: &serde_json::Value,
    stream_ctx: &StreamContext,
    stats: Arc<Mutex<RunStats>>,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
    overrides: Option<&SandboxOverrides>,
) -> Result<(f64, ReadSummary, Vec<Checkpoint>, HostTimings), PipelineError> {
    let phase_start = Instant::now();

    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let source_timings = Arc::new(Mutex::new(HostTimings::default()));

    let mut builder = ComponentHostState::builder()
        .pipeline(pipeline_name)
        .connector_id(connector_id)
        .stream(stream_ctx.stream_name.clone())
        .state_backend(state_backend)
        .sender(sender.clone())
        .source_checkpoints(source_checkpoints.clone())
        .timings(source_timings.clone())
        .config(source_config)
        .compression(compression);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(o) = overrides {
        builder = builder.overrides(o);
    }
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let linker = create_component_linker(&module.engine, "source", |linker| {
        source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(
            linker,
            |state| state,
        )
        .context("Failed to add rapidbyte source host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings =
        source_bindings::RapidbyteSource::instantiate(&mut store, &module.component, &linker)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let iface = bindings.rapidbyte_connector_source_connector();

    let source_config_json = serde_json::to_string(source_config)
        .context("Failed to serialize source config")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(
        connector = connector_id,
        version = connector_version,
        stream = stream_ctx.stream_name,
        "Opening source connector for stream"
    );
    let open_result = iface
        .call_open(&mut store, &source_config_json)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;
    if let Err(err) = open_result {
        return Err(PipelineError::Connector(source_error_to_sdk(err)));
    }

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
        Err(err) => {
            let _ = iface.call_close(&mut store);
            return Err(PipelineError::Connector(source_error_to_sdk(err)));
        }
    };

    tracing::info!(
        stream = stream_ctx.stream_name,
        records = summary.records_read,
        bytes = summary.bytes_read,
        "Source read complete for stream"
    );

    {
        let mut s = stats.lock().map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("run stats mutex poisoned"))
        })?;
        s.records_read += summary.records_read;
        s.bytes_read += summary.bytes_read;
    }

    let _ = sender.blocking_send(Frame::EndStream);

    tracing::info!(
        connector = connector_id,
        version = connector_version,
        stream = stream_ctx.stream_name,
        "Closing source connector for stream"
    );
    handle_close_result(
        iface.call_close(&mut store),
        "Source",
        &stream_ctx.stream_name,
        |err| source_error_to_sdk(err).to_string(),
    );

    let checkpoints = source_checkpoints
        .lock()
        .map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("source checkpoint mutex poisoned"))
        })?
        .drain(..)
        .collect::<Vec<_>>();
    let source_host_timings = source_timings
        .lock()
        .map_err(|_| {
            PipelineError::Infrastructure(anyhow::anyhow!("source timing mutex poisoned"))
        })?
        .clone();

    Ok((
        phase_start.elapsed().as_secs_f64(),
        summary,
        checkpoints,
        source_host_timings,
    ))
}

/// Run a destination connector for a single stream.
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub(crate) fn run_destination_stream(
    module: &LoadedComponent,
    receiver: mpsc::Receiver<Frame>,
    dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    connector_id: &str,
    connector_version: &str,
    dest_config: &serde_json::Value,
    stream_ctx: &StreamContext,
    stats: Arc<Mutex<RunStats>>,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
    overrides: Option<&SandboxOverrides>,
) -> Result<(f64, WriteSummary, f64, f64, Vec<Checkpoint>, HostTimings), PipelineError> {
    let phase_start = Instant::now();
    let vm_setup_start = Instant::now();

    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let dest_timings = Arc::new(Mutex::new(HostTimings::default()));

    let mut builder = ComponentHostState::builder()
        .pipeline(pipeline_name)
        .connector_id(connector_id)
        .stream(stream_ctx.stream_name.clone())
        .state_backend(state_backend)
        .receiver(receiver)
        .dest_checkpoints(dest_checkpoints.clone())
        .dlq_records(dlq_records.clone())
        .timings(dest_timings.clone())
        .config(dest_config)
        .compression(compression);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(o) = overrides {
        builder = builder.overrides(o);
    }
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = overrides.and_then(|o| o.timeout_seconds);
    (|| {
        let mut store = module.new_store(host_state, timeout);
        let linker = create_component_linker(&module.engine, "destination", |linker| {
            dest_bindings::RapidbyteDestination::add_to_linker::<
                    _,
                    HasSelf<_>,
                >(linker, |state| state)
                .context("Failed to add rapidbyte destination host imports")?;
            Ok(())
        })
        .map_err(PipelineError::Infrastructure)?;
        let bindings = dest_bindings::RapidbyteDestination::instantiate(
            &mut store,
            &module.component,
            &linker,
        )
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

        let iface = bindings.rapidbyte_connector_dest_connector();
        let vm_setup_secs = vm_setup_start.elapsed().as_secs_f64();

        let dest_config_json = serde_json::to_string(dest_config)
            .context("Failed to serialize destination config")
            .map_err(PipelineError::Infrastructure)?;

        tracing::info!(
            connector = connector_id,
            version = connector_version,
            stream = stream_ctx.stream_name,
            "Opening destination connector for stream"
        );
        let open_result = iface
            .call_open(&mut store, &dest_config_json)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;
        if let Err(err) = open_result {
            return Err(PipelineError::Connector(dest_error_to_sdk(err)));
        }

        let recv_start = Instant::now();
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
            Err(err) => {
                let _ = iface.call_close(&mut store);
                return Err(PipelineError::Connector(dest_error_to_sdk(err)));
            }
        };

        tracing::info!(
            stream = stream_ctx.stream_name,
            records = summary.records_written,
            bytes = summary.bytes_written,
            "Destination write complete for stream"
        );

        {
            let mut s = stats.lock().map_err(|_| {
                PipelineError::Infrastructure(anyhow::anyhow!("run stats mutex poisoned"))
            })?;
            s.records_written += summary.records_written;
        }

        let recv_secs = recv_start.elapsed().as_secs_f64();

        tracing::info!(
            connector = connector_id,
            version = connector_version,
            stream = stream_ctx.stream_name,
            "Closing destination connector for stream"
        );
        handle_close_result(
            iface.call_close(&mut store),
            "Destination",
            &stream_ctx.stream_name,
            |err| dest_error_to_sdk(err).to_string(),
        );

        let checkpoints = dest_checkpoints
            .lock()
            .map_err(|_| {
                PipelineError::Infrastructure(anyhow::anyhow!(
                    "destination checkpoint mutex poisoned"
                ))
            })?
            .drain(..)
            .collect::<Vec<_>>();
        let dest_host_timings = dest_timings
            .lock()
            .map_err(|_| {
                PipelineError::Infrastructure(anyhow::anyhow!("destination timing mutex poisoned"))
            })?
            .clone();

        Ok((
            phase_start.elapsed().as_secs_f64(),
            summary,
            vm_setup_secs,
            recv_secs,
            checkpoints,
            dest_host_timings,
        ))
    })()
}

/// Run a transform connector for a single stream.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_transform_stream(
    module: &LoadedComponent,
    receiver: mpsc::Receiver<Frame>,
    sender: mpsc::Sender<Frame>,
    state_backend: Arc<dyn StateBackend>,
    pipeline_name: &str,
    connector_id: &str,
    connector_version: &str,
    transform_config: &serde_json::Value,
    stream_ctx: &StreamContext,
    permissions: Option<&Permissions>,
    compression: Option<CompressionCodec>,
    overrides: Option<&SandboxOverrides>,
) -> Result<(f64, TransformSummary), PipelineError> {
    let phase_start = Instant::now();

    let source_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let dest_checkpoints: Arc<Mutex<Vec<Checkpoint>>> = Arc::new(Mutex::new(Vec::new()));
    let timings = Arc::new(Mutex::new(HostTimings::default()));

    let mut builder = ComponentHostState::builder()
        .pipeline(pipeline_name)
        .connector_id(connector_id)
        .stream(stream_ctx.stream_name.clone())
        .state_backend(state_backend)
        .sender(sender.clone())
        .receiver(receiver)
        .source_checkpoints(source_checkpoints)
        .dest_checkpoints(dest_checkpoints)
        .timings(timings)
        .config(transform_config)
        .compression(compression);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    if let Some(o) = overrides {
        builder = builder.overrides(o);
    }
    let host_state = builder.build().map_err(PipelineError::Infrastructure)?;

    let timeout = overrides.and_then(|o| o.timeout_seconds);
    let mut store = module.new_store(host_state, timeout);
    let linker = create_component_linker(&module.engine, "transform", |linker| {
        transform_bindings::RapidbyteTransform::add_to_linker::<
            _,
            HasSelf<_>,
        >(linker, |state| state)
        .context("Failed to add rapidbyte transform host imports")?;
        Ok(())
    })
    .map_err(PipelineError::Infrastructure)?;
    let bindings =
        transform_bindings::RapidbyteTransform::instantiate(&mut store, &module.component, &linker)
            .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;

    let iface = bindings.rapidbyte_connector_transform_connector();

    let transform_config_json = serde_json::to_string(transform_config)
        .context("Failed to serialize transform config")
        .map_err(PipelineError::Infrastructure)?;

    tracing::info!(
        connector = connector_id,
        version = connector_version,
        stream = stream_ctx.stream_name,
        "Opening transform connector for stream"
    );
    let open_result = iface
        .call_open(&mut store, &transform_config_json)
        .map_err(|e| PipelineError::Infrastructure(anyhow::anyhow!(e)))?;
    if let Err(err) = open_result {
        return Err(PipelineError::Connector(transform_error_to_sdk(err)));
    }

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
        Err(err) => {
            let _ = iface.call_close(&mut store);
            return Err(PipelineError::Connector(transform_error_to_sdk(err)));
        }
    };

    let _ = sender.blocking_send(Frame::EndStream);

    tracing::info!(
        connector = connector_id,
        version = connector_version,
        stream = stream_ctx.stream_name,
        "Closing transform connector for stream"
    );
    handle_close_result(
        iface.call_close(&mut store),
        "Transform",
        &stream_ctx.stream_name,
        |err| transform_error_to_sdk(err).to_string(),
    );

    Ok((phase_start.elapsed().as_secs_f64(), summary))
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

    let mut builder = ComponentHostState::builder()
        .pipeline("check")
        .connector_id(connector_id)
        .stream("check")
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    let host_state = builder.build()?;

    let mut store = module.new_store(host_state, None);
    let config_json = serde_json::to_string(config)?;

    match role {
        ConnectorRole::Source => {
            let linker = create_component_linker(&module.engine, "source", |linker| {
                source_bindings::RapidbyteSource::add_to_linker::<
                    _,
                    HasSelf<_>,
                >(linker, |state| state)?;
                Ok(())
            })?;
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
                .map(source_validation_to_sdk)
                .map_err(source_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store);
            result
        }
        ConnectorRole::Destination => {
            let linker = create_component_linker(&module.engine, "destination", |linker| {
                dest_bindings::RapidbyteDestination::add_to_linker::<
                    _,
                    HasSelf<_>,
                >(linker, |state| state)?;
                Ok(())
            })?;
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
                .map(dest_validation_to_sdk)
                .map_err(dest_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store);
            result
        }
        ConnectorRole::Transform => {
            let linker = create_component_linker(&module.engine, "transform", |linker| {
                transform_bindings::RapidbyteTransform::add_to_linker::<
                    _,
                    HasSelf<_>,
                >(linker, |state| state)?;
                Ok(())
            })?;
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
                .map(transform_validation_to_sdk)
                .map_err(transform_error_to_sdk)
                .map_err(|e| anyhow::anyhow!(e.to_string()));
            let _ = iface.call_close(&mut store);
            result
        }
    }
}

/// Discover available streams from a source connector.
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
    let mut builder = ComponentHostState::builder()
        .pipeline("discover")
        .connector_id(connector_id)
        .stream("discover")
        .state_backend(state)
        .config(config)
        .compression(None);
    if let Some(p) = permissions {
        builder = builder.permissions(p);
    }
    let host_state = builder.build()?;

    let mut store = module.new_store(host_state, None);
    let linker = create_component_linker(&module.engine, "source", |linker| {
        source_bindings::RapidbyteSource::add_to_linker::<_, HasSelf<_>>(
            linker,
            |state| state,
        )?;
        Ok(())
    })?;
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

#[cfg(test)]
mod tests {
    use super::handle_close_result;

    #[test]
    fn test_handle_close_result_ok() {
        handle_close_result::<String, _>(Ok(Ok(())), "Source", "users", |e| e);
    }
}
