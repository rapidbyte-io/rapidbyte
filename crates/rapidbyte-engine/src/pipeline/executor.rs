//! Single-stream pipeline execution: source -> transforms -> destination.
//!
//! # Frame protocol
//!
//! - Channels are `sync_mpsc::sync_channel` with capacity = `params.channel_capacity`
//! - Source emits `Frame::Data` followed by `Frame::EndStream`
//! - Transforms consume from upstream, emit to downstream, send `EndStream` when done
//! - Destination (or dry-run collector) consumes until `EndStream` or channel close
//! - Frame ordering: Data frames arrive in emission order; `EndStream` is always last

use std::sync::{mpsc as sync_mpsc, Arc, Mutex};

use rapidbyte_runtime::{Frame, LoadedComponent};
use rapidbyte_state::StateBackend;
use rapidbyte_types::envelope::DlqRecord;
use rapidbyte_types::metric::WriteSummary;
use rapidbyte_types::state::RunStats;
use rapidbyte_types::stream::StreamContext;

use crate::arrow::ipc_to_record_batches;
use crate::error::PipelineError;
use crate::execution::DryRunStreamResult;
use crate::pipeline::planner::StreamExecutionParams;
use crate::pipeline::scheduler::{collect_transform_results, StreamShardOutcome};
use crate::plugin::loader::TransformModule;
use crate::progress::{ProgressEvent, ProgressSender};
use crate::runner::{
    run_destination_stream, run_source_stream, run_transform_stream, StreamRunContext,
};

/// Determines how the destination stage is handled.
pub(crate) enum DestinationMode {
    /// Normal: run destination plugin.
    Normal,
    /// Dry-run: collect frames into memory instead.
    DryRun { limit: Option<u64> },
}

/// Execute a single stream through the source -> transforms -> destination pipeline.
///
/// Creates inter-stage channels, spawns blocking tasks for source, transforms,
/// and destination (or dry-run collector), then assembles a `StreamShardOutcome`.
///
/// # Errors
///
/// Returns `PipelineError` if any stage fails or a blocking task panics.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::similar_names
)]
pub(crate) async fn execute_single_stream(
    stream_ctx: StreamContext,
    params: Arc<StreamExecutionParams>,
    source_module: LoadedComponent,
    dest_module: LoadedComponent,
    transforms: Vec<TransformModule>,
    state: Arc<dyn StateBackend>,
    stats: Arc<Mutex<RunStats>>,
    run_dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    mode: DestinationMode,
    progress_tx: ProgressSender,
) -> Result<StreamShardOutcome, PipelineError> {
    let num_t = transforms.len();
    let mut channels = Vec::with_capacity(num_t + 1);
    for _ in 0..=num_t {
        channels.push(sync_mpsc::sync_channel::<Frame>(params.channel_capacity));
    }

    let (mut senders, mut receivers): (
        Vec<sync_mpsc::SyncSender<Frame>>,
        Vec<sync_mpsc::Receiver<Frame>>,
    ) = channels.into_iter().unzip();

    let source_tx = senders.remove(0);
    let dest_rx = receivers
        .pop()
        .ok_or_else(|| PipelineError::infra("Missing destination receiver"))?;

    let stream_ctx_for_src = stream_ctx.clone();
    let stream_ctx_for_dst = stream_ctx.clone();

    // Build per-batch progress callback for the source runner
    let on_batch_emitted: Option<Arc<dyn Fn(u64) + Send + Sync>> = {
        let tx = progress_tx.clone();
        Some(Arc::new(move |bytes: u64| {
            tx.emit(ProgressEvent::BatchEmitted { bytes });
        }) as Arc<dyn Fn(u64) + Send + Sync>)
    };

    let params_src = params.clone();
    let state_src = state.clone();
    let stats_src = stats.clone();
    let src_handle = tokio::task::spawn_blocking(move || {
        let ctx = StreamRunContext {
            component: &source_module,
            state_backend: state_src,
            pipeline_name: &params_src.pipeline.name,
            metric_run_label: &params_src.pipeline.metric_run_label,
            plugin_id: &params_src.source.id,
            plugin_version: &params_src.source.version,
            stream_ctx: &stream_ctx_for_src,
            permissions: params_src.source.permissions.as_ref(),
            compression: params_src.compression,
            overrides: params_src.source.overrides.as_ref(),
        };
        run_source_stream(
            &ctx,
            source_tx,
            &params_src.source.config,
            stats_src,
            on_batch_emitted,
        )
    });

    let mut transform_handles = Vec::with_capacity(num_t);
    for (i, t) in transforms.into_iter().enumerate() {
        let rx = receivers.remove(0);
        let tx = senders.remove(0);
        let state_t = state.clone();
        let dlq_records_t = run_dlq_records.clone();
        let stream_ctx_t = stream_ctx.clone();
        let params_t = params.clone();
        let t_handle = tokio::task::spawn_blocking(move || {
            let ctx = StreamRunContext {
                component: &t.module,
                state_backend: state_t,
                pipeline_name: &params_t.pipeline.name,
                metric_run_label: &params_t.pipeline.metric_run_label,
                plugin_id: &t.plugin_id,
                plugin_version: &t.plugin_version,
                stream_ctx: &stream_ctx_t,
                permissions: t.permissions.as_ref(),
                compression: params_t.compression,
                overrides: params_t.transform_overrides.get(i).and_then(Option::as_ref),
            };
            run_transform_stream(&ctx, rx, tx, dlq_records_t, i, &t.config)
        });
        transform_handles.push((i, t_handle));
    }

    // Branch on destination mode: normal vs dry-run
    match mode {
        DestinationMode::Normal => {
            run_normal_destination(
                stream_ctx,
                stream_ctx_for_dst,
                params,
                dest_module,
                state,
                stats,
                run_dlq_records,
                dest_rx,
                src_handle,
                transform_handles,
            )
            .await
        }
        DestinationMode::DryRun { limit } => {
            run_dry_run_collector(
                stream_ctx,
                params,
                limit,
                dest_rx,
                src_handle,
                transform_handles,
            )
            .await
        }
    }
}

/// Normal mode: run destination plugin and assemble results.
#[allow(clippy::too_many_arguments, clippy::similar_names)]
async fn run_normal_destination(
    stream_ctx: StreamContext,
    stream_ctx_for_dst: StreamContext,
    params: Arc<StreamExecutionParams>,
    dest_module: LoadedComponent,
    state: Arc<dyn StateBackend>,
    stats: Arc<Mutex<RunStats>>,
    run_dlq_records: Arc<Mutex<Vec<DlqRecord>>>,
    dest_rx: sync_mpsc::Receiver<Frame>,
    src_handle: tokio::task::JoinHandle<Result<crate::runner::SourceOutcome, PipelineError>>,
    transform_handles: Vec<(
        usize,
        tokio::task::JoinHandle<Result<crate::runner::TransformOutcome, PipelineError>>,
    )>,
) -> Result<StreamShardOutcome, PipelineError> {
    let dst_handle = tokio::task::spawn_blocking(move || {
        let ctx = StreamRunContext {
            component: &dest_module,
            state_backend: state,
            pipeline_name: &params.pipeline.name,
            metric_run_label: &params.pipeline.metric_run_label,
            plugin_id: &params.destination.id,
            plugin_version: &params.destination.version,
            stream_ctx: &stream_ctx_for_dst,
            permissions: params.destination.permissions.as_ref(),
            compression: params.compression,
            overrides: params.destination.overrides.as_ref(),
        };
        run_destination_stream(
            &ctx,
            dest_rx,
            run_dlq_records,
            &params.destination.config,
            stats,
        )
    });

    let src_result = src_handle.await.map_err(|e| {
        PipelineError::infra(format!(
            "Source task panicked for stream '{}': {}",
            stream_ctx.stream_name, e
        ))
    })?;

    let transforms = collect_transform_results(transform_handles, &stream_ctx.stream_name).await?;

    let dst_result = dst_handle.await.map_err(|e| {
        PipelineError::infra(format!(
            "Destination task panicked for stream '{}': {}",
            stream_ctx.stream_name, e
        ))
    })?;

    if let Some(transform_err) = transforms.first_error {
        return Err(transform_err);
    }

    let src = src_result?;
    let dst = dst_result?;

    Ok(StreamShardOutcome {
        stream_name: stream_ctx.stream_name.clone(),
        partition_index: stream_ctx.partition_index,
        partition_count: stream_ctx.partition_count,
        read_summary: src.summary,
        write_summary: dst.summary,
        source_checkpoints: src.checkpoints,
        dest_checkpoints: dst.checkpoints,
        source_duration_secs: src.duration_secs,
        dest_duration_secs: dst.duration_secs,
        wasm_instantiation_secs: dst.wasm_instantiation_secs,
        frame_receive_secs: dst.frame_receive_secs,
        transform_durations: transforms.durations,
        dry_run_result: None,
    })
}

/// Dry-run mode: collect frames into memory instead of running destination plugin.
async fn run_dry_run_collector(
    stream_ctx: StreamContext,
    params: Arc<StreamExecutionParams>,
    limit: Option<u64>,
    dest_rx: sync_mpsc::Receiver<Frame>,
    src_handle: tokio::task::JoinHandle<Result<crate::runner::SourceOutcome, PipelineError>>,
    transform_handles: Vec<(
        usize,
        tokio::task::JoinHandle<Result<crate::runner::TransformOutcome, PipelineError>>,
    )>,
) -> Result<StreamShardOutcome, PipelineError> {
    let compression = params.compression;
    let dry_run_stream_name = stream_ctx.stream_name.clone();
    let collector_handle = tokio::task::spawn_blocking(move || {
        collect_dry_run_frames(&dry_run_stream_name, &dest_rx, limit, compression)
    });

    let src_result = src_handle.await.map_err(|e| {
        PipelineError::infra(format!(
            "Source task panicked for stream '{}': {}",
            stream_ctx.stream_name, e
        ))
    })?;

    let transforms = collect_transform_results(transform_handles, &stream_ctx.stream_name).await?;

    let collected = collector_handle.await.map_err(|e| {
        PipelineError::infra(format!(
            "Dry-run collector task panicked for stream '{}': {}",
            stream_ctx.stream_name, e
        ))
    })??;

    if let Some(transform_err) = transforms.first_error {
        return Err(transform_err);
    }

    let src = src_result?;

    Ok(StreamShardOutcome {
        stream_name: stream_ctx.stream_name.clone(),
        partition_index: stream_ctx.partition_index,
        partition_count: stream_ctx.partition_count,
        read_summary: src.summary,
        write_summary: WriteSummary {
            records_written: 0,
            bytes_written: 0,
            batches_written: 0,
            checkpoint_count: 0,
            records_failed: 0,
        },
        source_checkpoints: src.checkpoints,
        dest_checkpoints: Vec::new(),
        source_duration_secs: src.duration_secs,
        dest_duration_secs: 0.0,
        wasm_instantiation_secs: 0.0,
        frame_receive_secs: 0.0,
        transform_durations: transforms.durations,
        dry_run_result: Some(collected),
    })
}

/// Collect frames from a channel, decode IPC, enforce row limit.
/// Used in dry-run mode instead of the destination runner.
fn collect_dry_run_frames(
    stream_name: &str,
    receiver: &sync_mpsc::Receiver<Frame>,
    limit: Option<u64>,
    compression: Option<rapidbyte_runtime::CompressionCodec>,
) -> Result<DryRunStreamResult, PipelineError> {
    let mut batches = Vec::new();
    let mut total_rows: u64 = 0;
    let mut total_bytes: u64 = 0;

    'recv: while let Ok(frame) = receiver.recv() {
        let Frame::Data { payload: data, .. } = frame else {
            break 'recv;
        };
        let ipc_bytes = match compression {
            Some(codec) => rapidbyte_runtime::compression::decompress(codec, &data)
                .map_err(|e| PipelineError::infra(format!("Dry-run decompression failed: {e}")))?,
            None => data.to_vec(),
        };

        let decoded = ipc_to_record_batches(&ipc_bytes).map_err(PipelineError::Infrastructure)?;

        for batch in decoded {
            let rows = batch.num_rows() as u64;
            total_bytes += batch.get_array_memory_size() as u64;

            if let Some(max) = limit {
                let remaining = max.saturating_sub(total_rows);
                if remaining == 0 {
                    break 'recv;
                }
                if rows > remaining {
                    #[allow(clippy::cast_possible_truncation)]
                    batches.push(batch.slice(0, remaining as usize));
                    total_rows += remaining;
                    break 'recv;
                }
            }

            total_rows += rows;
            batches.push(batch);
        }
    }

    Ok(DryRunStreamResult {
        stream_name: stream_name.to_string(),
        batches,
        total_rows,
        total_bytes,
    })
}

#[cfg(test)]
mod dry_run_tests {
    use super::*;
    use crate::arrow::record_batch_to_ipc;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn make_test_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let max = i64::try_from(n).expect("test row count must fit in i64");
        let ids: Vec<i64> = (0..max).collect();
        let names: Vec<String> = (0..n).map(|i| format!("row_{i}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(names)) as Arc<dyn Array>,
            ],
        )
        .unwrap()
    }

    #[test]
    fn collect_dry_run_frames_basic() {
        let (tx, rx) = sync_mpsc::sync_channel::<Frame>(16);
        let batch = make_test_batch(5);
        let ipc = record_batch_to_ipc(&batch).unwrap();
        tx.send(Frame::Data {
            payload: bytes::Bytes::from(ipc),
            checkpoint_id: 1,
        })
        .unwrap();
        tx.send(Frame::EndStream).unwrap();
        drop(tx);

        let result = collect_dry_run_frames("public.users", &rx, None, None).unwrap();
        assert_eq!(result.stream_name, "public.users");
        assert_eq!(result.total_rows, 5);
        assert_eq!(result.batches.len(), 1);
    }

    #[test]
    fn collect_dry_run_frames_with_limit() {
        let (tx, rx) = sync_mpsc::sync_channel::<Frame>(16);
        let batch = make_test_batch(100);
        let ipc = record_batch_to_ipc(&batch).unwrap();
        tx.send(Frame::Data {
            payload: bytes::Bytes::from(ipc),
            checkpoint_id: 1,
        })
        .unwrap();
        tx.send(Frame::EndStream).unwrap();
        drop(tx);

        let result = collect_dry_run_frames("public.users", &rx, Some(10), None).unwrap();
        assert_eq!(result.total_rows, 10);
        let total: usize = result.batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn collect_dry_run_frames_multiple_batches() {
        let (tx, rx) = sync_mpsc::sync_channel::<Frame>(16);
        for _ in 0..3 {
            let batch = make_test_batch(5);
            let ipc = record_batch_to_ipc(&batch).unwrap();
            tx.send(Frame::Data {
                payload: bytes::Bytes::from(ipc),
                checkpoint_id: 1,
            })
            .unwrap();
        }
        tx.send(Frame::EndStream).unwrap();
        drop(tx);

        let result = collect_dry_run_frames("public.users", &rx, Some(12), None).unwrap();
        assert_eq!(result.stream_name, "public.users");
        assert_eq!(result.total_rows, 12);
    }
}
