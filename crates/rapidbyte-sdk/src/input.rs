//! Typed lifecycle inputs for plugin authors.
//!
//! These inputs are deliberately small and explicit. They expose only the
//! capabilities that make sense for the lifecycle phase that is running.

use core::marker::PhantomData;

use crate::capabilities::{Cancel, Checkpoints, Emit, Log, Metrics, Network, Reader, State};
use crate::lifecycle::{ApplyRequest, TeardownRequest};
use crate::schema::StreamSchema;
use crate::stream::{CdcResumeToken, PartitionCoordinates, StreamContext};

/// Marker input passed to [`Source::init`] and [`Destination::init`].
#[derive(Debug, Clone, Copy, Default)]
pub struct InitInput<'a> {
    pub log: Log,
    pub metrics: Metrics,
    pub network: Network,
    _marker: PhantomData<&'a ()>,
}

impl<'a> InitInput<'a> {
    /// Create an empty init input.
    pub const fn new() -> Self {
        Self {
            log: Log,
            metrics: Metrics,
            network: Network,
            _marker: PhantomData,
        }
    }
}

/// Marker input passed to pre-flight checks.
#[derive(Debug, Clone, Copy, Default)]
pub struct PrerequisitesInput<'a> {
    pub log: Log,
    pub metrics: Metrics,
    pub network: Network,
    pub cancel: Cancel,
    _marker: PhantomData<&'a ()>,
}

impl<'a> PrerequisitesInput<'a> {
    /// Create an empty prerequisites input.
    pub const fn new() -> Self {
        Self {
            log: Log,
            metrics: Metrics,
            network: Network,
            cancel: Cancel,
            _marker: PhantomData,
        }
    }
}

/// Marker input passed to discovery.
#[derive(Debug, Clone, Copy, Default)]
pub struct DiscoverInput<'a> {
    pub log: Log,
    pub metrics: Metrics,
    pub network: Network,
    pub cancel: Cancel,
    _marker: PhantomData<&'a ()>,
}

impl<'a> DiscoverInput<'a> {
    /// Create an empty discovery input.
    pub const fn new() -> Self {
        Self {
            log: Log,
            metrics: Metrics,
            network: Network,
            cancel: Cancel,
            _marker: PhantomData,
        }
    }
}

/// Input passed to validation.
#[derive(Debug, Clone, Copy)]
pub struct ValidateInput<'a> {
    /// Optional upstream schema being validated against.
    pub upstream: Option<&'a StreamSchema>,
    /// Optional stream name when validation is scoped to a specific stream.
    pub stream_name: Option<&'a str>,
    pub log: Log,
    pub metrics: Metrics,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ValidateInput<'a> {
    /// Create a validation input.
    pub const fn new(upstream: Option<&'a StreamSchema>, stream_name: Option<&'a str>) -> Self {
        Self {
            upstream,
            stream_name,
            log: Log,
            metrics: Metrics,
            _marker: PhantomData,
        }
    }
}

/// Input passed to schema-apply hooks.
#[derive(Debug, Clone)]
pub struct ApplyInput<'a> {
    /// Streams being prepared or applied.
    pub request: ApplyRequest,
    pub log: Log,
    pub metrics: Metrics,
    pub state: State,
    pub checkpoints: Checkpoints,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ApplyInput<'a> {
    /// Create an apply input from a request.
    pub fn new(request: ApplyRequest) -> Self {
        Self {
            request,
            log: Log,
            metrics: Metrics,
            state: State,
            checkpoints: Checkpoints,
            _marker: PhantomData,
        }
    }
}

/// Input passed to read hooks.
#[derive(Debug, Clone)]
pub struct ReadInput<
    'a,
    EmitT = Emit,
    CancelT = Cancel,
    StateT = State,
    CheckpointsT = Checkpoints,
    MetricsT = Metrics,
    LogT = Log,
> {
    /// Stream being read.
    pub stream: StreamContext,
    pub emit: EmitT,
    pub cancel: CancelT,
    pub state: StateT,
    pub checkpoints: CheckpointsT,
    pub metrics: MetricsT,
    pub log: LogT,
    _marker: PhantomData<&'a ()>,
}

impl<'a, EmitT, CancelT, StateT, CheckpointsT, MetricsT, LogT>
    ReadInput<'a, EmitT, CancelT, StateT, CheckpointsT, MetricsT, LogT>
{
    /// Create a read input with explicit capability fakes.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: StreamContext,
        emit: EmitT,
        cancel: CancelT,
        state: StateT,
        checkpoints: CheckpointsT,
        metrics: MetricsT,
        log: LogT,
    ) -> Self {
        Self {
            stream,
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
            _marker: PhantomData,
        }
    }
}

impl<'a> ReadInput<'a> {
    /// Create a read input for a single stream.
    pub fn host(stream: StreamContext) -> Self {
        Self::new(stream, Emit, Cancel, State, Checkpoints, Metrics, Log)
    }
}

/// Input passed to destination write hooks.
#[derive(Debug, Clone)]
pub struct WriteInput<
    'a,
    ReaderT = Reader,
    CancelT = Cancel,
    StateT = State,
    CheckpointsT = Checkpoints,
> {
    /// Stream being written.
    pub stream: StreamContext,
    pub reader: ReaderT,
    pub cancel: CancelT,
    pub state: StateT,
    pub checkpoints: CheckpointsT,
    _marker: PhantomData<&'a ()>,
}

impl<'a, ReaderT, CancelT, StateT, CheckpointsT>
    WriteInput<'a, ReaderT, CancelT, StateT, CheckpointsT>
{
    /// Create a write input with explicit capability fakes.
    pub fn new(
        stream: StreamContext,
        reader: ReaderT,
        cancel: CancelT,
        state: StateT,
        checkpoints: CheckpointsT,
    ) -> Self {
        Self {
            stream,
            reader,
            cancel,
            state,
            checkpoints,
            _marker: PhantomData,
        }
    }
}

impl<'a> WriteInput<'a> {
    /// Create a write input for a single stream.
    pub fn host(stream: StreamContext) -> Self {
        Self::new(stream, Reader, Cancel, State, Checkpoints)
    }
}

/// Input passed to transform hooks.
#[derive(Debug, Clone)]
pub struct TransformInput<
    'a,
    EmitT = Emit,
    CancelT = Cancel,
    StateT = State,
    CheckpointsT = Checkpoints,
    MetricsT = Metrics,
    LogT = Log,
> {
    /// Stream being transformed.
    pub stream: StreamContext,
    pub plugin_id: String,
    pub emit: EmitT,
    pub cancel: CancelT,
    pub state: StateT,
    pub checkpoints: CheckpointsT,
    pub metrics: MetricsT,
    pub log: LogT,
    _marker: PhantomData<&'a ()>,
}

impl<'a, EmitT, CancelT, StateT, CheckpointsT, MetricsT, LogT>
    TransformInput<'a, EmitT, CancelT, StateT, CheckpointsT, MetricsT, LogT>
{
    /// Create a transform input with explicit capability fakes.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: StreamContext,
        plugin_id: impl Into<String>,
        emit: EmitT,
        cancel: CancelT,
        state: StateT,
        checkpoints: CheckpointsT,
        metrics: MetricsT,
        log: LogT,
    ) -> Self {
        Self {
            stream,
            plugin_id: plugin_id.into(),
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
            _marker: PhantomData,
        }
    }
}

impl<'a> TransformInput<'a> {
    /// Create a transform input for a single stream.
    pub fn host(stream: StreamContext, plugin_id: impl Into<String>) -> Self {
        Self::new(
            stream,
            plugin_id,
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        )
    }
}

/// Input passed to close hooks.
#[derive(Debug, Clone, Copy, Default)]
pub struct CloseInput<'a> {
    pub log: Log,
    _marker: PhantomData<&'a ()>,
}

impl<'a> CloseInput<'a> {
    /// Create an empty close input.
    pub const fn new() -> Self {
        Self {
            log: Log,
            _marker: PhantomData,
        }
    }
}

/// Input passed to teardown hooks.
#[derive(Debug, Clone)]
pub struct TeardownInput<'a> {
    /// Streams being torn down.
    pub request: TeardownRequest,
    pub log: Log,
    pub metrics: Metrics,
    pub state: State,
    _marker: PhantomData<&'a ()>,
}

impl<'a> TeardownInput<'a> {
    /// Create a teardown input from a request.
    pub fn new(request: TeardownRequest) -> Self {
        Self {
            request,
            log: Log,
            metrics: Metrics,
            state: State,
            _marker: PhantomData,
        }
    }
}

/// Input passed to partitioned source reads.
#[derive(Debug, Clone)]
pub struct PartitionedReadInput<
    'a,
    EmitT = Emit,
    CancelT = Cancel,
    StateT = State,
    CheckpointsT = Checkpoints,
    MetricsT = Metrics,
    LogT = Log,
> {
    pub stream: StreamContext,
    pub partition: PartitionCoordinates,
    pub emit: EmitT,
    pub cancel: CancelT,
    pub state: StateT,
    pub checkpoints: CheckpointsT,
    pub metrics: MetricsT,
    pub log: LogT,
    _marker: PhantomData<&'a ()>,
}

impl<'a, EmitT, CancelT, StateT, CheckpointsT, MetricsT, LogT>
    PartitionedReadInput<'a, EmitT, CancelT, StateT, CheckpointsT, MetricsT, LogT>
{
    /// Create a partitioned read input with explicit capability fakes.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: StreamContext,
        partition: PartitionCoordinates,
        emit: EmitT,
        cancel: CancelT,
        state: StateT,
        checkpoints: CheckpointsT,
        metrics: MetricsT,
        log: LogT,
    ) -> Self {
        Self {
            stream,
            partition,
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
            _marker: PhantomData,
        }
    }
}

impl<'a> PartitionedReadInput<'a> {
    /// Create a partitioned read input.
    pub fn host(stream: StreamContext, partition: PartitionCoordinates) -> Self {
        Self::new(
            stream,
            partition,
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        )
    }
}

/// Input passed to CDC source reads.
#[derive(Debug, Clone)]
pub struct CdcReadInput<
    'a,
    EmitT = Emit,
    CancelT = Cancel,
    StateT = State,
    CheckpointsT = Checkpoints,
    MetricsT = Metrics,
    LogT = Log,
> {
    pub stream: StreamContext,
    pub resume: CdcResumeToken,
    pub emit: EmitT,
    pub cancel: CancelT,
    pub state: StateT,
    pub checkpoints: CheckpointsT,
    pub metrics: MetricsT,
    pub log: LogT,
    _marker: PhantomData<&'a ()>,
}

impl<'a, EmitT, CancelT, StateT, CheckpointsT, MetricsT, LogT>
    CdcReadInput<'a, EmitT, CancelT, StateT, CheckpointsT, MetricsT, LogT>
{
    /// Create a CDC read input with explicit capability fakes.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: StreamContext,
        resume: CdcResumeToken,
        emit: EmitT,
        cancel: CancelT,
        state: StateT,
        checkpoints: CheckpointsT,
        metrics: MetricsT,
        log: LogT,
    ) -> Self {
        Self {
            stream,
            resume,
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
            _marker: PhantomData,
        }
    }
}

impl<'a> CdcReadInput<'a> {
    /// Create a CDC read input.
    pub fn host(stream: StreamContext, resume: CdcResumeToken) -> Self {
        Self::new(
            stream,
            resume,
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        )
    }
}

/// Input passed to multi-stream source reads.
#[derive(Debug, Clone)]
pub struct MultiStreamReadInput<'a> {
    pub streams: Vec<StreamContext>,
    pub emit: Emit,
    pub cancel: Cancel,
    pub state: State,
    pub checkpoints: Checkpoints,
    pub metrics: Metrics,
    pub log: Log,
    _marker: PhantomData<&'a ()>,
}

impl<'a> MultiStreamReadInput<'a> {
    /// Create a multi-stream read input with explicit capability fakes.
    pub fn new(
        streams: Vec<StreamContext>,
        emit: Emit,
        cancel: Cancel,
        state: State,
        checkpoints: Checkpoints,
        metrics: Metrics,
        log: Log,
    ) -> Self {
        Self {
            streams,
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
            _marker: PhantomData,
        }
    }

    /// Create a multi-stream read input for a host-owned lifecycle call.
    pub fn host(streams: Vec<StreamContext>) -> Self {
        Self::new(streams, Emit, Cancel, State, Checkpoints, Metrics, Log)
    }
}

/// Input passed to bulk destination writes.
#[derive(Debug, Clone)]
pub struct BulkWriteInput<
    'a,
    ReaderT = Reader,
    CancelT = Cancel,
    StateT = State,
    CheckpointsT = Checkpoints,
> {
    pub stream: StreamContext,
    pub reader: ReaderT,
    pub cancel: CancelT,
    pub state: StateT,
    pub checkpoints: CheckpointsT,
    _marker: PhantomData<&'a ()>,
}

impl<'a, ReaderT, CancelT, StateT, CheckpointsT>
    BulkWriteInput<'a, ReaderT, CancelT, StateT, CheckpointsT>
{
    /// Create a bulk write input with explicit capability fakes.
    pub fn new(
        stream: StreamContext,
        reader: ReaderT,
        cancel: CancelT,
        state: StateT,
        checkpoints: CheckpointsT,
    ) -> Self {
        Self {
            stream,
            reader,
            cancel,
            state,
            checkpoints,
            _marker: PhantomData,
        }
    }
}

impl<'a> BulkWriteInput<'a> {
    /// Create a bulk write input.
    pub fn host(stream: StreamContext) -> Self {
        Self::new(stream, Reader, Cancel, State, Checkpoints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::checkpoint::{CheckpointKind, StateScope};
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rapidbyte_types::wire::SyncMode;
    use std::sync::Arc;

    fn test_stream(name: &str) -> StreamContext {
        let mut stream = StreamContext::test_default(name);
        stream.stream_index = 7;
        stream.sync_mode = SyncMode::FullRefresh;
        stream
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).expect("batch")
    }

    #[test]
    fn init_input_exposes_only_plugin_scope_capabilities() {
        let InitInput {
            log,
            metrics,
            network,
            _marker,
        } = InitInput::new();

        log.info("init");
        let _ = metrics;
        assert_eq!(std::mem::size_of_val(&network), 0);
    }

    #[test]
    fn validate_input_requires_upstream_and_stream_name() {
        let schema = StreamSchema {
            fields: vec![],
            primary_key: vec!["id".into()],
            partition_keys: vec![],
            source_defined_cursor: Some("updated_at".into()),
            schema_id: Some("schema-v1".into()),
        };

        let input = ValidateInput::new(Some(&schema), Some("users"));
        assert_eq!(
            input.upstream.map(|s| s.schema_id.as_deref()),
            Some(Some("schema-v1"))
        );
        assert_eq!(input.stream_name, Some("users"));
    }

    #[test]
    fn read_input_host_and_new_are_available() {
        let stream = test_stream("users");
        let host = ReadInput::host(stream.clone());
        let explicit = ReadInput::new(
            stream.clone(),
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        );

        assert_eq!(host.stream.stream_name, "users");
        assert_eq!(explicit.stream.stream_name, "users");
        assert_eq!(std::mem::size_of_val(&host.emit), 0);
        assert_eq!(std::mem::size_of_val(&explicit.cancel), 0);
    }

    #[test]
    fn read_input_exposes_read_capabilities_and_no_extras() {
        let stream = test_stream("users");
        let input = ReadInput::host(stream.clone());

        let ReadInput {
            stream,
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
            _marker,
        } = input;

        let stream_name = stream.stream_name.clone();
        cancel.check().expect("cancel");
        state
            .put(StateScope::Stream, "key", "value")
            .expect("state put");
        checkpoints
            .begin(CheckpointKind::Source)
            .expect("checkpoint");
        let _ = metrics;
        log.debug("read");
        emit.batch_for_stream(stream.stream_index, &test_batch())
            .expect("emit");
        assert_eq!(stream_name, "users");
    }

    #[test]
    fn write_input_host_and_new_are_available() {
        let stream = test_stream("orders");
        let host = WriteInput::host(stream.clone());
        let explicit = WriteInput::new(stream.clone(), Reader, Cancel, State, Checkpoints);

        assert_eq!(host.stream.stream_name, "orders");
        assert_eq!(explicit.stream.stream_name, "orders");
        assert_eq!(std::mem::size_of_val(&host.reader), 0);
        assert_eq!(std::mem::size_of_val(&explicit.cancel), 0);
    }

    #[test]
    fn write_input_exposes_write_capabilities_and_no_extras() {
        let stream = test_stream("orders");
        let input = WriteInput::host(stream.clone());

        let WriteInput {
            stream,
            reader,
            cancel,
            state,
            checkpoints,
            _marker,
        } = input;

        let stream_name = stream.stream_name.clone();
        assert!(reader.next_batch(1024).expect("next batch").is_none());
        cancel.check().expect("cancel");
        state
            .put(StateScope::Stream, "key", "value")
            .expect("state put");
        let txn = checkpoints.begin(CheckpointKind::Dest).expect("checkpoint");
        txn.commit(0, 0).expect("commit");
        assert_eq!(stream_name, "orders");
    }

    #[test]
    fn partitioned_cdc_multi_and_bulk_inputs_construct() {
        let stream = test_stream("events");
        let bulk = BulkWriteInput::host(stream.clone());
        let explicit_bulk = BulkWriteInput::new(stream.clone(), Reader, Cancel, State, Checkpoints);
        let multi = MultiStreamReadInput::host(vec![stream.clone()]);
        let explicit_multi = MultiStreamReadInput::new(
            vec![stream.clone()],
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        );
        let partition = PartitionCoordinates {
            count: 4,
            index: 1,
            strategy: crate::stream::PartitionStrategy::Range,
        };
        let partitioned = PartitionedReadInput::host(stream.clone(), partition.clone());
        let explicit_partitioned = PartitionedReadInput::new(
            stream.clone(),
            partition,
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        );
        let cdc = CdcReadInput::host(
            stream.clone(),
            CdcResumeToken {
                value: Some("42".into()),
                cursor_type: rapidbyte_types::cursor::CursorType::Lsn,
            },
        );
        let explicit_cdc = CdcReadInput::new(
            stream,
            CdcResumeToken {
                value: Some("42".into()),
                cursor_type: rapidbyte_types::cursor::CursorType::Lsn,
            },
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        );

        assert_eq!(bulk.stream.stream_name, "events");
        assert_eq!(explicit_bulk.stream.stream_name, "events");
        assert_eq!(multi.streams.len(), 1);
        assert_eq!(explicit_multi.streams.len(), 1);
        assert_eq!(std::mem::size_of_val(&explicit_multi.emit), 0);
        assert_eq!(cdc.resume.value.as_deref(), Some("42"));
        assert_eq!(explicit_cdc.resume.value.as_deref(), Some("42"));
        assert_eq!(partitioned.partition.count, 4);
        assert_eq!(explicit_partitioned.partition.count, 4);
    }
}
