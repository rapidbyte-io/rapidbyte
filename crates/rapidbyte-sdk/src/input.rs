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
    pub const fn new(upstream: Option<&'a StreamSchema>) -> Self {
        Self {
            upstream,
            stream_name: None,
            log: Log,
            metrics: Metrics,
            _marker: PhantomData,
        }
    }

    /// Associate validation with a specific stream identity.
    pub const fn with_stream_name(mut self, stream_name: Option<&'a str>) -> Self {
        self.stream_name = stream_name;
        self
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
    pub dry_run: bool,
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
    pub fn with_capabilities(
        stream: StreamContext,
        dry_run: bool,
        emit: EmitT,
        cancel: CancelT,
        state: StateT,
        checkpoints: CheckpointsT,
        metrics: MetricsT,
        log: LogT,
    ) -> Self {
        Self {
            stream,
            dry_run,
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
    pub fn new(stream: StreamContext) -> Self {
        Self::with_capabilities(
            stream,
            false,
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        )
    }

    /// Create a read input for a single stream with an explicit dry-run flag.
    pub fn with_dry_run(stream: StreamContext, dry_run: bool) -> Self {
        Self::with_capabilities(
            stream,
            dry_run,
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        )
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
    pub dry_run: bool,
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
    pub fn with_capabilities(
        stream: StreamContext,
        dry_run: bool,
        reader: ReaderT,
        cancel: CancelT,
        state: StateT,
        checkpoints: CheckpointsT,
    ) -> Self {
        Self {
            stream,
            dry_run,
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
    pub fn new(stream: StreamContext) -> Self {
        Self::with_capabilities(stream, false, Reader, Cancel, State, Checkpoints)
    }

    /// Create a write input for a single stream with an explicit dry-run flag.
    pub fn with_dry_run(stream: StreamContext, dry_run: bool) -> Self {
        Self::with_capabilities(stream, dry_run, Reader, Cancel, State, Checkpoints)
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
    pub dry_run: bool,
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
    pub fn with_capabilities(
        stream: StreamContext,
        plugin_id: impl Into<String>,
        dry_run: bool,
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
            dry_run,
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
    pub fn new(stream: StreamContext) -> Self {
        Self::with_capabilities(
            stream,
            "",
            false,
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        )
    }

    /// Create a transform input for a single stream with an explicit dry-run flag.
    pub fn with_dry_run(stream: StreamContext, dry_run: bool) -> Self {
        Self::with_capabilities(
            stream,
            "",
            dry_run,
            Emit,
            Cancel,
            State,
            Checkpoints,
            Metrics,
            Log,
        )
    }

    /// Create a transform input for a single stream with explicit runtime metadata.
    pub fn with_runtime(
        stream: StreamContext,
        plugin_id: impl Into<String>,
        dry_run: bool,
    ) -> Self {
        Self::with_capabilities(
            stream,
            plugin_id,
            dry_run,
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
    pub dry_run: bool,
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
    pub fn with_capabilities(
        stream: StreamContext,
        partition: PartitionCoordinates,
        dry_run: bool,
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
            dry_run,
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
    pub fn new(stream: StreamContext, partition: PartitionCoordinates) -> Self {
        Self::with_capabilities(
            stream,
            partition,
            false,
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
    pub dry_run: bool,
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
    pub fn with_capabilities(
        stream: StreamContext,
        resume: CdcResumeToken,
        dry_run: bool,
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
            dry_run,
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
    pub fn new(stream: StreamContext, resume: CdcResumeToken) -> Self {
        Self::with_capabilities(
            stream,
            resume,
            false,
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
    /// Create a multi-stream read input.
    pub fn new(streams: Vec<StreamContext>) -> Self {
        Self {
            streams,
            emit: Emit,
            cancel: Cancel,
            state: State,
            checkpoints: Checkpoints,
            metrics: Metrics,
            log: Log,
            _marker: PhantomData,
        }
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
    pub dry_run: bool,
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
    pub fn with_capabilities(
        stream: StreamContext,
        dry_run: bool,
        reader: ReaderT,
        cancel: CancelT,
        state: StateT,
        checkpoints: CheckpointsT,
    ) -> Self {
        Self {
            stream,
            dry_run,
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
    pub fn new(stream: StreamContext) -> Self {
        Self::with_capabilities(stream, false, Reader, Cancel, State, Checkpoints)
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
    fn read_input_exposes_read_capabilities_and_no_extras() {
        let stream = test_stream("users");
        let input = ReadInput::new(stream.clone());

        let ReadInput {
            stream,
            dry_run,
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
            _marker,
        } = input;

        let stream_name = stream.stream_name.clone();
        assert!(!dry_run);
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
    fn write_input_exposes_write_capabilities_and_no_extras() {
        let stream = test_stream("orders");
        let input = WriteInput::new(stream.clone());

        let WriteInput {
            stream,
            dry_run,
            reader,
            cancel,
            state,
            checkpoints,
            _marker,
        } = input;

        let stream_name = stream.stream_name.clone();
        assert!(!dry_run);
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
    fn bulk_and_multi_stream_inputs_construct() {
        let stream = test_stream("events");
        let bulk = BulkWriteInput::new(stream.clone());
        let multi = MultiStreamReadInput::new(vec![stream.clone()]);
        let partition = PartitionCoordinates {
            count: 4,
            index: 1,
            strategy: crate::stream::PartitionStrategy::Range,
        };
        let cdc = CdcReadInput::new(
            stream.clone(),
            CdcResumeToken {
                value: Some("42".into()),
                cursor_type: rapidbyte_types::cursor::CursorType::Lsn,
            },
        );
        let partitioned = PartitionedReadInput::new(stream, partition);

        assert_eq!(bulk.stream.stream_name, "events");
        assert_eq!(multi.streams.len(), 1);
        assert_eq!(cdc.resume.value.as_deref(), Some("42"));
        assert_eq!(partitioned.partition.count, 4);
    }
}
