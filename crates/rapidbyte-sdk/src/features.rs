//! Feature trait contracts.
//!
//! When a plugin declares a [`Feature`] in its manifest, the SDK requires
//! the corresponding trait to be implemented. The `#[plugin]` proc macro
//! enforces this at compile time.

use crate::error::PluginError;
use crate::metric::{ReadSummary, WriteSummary};
use crate::plugin::{BulkWriteInput, CdcReadInput, MultiStreamReadInput, PartitionedReadInput};
use crate::plugin::{Destination, Source};
use crate::run::RunSummary;

/// Required when a source declares `Feature::PartitionedRead`.
///
/// The generated WIT glue dispatches to `read_partition` when partition
/// coordinates are present in the stream context.
#[allow(async_fn_in_trait)]
pub trait PartitionedSource {
    async fn read_partition(
        &self,
        input: PartitionedReadInput<'_>,
    ) -> Result<ReadSummary, PluginError>;
}

/// Required when a source declares `Feature::Cdc`.
///
/// The generated WIT glue dispatches to `read_changes` when the stream's
/// sync mode is `Cdc`.
#[allow(async_fn_in_trait)]
pub trait CdcSource {
    async fn read_changes(&self, input: CdcReadInput<'_>) -> Result<ReadSummary, PluginError>;
}

/// Multi-stream source — receives all streams in one call.
#[allow(async_fn_in_trait)]
pub trait MultiStreamSource: Source {
    async fn read_streams(
        &self,
        input: MultiStreamReadInput<'_>,
    ) -> Result<RunSummary, PluginError>;
}

/// Multi-stream CDC — one replication slot, many tables.
#[allow(async_fn_in_trait)]
pub trait MultiStreamCdcSource: MultiStreamSource + CdcSource {
    async fn read_all_changes(
        &self,
        input: MultiStreamReadInput<'_>,
    ) -> Result<RunSummary, PluginError>;
}

/// Bulk-optimized destination (COPY, multipart upload, load jobs).
#[allow(async_fn_in_trait)]
pub trait BulkDestination: Destination {
    async fn write_bulk(&self, input: BulkWriteInput<'_>) -> Result<WriteSummary, PluginError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::{
        BulkWriteInput, CdcReadInput, InitInput, MultiStreamReadInput, PartitionedReadInput,
        TransformInput, WriteInput,
    };
    use crate::plugin::{Destination, Source, Transform};
    use crate::stream::{CdcResumeToken, PartitionCoordinates, PartitionStrategy, StreamContext};
    use rapidbyte_types::cursor::CursorType;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestConfig;

    fn test_stream_context(name: &str) -> StreamContext {
        StreamContext::test_default(name)
    }

    struct TestSource;

    impl Source for TestSource {
        type Config = TestConfig;

        async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError> {
            let _ = config;
            Ok(Self)
        }

        async fn discover(
            &self,
            _input: crate::plugin::DiscoverInput<'_>,
        ) -> Result<Vec<crate::discovery::DiscoveredStream>, PluginError> {
            Ok(vec![])
        }

        async fn read(
            &self,
            _input: crate::plugin::ReadInput<'_>,
        ) -> Result<ReadSummary, PluginError> {
            Ok(ReadSummary {
                records_read: 0,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
            })
        }
    }

    impl PartitionedSource for TestSource {
        async fn read_partition(
            &self,
            input: PartitionedReadInput<'_>,
        ) -> Result<ReadSummary, PluginError> {
            let _ = input;
            Ok(ReadSummary {
                records_read: 1,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
            })
        }
    }

    impl CdcSource for TestSource {
        async fn read_changes(&self, input: CdcReadInput<'_>) -> Result<ReadSummary, PluginError> {
            let _ = input;
            Ok(ReadSummary {
                records_read: 2,
                bytes_read: 0,
                batches_emitted: 0,
                checkpoint_count: 0,
                records_skipped: 0,
            })
        }
    }

    impl MultiStreamSource for TestSource {
        async fn read_streams(
            &self,
            input: MultiStreamReadInput<'_>,
        ) -> Result<crate::run::RunSummary, PluginError> {
            Ok(crate::run::RunSummary {
                results: input
                    .streams
                    .into_iter()
                    .enumerate()
                    .map(|(index, stream)| crate::run::StreamResult {
                        stream_index: index as u32,
                        stream_name: stream.stream_name,
                        outcome_json: "{}".into(),
                        succeeded: true,
                    })
                    .collect(),
            })
        }
    }

    impl MultiStreamCdcSource for TestSource {
        async fn read_all_changes(
            &self,
            input: MultiStreamReadInput<'_>,
        ) -> Result<crate::run::RunSummary, PluginError> {
            self.read_streams(input).await
        }
    }

    struct TestDest;

    impl Destination for TestDest {
        type Config = TestConfig;

        async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError> {
            let _ = config;
            Ok(Self)
        }

        async fn write(
            &self,
            _input: WriteInput<'_>,
        ) -> Result<crate::metric::WriteSummary, PluginError> {
            Ok(crate::metric::WriteSummary {
                records_written: 0,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
            })
        }
    }

    impl BulkDestination for TestDest {
        async fn write_bulk(&self, input: BulkWriteInput<'_>) -> Result<WriteSummary, PluginError> {
            let _ = input;
            Ok(WriteSummary {
                records_written: 1,
                bytes_written: 0,
                batches_written: 0,
                checkpoint_count: 0,
                records_failed: 0,
            })
        }
    }

    struct TestTransform;

    impl Transform for TestTransform {
        type Config = TestConfig;

        async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError> {
            let _ = config;
            Ok(Self)
        }

        async fn transform(
            &self,
            _input: TransformInput<'_>,
        ) -> Result<crate::metric::TransformSummary, PluginError> {
            Ok(crate::metric::TransformSummary {
                records_in: 0,
                records_out: 0,
                bytes_in: 0,
                bytes_out: 0,
                batches_processed: 0,
            })
        }
    }

    #[allow(dead_code)]
    fn assert_partitioned_source<T: Source + PartitionedSource>() {}
    #[allow(dead_code)]
    fn assert_cdc_source<T: Source + CdcSource>() {}
    #[allow(dead_code)]
    fn assert_multi_stream_source<T: Source + MultiStreamSource>() {}
    #[allow(dead_code)]
    fn assert_multi_stream_cdc_source<T: MultiStreamSource + CdcSource + MultiStreamCdcSource>() {}
    #[allow(dead_code)]
    fn assert_bulk_destination<T: Destination + BulkDestination>() {}

    #[test]
    fn feature_trait_shapes_compile() {
        fn assert_source<T: Source>() {}
        fn assert_dest<T: Destination>() {}
        fn assert_transform<T: Transform>() {}
        fn assert_partitioned<T: PartitionedSource>() {}
        fn assert_cdc<T: CdcSource>() {}
        fn assert_multi<T: MultiStreamSource>() {}
        fn assert_multi_cdc<T: MultiStreamCdcSource>() {}
        fn assert_bulk<T: BulkDestination>() {}

        assert_source::<TestSource>();
        assert_dest::<TestDest>();
        assert_transform::<TestTransform>();
        assert_partitioned::<TestSource>();
        assert_cdc::<TestSource>();
        assert_multi::<TestSource>();
        assert_multi_cdc::<TestSource>();
        assert_bulk::<TestDest>();
    }

    #[test]
    fn typed_feature_inputs_are_constructible() {
        let stream = test_stream_context("users");
        let partition = PartitionCoordinates {
            count: 4,
            index: 1,
            strategy: PartitionStrategy::Range,
        };
        let resume = CdcResumeToken {
            value: Some("42".into()),
            cursor_type: CursorType::Lsn,
        };

        let partition_input = PartitionedReadInput::new(stream.clone(), partition);
        let cdc_input = CdcReadInput::new(stream.clone(), resume);
        let multi_input = MultiStreamReadInput::new(vec![stream.clone()]);
        let bulk_input = BulkWriteInput::new(stream);

        assert_eq!(partition_input.partition.count, 4);
        assert_eq!(cdc_input.resume.value.as_deref(), Some("42"));
        assert_eq!(multi_input.streams.len(), 1);
        assert_eq!(bulk_input.stream.stream_name, "users");
    }
}
