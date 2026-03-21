//! Feature trait contracts.
//!
//! When a plugin declares a [`Feature`] in its manifest, the SDK requires
//! the corresponding trait to be implemented. The `#[plugin]` proc macro
//! enforces this at compile time.

use crate::context::Context;
use crate::error::PluginError;
use crate::metric::{ReadSummary, WriteSummary};
use crate::plugin::{Destination, Source};
use crate::run::RunSummary;
use crate::stream::{CdcResumeToken, PartitionCoordinates, StreamContext};

/// Required when a source declares `Feature::PartitionedRead`.
///
/// The generated WIT glue dispatches to `read_partition` when partition
/// coordinates are present in the `StreamContext`.
#[allow(async_fn_in_trait)]
pub trait PartitionedSource {
    async fn read_partition(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
        partition: PartitionCoordinates,
    ) -> Result<ReadSummary, PluginError>;
}

/// Required when a source declares `Feature::Cdc`.
///
/// The generated WIT glue dispatches to `read_changes` when the stream's
/// sync mode is `Cdc`.
#[allow(async_fn_in_trait)]
pub trait CdcSource {
    async fn read_changes(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
        resume: CdcResumeToken,
    ) -> Result<ReadSummary, PluginError>;
}

/// Multi-stream source — receives all streams in one call.
#[allow(async_fn_in_trait)]
pub trait MultiStreamSource: Source {
    async fn read_streams(
        &self,
        ctx: &Context,
        streams: Vec<StreamContext>,
    ) -> Result<RunSummary, PluginError>;
}

/// Multi-stream CDC — one replication slot, many tables.
#[allow(async_fn_in_trait)]
pub trait MultiStreamCdcSource: MultiStreamSource + CdcSource {
    async fn read_all_changes(
        &self,
        ctx: &Context,
        streams: Vec<StreamContext>,
    ) -> Result<RunSummary, PluginError>;
}

/// Bulk-optimized destination (COPY, multipart upload, load jobs).
#[allow(async_fn_in_trait)]
pub trait BulkDestination: Destination {
    async fn write_bulk(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::Source;

    // Verify trait shapes are compatible — if this compiles, the trait
    // signatures are consistent with Source/Destination.
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
}
