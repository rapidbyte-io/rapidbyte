//! Source plugin for `PostgreSQL`.
//!
//! Implements discovery and read paths (full-refresh, incremental cursor reads,
//! and CDC via `pgoutput` logical replication) and streams Arrow IPC batches to
//! the host.

mod cdc;
mod client;
mod config;
mod cursor;
mod discovery;
mod encode;
mod metrics;
mod prerequisites;
mod query;
mod reader;
mod types;

use std::time::Instant;

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::schema::StreamSchema;

#[rapidbyte_sdk::plugin(source)]
pub struct SourcePostgres {
    config: config::Config,
}

impl Source for SourcePostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        config.validate()?;
        Ok(Self { config })
    }

    async fn discover(&self, ctx: &Context) -> Result<Vec<DiscoveredStream>, PluginError> {
        let _ = ctx;
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        discovery::discover_catalog(&client)
            .await
            .map_err(|e| PluginError::transient_db("DISCOVERY_FAILED", e))
    }

    async fn validate(
        &self,
        ctx: &Context,
        _upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        let _ = ctx;
        client::validate(&self.config).await
    }

    async fn prerequisites(&self, ctx: &Context) -> Result<PrerequisitesReport, PluginError> {
        let _ = ctx;
        prerequisites::prerequisites(&self.config).await
    }

    async fn read(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<ReadSummary, PluginError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        reader::read_stream(&client, ctx, &stream, connect_secs)
            .await
            .map_err(|e| PluginError::internal("READ_FAILED", e))
    }

    async fn close(&self, ctx: &Context) -> Result<(), PluginError> {
        ctx.log(LogLevel::Info, "source-postgres: close (no-op)");
        Ok(())
    }
}

impl PartitionedSource for SourcePostgres {
    async fn read_partition(
        &self,
        ctx: &Context,
        stream: StreamContext,
        _partition: PartitionCoordinates,
    ) -> Result<ReadSummary, PluginError> {
        // Partition coordinates are already embedded in StreamContext;
        // reader::read_stream extracts them via stream.partition_coordinates().
        self.read(ctx, stream).await
    }
}

impl CdcSource for SourcePostgres {
    async fn read_changes(
        &self,
        ctx: &Context,
        stream: StreamContext,
        _resume: CdcResumeToken,
    ) -> Result<ReadSummary, PluginError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        cdc::read_cdc_changes(&client, ctx, &stream, &self.config, connect_secs)
            .await
            .map_err(|e| PluginError::internal("CDC_READ_FAILED", e))
    }
}
