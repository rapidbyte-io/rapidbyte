//! Source connector for PostgreSQL.
//!
//! Implements discovery and read paths (full-refresh, incremental cursor reads,
//! and CDC logical replication) and streams Arrow IPC batches to the host.

mod cdc;
mod client;
pub mod config;
mod cursor;
mod discovery;
mod encode;
mod metrics;
mod reader;
pub mod schema;
pub mod types;

use std::time::Instant;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::connector(source)]
pub struct SourcePostgres {
    config: config::Config,
}

impl Source for SourcePostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        config.validate()?;
        Ok((
            Self { config },
            ConnectorInfo {
                protocol_version: ProtocolVersion::V2,
                features: vec![Feature::Cdc],
                default_max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn discover(&mut self, ctx: &Context) -> Result<Catalog, ConnectorError> {
        let _ = ctx;
        let client = client::connect(&self.config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        discovery::discover_catalog(&client)
            .await
            .map(|streams| Catalog { streams })
            .map_err(|e| ConnectorError::transient_db("DISCOVERY_FAILED", e))
    }

    async fn validate(
        config: &Self::Config,
        ctx: &Context,
    ) -> Result<ValidationResult, ConnectorError> {
        let _ = ctx;
        client::validate(config).await
    }

    async fn read(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<ReadSummary, ConnectorError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        match stream.sync_mode {
            SyncMode::Cdc => {
                cdc::read_cdc_changes(&client, ctx, &stream, &self.config, connect_secs)
                    .await
                    .map_err(|e| ConnectorError::internal("CDC_READ_FAILED", e))
            }
            _ => reader::read_stream(&client, ctx, &stream, connect_secs)
                .await
                .map_err(|e| ConnectorError::internal("READ_FAILED", e)),
        }
    }

    async fn close(&mut self, ctx: &Context) -> Result<(), ConnectorError> {
        ctx.log(LogLevel::Info, "source-postgres: close (no-op)");
        Ok(())
    }
}
