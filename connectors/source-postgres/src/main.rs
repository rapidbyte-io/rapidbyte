mod cdc;
mod client;
pub mod config;
mod identifier;
mod reader;
pub mod schema;

use std::time::Instant;

use rapidbyte_sdk::connector::Source;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    Catalog, ConnectorInfo, Feature, ProtocolVersion, ReadSummary, StreamContext, SyncMode,
};

pub struct SourcePostgres {
    config: config::Config,
}

impl Source for SourcePostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        host_ffi::log(
            2,
            &format!(
                "source-postgres: open with host={} db={}",
                config.host, config.database
            ),
        );
        Ok((
            Self { config },
            ConnectorInfo {
                protocol_version: ProtocolVersion::V2,
                features: vec![Feature::Cdc],
                default_max_batch_bytes: 64 * 1024 * 1024,
            },
        ))
    }

    async fn discover(&mut self) -> Result<Catalog, ConnectorError> {
        let client = client::connect(&self.config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        schema::discover_catalog(&client)
            .await
            .map(|streams| Catalog { streams })
            .map_err(|e| ConnectorError::transient_db("DISCOVERY_FAILED", e))
    }

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        client::validate(config).await
    }

    async fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        match ctx.sync_mode {
            SyncMode::Cdc => cdc::read_cdc_changes(&client, &ctx, &self.config, connect_secs)
                .await
                .map_err(|e| ConnectorError::internal("CDC_READ_FAILED", e)),
            _ => reader::read_stream(&client, &ctx, connect_secs)
                .await
                .map_err(|e| ConnectorError::internal("READ_FAILED", e)),
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        host_ffi::log(2, "source-postgres: close (no-op)");
        Ok(())
    }
}

rapidbyte_sdk::connector_main!(source, SourcePostgres);
