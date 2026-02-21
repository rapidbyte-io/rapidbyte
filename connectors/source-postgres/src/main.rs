pub mod config;
mod client;
pub mod schema;
mod reader;

use std::time::Instant;

use rapidbyte_sdk::connector::SourceConnector;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{Catalog, ConnectorInfo, ReadSummary, StreamContext};

pub struct SourcePostgres {
    config: config::Config,
}

impl SourceConnector for SourcePostgres {
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
                protocol_version: "2".to_string(),
                features: vec![],
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
        reader::read_stream(&client, &ctx, connect_secs)
            .await
            .map_err(|e| ConnectorError::internal("READ_FAILED", e))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        host_ffi::log(2, "source-postgres: close (no-op)");
        Ok(())
    }
}

rapidbyte_sdk::source_connector_main!(SourcePostgres);
