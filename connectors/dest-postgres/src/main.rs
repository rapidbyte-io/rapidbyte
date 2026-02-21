mod config;
mod client;
mod ddl;
mod batch;
mod writer;

use rapidbyte_sdk::connector::DestinationConnector;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{ConnectorInfo, Feature, StreamContext, WriteSummary};

pub struct DestPostgres {
    config: config::Config,
}

impl DestinationConnector for DestPostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        config.validate()?;
        host_ffi::log(
            2,
            &format!(
                "dest-postgres: open with host={} db={} schema={} load_method={}",
                config.host, config.database, config.schema, config.load_method
            ),
        );
        let mut features = vec![Feature::ExactlyOnce];
        if config.load_method == "copy" {
            features.push(Feature::BulkLoadCopy);
        }
        Ok((
            Self { config },
            ConnectorInfo {
                protocol_version: "2".to_string(),
                features,
                default_max_batch_bytes: 64 * 1024 * 1024,
            },
        ))
    }

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
        client::validate(config).await
    }

    async fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError> {
        writer::write_stream(&self.config, &ctx).await
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        host_ffi::log(2, "dest-postgres: close (no-op)");
        Ok(())
    }
}

rapidbyte_sdk::dest_connector_main!(DestPostgres);
