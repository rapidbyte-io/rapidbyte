//! Destination connector for PostgreSQL.
//!
//! Receives Arrow IPC batches from the host and writes them to PostgreSQL
//! with transactional checkpoints and schema evolution handling.

mod batch;
mod client;
mod config;
mod ddl;
mod writer;

use config::LoadMethod;
use rapidbyte_sdk::connector::Destination;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{
    ConnectorInfo, Feature, ProtocolVersion, StreamContext, WriteSummary, DEFAULT_MAX_BATCH_BYTES,
};

pub struct DestPostgres {
    config: config::Config,
}

impl Destination for DestPostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {
        host_ffi::log(
            2,
            &format!(
                "dest-postgres: open with host={} db={} schema={} load_method={}",
                config.host, config.database, config.schema, config.load_method
            ),
        );
        let mut features = vec![Feature::ExactlyOnce];
        if config.load_method == LoadMethod::Copy {
            features.push(Feature::BulkLoadCopy);
        }
        Ok((
            Self { config },
            ConnectorInfo {
                protocol_version: ProtocolVersion::V2,
                features,
                default_max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
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

rapidbyte_sdk::connector_main!(destination, DestPostgres);
rapidbyte_sdk::embed_manifest!();
rapidbyte_sdk::embed_config_schema!(config::Config);
