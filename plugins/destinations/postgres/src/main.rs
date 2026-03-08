//! Destination plugin for `PostgreSQL`.
//!
//! Receives Arrow IPC batches from the host and writes them to `PostgreSQL`
//! with transactional checkpoints and schema evolution handling.

mod client;
mod config;
mod copy;
mod ddl;
mod decode;
mod insert;
mod pg_error;
mod type_map;
mod watermark;
mod writer;

use rapidbyte_sdk::prelude::*;

use config::LoadMethod;

#[rapidbyte_sdk::plugin(destination)]
pub struct DestPostgres {
    config: config::Config,
}

impl Destination for DestPostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
        let mut features = vec![Feature::ExactlyOnce];
        if config.load_method == LoadMethod::Copy {
            features.push(Feature::BulkLoad);
        }
        Ok((
            Self { config },
            PluginInfo {
                protocol_version: ProtocolVersion::V5,
                features,
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn validate(
        config: &Self::Config,
        _ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        client::validate(config).await
    }

    async fn write(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError> {
        writer::write_stream(&self.config, ctx, &stream).await
    }

    async fn close(&mut self, ctx: &Context) -> Result<(), PluginError> {
        ctx.log(LogLevel::Info, "dest-postgres: close (no-op)");
        Ok(())
    }
}

impl BulkLoadDestination for DestPostgres {
    async fn write_bulk(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError> {
        writer::write_stream(&self.config, ctx, &stream).await
    }
}
