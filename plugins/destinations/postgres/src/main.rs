//! Destination plugin for `PostgreSQL`.
//!
//! Receives Arrow IPC batches from the host and writes them to `PostgreSQL`
//! with transactional checkpoints and schema evolution handling.

mod client;
mod config;
mod contract;
mod copy;
mod ddl;
mod decode;
mod insert;
mod metrics;
mod pg_error;
mod session;
mod types;
mod watermark;
mod writer;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::plugin(destination)]
pub struct DestPostgres {
    config: config::Config,
}

impl Destination for DestPostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        Ok(Self { config })
    }

    async fn validate(
        &self,
        _ctx: &Context,
        _upstream: Option<&rapidbyte_sdk::schema::StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        client::validate(&self.config).await
    }

    async fn write(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError> {
        writer::write_stream(&self.config, ctx, &stream).await
    }

    async fn close(&self, ctx: &Context) -> Result<(), PluginError> {
        ctx.log(LogLevel::Info, "dest-postgres: close (no-op)");
        Ok(())
    }
}
