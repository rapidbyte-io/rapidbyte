use rapidbyte_sdk::context::Context;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, ConfigSchema)]
pub struct Config {
    table: String,
}

#[rapidbyte_sdk::plugin(destination)]
pub struct OldContextDestination;

impl Destination for OldContextDestination {
    type Config = Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        let _ = config.table;
        Ok(Self)
    }

    async fn write(&self, _ctx: &Context, _stream: StreamContext) -> Result<WriteSummary, PluginError> {
        Ok(WriteSummary {
            records_written: 0,
            bytes_written: 0,
            batches_written: 0,
            checkpoint_count: 0,
            records_failed: 0,
        })
    }
}
