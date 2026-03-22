use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, ConfigSchema)]
pub struct Config {
    table: String,
}

#[rapidbyte_sdk::plugin(destination)]
pub struct DestinationFixture;

impl Destination for DestinationFixture {
    type Config = Config;

    async fn init(config: Self::Config, input: InitInput<'_>) -> Result<Self, PluginError> {
        let _ = (config.table, input);
        Ok(Self)
    }

    async fn write(&self, input: WriteInput<'_>) -> Result<WriteSummary, PluginError> {
        let _ = input.stream.stream_name;
        Ok(WriteSummary {
            records_written: 0,
            bytes_written: 0,
            batches_written: 0,
            checkpoint_count: 0,
            records_failed: 0,
        })
    }
}
