use rapidbyte_sdk::context::Context;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, ConfigSchema)]
struct Config {
    endpoint: String,
}

#[rapidbyte_sdk::plugin(source)]
pub struct OldContextSource;

impl Source for OldContextSource {
    type Config = Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        let _ = config.endpoint;
        Ok(Self)
    }

    async fn discover(&self, _ctx: &Context) -> Result<Vec<DiscoveredStream>, PluginError> {
        Ok(vec![])
    }

    async fn read(&self, _ctx: &Context, _stream: StreamContext) -> Result<ReadSummary, PluginError> {
        Ok(ReadSummary {
            records_read: 0,
            bytes_read: 0,
            batches_emitted: 0,
            checkpoint_count: 0,
            records_skipped: 0,
        })
    }
}
