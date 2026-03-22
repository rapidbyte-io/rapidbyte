use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, ConfigSchema)]
pub struct Config {
    rule: String,
}

#[rapidbyte_sdk::plugin(transform)]
pub struct TransformFixture;

impl Transform for TransformFixture {
    type Config = Config;

    async fn init(config: Self::Config, input: InitInput<'_>) -> Result<Self, PluginError> {
        let _ = (config.rule, input);
        Ok(Self)
    }

    async fn transform(&self, input: TransformInput<'_>) -> Result<TransformSummary, PluginError> {
        let _ = input.stream.stream_name;
        Ok(TransformSummary {
            records_in: 0,
            records_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            batches_processed: 0,
        })
    }
}
