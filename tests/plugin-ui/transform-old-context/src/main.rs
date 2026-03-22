use rapidbyte_sdk::context::Context;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, ConfigSchema)]
pub struct Config {
    rule: String,
}

#[rapidbyte_sdk::plugin(transform)]
pub struct OldContextTransform;

impl Transform for OldContextTransform {
    type Config = Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        let _ = config.rule;
        Ok(Self)
    }

    async fn transform(
        &self,
        _ctx: &Context,
        _stream: StreamContext,
    ) -> Result<TransformSummary, PluginError> {
        Ok(TransformSummary {
            records_in: 0,
            records_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            batches_processed: 0,
        })
    }
}
