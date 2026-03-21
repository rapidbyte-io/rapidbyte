//! Minimal test transform plugin for integration tests.
//!
//! Pass-through transform: reads batches from upstream and emits them
//! unchanged to downstream. Supports `should_fail` to simulate errors.

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::schema::StreamSchema;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// If true, the transform will return an error
    #[serde(default)]
    pub should_fail: bool,
}

#[rapidbyte_sdk::plugin(transform)]
pub struct TestTransform {
    config: Config,
}

impl Transform for TestTransform {
    type Config = Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        Ok(Self { config })
    }

    async fn validate(
        &self,
        _ctx: &Context,
        _upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Test transform config is valid"))
    }

    async fn transform(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError> {
        if self.config.should_fail {
            return Err(PluginError::internal(
                "TEST_FAILURE",
                "Simulated transform failure",
            ));
        }

        ctx.log(LogLevel::Info, "test-transform: pass-through mode");

        let mut records_in: u64 = 0;
        let mut records_out: u64 = 0;
        let mut bytes_in: u64 = 0;
        let mut bytes_out: u64 = 0;
        let mut batches_processed: u64 = 0;

        while let Some((_schema, batches)) = ctx.next_batch(stream.limits.max_batch_bytes)? {
            for batch in &batches {
                records_in += batch.num_rows() as u64;
                bytes_in += batch.get_array_memory_size() as u64;

                ctx.emit_batch(batch)?;

                records_out += batch.num_rows() as u64;
                bytes_out += batch.get_array_memory_size() as u64;
                batches_processed += 1;
            }
        }

        Ok(TransformSummary {
            records_in,
            records_out,
            bytes_in,
            bytes_out,
            batches_processed,
        })
    }
}
