//! Minimal test transform plugin for integration tests.
//!
//! Pass-through transform: reads batches from upstream and emits them
//! unchanged to downstream. Supports `should_fail` to simulate errors.

use rapidbyte_sdk::prelude::*;
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

    async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError> {
        Ok(Self { config })
    }

    async fn validate(
        &self,
        _input: ValidateInput<'_>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Test transform config is valid"))
    }

    async fn transform(
        &self,
        input: TransformInput<'_>,
    ) -> Result<TransformSummary, PluginError> {
        if self.config.should_fail {
            return Err(PluginError::internal(
                "TEST_FAILURE",
                "Simulated transform failure",
            ));
        }

        input.log.info("test-transform: pass-through mode");

        let mut records_in: u64 = 0;
        let mut records_out: u64 = 0;
        let mut bytes_in: u64 = 0;
        let mut bytes_out: u64 = 0;
        let mut batches_processed: u64 = 0;

        while let Some((_schema, batches)) =
            rapidbyte_sdk::host_ffi::next_batch(input.stream.limits.max_batch_bytes)?
        {
            for batch in &batches {
                records_in += batch.num_rows() as u64;
                bytes_in += batch.get_array_memory_size() as u64;

                input.emit.batch_for_stream(input.stream.stream_index, batch)?;

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
