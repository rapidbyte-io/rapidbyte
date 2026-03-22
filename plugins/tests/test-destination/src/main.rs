//! Minimal test destination plugin for integration tests.
//!
//! Consumes Arrow batches from the host pipeline, counting rows received.
//! Supports `should_fail` to simulate write failures.

use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// If true, the destination will return an error during write
    #[serde(default)]
    pub should_fail: bool,
}

#[rapidbyte_sdk::plugin(destination)]
pub struct TestDestination {
    config: Config,
}

impl Destination for TestDestination {
    type Config = Config;

    async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError> {
        Ok(Self { config })
    }

    async fn validate(
        &self,
        _input: ValidateInput<'_>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success(
            "Test destination config is valid",
        ))
    }

    async fn write(
        &self,
        input: WriteInput<'_>,
    ) -> Result<WriteSummary, PluginError> {
        if self.config.should_fail {
            return Err(PluginError::internal(
                "TEST_FAILURE",
                "Simulated destination failure",
            ));
        }

        rapidbyte_sdk::host_ffi::log(LogLevel::Info as i32, "test-destination: consuming batches");

        let mut records_written: u64 = 0;
        let mut bytes_written: u64 = 0;
        let mut batches_written: u64 = 0;

        while let Some((_schema, batches)) = input.reader.next_batch(input.stream.limits.max_batch_bytes)? {
            for batch in &batches {
                records_written += batch.num_rows() as u64;
                bytes_written += batch.get_array_memory_size() as u64;
                batches_written += 1;
            }
        }

        rapidbyte_sdk::host_ffi::log(
            LogLevel::Info as i32,
            &format!(
                "test-destination: wrote {} records in {} batches",
                records_written, batches_written
            ),
        );

        Ok(WriteSummary {
            records_written,
            bytes_written,
            batches_written,
            checkpoint_count: 0,
            records_failed: 0,
        })
    }
}
