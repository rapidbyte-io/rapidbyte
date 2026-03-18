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

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {
        Ok((
            Self { config },
            PluginInfo {
                protocol_version: ProtocolVersion::V6,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            },
        ))
    }

    async fn validate(
        _config: &Self::Config,
        _ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {
        Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: "Test destination config is valid".to_string(),
            warnings: Vec::new(),
        })
    }

    async fn write(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError> {
        if self.config.should_fail {
            return Err(PluginError::internal(
                "TEST_FAILURE",
                "Simulated destination failure",
            ));
        }

        ctx.log(LogLevel::Info, "test-destination: consuming batches");

        let mut records_written: u64 = 0;
        let mut bytes_written: u64 = 0;
        let mut batches_written: u64 = 0;

        while let Some((_schema, batches)) = ctx.next_batch(stream.limits.max_batch_bytes)? {
            for batch in &batches {
                records_written += batch.num_rows() as u64;
                bytes_written += batch.get_array_memory_size() as u64;
                batches_written += 1;
            }
        }

        ctx.log(
            LogLevel::Info,
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
