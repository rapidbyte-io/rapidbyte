//! Minimal test source plugin for integration tests.
//!
//! Emits a configurable number of simple Arrow batches with `id` (Int32)
//! and `value` (Utf8) columns. Supports `should_fail` to simulate errors.

use std::sync::Arc;

use ::arrow::array::{Int32Array, StringArray};
use ::arrow::datatypes::{DataType, Field, Schema};
use ::arrow::record_batch::RecordBatch;
use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::schema::{SchemaField, StreamSchema};
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// Number of rows to emit
    #[serde(default = "default_row_count")]
    pub row_count: u64,

    /// If true, the source will return an error during read
    #[serde(default)]
    pub should_fail: bool,

    /// Number of rows per batch (default 10)
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
}

fn default_row_count() -> u64 {
    10
}

fn default_batch_size() -> u64 {
    10
}

#[rapidbyte_sdk::plugin(source)]
pub struct TestSource {
    config: Config,
}

impl Source for TestSource {
    type Config = Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        Ok(Self { config })
    }

    async fn discover(&self, ctx: &Context) -> Result<Vec<DiscoveredStream>, PluginError> {
        ctx.log(LogLevel::Info, "test-source: discover");
        Ok(vec![DiscoveredStream {
            name: "test-stream".to_string(),
            schema: StreamSchema {
                fields: vec![
                    SchemaField::new("id", "int32", false).with_primary_key(true),
                    SchemaField::new("value", "utf8", true),
                ],
                primary_key: vec!["id".to_string()],
                partition_keys: vec![],
                source_defined_cursor: None,
                schema_id: None,
            },
            supported_sync_modes: vec![SyncMode::FullRefresh],
            default_cursor_field: None,
            estimated_row_count: None,
            metadata_json: None,
        }])
    }

    async fn validate(
        &self,
        _ctx: &Context,
        _upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Test source config is valid"))
    }

    async fn read(
        &self,
        ctx: &Context,
        _stream: StreamContext,
    ) -> Result<ReadSummary, PluginError> {
        if self.config.should_fail {
            return Err(PluginError::internal(
                "TEST_FAILURE",
                "Simulated source failure",
            ));
        }

        ctx.log(
            LogLevel::Info,
            &format!("test-source: emitting {} rows", self.config.row_count),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let mut total_emitted: u64 = 0;
        let mut batches_emitted: u64 = 0;
        let mut bytes_read: u64 = 0;

        while total_emitted < self.config.row_count {
            let remaining = self.config.row_count - total_emitted;
            let batch_rows = remaining.min(self.config.batch_size) as usize;

            let start = total_emitted as i32;
            let ids: Vec<i32> = (start..start + batch_rows as i32).collect();
            let values: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(StringArray::from(
                        values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    )),
                ],
            )
            .map_err(|e| {
                PluginError::internal("BATCH_BUILD", format!("failed to build batch: {e}"))
            })?;

            bytes_read += batch.get_array_memory_size() as u64;
            ctx.emit_batch(&batch)?;

            total_emitted += batch_rows as u64;
            batches_emitted += 1;
        }

        Ok(ReadSummary {
            records_read: total_emitted,
            bytes_read,
            batches_emitted,
            checkpoint_count: 0,
            records_skipped: 0,
        })
    }
}
