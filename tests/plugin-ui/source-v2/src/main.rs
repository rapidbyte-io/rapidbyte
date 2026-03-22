use rapidbyte_sdk::prelude::*;
use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, ConfigSchema)]
pub struct Config {
    endpoint: String,
}

#[rapidbyte_sdk::plugin(source)]
pub struct SourceFixture;

impl Source for SourceFixture {
    type Config = Config;

    async fn init(config: Self::Config, input: InitInput<'_>) -> Result<Self, PluginError> {
        let _ = config.endpoint;
        let _ = input;
        Ok(Self)
    }

    async fn discover(&self, input: DiscoverInput<'_>) -> Result<Vec<DiscoveredStream>, PluginError> {
        let _ = input;
        Ok(vec![])
    }

    async fn read(&self, input: ReadInput<'_>) -> Result<ReadSummary, PluginError> {
        let _ = input.stream.stream_name;
        Ok(ReadSummary {
            records_read: 0,
            bytes_read: 0,
            batches_emitted: 0,
            checkpoint_count: 0,
            records_skipped: 0,
        })
    }
}

impl PartitionedSource for SourceFixture {
    async fn read_partition(&self, input: PartitionedReadInput<'_>) -> Result<ReadSummary, PluginError> {
        let _ = input.partition.index;
        Ok(ReadSummary {
            records_read: 0,
            bytes_read: 0,
            batches_emitted: 0,
            checkpoint_count: 0,
            records_skipped: 0,
        })
    }
}

impl CdcSource for SourceFixture {
    async fn read_changes(&self, input: CdcReadInput<'_>) -> Result<ReadSummary, PluginError> {
        let _ = input.resume.value;
        Ok(ReadSummary {
            records_read: 0,
            bytes_read: 0,
            batches_emitted: 0,
            checkpoint_count: 0,
            records_skipped: 0,
        })
    }
}
