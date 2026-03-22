//! Source plugin for `PostgreSQL`.
//!
//! Implements discovery and read paths (full-refresh, incremental cursor reads,
//! and CDC via `pgoutput` logical replication) and streams Arrow IPC batches to
//! the host.

mod cdc;
mod client;
mod config;
mod cursor;
mod discovery;
mod encode;
mod diagnostics;
mod metrics;
mod prerequisites;
mod query;
mod reader;
mod types;

use std::time::Instant;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::plugin(source)]
pub struct SourcePostgres {
    config: config::Config,
    discovery_settings: config::DiscoverySettings,
}

impl Source for SourcePostgres {
    type Config = config::Config;

    async fn init(config: Self::Config, input: InitInput<'_>) -> Result<Self, PluginError> {
        config.validate()?;
        let discovery_settings = config.discovery_settings();
        input
            .log
            .info("source-postgres: init complete");
        Ok(Self {
            config,
            discovery_settings,
        })
    }

    async fn prerequisites(
        &self,
        input: PrerequisitesInput<'_>,
    ) -> Result<PrerequisitesReport, PluginError> {
        prerequisites::prerequisites(&self.config, input).await
    }

    async fn discover(&self, input: DiscoverInput<'_>) -> Result<Vec<DiscoveredStream>, PluginError> {
        input
            .cancel
            .check()
            .map_err(|e| PluginError::cancelled("CANCELLED", e.message))?;
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        discovery::discover_catalog_with_settings(&client, &self.discovery_settings, input)
            .await
            .map_err(|e| PluginError::transient_db("DISCOVERY_FAILED", e))
    }

    async fn validate(&self, _input: ValidateInput<'_>) -> Result<ValidationReport, PluginError> {
        client::validate(&self.config).await
    }

    async fn read(&self, input: ReadInput<'_>) -> Result<ReadSummary, PluginError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        reader::read_stream(&client, input, connect_secs)
            .await
            .map_err(|e| PluginError::internal("READ_FAILED", e))
    }

    async fn close(&self, input: CloseInput<'_>) -> Result<(), PluginError> {
        input.log.info("source-postgres: close (no-op)");
        Ok(())
    }
}

impl PartitionedSource for SourcePostgres {
    async fn read_partition(
        &self,
        input: PartitionedReadInput<'_>,
    ) -> Result<ReadSummary, PluginError> {
        // Partition coordinates are already embedded in StreamContext;
        // reader::read_stream extracts them via stream.partition_coordinates().
        let PartitionedReadInput {
            stream,
            dry_run,
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
            ..
        } = input;
        self.read(ReadInput::with_capabilities(
            stream,
            dry_run,
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
        ))
        .await
    }
}

impl CdcSource for SourcePostgres {
    async fn read_changes(
        &self,
        input: CdcReadInput<'_>,
    ) -> Result<ReadSummary, PluginError> {
        let CdcReadInput {
            stream,
            resume,
            dry_run,
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
            ..
        } = input;
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();
        let input = CdcReadInput::with_capabilities(
            stream,
            normalize_cdc_resume_token(&resume),
            dry_run,
            emit,
            cancel,
            state,
            checkpoints,
            metrics,
            log,
        );

        cdc::read_cdc_changes(&client, input, &self.config, connect_secs)
            .await
            .map_err(|e| PluginError::internal("CDC_READ_FAILED", e))
    }
}

fn normalize_cdc_resume_token(resume: &CdcResumeToken) -> CdcResumeToken {
    if resume.value.is_none() {
        CdcResumeToken {
            value: None,
            cursor_type: rapidbyte_sdk::cursor::CursorType::Lsn,
        }
    } else {
        resume.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_cdc_resume_token;
    use crate::SourcePostgres;
    use rapidbyte_sdk::cursor::CursorType;
    use rapidbyte_sdk::input::{
        CdcReadInput, CloseInput, DiscoverInput, InitInput, PartitionedReadInput, PrerequisitesInput,
        ReadInput, ValidateInput,
    };
    use rapidbyte_sdk::prelude::*;
    use rapidbyte_sdk::stream::{CdcResumeToken, PartitionCoordinates, PartitionStrategy, StreamContext};

    #[test]
    fn normalize_cdc_resume_token_promotes_missing_resume_to_lsn() {
        let normalized = normalize_cdc_resume_token(&CdcResumeToken {
            value: None,
            cursor_type: CursorType::Utf8,
        });

        assert_eq!(normalized.value, None);
        assert_eq!(normalized.cursor_type, CursorType::Lsn);
    }

    #[tokio::test]
    async fn init_stores_shared_discovery_settings_and_v2_method_calls_compile() {
        let config = crate::config::Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "test".to_string(),
            schema: Some("analytics".to_string()),
            replication_slot: Some("slot_a".to_string()),
            publication: Some("pub_a".to_string()),
        };

        let source = SourcePostgres::init(config.clone(), InitInput::new())
            .await
            .expect("init");

        assert_eq!(source.discovery_settings.schema, config.schema);
        assert_eq!(source.discovery_settings.publication, config.publication);

        let stream = StreamContext::test_default("users");
        let partition = PartitionCoordinates {
            count: 2,
            index: 0,
            strategy: PartitionStrategy::Range,
        };

        // This is intentionally compile-shape coverage for the connector's v2
        // lifecycle surface; the behavior of the async paths is covered by the
        // connector's existing focused unit tests.
        fn assert_source_method_calls_compile(
            source: &SourcePostgres,
            stream: StreamContext,
            partition: PartitionCoordinates,
        ) {
            let _ = source.discover(DiscoverInput::new());
            let _ = source.prerequisites(PrerequisitesInput::new());
            let _ = source.validate(ValidateInput::new(None));
            let _ = source.read(ReadInput::new(stream.clone()));
            let _ = source.read_partition(PartitionedReadInput::new(stream.clone(), partition));
            let _ = source.read_changes(CdcReadInput::new(
                stream,
                CdcResumeToken {
                    value: None,
                    cursor_type: CursorType::Utf8,
                },
            ));
            let _ = source.close(CloseInput::new());
        }

        assert_source_method_calls_compile(&source, stream, partition);
    }
}
