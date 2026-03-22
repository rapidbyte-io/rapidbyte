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
use rapidbyte_sdk::schema::StreamSchema;

#[rapidbyte_sdk::plugin(source)]
pub struct SourcePostgres {
    config: config::Config,
    discovery_settings: config::DiscoverySettings,
}

impl Source for SourcePostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        config.validate()?;
        let discovery_settings = config.discovery_settings();
        Ok(Self {
            config,
            discovery_settings,
        })
    }

    async fn prerequisites(&self, ctx: &Context) -> Result<PrerequisitesReport, PluginError> {
        prerequisites::prerequisites(&self.config, ctx).await
    }

    async fn discover(&self, ctx: &Context) -> Result<Vec<DiscoveredStream>, PluginError> {
        let _ = ctx;
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        discovery::discover_catalog_with_settings(&client, &self.discovery_settings)
            .await
            .map_err(|e| PluginError::transient_db("DISCOVERY_FAILED", e))
    }

    async fn validate(
        &self,
        ctx: &Context,
        _upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        let _ = ctx;
        client::validate(&self.config).await
    }

    async fn read(&self, ctx: &Context, stream: StreamContext) -> Result<ReadSummary, PluginError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();

        reader::read_stream(&client, ctx, &stream, connect_secs)
            .await
            .map_err(|e| PluginError::internal("READ_FAILED", e))
    }

    async fn close(&self, ctx: &Context) -> Result<(), PluginError> {
        ctx.log(LogLevel::Info, "source-postgres: close (no-op)");
        Ok(())
    }
}

impl PartitionedSource for SourcePostgres {
    async fn read_partition(
        &self,
        ctx: &Context,
        stream: StreamContext,
        _partition: PartitionCoordinates,
    ) -> Result<ReadSummary, PluginError> {
        // Partition coordinates are already embedded in StreamContext;
        // reader::read_stream extracts them via stream.partition_coordinates().
        self.read(ctx, stream).await
    }
}

impl CdcSource for SourcePostgres {
    async fn read_changes(
        &self,
        ctx: &Context,
        stream: StreamContext,
        resume: CdcResumeToken,
    ) -> Result<ReadSummary, PluginError> {
        let connect_start = Instant::now();
        let client = client::connect(&self.config)
            .await
            .map_err(|e| PluginError::transient_network("CONNECTION_FAILED", e))?;
        let connect_secs = connect_start.elapsed().as_secs_f64();
        let resume = normalize_cdc_resume_token(&resume);

        cdc::read_cdc_changes(&client, ctx, &stream, &resume, &self.config, connect_secs)
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
    use rapidbyte_sdk::cursor::CursorType;
    use rapidbyte_sdk::stream::CdcResumeToken;

    #[test]
    fn normalize_cdc_resume_token_promotes_missing_resume_to_lsn() {
        let normalized = normalize_cdc_resume_token(&CdcResumeToken {
            value: None,
            cursor_type: CursorType::Utf8,
        });

        assert_eq!(normalized.value, None);
        assert_eq!(normalized.cursor_type, CursorType::Lsn);
    }
}
