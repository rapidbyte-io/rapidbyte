//! Destination plugin for `PostgreSQL`.
//!
//! Receives Arrow IPC batches from the host and writes them to `PostgreSQL`
//! with transactional checkpoints and schema evolution handling.

mod client;
mod config;
mod contract;
mod copy;
mod ddl;
mod decode;
mod insert;
mod metrics;
mod pg_error;
mod prerequisites;
mod session;
mod types;
mod validate;
mod apply;
mod watermark;
mod writer;

use std::collections::HashMap;
use std::sync::Mutex;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::plugin(destination)]
pub struct DestPostgres {
    config: config::Config,
    prepared_contracts: Mutex<HashMap<String, contract::WriteContract>>,
}

impl Destination for DestPostgres {
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<Self, PluginError> {
        Ok(Self {
            config,
            prepared_contracts: Mutex::new(HashMap::new()),
        })
    }

    async fn prerequisites(&self, ctx: &Context) -> Result<PrerequisitesReport, PluginError> {
        prerequisites::prerequisites(&self.config, ctx).await
    }

    async fn validate(
        &self,
        _ctx: &Context,
        upstream: Option<&rapidbyte_sdk::schema::StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        validate::validate(&self.config, upstream)
    }

    async fn apply(
        &self,
        ctx: &Context,
        request: ApplyRequest,
    ) -> Result<ApplyReport, PluginError> {
        apply::apply(&self.config, ctx, request, &self.prepared_contracts).await
    }

    async fn write(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError> {
        let prepared = self
            .prepared_contracts
            .lock()
            .expect("prepared contract cache poisoned")
            .get(&stream.stream_name)
            .cloned();
        writer::write_stream(&self.config, prepared, ctx, &stream).await
    }

    async fn close(&self, ctx: &Context) -> Result<(), PluginError> {
        ctx.log(LogLevel::Info, "dest-postgres: close (no-op)");
        Ok(())
    }
}

impl BulkDestination for DestPostgres {
    async fn write_bulk(
        &self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<WriteSummary, PluginError> {
        let prepared = self
            .prepared_contracts
            .lock()
            .expect("prepared contract cache poisoned")
            .get(&stream.stream_name)
            .cloned();
        writer::write_stream(&self.config, prepared, ctx, &stream).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn assert_bulk_destination_impl<T: Destination + BulkDestination>() {}

    #[test]
    fn dest_postgres_implements_bulk_destination() {
        assert_bulk_destination_impl::<DestPostgres>();
    }

    #[test]
    fn dest_postgres_starts_without_prepared_contracts() {
        let plugin = DestPostgres {
            config: config::Config {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password: String::new(),
                database: "postgres".to_string(),
                schema: "public".to_string(),
                load_method: config::LoadMethod::Copy,
                copy_flush_bytes: None,
            },
            prepared_contracts: Mutex::new(HashMap::new()),
        };

        assert!(plugin
            .prepared_contracts
            .lock()
            .expect("prepared contract cache")
            .is_empty());
    }
}
