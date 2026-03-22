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
mod diagnostics;
mod pg_error;
mod prerequisites;
mod session;
mod types;
mod validate;
mod apply;
mod watermark;
mod writer;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::plugin(destination)]
pub struct DestPostgres {
    config: config::Config,
}

impl Destination for DestPostgres {
    type Config = config::Config;

    async fn init(config: Self::Config, _input: InitInput<'_>) -> Result<Self, PluginError> {
        Ok(Self { config })
    }

    async fn prerequisites(
        &self,
        input: PrerequisitesInput<'_>,
    ) -> Result<PrerequisitesReport, PluginError> {
        prerequisites::prerequisites(&self.config, input).await
    }

    async fn validate(&self, input: ValidateInput<'_>) -> Result<ValidationReport, PluginError> {
        validate::validate(&self.config, input.upstream)
    }

    async fn apply(&self, input: ApplyInput<'_>) -> Result<ApplyReport, PluginError> {
        apply::apply(&self.config, input).await
    }

    async fn write(&self, input: WriteInput<'_>) -> Result<WriteSummary, PluginError> {
        writer::write_stream(&self.config, input).await
    }

    async fn close(&self, input: CloseInput<'_>) -> Result<(), PluginError> {
        input.log.info("dest-postgres: close (no-op)");
        Ok(())
    }
}

impl BulkDestination for DestPostgres {
    async fn write_bulk(&self, input: BulkWriteInput<'_>) -> Result<WriteSummary, PluginError> {
        writer::write_bulk_stream(&self.config, input).await
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
    fn dest_postgres_default_config_has_public_schema() {
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
        };
        assert_eq!(plugin.config.schema, "public");
    }

    #[tokio::test]
    async fn dest_postgres_uses_typed_lifecycle_inputs_without_context() {
        let config = config::Config {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
            schema: "public".to_string(),
            load_method: config::LoadMethod::Copy,
            copy_flush_bytes: None,
        };

        let plugin = DestPostgres::init(config, InitInput::new())
            .await
            .expect("init");

        let stream = StreamContext::test_default("users");
        let apply_request = ApplyRequest {
            streams: vec![stream.clone()],
            dry_run: true,
        };

        let _ = plugin.prerequisites(PrerequisitesInput::new());
        let _ = plugin.validate(ValidateInput::new(None));
        let _ = plugin.apply(ApplyInput::new(apply_request));
        let _ = plugin.write(WriteInput::new(stream.clone()));
        let _ = plugin.write_bulk(BulkWriteInput::new(stream));
        let _ = plugin.close(CloseInput::new());
    }
}
