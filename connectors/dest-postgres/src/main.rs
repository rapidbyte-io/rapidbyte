mod config;
mod ddl;
mod format;
mod loader;
pub mod sink;

use rapidbyte_sdk::connector::DestinationConnector;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult, ValidationStatus};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{Feature, OpenContext, OpenInfo, StreamContext, WriteSummary};

#[derive(Default)]
pub struct DestPostgres {
    config: Option<config::Config>,
}

impl DestinationConnector for DestPostgres {
    fn open(&mut self, ctx: OpenContext) -> Result<OpenInfo, ConnectorError> {
        let config = config::Config::from_open_context(&ctx)?;

        host_ffi::log(
            2,
            &format!(
                "dest-postgres: open with host={} db={} schema={} load_method={}",
                config.host, config.database, config.schema, config.load_method
            ),
        );

        let mut features = vec![Feature::ExactlyOnce];
        if config.load_method == "copy" {
            features.push(Feature::BulkLoadCopy);
        }

        self.config = Some(config);

        Ok(OpenInfo {
            protocol_version: "2".to_string(),
            features,
            default_max_batch_bytes: 64 * 1024 * 1024,
        })
    }

    fn validate(&mut self) -> Result<ValidationResult, ConnectorError> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| ConnectorError::config("NOT_OPENED", "Call open first"))?;

        let rt = config::create_runtime();
        rt.block_on(async {
            let client = sink::connect(config)
                .await
                .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;

            client
                .query_one("SELECT 1", &[])
                .await
                .map_err(|e| {
                    ConnectorError::transient_network(
                        "CONNECTION_TEST_FAILED",
                        format!("Connection test failed: {}", e),
                    )
                })?;

            // Also verify the target schema exists
            let schema_check = client
                .query_one(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1",
                    &[&config.schema],
                )
                .await;

            let message = match schema_check {
                Ok(_) => format!(
                    "Connected to {}:{}/{} (schema: {})",
                    config.host, config.port, config.database, config.schema
                ),
                Err(_) => format!(
                    "Connected to {}:{}/{} (schema '{}' does not exist, will be created)",
                    config.host, config.port, config.database, config.schema
                ),
            };

            Ok(ValidationResult {
                status: ValidationStatus::Success,
                message,
            })
        })
    }

    fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError> {
        let config = self.config.as_ref().ok_or_else(|| {
            ConnectorError::config("NO_CONFIG", "No config available. Call open first.")
        })?;
        sink::write_stream(config, &ctx)
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        host_ffi::log(2, "dest-postgres: close (no-op)");
        Ok(())
    }
}

rapidbyte_sdk::dest_connector_main!(DestPostgres);
