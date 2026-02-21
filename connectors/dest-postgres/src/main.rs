mod config;
mod ddl;
mod format;
mod loader;
pub mod sink;

use rapidbyte_sdk::connector::DestinationConnector;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult, ValidationStatus};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{Feature, OpenInfo, StreamContext, WriteSummary};

pub struct DestPostgres {
    config: config::Config,
}

impl DestinationConnector for DestPostgres {
    type Config = config::Config;

    async fn connect(config: Self::Config) -> Result<(Self, OpenInfo), ConnectorError> {
        config.validate()?;
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
        Ok((
            Self { config },
            OpenInfo {
                protocol_version: "2".to_string(),
                features,
                default_max_batch_bytes: 64 * 1024 * 1024,
            },
        ))
    }

    async fn validate(config: &Self::Config) -> Result<ValidationResult, ConnectorError> {
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
    }

    async fn write(&mut self, ctx: StreamContext) -> Result<WriteSummary, ConnectorError> {
        sink::write_stream(&self.config, &ctx).await
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        host_ffi::log(2, "dest-postgres: close (no-op)");
        Ok(())
    }
}

rapidbyte_sdk::dest_connector_main!(DestPostgres);
