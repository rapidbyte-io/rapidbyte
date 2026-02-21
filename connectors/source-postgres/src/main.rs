pub mod config;
pub mod schema;
pub mod source;

use std::time::Instant;

use rapidbyte_sdk::connector::SourceConnector;
use rapidbyte_sdk::errors::{ConnectorError, ValidationResult, ValidationStatus};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{Catalog, OpenContext, OpenInfo, ReadSummary, StreamContext};

#[derive(Default)]
pub struct SourcePostgres {
    config: Option<config::Config>,
}

impl SourceConnector for SourcePostgres {
    fn open(&mut self, ctx: OpenContext) -> Result<OpenInfo, ConnectorError> {
        let config = config::Config::from_open_context(&ctx)?;
        host_ffi::log(
            2,
            &format!(
                "source-postgres: open with host={} db={}",
                config.host, config.database
            ),
        );
        self.config = Some(config);
        Ok(OpenInfo {
            protocol_version: "2".to_string(),
            features: vec![],
            default_max_batch_bytes: 64 * 1024 * 1024,
        })
    }

    fn discover(&mut self) -> Result<Catalog, ConnectorError> {
        let config = self.config.as_ref().ok_or_else(|| {
            ConnectorError::config("NOT_OPENED", "open must be called before discover")
        })?;
        let rt = config::create_runtime();
        rt.block_on(async {
            let client = source::connect(config)
                .await
                .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
            schema::discover_catalog(&client)
                .await
                .map(|streams| Catalog { streams })
                .map_err(|e| ConnectorError::transient_db("DISCOVERY_FAILED", e))
        })
    }

    fn validate(&mut self) -> Result<ValidationResult, ConnectorError> {
        let config = self.config.as_ref().ok_or_else(|| {
            ConnectorError::config("NOT_OPENED", "Call open first")
        })?;
        let rt = config::create_runtime();
        rt.block_on(async {
            let client = source::connect(config)
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
            Ok(ValidationResult {
                status: ValidationStatus::Success,
                message: format!(
                    "Connected to {}:{}/{}",
                    config.host, config.port, config.database
                ),
            })
        })
    }

    fn read(&mut self, ctx: StreamContext) -> Result<ReadSummary, ConnectorError> {
        let config = self.config.as_ref().ok_or_else(|| {
            ConnectorError::config("NO_CONFIG", "No config available. Call open first.")
        })?;
        let rt = config::create_runtime();
        rt.block_on(async {
            let connect_start = Instant::now();
            let client = source::connect(config)
                .await
                .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
            let connect_secs = connect_start.elapsed().as_secs_f64();
            source::read_stream(&client, &ctx, connect_secs)
                .await
                .map_err(|e| ConnectorError::internal("READ_FAILED", e))
        })
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        host_ffi::log(2, "source-postgres: close (no-op)");
        Ok(())
    }
}

rapidbyte_sdk::source_connector_main!(SourcePostgres);
