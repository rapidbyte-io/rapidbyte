mod config;
mod format;
pub mod sink;

use std::time::Instant;

use rapidbyte_sdk::connector::DestinationConnector;
use rapidbyte_sdk::errors::{CommitState, ConnectorError, ValidationResult, ValidationStatus};
use rapidbyte_sdk::host_ffi;
use rapidbyte_sdk::protocol::{Feature, OpenContext, OpenInfo, StreamContext, WritePerf, WriteSummary};
use rapidbyte_sdk::validation::validate_pg_identifier;
use tokio_postgres::NoTls;

#[derive(Default)]
pub struct DestPostgres {
    config: Option<config::Config>,
}

/// Connect to PostgreSQL using the provided config.
async fn connect(config: &config::Config) -> Result<tokio_postgres::Client, String> {
    let conn_str = config.connection_string();
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .map_err(|e| format!("Connection failed: {}", e))?;

    // Spawn the connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            host_ffi::log(0, &format!("PostgreSQL connection error: {}", e));
        }
    });

    Ok(client)
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
            protocol_version: "1".to_string(),
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
            let client = connect(config)
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
            ConnectorError::config("NO_CONFIG", "No config available. Call rb_open first.")
        })?;

        // Validate identifiers before interpolating into SQL
        validate_pg_identifier(&ctx.stream_name)
            .map_err(|e| {
                ConnectorError::config(
                    "INVALID_IDENTIFIER",
                    format!("Invalid stream name: {}", e),
                )
            })?;
        validate_pg_identifier(&config.schema)
            .map_err(|e| {
                ConnectorError::config(
                    "INVALID_IDENTIFIER",
                    format!("Invalid schema name: {}", e),
                )
            })?;

        let rt = config::create_runtime();
        rt.block_on(async {
            // Phase 1: Connect
            let connect_start = Instant::now();
            let client = connect(config)
                .await
                .map_err(|e| ConnectorError::transient_network("CONNECTION_FAILED", e))?;
            let connect_secs = connect_start.elapsed().as_secs_f64();

            // Phase 2: Session lifecycle
            let mut session = sink::WriteSession::begin(
                &client,
                &config.schema,
                sink::SessionConfig {
                    stream_name: ctx.stream_name.clone(),
                    write_mode: ctx.write_mode.clone(),
                    load_method: config.load_method.clone(),
                    schema_policy: ctx.policies.schema_evolution,
                    on_data_error: ctx.policies.on_data_error,
                    checkpoint: sink::CheckpointConfig {
                        interval_bytes: ctx.limits.checkpoint_interval_bytes,
                        interval_rows: ctx.limits.checkpoint_interval_rows,
                        interval_seconds: ctx.limits.checkpoint_interval_seconds,
                    },
                },
            )
            .await
            .map_err(|e| ConnectorError::transient_db("SESSION_BEGIN_FAILED", e))?;

            // Phase 3: Pull loop — read batches from host
            let mut buf: Vec<u8> = Vec::new();
            let mut loop_error: Option<String> = None;

            loop {
                match host_ffi::next_batch(&mut buf, ctx.limits.max_batch_bytes) {
                    Ok(None) => break,
                    Ok(Some(n)) => {
                        if let Err(e) = session.process_batch(&buf[..n]).await {
                            loop_error = Some(e);
                            break;
                        }
                    }
                    Err(e) => {
                        loop_error = Some(format!("next_batch failed: {}", e));
                        break;
                    }
                }
            }

            // Handle errors
            if let Some(err) = loop_error {
                session.rollback().await;
                return Err(
                    ConnectorError::transient_db("WRITE_FAILED", err)
                        .with_commit_state(CommitState::BeforeCommit),
                );
            }

            // Phase 4: Commit
            let result = session.commit().await.map_err(|e| {
                ConnectorError::transient_db("COMMIT_FAILED", e)
                    .with_commit_state(CommitState::AfterCommitUnknown)
            })?;

            Ok(WriteSummary {
                records_written: result.total_rows,
                bytes_written: result.total_bytes,
                batches_written: result.batches_written,
                checkpoint_count: result.checkpoint_count,
                records_failed: result.total_failed,
                perf: Some(WritePerf {
                    connect_secs,
                    flush_secs: result.flush_secs,
                    commit_secs: result.commit_secs,
                    arrow_decode_secs: result.arrow_decode_secs,
                }),
            })
        })
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        host_ffi::log(2, "dest-postgres: close (no-op)");
        Ok(())
    }
}

rapidbyte_sdk::dest_connector_main!(DestPostgres);
