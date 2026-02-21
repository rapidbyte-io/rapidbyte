use anyhow::{anyhow, Context};
use tokio_postgres::{Client, Config as PgConfig, NoTls};

use rapidbyte_sdk::errors::{ConnectorError, ValidationResult, ValidationStatus};
use rapidbyte_sdk::host_ffi;

/// Connect to PostgreSQL using the provided config.
pub(crate) async fn connect(config: &crate::config::Config) -> Result<Client, String> {
    connect_inner(config).await.map_err(|e| e.to_string())
}

async fn connect_inner(config: &crate::config::Config) -> anyhow::Result<Client> {
    let mut pg = PgConfig::new();
    pg.host(&config.host);
    pg.port(config.port);
    pg.user(&config.user);
    if !config.password.is_empty() {
        pg.password(&config.password);
    }
    pg.dbname(&config.database);

    let stream = rapidbyte_sdk::host_tcp::HostTcpStream::connect(&config.host, config.port)
        .map_err(|e| anyhow!("Connection failed: {e}"))?;
    let (client, connection) = pg
        .connect_raw(stream, NoTls)
        .await
        .context("Connection failed")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            host_ffi::log(0, &format!("PostgreSQL connection error: {}", e));
        }
    });

    Ok(client)
}

/// Validate PostgreSQL connectivity.
pub(crate) async fn validate(config: &crate::config::Config) -> Result<ValidationResult, ConnectorError> {
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
    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: format!(
            "Connected to {}:{}/{}",
            config.host, config.port, config.database
        ),
    })
}
