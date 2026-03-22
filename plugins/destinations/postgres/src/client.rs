//! `PostgreSQL` client connection helpers for dest-postgres.

use tokio_postgres::{Client, Config as PgConfig, NoTls};

/// Connect to `PostgreSQL` using the provided config.
///
/// # Errors
///
/// Returns `Err` if the TCP connection or `PostgreSQL` authentication fails.
pub(crate) async fn connect(config: &crate::config::Config) -> Result<Client, String> {
    let mut pg = PgConfig::new();
    pg.host(&config.host);
    pg.port(config.port);
    pg.user(&config.user);
    if !config.password.is_empty() {
        pg.password(&config.password);
    }
    pg.dbname(&config.database);

    let stream = rapidbyte_sdk::host_tcp::HostTcpStream::connect(&config.host, config.port)
        .map_err(|e| format!("Connection failed: {e}"))?;
    let (client, connection) = pg
        .connect_raw(stream, NoTls)
        .await
        .map_err(|e| format!("Connection failed: {e}"))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            rapidbyte_sdk::host_ffi::log_error(&format!("PostgreSQL connection error: {e}"));
        }
    });

    client
        .execute("SET synchronous_commit = OFF", &[])
        .await
        .map_err(|e| format!("Failed to set synchronous_commit: {e}"))?;

    Ok(client)
}
