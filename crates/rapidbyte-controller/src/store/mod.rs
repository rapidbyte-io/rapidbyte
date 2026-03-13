//! Durable controller metadata store bootstrap.

use tokio_postgres::NoTls;

pub const CONTROLLER_METADATA_MIGRATIONS: &str =
    include_str!("migrations/0001_controller_metadata.sql");

/// Connect to the metadata store and apply the checked-in migration set.
///
/// # Errors
///
/// Returns an error if the database cannot be reached or the schema migration
/// fails.
pub async fn initialize_metadata_store(database_url: &str) -> anyhow::Result<()> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::error!(?error, "controller metadata store connection failed");
        }
    });
    client.batch_execute(CONTROLLER_METADATA_MIGRATIONS).await?;
    Ok(())
}
