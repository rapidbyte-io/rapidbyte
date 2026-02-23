//! Replace-mode staging table lifecycle helpers.

use pg_escape::quote_identifier;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;

/// Drop an existing staging table if it exists.
async fn drop_staging_table(
    ctx: &Context,
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<(), String> {
    let staging_table = format!(
        "{}.{}",
        quote_identifier(target_schema),
        quote_identifier(&format!("{}__rb_staging", stream_name))
    );
    let sql = format!("DROP TABLE IF EXISTS {} CASCADE", staging_table);
    client
        .execute(&sql, &[])
        .await
        .map_err(|e| format!("DROP staging table failed for {}: {e}", staging_table))?;
    ctx.log(
        LogLevel::Debug,
        &format!("dest-postgres: dropped staging table {}", staging_table),
    );
    Ok(())
}

/// Atomically swap a staging table into the target position.
pub(crate) async fn swap_staging_table(
    ctx: &Context,
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<(), String> {
    let target_table = format!(
        "{}.{}",
        quote_identifier(target_schema),
        quote_identifier(stream_name)
    );
    let staging_table = format!(
        "{}.{}",
        quote_identifier(target_schema),
        quote_identifier(&format!("{}__rb_staging", stream_name))
    );
    let staging_name_only = quote_identifier(stream_name);

    client
        .execute("BEGIN", &[])
        .await
        .map_err(|e| format!("Swap BEGIN failed: {e}"))?;

    let drop_sql = format!("DROP TABLE IF EXISTS {} CASCADE", target_table);
    if let Err(e) = client.execute(&drop_sql, &[]).await {
        let _ = client.execute("ROLLBACK", &[]).await;
        return Err(format!("Swap DROP failed for {}: {}", target_table, e));
    }

    let rename_sql = format!(
        "ALTER TABLE {} RENAME TO {}",
        staging_table, staging_name_only
    );
    if let Err(e) = client.execute(&rename_sql, &[]).await {
        let _ = client.execute("ROLLBACK", &[]).await;
        return Err(format!("Swap RENAME failed: {}", e));
    }

    client
        .execute("COMMIT", &[])
        .await
        .map_err(|e| format!("Swap COMMIT failed: {e}"))?;

    ctx.log(
        LogLevel::Info,
        &format!(
            "dest-postgres: atomic swap {} -> {}",
            staging_table, target_table
        ),
    );
    Ok(())
}

/// Prepare a fresh staging table for Replace mode.
pub(crate) async fn prepare_staging(
    ctx: &Context,
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<String, String> {
    drop_staging_table(ctx, client, target_schema, stream_name).await?;
    Ok(format!("{}__rb_staging", stream_name))
}
