//! Replace-mode staging table lifecycle helpers.

use pg_escape::quote_identifier;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;

pub(crate) const REPLACE_PREPARED_MARKER_PREFIX: &str =
    "rapidbyte:dest-postgres:replace-prepared:";

/// Build the unqualified staging table name for a stream.
fn staging_name(stream_name: &str) -> String {
    format!("{stream_name}__rb_staging")
}

/// Drop an existing staging table if it exists.
async fn drop_staging_table(
    ctx: &Context,
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<(), String> {
    let staging_table = crate::decode::qualified_name(target_schema, &staging_name(stream_name));
    let sql = format!("DROP TABLE IF EXISTS {staging_table} CASCADE");
    client
        .execute(&sql, &[])
        .await
        .map_err(|e| format!("DROP staging table failed for {staging_table}: {e}"))?;
    ctx.log(
        LogLevel::Debug,
        &format!("dest-postgres: dropped staging table {staging_table}"),
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
    let target_table = crate::decode::qualified_name(target_schema, stream_name);
    let staging_table = crate::decode::qualified_name(target_schema, &staging_name(stream_name));
    let staging_name_only = quote_identifier(stream_name);

    client
        .execute("BEGIN", &[])
        .await
        .map_err(|e| format!("Swap BEGIN failed: {e}"))?;

    let drop_sql = format!("DROP TABLE IF EXISTS {target_table} CASCADE");
    if let Err(e) = client.execute(&drop_sql, &[]).await {
        let _ = client.execute("ROLLBACK", &[]).await;
        return Err(format!("Swap DROP failed for {target_table}: {e}"));
    }

    let rename_sql = format!("ALTER TABLE {staging_table} RENAME TO {staging_name_only}");
    if let Err(e) = client.execute(&rename_sql, &[]).await {
        let _ = client.execute("ROLLBACK", &[]).await;
        return Err(format!("Swap RENAME failed: {e}"));
    }

    client
        .execute("COMMIT", &[])
        .await
        .map_err(|e| format!("Swap COMMIT failed: {e}"))?;

    ctx.log(
        LogLevel::Info,
        &format!("dest-postgres: atomic swap {staging_table} -> {target_table}"),
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
    Ok(staging_name(stream_name))
}

/// Mark a staging table as prepared by the current apply/write lifecycle.
pub(crate) async fn mark_staging_prepared(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
    schema_signature: Option<&str>,
) -> Result<(), String> {
    let Some(schema_signature) = schema_signature else {
        return Ok(());
    };
    let staging_table = crate::decode::qualified_name(target_schema, &staging_name(stream_name));
    let sql = format!(
        "COMMENT ON TABLE {staging_table} IS '{}'",
        format!("{REPLACE_PREPARED_MARKER_PREFIX}{schema_signature}").replace('\'', "''")
    );
    client
        .execute(&sql, &[])
        .await
        .map_err(|e| format!("COMMENT ON staging table failed for {staging_table}: {e}"))?;
    Ok(())
}

/// Check whether a staging table has been marked as prepared.
pub(crate) async fn staging_prepared_signature(
    client: &Client,
    target_schema: &str,
    stream_name: &str,
) -> Result<Option<String>, String> {
    let staging_table = crate::decode::qualified_name(target_schema, &staging_name(stream_name));
    let row = client
        .query_one(
            "SELECT obj_description(to_regclass($1), 'pg_class')",
            &[&staging_table],
        )
        .await
        .map_err(|e| format!("querying staging marker failed for {staging_table}: {e}"))?;
    let comment: Option<String> = row.get(0);
    Ok(comment.and_then(|comment| {
        comment
            .strip_prefix(REPLACE_PREPARED_MARKER_PREFIX)
            .map(str::to_owned)
    }))
}
