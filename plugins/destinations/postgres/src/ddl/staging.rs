//! Replace-mode staging table lifecycle helpers.

use pg_escape::quote_identifier;
use tokio_postgres::Client;

use rapidbyte_sdk::prelude::*;
use serde::{Deserialize, Serialize};

pub(crate) const CONTRACT_HANDOFF_PREFIX: &str = "rapidbyte:dest-postgres:handoff:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContractHandoff {
    pub schema_signature: String,
    pub ignored_columns: Vec<String>,
    pub type_null_columns: Vec<String>,
}

impl ContractHandoff {
    pub(crate) fn from_contract(
        contract: &crate::contract::WriteContract,
        schema_signature: Option<String>,
    ) -> Option<Self> {
        let schema_signature = schema_signature?;
        Some(Self {
            schema_signature,
            ignored_columns: contract.ignored_columns.iter().cloned().collect(),
            type_null_columns: contract.type_null_columns.iter().cloned().collect(),
        })
    }
}

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

fn drop_target_table_sql(target_schema: &str, stream_name: &str) -> String {
    let target_table = crate::decode::qualified_name(target_schema, stream_name);
    format!("DROP TABLE IF EXISTS {target_table}")
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

    let drop_sql = drop_target_table_sql(target_schema, stream_name);
    if let Err(e) = client.execute(&drop_sql, &[]).await {
        let _ = client.execute("ROLLBACK", &[]).await;
        if e.as_db_error().is_some_and(|db| db.code().code() == "2BP01") {
            return Err(format!(
                "Swap DROP blocked for {target_table}: dependent objects still reference the target table. Replace mode requires a connector-owned table or those dependents to be removed first."
            ));
        }
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
pub(crate) async fn write_contract_handoff(
    client: &Client,
    qualified_table: &str,
    handoff: &ContractHandoff,
) -> Result<(), String> {
    let payload = serde_json::to_string(handoff)
        .map_err(|e| format!("serializing contract handoff failed: {e}"))?;
    let sql = format!(
        "COMMENT ON TABLE {qualified_table} IS '{}'",
        format!("{CONTRACT_HANDOFF_PREFIX}{payload}").replace('\'', "''")
    );
    client
        .execute(&sql, &[])
        .await
        .map_err(|e| format!("COMMENT ON table failed for {qualified_table}: {e}"))?;
    Ok(())
}

/// Read the persisted contract handoff for a table or staging table.
pub(crate) async fn read_contract_handoff(
    client: &Client,
    qualified_table: &str,
) -> Result<Option<ContractHandoff>, String> {
    let row = client
        .query_one(
            "SELECT obj_description(to_regclass($1), 'pg_class')",
            &[&qualified_table],
        )
        .await
        .map_err(|e| format!("querying contract handoff failed for {qualified_table}: {e}"))?;
    let comment: Option<String> = row.get(0);
    let Some(comment) = comment else {
        return Ok(None);
    };

    let Some(payload) = comment.strip_prefix(CONTRACT_HANDOFF_PREFIX) else {
        return Ok(None);
    };

    let handoff = serde_json::from_str(payload)
        .map_err(|e| format!("parsing contract handoff failed for {qualified_table}: {e}"))?;
    Ok(Some(handoff))
}

#[cfg(test)]
mod tests {
    use super::drop_target_table_sql;

    #[test]
    fn drop_target_table_sql_does_not_use_cascade() {
        let sql = drop_target_table_sql("public", "users");
        assert_eq!(sql, "DROP TABLE IF EXISTS public.users");
        assert!(!sql.contains("CASCADE"));
    }
}
