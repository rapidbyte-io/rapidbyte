mod connectors;
mod container;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use rapidbyte_engine::config::parser;
use rapidbyte_engine::config::validator;
use rapidbyte_engine::execution::{ExecutionOptions, PipelineOutcome};
use tokio_postgres::NoTls;

static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct HarnessContext {
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_db: String,
    pub postgres_user: String,
    pub postgres_pass: String,
    pub connector_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SchemaPair {
    pub source_users_table: String,
    pub source_orders_table: String,
    pub destination_schema: String,
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub records_read: u64,
    pub records_written: u64,
}

pub async fn bootstrap() -> Result<HarnessContext> {
    let postgres_port = container::shared_postgres_port()?;
    let connector_dir = connectors::prepare_connector_dir()?;
    std::env::set_var("RAPIDBYTE_CONNECTOR_DIR", &connector_dir);

    Ok(HarnessContext {
        postgres_host: "127.0.0.1".to_string(),
        postgres_port,
        postgres_db: "postgres".to_string(),
        postgres_user: "postgres".to_string(),
        postgres_pass: "postgres".to_string(),
        connector_dir,
    })
}

impl HarnessContext {
    pub async fn allocate_schema_pair(&self, test_name: &str) -> Result<SchemaPair> {
        let schema_id = NEXT_SCHEMA_ID.fetch_add(1, Ordering::Relaxed);
        let source_users_table = format!("users_{}_{}", sanitize_identifier(test_name), schema_id);
        let source_orders_table =
            format!("orders_{}_{}", sanitize_identifier(test_name), schema_id);
        let destination_schema = format!("dst_{}_{}", sanitize_identifier(test_name), schema_id);

        let client = self.connect().await?;
        client
            .execute(&format!("CREATE SCHEMA \"{destination_schema}\""), &[])
            .await
            .with_context(|| format!("failed to create destination schema {destination_schema}"))?;

        Ok(SchemaPair {
            source_users_table,
            source_orders_table,
            destination_schema,
        })
    }

    pub async fn drop_schema_pair(&self, schemas: &SchemaPair) -> Result<()> {
        let client = self.connect().await?;
        client
            .execute(
                &format!(
                    "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                    schemas.destination_schema
                ),
                &[],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to drop destination schema {}",
                    schemas.destination_schema
                )
            })?;
        client
            .execute(
                &format!(
                    "DROP TABLE IF EXISTS public.\"{}\" CASCADE",
                    schemas.source_users_table
                ),
                &[],
            )
            .await
            .with_context(|| {
                format!("failed to drop source table {}", schemas.source_users_table)
            })?;
        client
            .execute(
                &format!(
                    "DROP TABLE IF EXISTS public.\"{}\" CASCADE",
                    schemas.source_orders_table
                ),
                &[],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to drop source table {}",
                    schemas.source_orders_table
                )
            })?;
        Ok(())
    }

    pub async fn seed_basic_source_data(&self, schemas: &SchemaPair) -> Result<()> {
        let client = self.connect().await?;
        client
            .batch_execute(&format!(
                "
                CREATE TABLE public.\"{users_table}\" (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT
                );

                INSERT INTO public.\"{users_table}\" (name, email)
                VALUES
                    ('Alice', 'alice@example.com'),
                    ('Bob', 'bob@example.com'),
                    ('Carol', 'carol@example.com');

                CREATE TABLE public.\"{orders_table}\" (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    amount_cents INTEGER NOT NULL,
                    status TEXT NOT NULL
                );

                INSERT INTO public.\"{orders_table}\" (user_id, amount_cents, status)
                VALUES
                    (1, 5000, 'completed'),
                    (2, 12000, 'pending'),
                    (1, 3500, 'completed');
                ",
                users_table = schemas.source_users_table,
                orders_table = schemas.source_orders_table,
            ))
            .await
            .context("failed to seed source schema")?;
        Ok(())
    }

    pub async fn run_full_refresh_pipeline(&self, schemas: &SchemaPair) -> Result<RunSummary> {
        let state_file =
            tempfile::NamedTempFile::new().context("failed to create state db file")?;
        self.run_pipeline(schemas, "full_refresh", "append", state_file.path())
            .await
    }

    pub async fn run_pipeline(
        &self,
        schemas: &SchemaPair,
        sync_mode: &str,
        write_mode: &str,
        state_db_path: &std::path::Path,
    ) -> Result<RunSummary> {
        self.run_pipeline_with_compression(schemas, sync_mode, write_mode, state_db_path, None)
            .await
    }

    pub async fn run_pipeline_with_compression(
        &self,
        schemas: &SchemaPair,
        sync_mode: &str,
        write_mode: &str,
        state_db_path: &std::path::Path,
        compression: Option<&str>,
    ) -> Result<RunSummary> {
        self.run_pipeline_with_policies(
            schemas,
            sync_mode,
            write_mode,
            state_db_path,
            compression,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run_pipeline_with_policies(
        &self,
        schemas: &SchemaPair,
        sync_mode: &str,
        write_mode: &str,
        state_db_path: &std::path::Path,
        compression: Option<&str>,
        on_data_error: Option<&str>,
        schema_evolution_block: Option<&str>,
    ) -> Result<RunSummary> {
        let pipeline_yaml = render_pipeline_yaml(
            self,
            schemas,
            sync_mode,
            write_mode,
            state_db_path,
            compression,
            on_data_error,
            schema_evolution_block,
        );

        let config =
            parser::parse_pipeline_str(&pipeline_yaml).context("failed to parse pipeline")?;
        validator::validate_pipeline(&config).context("failed to validate pipeline")?;

        let outcome =
            rapidbyte_engine::orchestrator::run_pipeline(&config, &ExecutionOptions::default())
                .await
                .context("pipeline execution failed")?;

        let run = match outcome {
            PipelineOutcome::Run(run) => run,
            PipelineOutcome::DryRun(_) => anyhow::bail!("expected run outcome for e2e test"),
        };

        Ok(RunSummary {
            records_read: run.counts.records_read,
            records_written: run.counts.records_written,
        })
    }

    pub async fn insert_source_user(
        &self,
        schemas: &SchemaPair,
        name: &str,
        email: &str,
    ) -> Result<()> {
        let client = self.connect().await?;
        client
            .execute(
                &format!(
                    "INSERT INTO public.\"{}\" (name, email) VALUES ($1, $2)",
                    schemas.source_users_table
                ),
                &[&name, &email],
            )
            .await
            .context("failed to insert source user")?;
        Ok(())
    }

    pub async fn add_source_user_column(
        &self,
        schemas: &SchemaPair,
        column_name: &str,
    ) -> Result<()> {
        let client = self.connect().await?;
        client
            .execute(
                &format!(
                    "ALTER TABLE public.\"{}\" ADD COLUMN \"{}\" TEXT",
                    schemas.source_users_table, column_name
                ),
                &[],
            )
            .await
            .context("failed to add source user column")?;
        Ok(())
    }

    pub async fn run_transform_pipeline(
        &self,
        schemas: &SchemaPair,
        query: &str,
        state_db_path: &std::path::Path,
    ) -> Result<RunSummary> {
        let pipeline_yaml = render_transform_yaml(self, schemas, query, state_db_path);

        let config =
            parser::parse_pipeline_str(&pipeline_yaml).context("failed to parse pipeline")?;
        validator::validate_pipeline(&config).context("failed to validate pipeline")?;

        let outcome =
            rapidbyte_engine::orchestrator::run_pipeline(&config, &ExecutionOptions::default())
                .await
                .context("pipeline execution failed")?;

        let run = match outcome {
            PipelineOutcome::Run(run) => run,
            PipelineOutcome::DryRun(_) => anyhow::bail!("expected run outcome for e2e test"),
        };

        Ok(RunSummary {
            records_read: run.counts.records_read,
            records_written: run.counts.records_written,
        })
    }

    pub async fn table_rows_snapshot(
        &self,
        schema: &str,
        table: &str,
        columns: &[&str],
        order_by: &str,
    ) -> Result<String> {
        let select = columns
            .iter()
            .map(|col| format!("COALESCE(\"{col}\"::text, 'NULL')"))
            .collect::<Vec<_>>()
            .join(", ");
        let query =
            format!("SELECT {select} FROM \"{schema}\".\"{table}\" ORDER BY \"{order_by}\"");

        let client = self.connect().await?;
        let rows = client
            .query(&query, &[])
            .await
            .with_context(|| format!("failed snapshot query for {schema}.{table}"))?;

        let mut lines = Vec::with_capacity(rows.len());
        for row in rows {
            let mut parts = Vec::with_capacity(columns.len());
            for idx in 0..columns.len() {
                let val: String = row.get(idx);
                parts.push(val);
            }
            lines.push(parts.join("|"));
        }

        Ok(lines.join("\n"))
    }

    pub async fn run_cdc_pipeline(
        &self,
        schemas: &SchemaPair,
        state_db_path: &std::path::Path,
    ) -> Result<RunSummary> {
        let slot = format!("rapidbyte_{}", schemas.source_users_table);
        let publication = format!("rapidbyte_{}", schemas.source_users_table);
        let client = self.connect().await?;

        client
            .batch_execute(&format!(
                "
                DROP TABLE IF EXISTS public.\"{table}\" CASCADE;
                CREATE TABLE public.\"{table}\" (
                    id INT PRIMARY KEY,
                    name TEXT NOT NULL
                );
                DROP PUBLICATION IF EXISTS \"{publication}\";
                CREATE PUBLICATION \"{publication}\" FOR TABLE public.\"{table}\";
                ",
                table = schemas.source_users_table,
            ))
            .await
            .context("failed to prepare cdc source table/publication")?;

        let _ = client
            .query("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await;
        client
            .query_one(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await
            .context("failed to create replication slot")?;

        client
            .batch_execute(&format!(
                "
                INSERT INTO public.\"{table}\" (id, name) VALUES (1, 'Alice'), (2, 'Bob');
                UPDATE public.\"{table}\" SET name = 'Bobby' WHERE id = 2;
                DELETE FROM public.\"{table}\" WHERE id = 1;
                ",
                table = schemas.source_users_table,
            ))
            .await
            .context("failed to seed cdc change stream")?;

        let pipeline_yaml = render_cdc_yaml(self, schemas, &slot, &publication, state_db_path);

        let config =
            parser::parse_pipeline_str(&pipeline_yaml).context("failed to parse pipeline")?;
        validator::validate_pipeline(&config).context("failed to validate pipeline")?;

        let outcome =
            rapidbyte_engine::orchestrator::run_pipeline(&config, &ExecutionOptions::default())
                .await
                .context("pipeline execution failed")?;

        let run = match outcome {
            PipelineOutcome::Run(run) => run,
            PipelineOutcome::DryRun(_) => anyhow::bail!("expected run outcome for e2e test"),
        };

        let _ = client
            .query("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await;
        client
            .execute(
                &format!("DROP PUBLICATION IF EXISTS \"{publication}\""),
                &[],
            )
            .await
            .context("failed to drop publication")?;

        Ok(RunSummary {
            records_read: run.counts.records_read,
            records_written: run.counts.records_written,
        })
    }

    pub async fn table_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let client = self.connect().await?;
        let query = format!("SELECT COUNT(*) FROM \"{schema}\".\"{table}\"");
        let row = client
            .query_one(&query, &[])
            .await
            .with_context(|| format!("failed to count rows in {schema}.{table}"))?;
        Ok(row.get::<_, i64>(0))
    }

    async fn connect(&self) -> Result<tokio_postgres::Client> {
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            self.postgres_host,
            self.postgres_port,
            self.postgres_user,
            self.postgres_pass,
            self.postgres_db
        );

        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .context("failed to connect to postgres")?;

        tokio::spawn(async move {
            let _ = connection.await;
        });

        Ok(client)
    }
}

fn sanitize_identifier(input: &str) -> String {
    input
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn render_pipeline_yaml(
    context: &HarnessContext,
    schemas: &SchemaPair,
    sync_mode: &str,
    write_mode: &str,
    state_db_path: &std::path::Path,
    compression: Option<&str>,
    on_data_error: Option<&str>,
    schema_evolution_block: Option<&str>,
) -> String {
    let cursor_field = if sync_mode == "incremental" {
        "\n      cursor_field: id"
    } else {
        ""
    };
    let primary_key = if write_mode == "upsert" {
        "\n  primary_key: [id]"
    } else {
        ""
    };
    let resources = compression
        .map(|codec| format!("\nresources:\n  compression: {codec}\n"))
        .unwrap_or_default();
    let on_data_error = on_data_error
        .map(|value| format!("\n  on_data_error: {value}"))
        .unwrap_or_default();
    let schema_evolution = schema_evolution_block
        .map(|block| format!("\n  schema_evolution:\n{block}"))
        .unwrap_or_default();

    format!(
        r#"version: "1.0"
pipeline: e2e_full_refresh

source:
  use: source-postgres
  config:
    host: {source_host}
    port: {source_port}
    user: {source_user}
    password: {source_password}
    database: {source_database}
  streams:
    - name: {users_table}
      sync_mode: {sync_mode}{cursor_field}
    - name: {orders_table}
      sync_mode: {sync_mode}{cursor_field}

destination:
  use: dest-postgres
  config:
    host: {dest_host}
    port: {dest_port}
    user: {dest_user}
    password: {dest_password}
    database: {dest_database}
    schema: {dest_schema}
  write_mode: {write_mode}{primary_key}{on_data_error}{schema_evolution}

state:
  backend: sqlite
  connection: {state_db_path}
{resources}
"#,
        users_table = schemas.source_users_table,
        orders_table = schemas.source_orders_table,
        sync_mode = sync_mode,
        cursor_field = cursor_field,
        write_mode = write_mode,
        primary_key = primary_key,
        source_host = context.postgres_host,
        source_port = context.postgres_port,
        source_user = context.postgres_user,
        source_password = context.postgres_pass,
        source_database = context.postgres_db,
        dest_host = context.postgres_host,
        dest_port = context.postgres_port,
        dest_user = context.postgres_user,
        dest_password = context.postgres_pass,
        dest_database = context.postgres_db,
        dest_schema = schemas.destination_schema,
        state_db_path = state_db_path.display(),
        resources = resources,
        on_data_error = on_data_error,
        schema_evolution = schema_evolution,
    )
}

fn render_transform_yaml(
    context: &HarnessContext,
    schemas: &SchemaPair,
    query: &str,
    state_db_path: &std::path::Path,
) -> String {
    format!(
        r#"version: "1.0"
pipeline: e2e_transform

source:
  use: source-postgres
  config:
    host: {host}
    port: {port}
    user: {user}
    password: {password}
    database: {database}
  streams:
    - name: {users_table}
      sync_mode: full_refresh

transforms:
  - use: transform-sql
    config:
      query: >-
        {query}

destination:
  use: dest-postgres
  config:
    host: {host}
    port: {port}
    user: {user}
    password: {password}
    database: {database}
    schema: {dest_schema}
  write_mode: append

state:
  backend: sqlite
  connection: {state_db_path}
"#,
        host = context.postgres_host,
        port = context.postgres_port,
        user = context.postgres_user,
        password = context.postgres_pass,
        database = context.postgres_db,
        users_table = schemas.source_users_table,
        dest_schema = schemas.destination_schema,
        query = query,
        state_db_path = state_db_path.display(),
    )
}

fn render_cdc_yaml(
    context: &HarnessContext,
    schemas: &SchemaPair,
    slot: &str,
    publication: &str,
    state_db_path: &std::path::Path,
) -> String {
    format!(
        r#"version: "1.0"
pipeline: e2e_cdc

source:
  use: source-postgres
  config:
    host: {host}
    port: {port}
    user: {user}
    password: {password}
    database: {database}
    replication_slot: {slot}
    publication: {publication}
  streams:
    - name: {users_table}
      sync_mode: cdc

destination:
  use: dest-postgres
  config:
    host: {host}
    port: {port}
    user: {user}
    password: {password}
    database: {database}
    schema: {dest_schema}
  write_mode: append

state:
  backend: sqlite
  connection: {state_db_path}
"#,
        host = context.postgres_host,
        port = context.postgres_port,
        user = context.postgres_user,
        password = context.postgres_pass,
        database = context.postgres_db,
        slot = slot,
        publication = publication,
        users_table = schemas.source_users_table,
        dest_schema = schemas.destination_schema,
        state_db_path = state_db_path.display(),
    )
}
