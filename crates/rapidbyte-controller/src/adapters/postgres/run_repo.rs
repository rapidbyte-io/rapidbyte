//! Postgres run repository adapter for v2 control-plane runs.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

use crate::domain::run::{Run, RunState};
use crate::ports::repositories::{RunRepository, StoredRun};

pub const CONTROLLER_V2_MIGRATIONS: &str = include_str!("migrations/0002_controller_v2.sql");

#[derive(Clone)]
pub struct PostgresRunRepository {
    client: Arc<Mutex<tokio_postgres::Client>>,
}

impl PostgresRunRepository {
    /// Connect to Postgres and apply V2 run migrations.
    ///
    /// # Errors
    ///
    /// Returns an error if connection or migration fails.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(?error, "controller v2 postgres connection failed");
            }
        });

        client.batch_execute(CONTROLLER_V2_MIGRATIONS).await?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    #[must_use]
    pub fn shared_client(&self) -> Arc<Mutex<tokio_postgres::Client>> {
        Arc::clone(&self.client)
    }

    fn row_to_stored_run(row: &tokio_postgres::Row) -> anyhow::Result<StoredRun> {
        let state = parse_state(row.get::<_, String>("state").as_str())?;
        Ok(StoredRun {
            run: Run {
                id: crate::domain::run::RunId::new(row.get::<_, String>("run_id")),
                state,
            },
            retryable: row.get("retryable"),
        })
    }
}

#[async_trait]
impl RunRepository for PostgresRunRepository {
    async fn create_or_get_by_idempotency(
        &self,
        run: Run,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<StoredRun> {
        let client = self.client.lock().await;

        if let Some(key) = idempotency_key {
            if let Some(row) = client
                .query_opt(
                    "SELECT run_id, state, retryable FROM controller_v2_runs WHERE idempotency_key = $1",
                    &[&key],
                )
                .await?
            {
                return Self::row_to_stored_run(&row);
            }
        }

        let run_id = run.id.as_str().to_owned();
        let state = format_state(run.state);
        let row = client
            .query_one(
                "INSERT INTO controller_v2_runs (run_id, state, retryable, idempotency_key)
                 VALUES ($1, $2, FALSE, $3)
                 RETURNING run_id, state, retryable",
                &[&run_id, &state, &idempotency_key],
            )
            .await?;
        Self::row_to_stored_run(&row)
    }

    async fn get(&self, run_id: &crate::domain::run::RunId) -> anyhow::Result<Option<StoredRun>> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT run_id, state, retryable FROM controller_v2_runs WHERE run_id = $1",
                &[&run_id.as_str()],
            )
            .await?;
        row.map(|r| Self::row_to_stored_run(&r)).transpose()
    }

    async fn queue_retry(&self, run_id: &crate::domain::run::RunId) -> anyhow::Result<()> {
        let client = self.client.lock().await;
        let state = format_state(RunState::Queued);
        let updated = client
            .execute(
                "UPDATE controller_v2_runs
                 SET state = $2, updated_at = NOW()
                 WHERE run_id = $1",
                &[&run_id.as_str(), &state],
            )
            .await?;
        if updated == 0 {
            anyhow::bail!("run not found: {}", run_id.as_str());
        }
        Ok(())
    }

    async fn list(&self, limit: usize) -> anyhow::Result<Vec<StoredRun>> {
        let limit_i64 = i64::try_from(limit)?;
        let client = self.client.lock().await;
        let rows = client
            .query(
                "SELECT run_id, state, retryable
                 FROM controller_v2_runs
                 ORDER BY created_at DESC
                 LIMIT $1",
                &[&limit_i64],
            )
            .await?;

        rows.iter().map(Self::row_to_stored_run).collect()
    }

    async fn mark_cancelled(&self, run_id: &crate::domain::run::RunId) -> anyhow::Result<()> {
        let client = self.client.lock().await;
        let state = format_state(RunState::Cancelled);
        let updated = client
            .execute(
                "UPDATE controller_v2_runs
                 SET state = $2, updated_at = NOW()
                 WHERE run_id = $1",
                &[&run_id.as_str(), &state],
            )
            .await?;
        if updated == 0 {
            anyhow::bail!("run not found: {}", run_id.as_str());
        }
        Ok(())
    }
}

fn format_state(state: RunState) -> &'static str {
    match state {
        RunState::Accepted => "accepted",
        RunState::Queued => "queued",
        RunState::Dispatched => "dispatched",
        RunState::Executing => "executing",
        RunState::PreviewReady => "preview_ready",
        RunState::FailedRetryable => "failed_retryable",
        RunState::FailedTerminal => "failed_terminal",
        RunState::CancelRequested => "cancel_requested",
        RunState::Cancelled => "cancelled",
        RunState::Reconciliation => "reconciliation",
        RunState::Succeeded => "succeeded",
    }
}

fn parse_state(value: &str) -> anyhow::Result<RunState> {
    match value {
        "accepted" => Ok(RunState::Accepted),
        "queued" => Ok(RunState::Queued),
        "dispatched" => Ok(RunState::Dispatched),
        "executing" => Ok(RunState::Executing),
        "preview_ready" => Ok(RunState::PreviewReady),
        "failed_retryable" => Ok(RunState::FailedRetryable),
        "failed_terminal" => Ok(RunState::FailedTerminal),
        "cancel_requested" => Ok(RunState::CancelRequested),
        "cancelled" => Ok(RunState::Cancelled),
        "reconciliation" => Ok(RunState::Reconciliation),
        "succeeded" => Ok(RunState::Succeeded),
        _ => anyhow::bail!("unknown run state: {value}"),
    }
}
