//! Postgres-backed transaction boundary.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::ports::unit_of_work::UnitOfWork;

#[derive(Clone)]
pub struct PostgresUnitOfWork {
    client: Arc<Mutex<tokio_postgres::Client>>,
    transaction_gate: Arc<Mutex<()>>,
}

impl PostgresUnitOfWork {
    #[must_use]
    pub fn new(client: Arc<Mutex<tokio_postgres::Client>>) -> Self {
        Self {
            client,
            transaction_gate: Arc::new(Mutex::new(())),
        }
    }
}

#[async_trait::async_trait]
impl UnitOfWork for PostgresUnitOfWork {
    async fn in_transaction<T, F, Fut>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = anyhow::Result<T>> + Send,
        T: Send,
    {
        let gate_guard = self.transaction_gate.lock().await;
        {
            let client = self.client.lock().await;
            client.batch_execute("BEGIN").await?;
        }

        let outcome = f().await;

        {
            let client = self.client.lock().await;
            if outcome.is_ok() {
                client.batch_execute("COMMIT").await?;
            } else {
                client.batch_execute("ROLLBACK").await?;
            }
        }

        drop(gate_guard);
        outcome
    }
}
