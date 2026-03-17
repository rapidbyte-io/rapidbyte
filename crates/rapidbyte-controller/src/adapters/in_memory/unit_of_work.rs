//! In-memory transactional unit-of-work adapter.

use std::sync::Arc;

use crate::adapters::in_memory::repos::InMemoryRunRepository;
use crate::ports::unit_of_work::UnitOfWork;

#[derive(Debug, Clone)]
pub struct InMemoryUnitOfWork {
    run_repository: Arc<InMemoryRunRepository>,
}

impl InMemoryUnitOfWork {
    #[must_use]
    pub fn new(run_repository: Arc<InMemoryRunRepository>) -> Self {
        Self { run_repository }
    }
}

#[async_trait::async_trait]
impl UnitOfWork for InMemoryUnitOfWork {
    async fn in_transaction<T, F, Fut>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = anyhow::Result<T>> + Send,
        T: Send,
    {
        let snapshot = self.run_repository.snapshot();
        match f().await {
            Ok(value) => Ok(value),
            Err(error) => {
                self.run_repository.restore(snapshot);
                Err(error)
            }
        }
    }
}
