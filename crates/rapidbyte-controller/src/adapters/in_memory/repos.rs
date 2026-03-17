//! In-memory repository adapters.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::domain::run::{Run, RunId, RunState};
use crate::ports::repositories::{RunRepository, StoredRun};

#[derive(Debug, Default)]
pub struct InMemoryRunRepository {
    runs: Mutex<HashMap<String, StoredRun>>,
    idempotency: Mutex<HashMap<String, String>>,
}

impl InMemoryRunRepository {
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn snapshot(&self) -> (HashMap<String, StoredRun>, HashMap<String, String>) {
        (
            self.runs.lock().expect("lock poisoned").clone(),
            self.idempotency.lock().expect("lock poisoned").clone(),
        )
    }

    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn restore(&self, snapshot: (HashMap<String, StoredRun>, HashMap<String, String>)) {
        let (runs, idempotency) = snapshot;
        *self.runs.lock().expect("lock poisoned") = runs;
        *self.idempotency.lock().expect("lock poisoned") = idempotency;
    }
}

#[async_trait]
impl RunRepository for InMemoryRunRepository {
    async fn create_or_get_by_idempotency(
        &self,
        run: Run,
        idempotency_key: Option<&str>,
    ) -> anyhow::Result<StoredRun> {
        if let Some(key) = idempotency_key {
            if let Some(run_id) = self.idempotency.lock().expect("lock poisoned").get(key) {
                return self
                    .runs
                    .lock()
                    .expect("lock poisoned")
                    .get(run_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("idempotency index points to missing run"));
            }
        }

        let run_id = run.id.as_str().to_owned();
        let stored = StoredRun {
            run,
            retryable: false,
        };
        self.runs
            .lock()
            .expect("lock poisoned")
            .insert(run_id.clone(), stored.clone());

        if let Some(key) = idempotency_key {
            self.idempotency
                .lock()
                .expect("lock poisoned")
                .insert(key.to_owned(), run_id);
        }
        Ok(stored)
    }

    async fn get(&self, run_id: &RunId) -> anyhow::Result<Option<StoredRun>> {
        Ok(self
            .runs
            .lock()
            .expect("lock poisoned")
            .get(run_id.as_str())
            .cloned())
    }

    async fn queue_retry(&self, run_id: &RunId) -> anyhow::Result<()> {
        let mut runs = self.runs.lock().expect("lock poisoned");
        let stored = runs
            .get_mut(run_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("run not found: {}", run_id.as_str()))?;
        if stored.run.state == RunState::FailedRetryable {
            stored.run.transition(RunState::Queued)?;
            return Ok(());
        }
        anyhow::bail!("run is not in retryable state")
    }

    async fn list(&self, limit: usize) -> anyhow::Result<Vec<StoredRun>> {
        let mut runs = self
            .runs
            .lock()
            .expect("lock poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        runs.sort_by(|a, b| a.run.id.as_str().cmp(b.run.id.as_str()));
        runs.truncate(limit);
        Ok(runs)
    }

    async fn mark_cancelled(&self, run_id: &RunId) -> anyhow::Result<()> {
        let mut runs = self.runs.lock().expect("lock poisoned");
        let stored = runs
            .get_mut(run_id.as_str())
            .ok_or_else(|| anyhow::anyhow!("run not found: {}", run_id.as_str()))?;
        if stored.run.state == RunState::Queued {
            stored.run.transition(RunState::CancelRequested)?;
        }
        if stored.run.state == RunState::CancelRequested {
            stored.run.transition(RunState::Cancelled)?;
            return Ok(());
        }
        anyhow::bail!("run cannot be cancelled from current state")
    }
}
