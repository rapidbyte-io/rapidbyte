use async_trait::async_trait;
use sqlx::PgPool;

use crate::domain::ports::log_store::{
    LogError, LogFilter, LogStore, LogStreamFilter, StoredLogEntry,
};
use crate::traits::{EventStream, PaginatedList};

pub struct PgLogStore {
    // Retained for future LISTEN/NOTIFY and query implementations.
    #[allow(dead_code)]
    pool: PgPool,
}

impl PgLogStore {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl LogStore for PgLogStore {
    async fn query(&self, _filter: LogFilter) -> Result<PaginatedList<StoredLogEntry>, LogError> {
        // Log ingestion is not yet implemented; return an empty page.
        Ok(PaginatedList {
            items: vec![],
            next_cursor: None,
        })
    }

    async fn subscribe(
        &self,
        _filter: LogStreamFilter,
    ) -> Result<EventStream<StoredLogEntry>, LogError> {
        // Real-time log streaming via LISTEN/NOTIFY is not yet implemented.
        let stream = futures::stream::empty();
        Ok(Box::pin(stream))
    }
}
