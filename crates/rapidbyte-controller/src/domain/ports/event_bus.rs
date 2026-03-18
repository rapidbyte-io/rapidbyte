use std::pin::Pin;

use async_trait::async_trait;
use tokio_stream::Stream;

use crate::domain::event::DomainEvent;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct EventBusError(pub Box<dyn std::error::Error + Send + Sync>);

pub type EventStream = Pin<Box<dyn Stream<Item = DomainEvent> + Send>>;

#[async_trait]
pub trait EventBus: Send + Sync {
    async fn publish(&self, event: DomainEvent) -> Result<(), EventBusError>;
    async fn subscribe(&self, run_id: &str) -> Result<EventStream, EventBusError>;
    /// Remove the broadcast entry for a `run_id` if no active receivers remain.
    async fn cleanup(&self, run_id: &str);
}
