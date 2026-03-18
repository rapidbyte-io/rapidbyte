use crate::domain::error::DomainError;
use crate::domain::ports::event_bus::EventBusError;
use crate::domain::ports::repository::RepositoryError;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: String },
    #[error("run already exists: {run_id}")]
    AlreadyExists { run_id: String },
    #[error("repository error: {0}")]
    Repository(#[from] RepositoryError),
    #[error("event bus error: {0}")]
    EventBus(#[from] EventBusError),
    #[error("secret resolution failed: {0}")]
    SecretResolution(String),
}
