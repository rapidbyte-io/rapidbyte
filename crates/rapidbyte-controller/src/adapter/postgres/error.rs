use crate::domain::ports::repository::RepositoryError;

pub fn box_err(e: impl std::error::Error + Send + Sync + 'static) -> RepositoryError {
    RepositoryError(Box::new(e))
}
