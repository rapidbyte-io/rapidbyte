pub mod connection;
pub mod operations;
pub mod pipeline;
pub mod plugin;
pub mod run;
pub mod server;

use std::sync::Arc;

use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::traits::ServiceError;

/// Holds all driving-port service trait implementations.
/// Both REST and gRPC adapters share this struct.
pub struct AppServices {
    pub(crate) ctx: Arc<AppContext>,
    pub(crate) started_at: chrono::DateTime<chrono::Utc>,
    pub(crate) listen_addr: std::net::SocketAddr,
}

impl AppServices {
    /// Create a new `AppServices` instance.
    #[must_use]
    pub fn new(
        ctx: Arc<AppContext>,
        started_at: chrono::DateTime<chrono::Utc>,
        listen_addr: std::net::SocketAddr,
    ) -> Self {
        Self {
            ctx,
            started_at,
            listen_addr,
        }
    }
}

/// Map an [`AppError`] to a [`ServiceError`] for use by service implementations.
pub(crate) fn app_error_to_service(err: AppError) -> ServiceError {
    match err {
        AppError::NotFound { entity, id } => ServiceError::NotFound {
            resource: entity.to_lowercase().to_string(),
            id,
        },
        AppError::AlreadyExists { run_id } => ServiceError::Conflict {
            message: format!("run already exists: {run_id}"),
        },
        AppError::Domain(e) => ServiceError::Conflict {
            message: e.to_string(),
        },
        _ => ServiceError::Internal {
            message: err.to_string(),
        },
    }
}
