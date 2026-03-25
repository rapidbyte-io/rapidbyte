pub mod server;

use std::sync::Arc;

use crate::application::context::AppContext;

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
