use async_trait::async_trait;

use crate::traits::server::{HealthStatus, ServerConfigInfo, ServerService, VersionInfo};
use crate::traits::ServiceError;

use super::AppServices;

#[async_trait]
impl ServerService for AppServices {
    async fn health(&self) -> Result<HealthStatus, ServiceError> {
        let now = self.ctx.clock.now();
        let uptime_secs = now
            .signed_duration_since(self.started_at)
            .num_seconds()
            .max(0) as u64;

        Ok(HealthStatus {
            status: "healthy".to_string(),
            mode: "controller".to_string(),
            uptime_secs,
            state_backend: "postgres".to_string(),
            state_backend_healthy: true,
            agents_connected: 0,
        })
    }

    async fn version(&self) -> Result<VersionInfo, ServiceError> {
        Ok(VersionInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            mode: "controller".to_string(),
        })
    }

    async fn config(&self) -> Result<ServerConfigInfo, ServiceError> {
        Ok(ServerConfigInfo {
            mode: "controller".to_string(),
            port: self.listen_addr.port(),
            state_backend: "postgres".to_string(),
            auth_required: !self.ctx.config.allow_unauthenticated,
        })
    }
}
