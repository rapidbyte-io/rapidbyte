use std::time::Instant;

use async_trait::async_trait;

use crate::error::ApiError;
use crate::traits::ServerService;
use crate::types::{HealthStatus, ServerConfig, VersionInfo};

type Result<T> = std::result::Result<T, ApiError>;

/// Local-mode implementation of [`ServerService`].
///
/// Returns static health, version, and configuration data for a
/// single-process server (no distributed controller).
pub struct LocalServerService {
    started_at: Instant,
    port: u16,
    auth_required: bool,
}

impl LocalServerService {
    #[must_use]
    pub fn new(started_at: Instant, port: u16, auth_required: bool) -> Self {
        Self {
            started_at,
            port,
            auth_required,
        }
    }
}

#[async_trait]
impl ServerService for LocalServerService {
    async fn health(&self) -> Result<HealthStatus> {
        Ok(HealthStatus {
            status: "healthy".into(),
            mode: "serve".into(),
            uptime_secs: self.started_at.elapsed().as_secs(),
            state_backend: "sqlite".into(),
            state_backend_healthy: true,
            agents_connected: 0,
        })
    }

    async fn version(&self) -> Result<VersionInfo> {
        Ok(VersionInfo {
            version: env!("CARGO_PKG_VERSION").into(),
            rustc: None,
            wasmtime: None,
            mode: "serve".into(),
            plugins: vec![],
        })
    }

    async fn config(&self) -> Result<ServerConfig> {
        Ok(ServerConfig {
            mode: "serve".into(),
            port: self.port,
            metrics_port: None,
            state_backend: "sqlite".into(),
            registry_url: None,
            trust_policy: None,
            auth_required: self.auth_required,
            pipelines_discovered: 0,
            scheduler_active: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_returns_healthy() {
        let svc = LocalServerService::new(Instant::now(), 8080, false);
        let health = svc.health().await.unwrap();
        assert_eq!(health.status, "healthy");
        assert_eq!(health.mode, "serve");
    }

    #[tokio::test]
    async fn health_uptime_is_non_negative() {
        let svc = LocalServerService::new(Instant::now(), 8080, false);
        let health = svc.health().await.unwrap();
        // uptime_secs is u64 so always >= 0, but verify it returns without error
        assert!(health.uptime_secs < 5);
    }

    #[tokio::test]
    async fn version_returns_cargo_version() {
        let svc = LocalServerService::new(Instant::now(), 8080, false);
        let ver = svc.version().await.unwrap();
        assert_eq!(ver.version, env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn version_mode_is_serve() {
        let svc = LocalServerService::new(Instant::now(), 8080, false);
        let ver = svc.version().await.unwrap();
        assert_eq!(ver.mode, "serve");
        assert!(ver.plugins.is_empty());
    }

    #[tokio::test]
    async fn config_returns_defaults() {
        let svc = LocalServerService::new(Instant::now(), 8080, false);
        let cfg = svc.config().await.unwrap();
        assert_eq!(cfg.mode, "serve");
        assert_eq!(cfg.port, 8080);
        assert_eq!(cfg.state_backend, "sqlite");
        assert!(!cfg.auth_required);
        assert!(!cfg.scheduler_active);
        assert_eq!(cfg.pipelines_discovered, 0);
    }

    #[tokio::test]
    async fn config_reflects_actual_port_and_auth() {
        let svc = LocalServerService::new(Instant::now(), 9090, true);
        let cfg = svc.config().await.unwrap();
        assert_eq!(cfg.port, 9090);
        assert!(cfg.auth_required);
    }
}
