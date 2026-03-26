use async_trait::async_trait;

use crate::traits::connection::{
    ConnectionDetail, ConnectionDiscoverResponse, ConnectionService, ConnectionSummary,
    ConnectionTestResponse,
};
use crate::traits::ServiceError;

use super::AppServices;

#[async_trait]
impl ConnectionService for AppServices {
    async fn list(&self) -> Result<Vec<ConnectionSummary>, ServiceError> {
        // Pipeline-level connection discovery from project files is not yet
        // implemented in server mode.  Return an empty list for now.
        Ok(vec![])
    }

    async fn get(&self, name: &str) -> Result<ConnectionDetail, ServiceError> {
        // Connection config storage is not yet implemented in server mode.
        Err(ServiceError::NotFound {
            resource: "connection".into(),
            id: name.to_string(),
        })
    }

    async fn test(&self, name: &str) -> Result<ConnectionTestResponse, ServiceError> {
        // Connection config lookup is not yet implemented; the real implementation
        // will resolve the config then delegate to `ctx.connection_tester`.
        Err(ServiceError::NotFound {
            resource: "connection".into(),
            id: name.to_string(),
        })
    }

    async fn discover(
        &self,
        name: &str,
        _table: Option<&str>,
    ) -> Result<ConnectionDiscoverResponse, ServiceError> {
        // Connection config lookup is not yet implemented; the real implementation
        // will resolve the config then delegate to `ctx.connection_tester`.
        Err(ServiceError::NotFound {
            resource: "connection".into(),
            id: name.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::fake_app_services;

    #[tokio::test]
    async fn list_returns_empty() {
        let services = fake_app_services();
        let result = services.list().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn get_returns_not_found() {
        let services = fake_app_services();
        let result = services.get("nonexistent").await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_returns_not_found() {
        let services = fake_app_services();
        let result = services.test("nonexistent").await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn discover_returns_not_found() {
        let services = fake_app_services();
        let result = services.discover("nonexistent", None).await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }
}
