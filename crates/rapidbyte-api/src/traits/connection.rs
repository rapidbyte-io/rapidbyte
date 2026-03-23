use async_trait::async_trait;

use crate::error::ApiError;
use crate::types::{
    ConnectionDetail, ConnectionSummary, ConnectionTestResult, DiscoverRequest, DiscoverResult,
};

type Result<T> = std::result::Result<T, ApiError>;

/// Driving port for connection inspection and discovery.
#[async_trait]
pub trait ConnectionService: Send + Sync {
    async fn list(&self) -> Result<Vec<ConnectionSummary>>;
    async fn get(&self, name: &str) -> Result<ConnectionDetail>;
    async fn test(&self, name: &str) -> Result<ConnectionTestResult>;
    async fn discover(&self, request: DiscoverRequest) -> Result<DiscoverResult>;
}
