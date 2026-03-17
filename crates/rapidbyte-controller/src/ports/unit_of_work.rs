//! Transaction boundary abstraction.

#[async_trait::async_trait]
pub trait UnitOfWork: Send + Sync {
    /// Execute work inside one transaction boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction cannot complete.
    async fn in_transaction<T, F, Fut>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = anyhow::Result<T>> + Send,
        T: Send;
}
