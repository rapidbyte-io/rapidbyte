//! Adapter layer: concrete implementations of domain ports.

pub mod channel_progress;

pub use channel_progress::AtomicProgressCollector;

// Forward declaration — full implementation in Task 11.
use std::sync::Arc;

/// Concrete adapter handles returned alongside the context.
///
/// Provides access to concrete adapter types that have methods beyond their
/// port trait interfaces (e.g., `EngineExecutorStub::update_registry_config`).
pub struct AgentAdapters {
    /// Engine executor adapter (concrete type for registry config updates).
    pub engine_executor: Arc<EngineExecutorStub>,
    /// Progress collector adapter (concrete type for write-side updates).
    pub progress_collector: Arc<AtomicProgressCollector>,
}

/// Stub for `EngineExecutor` — replaced with full implementation in Task 11.
pub struct EngineExecutorStub;

impl EngineExecutorStub {
    /// Update the registry configuration received from the controller.
    #[allow(clippy::unused_async)] // Stub — real implementation will be async.
    pub async fn update_registry_config(&self, _config: rapidbyte_registry::RegistryConfig) {
        // Stub — real implementation stores config in RwLock<RegistryConfig>.
    }
}
