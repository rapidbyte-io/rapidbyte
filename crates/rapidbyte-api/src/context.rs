use std::sync::Arc;

use crate::traits::{
    ConnectionService, OperationsService, PipelineService, PluginService, RunService, ServerService,
};

/// Deployment mode for the API server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploymentMode {
    /// Local single-process mode (CLI-embedded server).
    Local,
    /// Distributed mode — delegates to a remote controller.
    Distributed {
        /// URL of the remote controller.
        controller_url: String,
    },
}

/// Dependency-injection container for API services.
///
/// Each field holds an `Arc<dyn Trait>` so the context is cheaply
/// cloneable and can be shared across request handlers.
pub struct ApiContext {
    pub pipelines: Arc<dyn PipelineService>,
    pub runs: Arc<dyn RunService>,
    pub connections: Arc<dyn ConnectionService>,
    pub plugins: Arc<dyn PluginService>,
    pub operations: Arc<dyn OperationsService>,
    pub server: Arc<dyn ServerService>,
}
