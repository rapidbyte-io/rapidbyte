/// Deployment mode for the API server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    /// Local single-process mode (CLI-embedded server).
    Local,
}

/// Dependency-injection container for API services.
#[derive(Debug, Clone)]
pub struct ApiContext {
    /// Current deployment mode.
    pub mode: DeploymentMode,
}
