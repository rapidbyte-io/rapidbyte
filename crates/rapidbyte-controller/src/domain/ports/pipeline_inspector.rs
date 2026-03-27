use async_trait::async_trait;
use serde::Serialize;

/// Output from a synchronous pipeline check operation.
#[derive(Debug, Clone, Serialize)]
pub struct CheckOutput {
    pub passed: bool,
    pub checks: serde_json::Value,
}

/// Output from a pipeline diff operation.
#[derive(Debug, Clone, Serialize)]
pub struct DiffOutput {
    pub streams: Vec<StreamDiffOutput>,
}

/// Per-stream diff output.
#[derive(Debug, Clone, Serialize)]
pub struct StreamDiffOutput {
    pub stream_name: String,
    pub changes: Vec<serde_json::Value>,
}

/// Errors that can occur during pipeline inspection.
#[derive(Debug, thiserror::Error)]
pub enum InspectorError {
    #[error("pipeline error: {0}")]
    Pipeline(String),
    #[error("plugin error: {0}")]
    Plugin(String),
}

/// Port for synchronous engine inspection operations (check, diff).
///
/// Implementations may delegate to the engine runtime or return stub results
/// when no engine context is available.
#[async_trait]
pub trait PipelineInspector: Send + Sync {
    /// Validate the pipeline config and test plugin connectivity.
    ///
    /// # Errors
    ///
    /// Returns [`InspectorError::Pipeline`] if the YAML is invalid, or
    /// [`InspectorError::Plugin`] if connectivity checks fail.
    async fn check(&self, pipeline_yaml: &str) -> Result<CheckOutput, InspectorError>;

    /// Detect schema drift since the last sync.
    ///
    /// # Errors
    ///
    /// Returns [`InspectorError::Pipeline`] if the YAML is invalid, or
    /// [`InspectorError::Plugin`] if schema introspection fails.
    async fn diff(&self, pipeline_yaml: &str) -> Result<DiffOutput, InspectorError>;
}
