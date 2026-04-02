use std::path::PathBuf;

use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct PipelineInfo {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineSourceError {
    #[error("pipeline not found: {0}")]
    NotFound(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("invalid pipeline YAML: {0}")]
    InvalidYaml(String),
}

#[async_trait]
pub trait PipelineSource: Send + Sync {
    /// List all discovered pipeline files in the project directory.
    async fn list(&self) -> Result<Vec<PipelineInfo>, PipelineSourceError>;

    /// Get a pipeline's raw YAML content by name.
    async fn get(&self, name: &str) -> Result<String, PipelineSourceError>;

    /// Read the project's connections.yml file, if it exists.
    async fn connections_yaml(&self) -> Result<Option<String>, PipelineSourceError>;
}
