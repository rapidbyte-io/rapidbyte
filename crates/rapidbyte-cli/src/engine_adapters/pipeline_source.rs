use std::path::{Path, PathBuf};

use async_trait::async_trait;
use rapidbyte_controller::domain::ports::pipeline_source::{
    PipelineInfo, PipelineSource, PipelineSourceError,
};

/// Filesystem-backed `PipelineSource`.
///
/// Scans the project directory for `*.yml` and `*.yaml` files and resolves
/// each file's pipeline name via `rapidbyte_pipeline_config::extract_pipeline_name`.
/// A `connections.yml` or `connections.yaml` file at the project root is
/// returned verbatim by [`FsPipelineSource::connections_yaml`] if present.
pub struct FsPipelineSource {
    project_dir: PathBuf,
}

impl FsPipelineSource {
    #[must_use]
    pub fn new(project_dir: PathBuf) -> Self {
        Self { project_dir }
    }

    /// Read a file from the project directory, returning a `PipelineSourceError::Io` on failure.
    async fn read_file(path: &Path) -> Result<String, PipelineSourceError> {
        tokio::fs::read_to_string(path)
            .await
            .map_err(|e| PipelineSourceError::Io(e.to_string()))
    }
}

#[async_trait]
impl PipelineSource for FsPipelineSource {
    async fn list(&self) -> Result<Vec<PipelineInfo>, PipelineSourceError> {
        let mut read_dir = tokio::fs::read_dir(&self.project_dir)
            .await
            .map_err(|e| PipelineSourceError::Io(e.to_string()))?;

        let mut pipelines = Vec::new();

        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| PipelineSourceError::Io(e.to_string()))?
        {
            let path = entry.path();
            let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
                continue;
            };
            if ext != "yml" && ext != "yaml" {
                continue;
            }

            // Skip connections files — they are not pipelines.
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            if stem == "connections" {
                continue;
            }

            let yaml = Self::read_file(&path).await?;
            match rapidbyte_pipeline_config::extract_pipeline_name(&yaml) {
                Ok(name) => pipelines.push(PipelineInfo { name, path }),
                Err(e) => {
                    // Skip invalid YAML files rather than failing the entire listing.
                    tracing::warn!(path = %path.display(), error = %e, "skipping invalid pipeline file");
                }
            }
        }

        Ok(pipelines)
    }

    async fn get(&self, name: &str) -> Result<String, PipelineSourceError> {
        let mut read_dir = tokio::fs::read_dir(&self.project_dir)
            .await
            .map_err(|e| PipelineSourceError::Io(e.to_string()))?;

        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| PipelineSourceError::Io(e.to_string()))?
        {
            let path = entry.path();
            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or_default();
            if ext != "yml" && ext != "yaml" {
                continue;
            }
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            if stem == "connections" {
                continue;
            }

            let yaml = Self::read_file(&path).await?;
            if let Ok(pipeline_name) = rapidbyte_pipeline_config::extract_pipeline_name(&yaml) {
                if pipeline_name == name {
                    return Ok(yaml);
                }
            }
        }

        Err(PipelineSourceError::NotFound(name.to_string()))
    }

    async fn connections_yaml(&self) -> Result<Option<String>, PipelineSourceError> {
        for filename in &["connections.yml", "connections.yaml"] {
            let path = self.project_dir.join(filename);
            match tokio::fs::read_to_string(&path).await {
                Ok(contents) => return Ok(Some(contents)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(PipelineSourceError::Io(e.to_string())),
            }
        }
        Ok(None)
    }
}
