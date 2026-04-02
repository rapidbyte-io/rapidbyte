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
        let mut seen_names = std::collections::HashSet::new();

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

            // Skip non-files (directories, symlinks, etc.)
            let is_file = entry
                .file_type()
                .await
                .map(|ft| ft.is_file())
                .unwrap_or(false);
            if !is_file {
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

            let yaml = match Self::read_file(&path).await {
                Ok(y) => y,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "skipping unreadable pipeline file");
                    continue;
                }
            };
            match rapidbyte_pipeline_config::extract_pipeline_name(&yaml) {
                Ok(name) if name != "unknown" && !name.is_empty() => {
                    if seen_names.contains(&name) {
                        tracing::warn!(path = %path.display(), name = %name, "skipping duplicate pipeline name");
                    } else {
                        seen_names.insert(name.clone());
                        pipelines.push(PipelineInfo { name, path });
                    }
                }
                Ok(name) => {
                    tracing::warn!(path = %path.display(), name = %name, "skipping pipeline with invalid name");
                }
                Err(e) => {
                    // Skip invalid YAML files rather than failing the entire listing.
                    tracing::warn!(path = %path.display(), error = %e, "skipping invalid pipeline file");
                }
            }
        }

        // Sort by path for deterministic ordering across runs.
        pipelines.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(pipelines)
    }

    async fn get(&self, name: &str) -> Result<String, PipelineSourceError> {
        let mut read_dir = tokio::fs::read_dir(&self.project_dir)
            .await
            .map_err(|e| PipelineSourceError::Io(e.to_string()))?;

        let mut matches: Vec<(std::path::PathBuf, String)> = Vec::new();

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
            let is_file = entry
                .file_type()
                .await
                .map(|ft| ft.is_file())
                .unwrap_or(false);
            if !is_file {
                continue;
            }
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            if stem == "connections" {
                continue;
            }

            let Ok(yaml) = Self::read_file(&path).await else {
                continue; // skip unreadable files
            };
            if let Ok(pipeline_name) = rapidbyte_pipeline_config::extract_pipeline_name(&yaml) {
                if pipeline_name != "unknown" && !pipeline_name.is_empty() && pipeline_name == name
                {
                    matches.push((path, yaml));
                }
            }
        }

        if matches.is_empty() {
            return Err(PipelineSourceError::NotFound(name.to_string()));
        }

        // Sort by path for deterministic selection, matching list() behavior.
        matches.sort_by(|(a, _), (b, _)| a.cmp(b));

        if matches.len() > 1 {
            let paths: Vec<String> = matches
                .iter()
                .map(|(p, _)| p.display().to_string())
                .collect();
            tracing::warn!(
                name = %name,
                files = %paths.join(", "),
                "duplicate pipeline name found; using first by path order"
            );
        }

        Ok(matches.into_iter().next().unwrap().1)
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
