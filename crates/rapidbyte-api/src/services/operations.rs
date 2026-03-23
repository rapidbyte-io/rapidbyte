//! Local-mode implementation of [`OperationsService`].
//!
//! Provides pipeline status from the catalog and run manager.
//! Operational actions (pause, resume, reset, freshness, logs) are
//! stubs for Phase 1.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use rapidbyte_pipeline_config::PipelineConfig;

use crate::error::ApiError;
use crate::run_manager::RunManager;
use crate::traits::OperationsService;
use crate::types::{
    EventStream, FreshnessFilter, FreshnessStatus, LastRunInfo, LogsRequest, LogsResult,
    LogsStreamFilter, PipelineState, PipelineStatus, PipelineStatusDetail, ResetRequest,
    ResetResult, SseEvent,
};

type Result<T> = std::result::Result<T, ApiError>;

/// Local-mode operations service.
///
/// Builds pipeline status from the catalog and optional run history
/// from the [`RunManager`]. Operational mutations are stubs.
pub struct LocalOperationsService {
    catalog: Arc<HashMap<String, PipelineConfig>>,
    run_manager: Arc<RunManager>,
}

impl LocalOperationsService {
    /// Create a new `LocalOperationsService`.
    #[must_use]
    pub fn new(
        catalog: Arc<HashMap<String, PipelineConfig>>,
        run_manager: Arc<RunManager>,
    ) -> Self {
        Self {
            catalog,
            run_manager,
        }
    }

    /// Look up a pipeline config by name, returning `NotFound` if absent.
    fn lookup(&self, name: &str) -> Result<&PipelineConfig> {
        self.catalog.get(name).ok_or_else(|| {
            ApiError::not_found(
                "pipeline_not_found",
                format!("Pipeline '{name}' does not exist"),
            )
        })
    }

    /// Find the most recent run for a given pipeline name.
    fn last_run_for(&self, pipeline: &str) -> Option<LastRunInfo> {
        let mut runs: Vec<_> = self
            .run_manager
            .list_runs()
            .into_iter()
            .filter(|r| r.pipeline == pipeline)
            .collect();

        runs.sort_by(|a, b| b.started_at.cmp(&a.started_at));

        runs.first().map(|r| LastRunInfo {
            run_id: r.run_id.clone(),
            status: r.status,
            finished_at: r.finished_at,
        })
    }
}

#[async_trait]
impl OperationsService for LocalOperationsService {
    async fn status(&self) -> Result<Vec<PipelineStatus>> {
        let mut items: Vec<PipelineStatus> = self
            .catalog
            .keys()
            .map(|name| {
                let last_run = self.last_run_for(name);
                PipelineStatus {
                    pipeline: name.clone(),
                    schedule: None,
                    state: PipelineState::Active,
                    last_run,
                    next_run_in: None,
                    health: "healthy".into(),
                }
            })
            .collect();

        // Stable sort for deterministic output.
        items.sort_by(|a, b| a.pipeline.cmp(&b.pipeline));
        Ok(items)
    }

    async fn pipeline_status(&self, name: &str) -> Result<PipelineStatusDetail> {
        let _config = self.lookup(name)?;

        // Collect recent runs for this pipeline.
        let mut runs: Vec<_> = self
            .run_manager
            .list_runs()
            .into_iter()
            .filter(|r| r.pipeline == name)
            .collect();
        runs.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        runs.truncate(10);

        let recent_runs: Vec<serde_json::Value> = runs
            .iter()
            .map(|r| {
                serde_json::json!({
                    "run_id": r.run_id,
                    "status": r.status,
                    "started_at": r.started_at.to_rfc3339(),
                    "finished_at": r.finished_at.map(|f| f.to_rfc3339()),
                })
            })
            .collect();

        Ok(PipelineStatusDetail {
            pipeline: name.to_string(),
            schedule: None,
            state: PipelineState::Active,
            health: "healthy".into(),
            recent_runs,
            streams: vec![],
            dlq_rows: 0,
            assertions: None,
        })
    }

    async fn pause(&self, _name: &str) -> Result<PipelineState> {
        Err(ApiError::not_implemented("pipeline pause"))
    }

    async fn resume(&self, _name: &str) -> Result<PipelineState> {
        Err(ApiError::not_implemented("pipeline resume"))
    }

    async fn reset(&self, _request: ResetRequest) -> Result<ResetResult> {
        Err(ApiError::not_implemented("pipeline reset"))
    }

    async fn freshness(&self, _filter: FreshnessFilter) -> Result<Vec<FreshnessStatus>> {
        Err(ApiError::not_implemented("freshness check"))
    }

    async fn logs(&self, _request: LogsRequest) -> Result<LogsResult> {
        Err(ApiError::not_implemented("pipeline logs"))
    }

    async fn logs_stream(&self, _filter: LogsStreamFilter) -> Result<EventStream<SseEvent>> {
        Err(ApiError::not_implemented("logs streaming"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal `PipelineConfig` from YAML for testing.
    fn minimal_pipeline_config(name: &str) -> PipelineConfig {
        let yaml = format!(
            r#"
version: "1.0"
pipeline: {name}
source:
  use: postgres
  config: {{}}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: postgres
  config: {{}}
  write_mode: append
"#
        );
        serde_yaml::from_str(&yaml).expect("minimal pipeline YAML must parse")
    }

    fn empty_catalog() -> Arc<HashMap<String, PipelineConfig>> {
        Arc::new(HashMap::new())
    }

    fn test_catalog() -> Arc<HashMap<String, PipelineConfig>> {
        let mut map = HashMap::new();
        map.insert("alpha".into(), minimal_pipeline_config("alpha"));
        map.insert("beta".into(), minimal_pipeline_config("beta"));
        Arc::new(map)
    }

    fn make_service(
        catalog: Arc<HashMap<String, PipelineConfig>>,
        run_manager: Arc<RunManager>,
    ) -> LocalOperationsService {
        LocalOperationsService::new(catalog, run_manager)
    }

    // ----- status -----

    #[tokio::test]
    async fn status_returns_all_pipelines() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(test_catalog(), mgr);
        let result = svc.status().await.unwrap();
        assert_eq!(result.len(), 2);
        let names: Vec<&str> = result.iter().map(|s| s.pipeline.as_str()).collect();
        assert!(names.contains(&"alpha"));
        assert!(names.contains(&"beta"));
    }

    #[tokio::test]
    async fn status_empty_catalog() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(empty_catalog(), mgr);
        let result = svc.status().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn status_sorted_by_pipeline_name() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(test_catalog(), mgr);
        let result = svc.status().await.unwrap();
        let names: Vec<&str> = result.iter().map(|s| s.pipeline.as_str()).collect();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn status_all_active_and_healthy() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(test_catalog(), mgr);
        let result = svc.status().await.unwrap();
        for status in &result {
            assert_eq!(status.state, PipelineState::Active);
            assert_eq!(status.health, "healthy");
            assert!(status.schedule.is_none());
            assert!(status.next_run_in.is_none());
        }
    }

    #[tokio::test]
    async fn status_includes_last_run_when_available() {
        let mgr = Arc::new(RunManager::new());
        let _run_id = mgr.create_run("alpha".into());
        let svc = make_service(test_catalog(), Arc::clone(&mgr));
        let result = svc.status().await.unwrap();
        let alpha = result.iter().find(|s| s.pipeline == "alpha").unwrap();
        assert!(alpha.last_run.is_some());
        let beta = result.iter().find(|s| s.pipeline == "beta").unwrap();
        assert!(beta.last_run.is_none());
    }

    // ----- pipeline_status -----

    #[tokio::test]
    async fn pipeline_status_returns_detail() {
        let mgr = Arc::new(RunManager::new());
        let _run_id = mgr.create_run("alpha".into());
        let svc = make_service(test_catalog(), Arc::clone(&mgr));
        let detail = svc.pipeline_status("alpha").await.unwrap();
        assert_eq!(detail.pipeline, "alpha");
        assert_eq!(detail.state, PipelineState::Active);
        assert_eq!(detail.health, "healthy");
        assert_eq!(detail.recent_runs.len(), 1);
        assert_eq!(detail.dlq_rows, 0);
        assert!(detail.assertions.is_none());
        assert!(detail.streams.is_empty());
    }

    #[tokio::test]
    async fn pipeline_status_not_found() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(test_catalog(), mgr);
        let err = svc.pipeline_status("nonexistent").await.unwrap_err();
        assert!(matches!(err, ApiError::NotFound { .. }));
    }

    #[tokio::test]
    async fn pipeline_status_no_runs() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(test_catalog(), mgr);
        let detail = svc.pipeline_status("alpha").await.unwrap();
        assert!(detail.recent_runs.is_empty());
    }

    #[tokio::test]
    async fn pipeline_status_limits_recent_runs() {
        let mgr = Arc::new(RunManager::new());
        for _ in 0..15 {
            let _ = mgr.create_run("alpha".into());
        }
        let svc = make_service(test_catalog(), Arc::clone(&mgr));
        let detail = svc.pipeline_status("alpha").await.unwrap();
        // Should be capped at 10.
        assert_eq!(detail.recent_runs.len(), 10);
    }

    // ----- stub methods -----

    #[tokio::test]
    async fn pause_returns_not_implemented() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(empty_catalog(), mgr);
        let err = svc.pause("any").await.unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn resume_returns_not_implemented() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(empty_catalog(), mgr);
        let err = svc.resume("any").await.unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn reset_returns_not_implemented() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(empty_catalog(), mgr);
        let err = svc
            .reset(ResetRequest {
                pipeline: "any".into(),
                stream: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn freshness_returns_not_implemented() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(empty_catalog(), mgr);
        let err = svc.freshness(FreshnessFilter::default()).await.unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn logs_returns_not_implemented() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(empty_catalog(), mgr);
        let err = svc
            .logs(LogsRequest {
                pipeline: "any".into(),
                ..LogsRequest::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn logs_stream_returns_not_implemented() {
        let mgr = Arc::new(RunManager::new());
        let svc = make_service(empty_catalog(), mgr);
        let result = svc.logs_stream(LogsStreamFilter::default()).await;
        assert!(matches!(result, Err(ApiError::NotImplemented { .. })));
    }
}
