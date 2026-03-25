use async_trait::async_trait;

use crate::application::submit;
use crate::traits::pipeline::{
    AssertRequest, AssertResult, BatchRunHandle, CheckResult, DiffResult, PipelineDetail,
    PipelineFilter, PipelineService, PipelineSummary, ResolvedConfig, RunHandle, SyncBatchRequest,
    SyncRequest, TeardownRequest,
};
use crate::traits::{PaginatedList, ServiceError};

use super::{app_error_to_service, AppServices};

#[async_trait]
impl PipelineService for AppServices {
    async fn list(
        &self,
        _filter: PipelineFilter,
    ) -> Result<PaginatedList<PipelineSummary>, ServiceError> {
        // Pipeline discovery from YAML files not yet implemented in server mode
        Ok(PaginatedList {
            items: vec![],
            next_cursor: None,
        })
    }

    async fn get(&self, _name: &str) -> Result<PipelineDetail, ServiceError> {
        Err(ServiceError::Internal {
            message: "pipeline detail not yet available in server mode".into(),
        })
    }

    async fn sync(&self, request: SyncRequest) -> Result<RunHandle, ServiceError> {
        // For now, sync requires the pipeline YAML to be submitted directly.
        // In server mode, the pipeline_yaml comes from the PipelineStore or project files.
        // For the REST API, we need the pipeline name to resolve to YAML.
        // Current implementation: use the pipeline name as-is and submit with empty YAML
        // placeholder. This will be enhanced when pipeline discovery is implemented.
        let result = submit::submit_pipeline(
            &self.ctx,
            None,             // no idempotency key from REST
            request.pipeline, // pipeline name used as identifier
            0,                // default max_retries
            None,             // no timeout
        )
        .await
        .map_err(app_error_to_service)?;

        Ok(RunHandle {
            run_id: result.run_id,
            status: "pending".into(),
            links: None, // populated by REST adapter
        })
    }

    async fn sync_batch(&self, _request: SyncBatchRequest) -> Result<BatchRunHandle, ServiceError> {
        Err(ServiceError::Internal {
            message: "batch sync not yet available in server mode".into(),
        })
    }

    async fn check(&self, _name: &str) -> Result<CheckResult, ServiceError> {
        Err(ServiceError::Internal {
            message: "check not yet available in server mode".into(),
        })
    }

    async fn check_apply(&self, _name: &str) -> Result<RunHandle, ServiceError> {
        Err(ServiceError::Internal {
            message: "check-apply not yet available in server mode".into(),
        })
    }

    async fn compile(&self, _name: &str) -> Result<ResolvedConfig, ServiceError> {
        Err(ServiceError::Internal {
            message: "compile not yet available in server mode".into(),
        })
    }

    async fn diff(&self, _name: &str) -> Result<DiffResult, ServiceError> {
        Err(ServiceError::Internal {
            message: "diff not yet available in server mode".into(),
        })
    }

    async fn assert(&self, _request: AssertRequest) -> Result<AssertResult, ServiceError> {
        Err(ServiceError::Internal {
            message: "assert not yet available in server mode".into(),
        })
    }

    async fn teardown(&self, _request: TeardownRequest) -> Result<RunHandle, ServiceError> {
        Err(ServiceError::Internal {
            message: "teardown not yet available in server mode".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::application::testing::fake_context;

    #[tokio::test]
    async fn sync_with_valid_yaml_returns_run_handle() {
        let tc = fake_context();
        let services = AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        );

        let request = SyncRequest {
            pipeline: "pipeline: test-pipe\nversion: '1.0'".into(),
            stream: None,
            full_refresh: false,
            cursor_start: None,
            cursor_end: None,
            dry_run: false,
        };

        let result = services.sync(request).await.unwrap();
        assert!(!result.run_id.is_empty());
        assert_eq!(result.status, "pending");
        assert!(result.links.is_none());
    }

    #[tokio::test]
    async fn sync_with_invalid_yaml_returns_error() {
        let tc = fake_context();
        let services = AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        );

        let request = SyncRequest {
            pipeline: "not: [valid: yaml: {{{}}}".into(),
            stream: None,
            full_refresh: false,
            cursor_start: None,
            cursor_end: None,
            dry_run: false,
        };

        let result = services.sync(request).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ServiceError::Conflict { .. }));
    }

    #[tokio::test]
    async fn list_returns_empty() {
        let tc = fake_context();
        let services = AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        );

        let result = services.list(PipelineFilter::default()).await.unwrap();
        assert!(result.items.is_empty());
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn get_returns_internal_error() {
        let tc = fake_context();
        let services = AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        );

        let result = services.get("any-pipeline").await;
        assert!(matches!(result, Err(ServiceError::Internal { .. })));
    }
}
