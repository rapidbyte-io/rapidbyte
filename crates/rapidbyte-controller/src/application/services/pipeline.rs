use async_trait::async_trait;

use crate::application::submit;
use crate::domain::ports::pipeline_source::PipelineSourceError;
use crate::domain::task::TaskOperation;
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
        Err(ServiceError::NotImplemented {
            feature: "pipeline detail".into(),
        })
    }

    async fn sync(&self, request: SyncRequest) -> Result<RunHandle, ServiceError> {
        // Resolve pipeline YAML from PipelineSource
        let yaml = self
            .ctx
            .pipeline_source
            .get(&request.pipeline)
            .await
            .map_err(|e| match e {
                PipelineSourceError::NotFound(name) => ServiceError::NotFound {
                    resource: "pipeline".into(),
                    id: name,
                },
                other => ServiceError::Internal {
                    message: other.to_string(),
                },
            })?;

        // Submit as async task
        let result = submit::submit_pipeline(
            &self.ctx,
            None, // no idempotency key from REST
            yaml,
            0,    // default max_retries
            None, // no timeout
            TaskOperation::Sync,
        )
        .await
        .map_err(app_error_to_service)?;

        Ok(RunHandle {
            run_id: result.run_id,
            status: "pending".into(),
            links: None,
        })
    }

    async fn sync_batch(&self, _request: SyncBatchRequest) -> Result<BatchRunHandle, ServiceError> {
        Err(ServiceError::NotImplemented {
            feature: "batch sync".into(),
        })
    }

    async fn check(&self, _name: &str) -> Result<CheckResult, ServiceError> {
        Err(ServiceError::NotImplemented {
            feature: "check".into(),
        })
    }

    async fn check_apply(&self, _name: &str) -> Result<RunHandle, ServiceError> {
        Err(ServiceError::NotImplemented {
            feature: "check-apply".into(),
        })
    }

    async fn compile(&self, _name: &str) -> Result<ResolvedConfig, ServiceError> {
        Err(ServiceError::NotImplemented {
            feature: "compile".into(),
        })
    }

    async fn diff(&self, _name: &str) -> Result<DiffResult, ServiceError> {
        Err(ServiceError::NotImplemented {
            feature: "diff".into(),
        })
    }

    async fn assert(&self, _request: AssertRequest) -> Result<AssertResult, ServiceError> {
        Err(ServiceError::NotImplemented {
            feature: "assert".into(),
        })
    }

    async fn teardown(&self, _request: TeardownRequest) -> Result<RunHandle, ServiceError> {
        Err(ServiceError::NotImplemented {
            feature: "teardown".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::application::services::AppServices;
    use crate::application::testing::{fake_app_services, fake_context, FakePipelineSource};

    #[tokio::test]
    async fn list_returns_empty() {
        let services = fake_app_services();

        let result = services.list(PipelineFilter::default()).await.unwrap();
        assert!(result.items.is_empty());
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn get_returns_not_implemented() {
        let services = fake_app_services();

        let result = services.get("any-pipeline").await;
        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }

    #[tokio::test]
    async fn sync_known_pipeline_returns_handle() {
        let mut tc = fake_context();
        tc.ctx.pipeline_source = Arc::new(
            FakePipelineSource::new()
                .with_pipeline("test-pipe", "pipeline: test-pipe\nversion: '1.0'"),
        );
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        ));

        let result = services
            .sync(SyncRequest {
                pipeline: "test-pipe".into(),
                stream: None,
                full_refresh: false,
                cursor_start: None,
                cursor_end: None,
                dry_run: false,
            })
            .await;

        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let handle = result.unwrap();
        assert_eq!(handle.status, "pending");
        assert!(!handle.run_id.is_empty());
    }

    #[tokio::test]
    async fn sync_unknown_pipeline_returns_not_found() {
        // fake_context uses FakePipelineSource::new() — empty, no pipelines
        let services = fake_app_services();

        let result = services
            .sync(SyncRequest {
                pipeline: "nonexistent".into(),
                stream: None,
                full_refresh: false,
                cursor_start: None,
                cursor_end: None,
                dry_run: false,
            })
            .await;

        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }
}
