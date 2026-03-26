use async_trait::async_trait;

use crate::traits::pipeline::{
    AssertRequest, AssertResult, BatchRunHandle, CheckResult, DiffResult, PipelineDetail,
    PipelineFilter, PipelineService, PipelineSummary, ResolvedConfig, RunHandle, SyncBatchRequest,
    SyncRequest, TeardownRequest,
};
use crate::traits::{PaginatedList, ServiceError};

use super::AppServices;

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

    async fn sync(&self, _request: SyncRequest) -> Result<RunHandle, ServiceError> {
        Err(ServiceError::NotImplemented {
            feature: "REST pipeline sync (requires pipeline YAML resolution)".into(),
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
    use super::*;
    use crate::application::testing::fake_app_services;

    #[tokio::test]
    async fn sync_returns_not_implemented() {
        let services = fake_app_services();

        let request = SyncRequest {
            pipeline: "pipeline: test-pipe\nversion: '1.0'".into(),
            stream: None,
            full_refresh: false,
            cursor_start: None,
            cursor_end: None,
            dry_run: false,
        };

        let result = services.sync(request).await;
        assert!(matches!(result, Err(ServiceError::NotImplemented { .. })));
    }

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
}
