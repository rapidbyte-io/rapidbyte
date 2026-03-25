use async_trait::async_trait;

use crate::domain::ports::cursor_store::PipelineState;
use crate::domain::ports::log_store::{LogFilter, LogStreamFilter, StoredLogEntry};
use crate::traits::error::{EventStream, ServiceError};
use crate::traits::operations::{
    FreshnessFilter, FreshnessStatus, LogsRequest, LogsResult, LogsStreamFilter, OperationsService,
    PipelineStateChange, PipelineStatus, PipelineStatusDetail, ResetRequest, ResetResult,
};

use super::AppServices;

#[async_trait]
impl OperationsService for AppServices {
    async fn status(&self) -> Result<Vec<PipelineStatus>, ServiceError> {
        // Pipeline discovery from project files not yet implemented in server mode.
        Ok(vec![])
    }

    async fn pipeline_status(&self, name: &str) -> Result<PipelineStatusDetail, ServiceError> {
        // Pipeline discovery not yet implemented; check cursors to build stream list.
        // Run get_pipeline_state and get_cursors concurrently since they are independent.
        let (state_result, cursors_result) = tokio::join!(
            self.ctx.cursor_store.get_pipeline_state(name),
            self.ctx.cursor_store.get_cursors(name),
        );

        let state = state_result
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?
            .unwrap_or(PipelineState::Active)
            .as_str()
            .to_string();

        let cursors = cursors_result.map_err(|e| ServiceError::Internal {
            message: e.to_string(),
        })?;

        if cursors.is_empty() {
            return Err(ServiceError::NotFound {
                resource: "pipeline".into(),
                id: name.to_string(),
            });
        }

        let streams = cursors
            .iter()
            .map(|c| crate::traits::operations::StreamStatus {
                name: c.stream.clone(),
                sync_mode: Some("incremental".into()),
                cursor_value: Some(c.cursor_value.clone()),
            })
            .collect();

        Ok(PipelineStatusDetail {
            pipeline: name.to_string(),
            state,
            health: "ok".into(),
            streams,
        })
    }

    async fn pause(&self, name: &str) -> Result<PipelineStateChange, ServiceError> {
        self.ctx
            .cursor_store
            .set_pipeline_state(name, PipelineState::Paused)
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;
        Ok(PipelineStateChange {
            pipeline: name.to_string(),
            state: "paused".into(),
            paused_at: Some(self.ctx.clock.now()),
            resumed_at: None,
        })
    }

    async fn resume(&self, name: &str) -> Result<PipelineStateChange, ServiceError> {
        self.ctx
            .cursor_store
            .set_pipeline_state(name, PipelineState::Active)
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;
        Ok(PipelineStateChange {
            pipeline: name.to_string(),
            state: "active".into(),
            paused_at: None,
            resumed_at: Some(self.ctx.clock.now()),
        })
    }

    async fn reset(&self, request: ResetRequest) -> Result<ResetResult, ServiceError> {
        let stream_ref = request.stream.as_deref();

        let cursors_cleared = self
            .ctx
            .cursor_store
            .clear(&request.pipeline, stream_ref)
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        let streams_reset = if let Some(s) = stream_ref {
            vec![s.to_string()]
        } else {
            vec![]
        };

        Ok(ResetResult {
            pipeline: request.pipeline,
            streams_reset,
            cursors_cleared,
            next_sync_mode: "full_refresh".into(),
        })
    }

    async fn freshness(
        &self,
        _filter: FreshnessFilter,
    ) -> Result<Vec<FreshnessStatus>, ServiceError> {
        // Pipeline discovery not yet implemented; cannot enumerate pipelines.
        Ok(vec![])
    }

    async fn logs(&self, request: LogsRequest) -> Result<LogsResult, ServiceError> {
        let filter = LogFilter {
            pipeline: request.pipeline,
            run_id: request.run_id,
            limit: request.limit.unwrap_or(100),
            cursor: request.cursor,
        };

        let page = self
            .ctx
            .log_store
            .query(filter)
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        Ok(LogsResult {
            items: page.items,
            next_cursor: page.next_cursor,
        })
    }

    async fn logs_stream(
        &self,
        filter: LogsStreamFilter,
    ) -> Result<EventStream<StoredLogEntry>, ServiceError> {
        let domain_filter = LogStreamFilter {
            pipeline: filter.pipeline,
        };

        let stream = self
            .ctx
            .log_store
            .subscribe(domain_filter)
            .await
            .map_err(|e| ServiceError::Internal {
                message: e.to_string(),
            })?;

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::fake_app_services;

    #[tokio::test]
    async fn status_returns_empty_vec() {
        let services = fake_app_services();
        let result = services.status().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn pipeline_status_no_cursors_returns_not_found() {
        let services = fake_app_services();
        let result = services.pipeline_status("nonexistent").await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn reset_with_no_cursors_returns_zero_cleared() {
        let services = fake_app_services();
        let result = services
            .reset(ResetRequest {
                pipeline: "test-pipeline".into(),
                stream: None,
            })
            .await
            .unwrap();
        assert_eq!(result.pipeline, "test-pipeline");
        assert_eq!(result.cursors_cleared, 0);
        assert_eq!(result.next_sync_mode, "full_refresh");
    }

    #[tokio::test]
    async fn logs_returns_empty_for_unknown_pipeline() {
        let services = fake_app_services();
        let result = services
            .logs(LogsRequest {
                pipeline: "test-pipeline".into(),
                run_id: None,
                limit: None,
                cursor: None,
            })
            .await
            .unwrap();
        assert!(result.items.is_empty());
        assert!(result.next_cursor.is_none());
    }
}
