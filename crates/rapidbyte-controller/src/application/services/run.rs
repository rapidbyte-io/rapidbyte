use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::application::{cancel, query};
use crate::domain::ports::repository::{Pagination, RunFilter as DomainRunFilter};
use crate::domain::run::{Run, RunState};
use crate::traits::error::{EventStream, PaginatedList, ServiceError};
use crate::traits::run::{
    BatchDetail, ProgressEvent, RunCounts, RunDetail, RunErrorInfo, RunFilter, RunService,
    RunSummary,
};

use super::{app_error_to_service, AppServices};

#[async_trait]
impl RunService for AppServices {
    async fn get(&self, run_id: &str) -> Result<RunDetail, ServiceError> {
        let run = query::get_run(&self.ctx, run_id)
            .await
            .map_err(app_error_to_service)?;
        Ok(run_to_detail(&run))
    }

    async fn list(&self, filter: RunFilter) -> Result<PaginatedList<RunSummary>, ServiceError> {
        // Reject unknown status values rather than silently ignoring them.
        if let Some(ref status) = filter.status {
            use std::str::FromStr;
            if RunState::from_str(status).is_err() {
                return Err(ServiceError::ValidationFailed {
                    details: vec![crate::traits::FieldError {
                        field: "status".into(),
                        reason: format!(
                            "unknown status '{status}'; valid values: pending, running, completed, failed, cancelled"
                        ),
                    }],
                });
            }
        }
        let domain_filter = to_domain_filter(&filter);
        let pagination = to_pagination(&filter);
        let page = query::list_runs(&self.ctx, domain_filter, pagination)
            .await
            .map_err(app_error_to_service)?;
        Ok(PaginatedList {
            items: page.runs.iter().map(run_to_summary).collect(),
            next_cursor: page.next_page_token,
        })
    }

    async fn events(&self, run_id: &str) -> Result<EventStream<ProgressEvent>, ServiceError> {
        // Verify run exists and capture pipeline name for Started events.
        let run = query::get_run(&self.ctx, run_id)
            .await
            .map_err(app_error_to_service)?;
        // Use Arc<str> so each event only pays an atomic ref-count increment
        // rather than a heap allocation for the pipeline name clone.
        let pipeline_name: Arc<str> = Arc::from(run.pipeline_name());

        let stream =
            self.ctx
                .event_bus
                .subscribe(run_id)
                .await
                .map_err(|e| ServiceError::Internal {
                    message: e.to_string(),
                })?;
        let mapped = stream.filter_map(move |event| {
            let pipeline = Arc::clone(&pipeline_name);
            async move { domain_event_to_progress(event, &pipeline) }
        });
        Ok(Box::pin(mapped))
    }

    async fn cancel(&self, run_id: &str) -> Result<RunDetail, ServiceError> {
        cancel::cancel_run(&self.ctx, run_id)
            .await
            .map_err(app_error_to_service)?;
        // Re-fetch to return updated state. cancel_run returns CancelResult { accepted }
        // but does not return the run itself, so a second DB round-trip is needed here.
        // This is a minor inefficiency; the tradeoff is accepted to keep cancel_run's
        // contract minimal (it is also called directly from the gRPC adapter).
        let run = query::get_run(&self.ctx, run_id)
            .await
            .map_err(app_error_to_service)?;
        Ok(run_to_detail(&run))
    }

    async fn get_batch(&self, _batch_id: &str) -> Result<BatchDetail, ServiceError> {
        // Batch support requires new domain types — not yet implemented
        Err(ServiceError::NotImplemented {
            feature: "batch operations".into(),
        })
    }

    async fn batch_events(
        &self,
        _batch_id: &str,
    ) -> Result<EventStream<ProgressEvent>, ServiceError> {
        Err(ServiceError::NotImplemented {
            feature: "batch operations".into(),
        })
    }
}

// --- Mapping helpers ---

fn run_to_detail(run: &Run) -> RunDetail {
    let is_terminal = matches!(
        run.state(),
        RunState::Completed | RunState::Failed | RunState::Cancelled
    );
    RunDetail {
        run_id: run.id().to_string(),
        pipeline: run.pipeline_name().to_string(),
        status: run.state().as_str().to_string(),
        started_at: Some(run.created_at()),
        finished_at: if is_terminal {
            Some(run.updated_at())
        } else {
            None
        },
        #[allow(clippy::cast_precision_loss)]
        duration_secs: run.metrics().map(|m| m.duration_ms as f64 / 1000.0),
        counts: run.metrics().map(|m| RunCounts {
            records_read: m.rows_read,
            records_written: m.rows_written,
            bytes_read: m.bytes_read,
            bytes_written: m.bytes_written,
        }),
        timing: None, // Requires per-stage timing breakdown from Run
        retry_count: run.current_attempt().saturating_sub(1),
        cancel_requested: run.is_cancel_requested(),
        max_retries: run.max_retries(),
        error: run.error().map(|e| RunErrorInfo {
            code: e.code.clone(),
            message: e.message.clone(),
        }),
    }
}

fn run_to_summary(run: &Run) -> RunSummary {
    RunSummary {
        run_id: run.id().to_string(),
        pipeline: run.pipeline_name().to_string(),
        status: run.state().as_str().to_string(),
        started_at: Some(run.created_at()),
        #[allow(clippy::cast_precision_loss)]
        duration_secs: run.metrics().map(|m| m.duration_ms as f64 / 1000.0),
        records_written: run.metrics().map(|m| m.rows_written),
        attempt: run.current_attempt(),
    }
}

fn domain_event_to_progress(
    event: crate::domain::event::DomainEvent,
    pipeline: &str,
) -> Option<ProgressEvent> {
    use crate::domain::event::DomainEvent;
    match event {
        DomainEvent::RunStateChanged { run_id, state, .. } => match state {
            RunState::Running => Some(ProgressEvent::Started {
                run_id,
                pipeline: pipeline.to_string(),
            }),
            RunState::Cancelled => Some(ProgressEvent::Cancelled { run_id }),
            _ => None,
        },
        DomainEvent::RunCompleted { run_id, metrics } => Some(ProgressEvent::Complete {
            run_id,
            status: RunState::Completed.as_str().to_string(),
            #[allow(clippy::cast_precision_loss)]
            duration_secs: Some(metrics.duration_ms as f64 / 1000.0),
        }),
        DomainEvent::RunFailed { run_id, error } => Some(ProgressEvent::Failed {
            run_id,
            error: RunErrorInfo {
                code: error.code,
                message: error.message,
            },
        }),
        DomainEvent::ProgressReported {
            run_id, message, ..
        } => Some(ProgressEvent::Progress {
            run_id,
            phase: message,
            stream: None,
            records_read: None,
            records_written: None,
        }),
        DomainEvent::RunCancelled { run_id } => Some(ProgressEvent::Cancelled { run_id }),
    }
}

fn to_domain_filter(filter: &RunFilter) -> DomainRunFilter {
    use std::str::FromStr;
    let state = filter
        .status
        .as_deref()
        .and_then(|s| RunState::from_str(s).ok());
    DomainRunFilter {
        state,
        pipeline: filter.pipeline.clone(),
    }
}

fn to_pagination(filter: &RunFilter) -> Pagination {
    Pagination {
        page_size: filter.limit,
        page_token: filter.cursor.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::application::testing::fake_context;

    #[tokio::test]
    async fn get_nonexistent_run_returns_not_found() {
        let tc = fake_context();
        let services = AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        );
        let result = services.get("nonexistent").await;
        assert!(matches!(result, Err(ServiceError::NotFound { .. })));
    }

    #[tokio::test]
    async fn list_filters_by_pipeline() {
        let tc = fake_context();

        // Submit two runs with different pipeline names
        let yaml_a = "pipeline: pipe-a\nversion: '1.0'";
        let yaml_b = "pipeline: pipe-b\nversion: '1.0'";
        crate::application::submit::submit_pipeline(&tc.ctx, None, yaml_a.to_string(), 0, None)
            .await
            .unwrap();
        crate::application::submit::submit_pipeline(&tc.ctx, None, yaml_b.to_string(), 0, None)
            .await
            .unwrap();

        let services = AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        );

        // List all — should see 2
        let all = services
            .list(crate::traits::run::RunFilter {
                pipeline: None,
                status: None,
                limit: 20,
                cursor: None,
            })
            .await
            .unwrap();
        assert_eq!(all.items.len(), 2);

        // Filter by pipe-a — should see 1
        let filtered = services
            .list(crate::traits::run::RunFilter {
                pipeline: Some("pipe-a".into()),
                status: None,
                limit: 20,
                cursor: None,
            })
            .await
            .unwrap();
        assert_eq!(filtered.items.len(), 1);
        assert_eq!(filtered.items[0].pipeline, "pipe-a");

        // Filter by nonexistent — should see 0
        let empty = services
            .list(crate::traits::run::RunFilter {
                pipeline: Some("nonexistent".into()),
                status: None,
                limit: 20,
                cursor: None,
            })
            .await
            .unwrap();
        assert_eq!(empty.items.len(), 0);
    }

    #[tokio::test]
    async fn list_rejects_invalid_status() {
        let tc = fake_context();
        let services = AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:8080".parse().unwrap(),
        );
        let result = services
            .list(crate::traits::run::RunFilter {
                pipeline: None,
                status: Some("bogus".into()),
                limit: 20,
                cursor: None,
            })
            .await;
        assert!(matches!(result, Err(ServiceError::ValidationFailed { .. })));
    }
}
