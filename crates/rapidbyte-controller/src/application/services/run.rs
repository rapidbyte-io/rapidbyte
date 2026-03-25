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
        // Verify run exists first
        query::get_run(&self.ctx, run_id)
            .await
            .map_err(app_error_to_service)?;
        // Subscribe to events
        let stream =
            self.ctx
                .event_bus
                .subscribe(run_id)
                .await
                .map_err(|e| ServiceError::Internal {
                    message: e.to_string(),
                })?;
        // Map DomainEvent -> ProgressEvent, filtering out None
        let mapped = stream.filter_map(|event| async move { domain_event_to_progress(event) });
        Ok(Box::pin(mapped))
    }

    async fn cancel(&self, run_id: &str) -> Result<RunDetail, ServiceError> {
        cancel::cancel_run(&self.ctx, run_id)
            .await
            .map_err(app_error_to_service)?;
        // Re-fetch to return updated state
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
        duration_secs: run.metrics().map(|m| m.duration_ms as f64 / 1000.0),
        counts: run.metrics().map(|m| RunCounts {
            records_read: m.rows_read,
            records_written: m.rows_written,
            bytes_read: m.bytes_read,
            bytes_written: m.bytes_written,
        }),
        timing: None, // Not available from current domain model
        retry_count: run.current_attempt().saturating_sub(1),
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
        duration_secs: run.metrics().map(|m| m.duration_ms as f64 / 1000.0),
        records_written: run.metrics().map(|m| m.rows_written),
    }
}

fn domain_event_to_progress(event: crate::domain::event::DomainEvent) -> Option<ProgressEvent> {
    use crate::domain::event::DomainEvent;
    match event {
        DomainEvent::RunStateChanged { run_id, state, .. } => match state {
            RunState::Running => Some(ProgressEvent::Started {
                run_id,
                pipeline: String::new(),
            }),
            RunState::Cancelled => Some(ProgressEvent::Cancelled { run_id }),
            _ => None,
        },
        DomainEvent::RunCompleted { run_id, metrics } => Some(ProgressEvent::Complete {
            run_id,
            status: "completed".into(),
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
    let state = filter.status.as_deref().and_then(|s| match s {
        "pending" | "Pending" => Some(RunState::Pending),
        "running" | "Running" => Some(RunState::Running),
        "completed" | "Completed" => Some(RunState::Completed),
        "failed" | "Failed" => Some(RunState::Failed),
        "cancelled" | "Cancelled" => Some(RunState::Cancelled),
        _ => None,
    });
    DomainRunFilter { state }
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
}
