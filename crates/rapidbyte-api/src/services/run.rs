//! Local-mode implementation of [`RunService`].
//!
//! Delegates to [`RunManager`] for run lookup, filtering, SSE
//! subscription, and cooperative cancellation.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::error::ApiError;
use crate::run_manager::{RunManager, RunSnapshot};
use crate::traits::RunService;
use crate::types::{
    BatchDetail, EventStream, PaginatedList, PipelineCounts, RunDetail, RunFilter, RunStatus,
    RunSummary, SseEvent,
};

type Result<T> = std::result::Result<T, ApiError>;

/// Local-mode run service.
///
/// Wraps a shared [`RunManager`] to implement run inspection,
/// listing, SSE event streaming, and cancellation.
pub struct LocalRunService {
    run_manager: Arc<RunManager>,
}

impl LocalRunService {
    /// Create a new `LocalRunService`.
    #[must_use]
    pub fn new(run_manager: Arc<RunManager>) -> Self {
        Self { run_manager }
    }
}

// ---------------------------------------------------------------------------
// Snapshot → DTO conversion
// ---------------------------------------------------------------------------

/// Convert a [`RunSnapshot`] into a [`RunDetail`] response DTO.
#[allow(clippy::cast_precision_loss)]
fn snapshot_to_detail(snap: &RunSnapshot) -> RunDetail {
    RunDetail {
        run_id: snap.run_id.clone(),
        pipeline: snap.pipeline.clone(),
        status: snap.status,
        started_at: snap.started_at,
        finished_at: snap.finished_at,
        duration_secs: snap
            .finished_at
            .map(|f| (f - snap.started_at).num_milliseconds() as f64 / 1000.0),
        trigger: "api".into(),
        counts: snap.result.as_ref().map(|r| PipelineCounts {
            records_read: r.counts.records_read,
            records_written: r.counts.records_written,
            bytes_read: r.counts.bytes_read,
            bytes_written: r.counts.bytes_written,
        }),
        streams: vec![],
        timing: None,
        retry_count: snap.result.as_ref().map_or(0, |r| r.retry_count),
        parallelism: snap.result.as_ref().map_or(1, |r| r.parallelism),
        error: snap.error.clone(),
    }
}

/// Convert a [`RunSnapshot`] into a [`RunSummary`] for list responses.
#[allow(clippy::cast_precision_loss)]
fn snapshot_to_summary(snap: &RunSnapshot) -> RunSummary {
    RunSummary {
        run_id: snap.run_id.clone(),
        pipeline: snap.pipeline.clone(),
        status: snap.status,
        started_at: snap.started_at,
        duration_secs: snap
            .finished_at
            .map(|f| (f - snap.started_at).num_milliseconds() as f64 / 1000.0),
        records_written: snap.result.as_ref().map(|r| r.counts.records_written),
    }
}

// ---------------------------------------------------------------------------
// RunService implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl RunService for LocalRunService {
    async fn get(&self, run_id: &str) -> Result<RunDetail> {
        let snap = self.run_manager.get(run_id).ok_or_else(|| {
            ApiError::not_found("run_not_found", format!("Run '{run_id}' not found"))
        })?;

        Ok(snapshot_to_detail(&snap))
    }

    async fn list(&self, filter: RunFilter) -> Result<PaginatedList<RunSummary>> {
        let mut runs = self.run_manager.list_runs();

        // Filter by pipeline name if specified.
        if let Some(ref pipeline) = filter.pipeline {
            runs.retain(|r| r.pipeline == *pipeline);
        }

        // Filter by status if specified.
        if let Some(status) = filter.status {
            runs.retain(|r| r.status == status);
        }

        // Sort by started_at descending (most recent first).
        runs.sort_by(|a, b| b.started_at.cmp(&a.started_at));

        // Paginate with limit (no real cursor for Phase 1).
        #[allow(clippy::cast_possible_truncation)]
        let limit = filter.limit as usize;
        runs.truncate(limit);

        let items: Vec<RunSummary> = runs.iter().map(snapshot_to_summary).collect();

        Ok(PaginatedList {
            items,
            next_cursor: None,
        })
    }

    #[allow(clippy::cast_precision_loss)]
    async fn events(&self, run_id: &str) -> Result<EventStream<SseEvent>> {
        let snap = self.run_manager.get(run_id).ok_or_else(|| {
            ApiError::not_found("run_not_found", format!("Run '{run_id}' not found"))
        })?;

        // If the run is already in a terminal state, return a stream that
        // immediately yields the final event and closes. This prevents the
        // client from hanging forever on a broadcast receiver that will
        // never receive another message.
        if snap.status.is_terminal() {
            let final_event = match snap.status {
                RunStatus::Completed => SseEvent::Complete {
                    run_id: snap.run_id.clone(),
                    status: RunStatus::Completed,
                    duration_secs: snap.finished_at.map_or(0.0, |f| {
                        (f - snap.started_at).num_milliseconds() as f64 / 1000.0
                    }),
                    counts: snap.result.as_ref().map(|r| PipelineCounts {
                        records_read: r.counts.records_read,
                        records_written: r.counts.records_written,
                        bytes_read: r.counts.bytes_read,
                        bytes_written: r.counts.bytes_written,
                    }),
                },
                RunStatus::Failed => SseEvent::Failed {
                    run_id: snap.run_id.clone(),
                    error: snap.error.unwrap_or_default(),
                },
                RunStatus::Cancelled => SseEvent::Cancelled {
                    run_id: snap.run_id.clone(),
                    reason: "cancelled".into(),
                },
                _ => unreachable!("is_terminal() returned true for non-terminal status"),
            };
            let stream = futures::stream::once(async move { final_event });
            return Ok(Box::pin(stream));
        }

        let mut rx = self.run_manager.subscribe(run_id).ok_or_else(|| {
            ApiError::not_found("run_not_found", format!("Run '{run_id}' not found"))
        })?;

        let stream = async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let is_terminal = event.is_terminal();
                        yield event;
                        if is_terminal { break; }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "SSE subscriber lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        };

        Ok(Box::pin(stream))
    }

    async fn cancel(&self, run_id: &str) -> Result<RunDetail> {
        let snap = self.run_manager.get(run_id).ok_or_else(|| {
            ApiError::not_found("run_not_found", format!("Run '{run_id}' not found"))
        })?;

        // Reject cancellation of terminal runs.
        if matches!(
            snap.status,
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
        ) {
            return Err(ApiError::conflict(
                "run_terminal",
                format!(
                    "Run '{run_id}' is already in terminal state: {:?}",
                    snap.status
                ),
            ));
        }

        self.run_manager.cancel(run_id);
        self.run_manager.update_status(run_id, RunStatus::Cancelled);

        // Re-fetch the updated snapshot.
        let snap = self
            .run_manager
            .get(run_id)
            .ok_or_else(|| ApiError::internal("run disappeared after cancel"))?;

        Ok(snapshot_to_detail(&snap))
    }

    async fn get_batch(&self, _batch_id: &str) -> Result<BatchDetail> {
        Err(ApiError::not_implemented("batch operations"))
    }

    async fn batch_events(&self, _batch_id: &str) -> Result<EventStream<SseEvent>> {
        Err(ApiError::not_implemented("batch events"))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn setup() -> (Arc<RunManager>, LocalRunService) {
        let mgr = Arc::new(RunManager::new());
        let svc = LocalRunService::new(Arc::clone(&mgr));
        (mgr, svc)
    }

    // ----- get -----

    #[tokio::test]
    async fn get_active_run() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("test-pipe".into());
        let detail = svc.get(&run_id).await.unwrap();
        assert_eq!(detail.pipeline, "test-pipe");
        assert_eq!(detail.status, RunStatus::Pending);
        assert_eq!(detail.trigger, "api");
        assert!(detail.finished_at.is_none());
        assert!(detail.duration_secs.is_none());
        assert!(detail.counts.is_none());
        assert!(detail.streams.is_empty());
        assert!(detail.timing.is_none());
        assert_eq!(detail.retry_count, 0);
        assert_eq!(detail.parallelism, 1);
        assert!(detail.error.is_none());
    }

    #[tokio::test]
    async fn get_nonexistent_returns_not_found() {
        let (_, svc) = setup();
        let err = svc.get("run_doesnotexist").await.unwrap_err();
        assert!(matches!(err, ApiError::NotFound { .. }));
    }

    #[tokio::test]
    async fn get_completed_run_has_duration() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        let result = rapidbyte_engine::PipelineResult {
            counts: rapidbyte_engine::PipelineCounts {
                records_read: 100,
                records_written: 95,
                bytes_read: 4096,
                bytes_written: 3800,
            },
            source: rapidbyte_engine::SourceTiming::default(),
            dest: rapidbyte_engine::DestTiming::default(),
            num_transforms: 0,
            total_transform_secs: 0.0,
            transform_load_times_ms: vec![],
            duration_secs: 1.5,
            wasm_overhead_secs: 0.1,
            retry_count: 2,
            parallelism: 4,
            stream_metrics: vec![],
        };
        mgr.complete(&run_id, result);
        let detail = svc.get(&run_id).await.unwrap();
        assert_eq!(detail.status, RunStatus::Completed);
        assert!(detail.finished_at.is_some());
        assert!(detail.duration_secs.is_some());
        let counts = detail.counts.unwrap();
        assert_eq!(counts.records_read, 100);
        assert_eq!(counts.records_written, 95);
        assert_eq!(counts.bytes_read, 4096);
        assert_eq!(counts.bytes_written, 3800);
        assert_eq!(detail.retry_count, 2);
        assert_eq!(detail.parallelism, 4);
    }

    #[tokio::test]
    async fn get_failed_run_has_error() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        mgr.fail(&run_id, "connection refused".into());
        let detail = svc.get(&run_id).await.unwrap();
        assert_eq!(detail.status, RunStatus::Failed);
        assert_eq!(detail.error.as_deref(), Some("connection refused"));
    }

    // ----- cancel -----

    #[tokio::test]
    async fn cancel_active_run() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        mgr.update_status(&run_id, RunStatus::Running);
        let detail = svc.cancel(&run_id).await.unwrap();
        assert_eq!(detail.status, RunStatus::Cancelled);
    }

    #[tokio::test]
    async fn cancel_pending_run() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        let detail = svc.cancel(&run_id).await.unwrap();
        assert_eq!(detail.status, RunStatus::Cancelled);
    }

    #[tokio::test]
    async fn cancel_completed_run_returns_conflict() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        mgr.update_status(&run_id, RunStatus::Completed);
        let err = svc.cancel(&run_id).await.unwrap_err();
        assert!(matches!(err, ApiError::Conflict { .. }));
    }

    #[tokio::test]
    async fn cancel_failed_run_returns_conflict() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        mgr.update_status(&run_id, RunStatus::Failed);
        let err = svc.cancel(&run_id).await.unwrap_err();
        assert!(matches!(err, ApiError::Conflict { .. }));
    }

    #[tokio::test]
    async fn cancel_already_cancelled_returns_conflict() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        mgr.update_status(&run_id, RunStatus::Cancelled);
        let err = svc.cancel(&run_id).await.unwrap_err();
        assert!(matches!(err, ApiError::Conflict { .. }));
    }

    #[tokio::test]
    async fn cancel_nonexistent_returns_not_found() {
        let (_, svc) = setup();
        let err = svc.cancel("run_nope").await.unwrap_err();
        assert!(matches!(err, ApiError::NotFound { .. }));
    }

    #[tokio::test]
    async fn cancel_triggers_cancellation_token() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        let token = mgr.cancel_token(&run_id).unwrap();
        assert!(!token.is_cancelled());
        let _ = svc.cancel(&run_id).await.unwrap();
        assert!(token.is_cancelled());
    }

    // ----- list -----

    #[tokio::test]
    async fn list_all_runs() {
        let (mgr, svc) = setup();
        let _ = mgr.create_run("pipe-a".into());
        let _ = mgr.create_run("pipe-b".into());
        let _ = mgr.create_run("pipe-c".into());
        let result = svc.list(RunFilter::default()).await.unwrap();
        assert_eq!(result.items.len(), 3);
        assert!(result.next_cursor.is_none());
    }

    #[tokio::test]
    async fn list_filters_by_pipeline() {
        let (mgr, svc) = setup();
        let _ = mgr.create_run("pipe-a".into());
        let _ = mgr.create_run("pipe-b".into());
        let _ = mgr.create_run("pipe-a".into());
        let filter = RunFilter {
            pipeline: Some("pipe-a".into()),
            ..RunFilter::default()
        };
        let result = svc.list(filter).await.unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(result.items.iter().all(|r| r.pipeline == "pipe-a"));
    }

    #[tokio::test]
    async fn list_filters_by_status() {
        let (mgr, svc) = setup();
        let id1 = mgr.create_run("p".into());
        let _id2 = mgr.create_run("p".into());
        mgr.update_status(&id1, RunStatus::Running);
        let filter = RunFilter {
            status: Some(RunStatus::Running),
            ..RunFilter::default()
        };
        let result = svc.list(filter).await.unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].status, RunStatus::Running);
    }

    #[tokio::test]
    async fn list_respects_limit() {
        let (mgr, svc) = setup();
        for i in 0..10 {
            let _ = mgr.create_run(format!("pipe-{i}"));
        }
        let filter = RunFilter {
            limit: 3,
            ..RunFilter::default()
        };
        let result = svc.list(filter).await.unwrap();
        assert_eq!(result.items.len(), 3);
    }

    #[tokio::test]
    async fn list_sorted_by_started_at_desc() {
        let (mgr, svc) = setup();
        // Create runs sequentially; ULID timestamps are monotonically increasing,
        // and started_at is Utc::now() at creation time.
        let _ = mgr.create_run("first".into());
        let _ = mgr.create_run("second".into());
        let _ = mgr.create_run("third".into());
        let result = svc.list(RunFilter::default()).await.unwrap();
        // Most recent first.
        for window in result.items.windows(2) {
            assert!(window[0].started_at >= window[1].started_at);
        }
    }

    #[tokio::test]
    async fn list_empty() {
        let (_, svc) = setup();
        let result = svc.list(RunFilter::default()).await.unwrap();
        assert!(result.items.is_empty());
    }

    #[tokio::test]
    async fn list_summary_has_correct_fields() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("my-pipe".into());
        mgr.update_status(&run_id, RunStatus::Running);
        let result = svc.list(RunFilter::default()).await.unwrap();
        assert_eq!(result.items.len(), 1);
        let summary = &result.items[0];
        assert_eq!(summary.pipeline, "my-pipe");
        assert_eq!(summary.status, RunStatus::Running);
        assert!(summary.duration_secs.is_none()); // not finished yet
        assert!(summary.records_written.is_none()); // no result yet
    }

    // ----- events -----

    #[tokio::test]
    async fn events_nonexistent_returns_not_found() {
        let (_, svc) = setup();
        let result = svc.events("run_nope").await;
        assert!(matches!(result, Err(ApiError::NotFound { .. })));
    }

    #[tokio::test]
    async fn events_receives_and_terminates() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());

        let mut stream = svc.events(&run_id).await.unwrap();

        // Send some events.
        mgr.send_event(
            &run_id,
            SseEvent::Progress {
                phase: "source".into(),
                stream: "users".into(),
                records_read: Some(50),
                records_written: None,
                bytes_read: None,
            },
        );
        mgr.send_event(
            &run_id,
            SseEvent::Complete {
                run_id: run_id.clone(),
                status: RunStatus::Completed,
                duration_secs: 1.5,
                counts: None,
            },
        );

        // First event should be progress.
        let event = stream.next().await.unwrap();
        assert!(matches!(event, SseEvent::Progress { .. }));

        // Second event should be complete (terminal), then stream ends.
        let event = stream.next().await.unwrap();
        assert!(matches!(event, SseEvent::Complete { .. }));

        // Stream should be exhausted after terminal event.
        let next = stream.next().await;
        assert!(next.is_none());
    }

    // ----- events: terminal run returns final event immediately (Bug 4) -----

    #[tokio::test]
    async fn events_completed_run_returns_final_event() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        let result = rapidbyte_engine::PipelineResult {
            counts: rapidbyte_engine::PipelineCounts {
                records_read: 42,
                records_written: 40,
                bytes_read: 1024,
                bytes_written: 900,
            },
            source: rapidbyte_engine::SourceTiming::default(),
            dest: rapidbyte_engine::DestTiming::default(),
            num_transforms: 0,
            total_transform_secs: 0.0,
            transform_load_times_ms: vec![],
            duration_secs: 2.0,
            wasm_overhead_secs: 0.0,
            retry_count: 0,
            parallelism: 1,
            stream_metrics: vec![],
        };
        mgr.complete(&run_id, result);

        // Subscribe AFTER completion — should not hang.
        let mut stream = svc.events(&run_id).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(matches!(event, SseEvent::Complete { .. }));

        // Stream should close after the single terminal event.
        let next = stream.next().await;
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn events_failed_run_returns_final_event() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        mgr.fail(&run_id, "connection refused".into());

        let mut stream = svc.events(&run_id).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(
            matches!(event, SseEvent::Failed { ref error, .. } if error == "connection refused")
        );

        let next = stream.next().await;
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn events_cancelled_run_returns_final_event() {
        let (mgr, svc) = setup();
        let run_id = mgr.create_run("p".into());
        mgr.update_status(&run_id, RunStatus::Cancelled);

        let mut stream = svc.events(&run_id).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(matches!(event, SseEvent::Cancelled { .. }));

        let next = stream.next().await;
        assert!(next.is_none());
    }

    // ----- batch stubs -----

    #[tokio::test]
    async fn get_batch_returns_not_implemented() {
        let (_, svc) = setup();
        let err = svc.get_batch("batch_123").await.unwrap_err();
        assert!(matches!(err, ApiError::NotImplemented { .. }));
    }

    #[tokio::test]
    async fn batch_events_returns_not_implemented() {
        let (_, svc) = setup();
        let result = svc.batch_events("batch_123").await;
        assert!(matches!(result, Err(ApiError::NotImplemented { .. })));
    }
}
