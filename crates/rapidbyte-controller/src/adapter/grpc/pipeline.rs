//! gRPC adapter for `PipelineService`.

use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::adapter::grpc::convert;
use crate::application::services::AppServices;
use crate::domain::ports::repository::{Pagination, RunFilter};
use crate::proto::rapidbyte::v1 as pb;
use crate::proto::rapidbyte::v1::pipeline_service_server::PipelineService;

pub struct PipelineGrpcService {
    services: Arc<AppServices>,
}

impl PipelineGrpcService {
    #[must_use]
    pub fn new(services: Arc<AppServices>) -> Self {
        Self { services }
    }
}

#[tonic::async_trait]
impl PipelineService for PipelineGrpcService {
    async fn submit_pipeline(
        &self,
        req: Request<pb::SubmitPipelineRequest>,
    ) -> Result<Response<pb::SubmitPipelineResponse>, Status> {
        let req = req.into_inner();
        let max_retries = req.options.as_ref().map_or(0, |o| o.max_retries);
        let timeout_seconds = req.options.as_ref().and_then(|o| {
            if o.timeout_seconds > 0 {
                Some(o.timeout_seconds)
            } else {
                None
            }
        });
        let idempotency_key = if req.idempotency_key.is_empty() {
            None
        } else {
            Some(req.idempotency_key)
        };

        let result = crate::application::submit::submit_pipeline(
            &self.services.ctx,
            idempotency_key,
            req.pipeline_yaml,
            max_retries,
            timeout_seconds,
        )
        .await
        .map_err(convert::app_error_to_status)?;

        Ok(Response::new(pb::SubmitPipelineResponse {
            run_id: result.run_id,
            already_exists: result.already_exists,
        }))
    }

    async fn get_run(
        &self,
        req: Request<pb::GetRunRequest>,
    ) -> Result<Response<pb::GetRunResponse>, Status> {
        let run_id = req.into_inner().run_id;
        let run = crate::application::query::get_run(&self.services.ctx, &run_id)
            .await
            .map_err(convert::app_error_to_status)?;

        Ok(Response::new(pb::GetRunResponse {
            run: Some(convert::run_to_detail(&run)),
        }))
    }

    async fn cancel_run(
        &self,
        req: Request<pb::CancelRunRequest>,
    ) -> Result<Response<pb::CancelRunResponse>, Status> {
        let run_id = req.into_inner().run_id;
        let result = crate::application::cancel::cancel_run(&self.services.ctx, &run_id)
            .await
            .map_err(convert::app_error_to_status)?;

        Ok(Response::new(pb::CancelRunResponse {
            accepted: result.accepted,
        }))
    }

    async fn list_runs(
        &self,
        req: Request<pb::ListRunsRequest>,
    ) -> Result<Response<pb::ListRunsResponse>, Status> {
        let req = req.into_inner();

        // Validate state_filter: reject unknown enum values instead of silently ignoring
        let state = if let Some(raw) = req.state_filter {
            if raw == 0 {
                // UNSPECIFIED → no filter
                None
            } else {
                Some(convert::proto_to_run_state_filter(raw).ok_or_else(|| {
                    Status::invalid_argument(format!("unknown state_filter value: {raw}"))
                })?)
            }
        } else {
            None
        };

        let page_token = if req.page_token.is_empty() {
            None
        } else {
            Some(req.page_token)
        };

        // Default page_size to 20 when 0 (proto default), cap at 1000
        let page_size = if req.page_size == 0 {
            20
        } else {
            req.page_size.min(1000)
        };

        let page = crate::application::query::list_runs(
            &self.services.ctx,
            RunFilter { state },
            Pagination {
                page_size,
                page_token,
            },
        )
        .await
        .map_err(convert::app_error_to_status)?;

        Ok(Response::new(pb::ListRunsResponse {
            runs: page.runs.iter().map(convert::run_to_summary).collect(),
            next_page_token: page.next_page_token.unwrap_or_default(),
        }))
    }

    type WatchRunStream = Pin<Box<dyn Stream<Item = Result<pb::RunEvent, Status>> + Send>>;

    #[allow(clippy::result_large_err)]
    async fn watch_run(
        &self,
        req: Request<pb::WatchRunRequest>,
    ) -> Result<Response<Self::WatchRunStream>, Status> {
        let run_id = req.into_inner().run_id;

        // Subscribe FIRST to avoid missing events between snapshot and subscribe.
        let event_stream = self
            .services
            .ctx
            .event_bus
            .subscribe(&run_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Validate run exists and get current state for catch-up.
        let run = match crate::application::query::get_run(&self.services.ctx, &run_id).await {
            Ok(run) => run,
            Err(e) => {
                drop(event_stream);
                self.services.ctx.event_bus.cleanup(&run_id).await;
                return Err(convert::app_error_to_status(e));
            }
        };

        let snapshot_state = run.state();
        let snapshot_attempt = run.current_attempt();
        let initial = convert::run_state_to_event(&run);

        // Filter subscription events: skip events that exactly match the snapshot
        // (state AND attempt), which are duplicates from events queued between
        // subscribe and get_run. Events with different attempts (retries) or
        // different states always pass through.
        let deduped = event_stream.filter_map(move |event| {
            let is_duplicate = match &event {
                crate::domain::event::DomainEvent::RunStateChanged { state, attempt, .. } => {
                    *state == snapshot_state && *attempt == snapshot_attempt
                }
                _ => false, // progress/completed/failed/cancelled always pass
            };
            if is_duplicate {
                None
            } else {
                Some(event)
            }
        });

        let mapped = deduped.map(|event| Ok(convert::domain_event_to_run_event(&event)));

        // Emit snapshot as first event, then live events
        let initial_stream = tokio_stream::once(Ok(initial));

        // Wrap in a cleanup stream that removes the subscriber entry on drop
        let event_bus = self.services.ctx.event_bus.clone();
        let cleanup_run_id = run_id.clone();
        let combined = initial_stream.chain(mapped);
        let with_cleanup = CleanupStream {
            inner: Box::pin(combined),
            event_bus: Some(event_bus),
            run_id: cleanup_run_id,
        };

        Ok(Response::new(Box::pin(with_cleanup)))
    }
}

use crate::domain::ports::event_bus::EventBus;
use std::task::{Context, Poll};

/// A stream wrapper that cleans up the event bus subscriber entry when dropped.
struct CleanupStream {
    inner: Pin<Box<dyn Stream<Item = Result<pb::RunEvent, Status>> + Send>>,
    event_bus: Option<Arc<dyn EventBus>>,
    run_id: String,
}

impl Stream for CleanupStream {
    type Item = Result<pb::RunEvent, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl Drop for CleanupStream {
    fn drop(&mut self) {
        if let Some(bus) = self.event_bus.take() {
            let run_id = self.run_id.clone();
            // Spawn cleanup as a background task since Drop can't be async
            tokio::spawn(async move {
                bus.cleanup(&run_id).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio_stream::StreamExt;
    use tonic::Request;

    use crate::application::services::AppServices;
    use crate::application::testing::fake_context;
    use crate::proto::rapidbyte::v1 as pb;
    use crate::proto::rapidbyte::v1::pipeline_service_server::PipelineService;

    use super::PipelineGrpcService;

    const YAML: &str = "pipeline: test-pipe\nversion: '1.0'";

    fn make_service(services: Arc<AppServices>) -> PipelineGrpcService {
        PipelineGrpcService::new(services)
    }

    #[tokio::test]
    async fn submit_empty_idempotency_key_treated_as_none() {
        let tc = fake_context();
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:50051".parse().unwrap(),
        ));
        let svc = make_service(Arc::clone(&services));

        let first = svc
            .submit_pipeline(Request::new(pb::SubmitPipelineRequest {
                pipeline_yaml: YAML.to_string(),
                idempotency_key: String::new(),
                options: None,
            }))
            .await
            .unwrap()
            .into_inner();

        let second = svc
            .submit_pipeline(Request::new(pb::SubmitPipelineRequest {
                pipeline_yaml: YAML.to_string(),
                idempotency_key: String::new(),
                options: None,
            }))
            .await
            .unwrap()
            .into_inner();

        // "" is treated as None, so each call creates a new run
        assert_ne!(first.run_id, second.run_id);
        assert!(!first.already_exists);
        assert!(!second.already_exists);
    }

    #[tokio::test]
    async fn submit_options_default_when_missing() {
        let tc = fake_context();
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:50051".parse().unwrap(),
        ));
        let svc = make_service(Arc::clone(&services));

        let resp = svc
            .submit_pipeline(Request::new(pb::SubmitPipelineRequest {
                pipeline_yaml: YAML.to_string(),
                idempotency_key: String::new(),
                options: None,
            }))
            .await
            .unwrap()
            .into_inner();

        // Verify max_retries defaults to 0 via get_run
        let run_resp = svc
            .get_run(Request::new(pb::GetRunRequest {
                run_id: resp.run_id,
            }))
            .await
            .unwrap()
            .into_inner();

        let detail = run_resp.run.unwrap();
        assert_eq!(detail.max_retries, 0);
    }

    #[tokio::test]
    async fn list_runs_page_size_zero_defaults_to_twenty() {
        let tc = fake_context();
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:50051".parse().unwrap(),
        ));
        let svc = make_service(Arc::clone(&services));

        // Submit 25 runs
        for _ in 0..25 {
            svc.submit_pipeline(Request::new(pb::SubmitPipelineRequest {
                pipeline_yaml: YAML.to_string(),
                idempotency_key: String::new(),
                options: None,
            }))
            .await
            .unwrap();
        }

        let resp = svc
            .list_runs(Request::new(pb::ListRunsRequest {
                state_filter: None,
                page_size: 0,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.runs.len(), 20);
    }

    #[tokio::test]
    async fn list_runs_page_size_clamped_to_thousand() {
        let tc = fake_context();
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:50051".parse().unwrap(),
        ));
        let svc = make_service(Arc::clone(&services));

        // Submit one run so the response is not empty
        svc.submit_pipeline(Request::new(pb::SubmitPipelineRequest {
            pipeline_yaml: YAML.to_string(),
            idempotency_key: String::new(),
            options: None,
        }))
        .await
        .unwrap();

        let resp = svc
            .list_runs(Request::new(pb::ListRunsRequest {
                state_filter: None,
                page_size: 5000,
                page_token: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        // Should not error and should return valid results
        assert_eq!(resp.runs.len(), 1);
    }

    #[tokio::test]
    async fn list_runs_unknown_state_filter_returns_invalid_argument() {
        let tc = fake_context();
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:50051".parse().unwrap(),
        ));
        let svc = make_service(services);

        let status = svc
            .list_runs(Request::new(pb::ListRunsRequest {
                state_filter: Some(99),
                page_size: 10,
                page_token: String::new(),
            }))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn cancel_run_returns_accepted_field() {
        let tc = fake_context();
        let services = Arc::new(AppServices::new(
            Arc::new(tc.ctx),
            chrono::Utc::now(),
            "0.0.0.0:50051".parse().unwrap(),
        ));
        let svc = make_service(Arc::clone(&services));

        // Submit a pipeline (Pending)
        let submit = svc
            .submit_pipeline(Request::new(pb::SubmitPipelineRequest {
                pipeline_yaml: YAML.to_string(),
                idempotency_key: String::new(),
                options: None,
            }))
            .await
            .unwrap()
            .into_inner();

        // Cancel the Pending run → accepted=true
        let first_cancel = svc
            .cancel_run(Request::new(pb::CancelRunRequest {
                run_id: submit.run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(first_cancel.accepted);

        // Cancel the same run again (now Cancelled) → accepted=false
        let second_cancel = svc
            .cancel_run(Request::new(pb::CancelRunRequest {
                run_id: submit.run_id,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!second_cancel.accepted);
    }

    #[tokio::test]
    async fn watch_run_dedup_skips_exact_snapshot_match() {
        let tc = fake_context();
        let ctx = Arc::new(tc.ctx);
        let services = Arc::new(AppServices::new(
            Arc::clone(&ctx),
            chrono::Utc::now(),
            "0.0.0.0:50051".parse().unwrap(),
        ));
        let svc = make_service(Arc::clone(&services));

        // Submit a pipeline
        let submit = svc
            .submit_pipeline(Request::new(pb::SubmitPipelineRequest {
                pipeline_yaml: YAML.to_string(),
                idempotency_key: String::new(),
                options: None,
            }))
            .await
            .unwrap()
            .into_inner();

        // Watch the run
        let mut stream = svc
            .watch_run(Request::new(pb::WatchRunRequest {
                run_id: submit.run_id.clone(),
            }))
            .await
            .unwrap()
            .into_inner();

        // First event should be the initial state (Pending)
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.run_id, submit.run_id);
        assert_eq!(
            first.state,
            crate::adapter::grpc::convert::run_state_to_proto(
                crate::domain::run::RunState::Pending
            )
        );

        // Publish a duplicate RunStateChanged (same state + attempt as snapshot)
        ctx.event_bus
            .publish(crate::domain::event::DomainEvent::RunStateChanged {
                run_id: submit.run_id.clone(),
                state: crate::domain::run::RunState::Pending,
                attempt: 1,
            })
            .await
            .unwrap();

        // The dedup filter should skip this duplicate. Verify no second event
        // arrives within 100ms.
        let timeout_result =
            tokio::time::timeout(std::time::Duration::from_millis(100), stream.next()).await;
        assert!(
            timeout_result.is_err(),
            "expected no event within 100ms (dedup should have filtered the duplicate)"
        );
    }
}
