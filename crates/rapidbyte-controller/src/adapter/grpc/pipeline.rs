//! gRPC adapter for `PipelineService`.

use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::adapter::grpc::convert;
use crate::application::context::AppContext;
use crate::domain::ports::repository::{Pagination, RunFilter};
use crate::proto::rapidbyte::v1 as pb;
use crate::proto::rapidbyte::v1::pipeline_service_server::PipelineService;

pub struct PipelineGrpcService {
    ctx: Arc<AppContext>,
}

impl PipelineGrpcService {
    #[must_use]
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx }
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
            &self.ctx,
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
        let run = crate::application::query::get_run(&self.ctx, &run_id)
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
        let result = crate::application::cancel::cancel_run(&self.ctx, &run_id)
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
            &self.ctx,
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
            .ctx
            .event_bus
            .subscribe(&run_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Validate run exists and get current state for catch-up.
        let run = match crate::application::query::get_run(&self.ctx, &run_id).await {
            Ok(run) => run,
            Err(e) => {
                drop(event_stream);
                self.ctx.event_bus.cleanup(&run_id).await;
                return Err(convert::app_error_to_status(e));
            }
        };

        let snapshot_state = run.state();
        let initial = convert::run_state_to_event(&run);

        // Filter subscription events: skip any state-change events that don't
        // advance past the snapshot (prevents out-of-order regression). Progress
        // events are always forwarded since they're ephemeral.
        let snapshot_proto_state = convert::run_state_to_proto(snapshot_state);
        let deduped = event_stream.filter_map(move |event| {
            let dominated = match &event {
                crate::domain::event::DomainEvent::RunStateChanged { state, .. } => {
                    convert::run_state_to_proto(*state) <= snapshot_proto_state
                }
                _ => false, // progress/completed/failed/cancelled always pass
            };
            if dominated {
                None
            } else {
                Some(event)
            }
        });

        let mapped = deduped.map(|event| Ok(convert::domain_event_to_run_event(&event)));

        // Emit snapshot as first event, then live events
        let initial_stream = tokio_stream::once(Ok(initial));

        // Wrap in a cleanup stream that removes the subscriber entry on drop
        let event_bus = self.ctx.event_bus.clone();
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
