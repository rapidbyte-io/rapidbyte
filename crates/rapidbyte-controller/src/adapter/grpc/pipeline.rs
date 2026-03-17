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
        let state = req
            .state_filter
            .and_then(convert::proto_to_run_state_filter);
        let page_token = if req.page_token.is_empty() {
            None
        } else {
            Some(req.page_token)
        };

        let page = crate::application::query::list_runs(
            &self.ctx,
            RunFilter { state },
            Pagination {
                page_size: req.page_size,
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

    async fn watch_run(
        &self,
        req: Request<pb::WatchRunRequest>,
    ) -> Result<Response<Self::WatchRunStream>, Status> {
        let run_id = req.into_inner().run_id;

        // Get current state as the first event
        let run = crate::application::query::get_run(&self.ctx, &run_id)
            .await
            .map_err(convert::app_error_to_status)?;
        let initial = convert::run_state_to_event(&run);

        // Subscribe to future events
        let event_stream = self
            .ctx
            .event_bus
            .subscribe(&run_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Map domain events to proto events
        let mapped = event_stream.map(|event| Ok(convert::domain_event_to_run_event(&event)));

        // Prepend the initial event
        let initial_stream = tokio_stream::once(Ok(initial));
        let combined = initial_stream.chain(mapped);

        Ok(Response::new(Box::pin(combined)))
    }
}
