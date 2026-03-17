//! v2 `ControlPlane` gRPC handler.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_stream::Stream;

use crate::app::cancel_run::{CancelRunCommand, CancelRunUseCase};
use crate::app::get_run::{GetRunCommand, GetRunUseCase};
use crate::app::list_runs::{ListRunsCommand, ListRunsUseCase};
use crate::app::retry_run::{RetryRunCommand, RetryRunUseCase};
use crate::app::submit_run::{SubmitRunCommand, SubmitRunUseCase};
use crate::domain::run::RunId;
use crate::ports::clock::Clock;
use crate::ports::event_bus::EventBus;
use crate::ports::id_generator::IdGenerator;
use crate::ports::repositories::RunRepository;
use crate::ports::unit_of_work::UnitOfWork;
use crate::proto::rapidbyte::v2::control_plane_server::ControlPlane;
use crate::proto::rapidbyte::v2::{
    CancelRunRequest, CancelRunResponse, GetRunRequest, GetRunResponse, ListRunsRequest,
    ListRunsResponse, RetryRunRequest, RetryRunResponse, RunEvent, RunSummary, StreamRunRequest,
    SubmitRunRequest, SubmitRunResponse,
};

type RunEventStream = Pin<Box<dyn Stream<Item = Result<RunEvent, tonic::Status>> + Send>>;

#[async_trait]
pub trait SubmitRunExecutor: Send + Sync {
    async fn execute_submit(
        &self,
        command: SubmitRunCommand,
    ) -> anyhow::Result<crate::app::submit_run::SubmitRunOutput>;
}

#[async_trait]
impl<R, E, C, I, U> SubmitRunExecutor for SubmitRunUseCase<R, E, C, I, U>
where
    R: RunRepository,
    E: EventBus,
    C: Clock,
    I: IdGenerator,
    U: UnitOfWork,
{
    async fn execute_submit(
        &self,
        command: SubmitRunCommand,
    ) -> anyhow::Result<crate::app::submit_run::SubmitRunOutput> {
        self.execute(command).await
    }
}

#[async_trait]
pub trait GetRunExecutor: Send + Sync {
    async fn execute_get(
        &self,
        command: GetRunCommand,
    ) -> anyhow::Result<crate::app::get_run::GetRunOutput>;
}

#[async_trait]
impl<R> GetRunExecutor for GetRunUseCase<R>
where
    R: RunRepository,
{
    async fn execute_get(
        &self,
        command: GetRunCommand,
    ) -> anyhow::Result<crate::app::get_run::GetRunOutput> {
        self.execute(command).await
    }
}

#[async_trait]
pub trait ListRunsExecutor: Send + Sync {
    async fn execute_list(
        &self,
        command: ListRunsCommand,
    ) -> anyhow::Result<crate::app::list_runs::ListRunsOutput>;
}

#[async_trait]
impl<R> ListRunsExecutor for ListRunsUseCase<R>
where
    R: RunRepository,
{
    async fn execute_list(
        &self,
        command: ListRunsCommand,
    ) -> anyhow::Result<crate::app::list_runs::ListRunsOutput> {
        self.execute(command).await
    }
}

#[async_trait]
pub trait CancelRunExecutor: Send + Sync {
    async fn execute_cancel(&self, command: CancelRunCommand) -> anyhow::Result<()>;
}

#[async_trait]
impl<R, U> CancelRunExecutor for CancelRunUseCase<R, U>
where
    R: RunRepository,
    U: UnitOfWork,
{
    async fn execute_cancel(&self, command: CancelRunCommand) -> anyhow::Result<()> {
        self.execute(command).await
    }
}

#[async_trait]
pub trait RetryRunExecutor: Send + Sync {
    async fn execute_retry(&self, command: RetryRunCommand) -> anyhow::Result<()>;
}

#[async_trait]
impl<R, U> RetryRunExecutor for RetryRunUseCase<R, U>
where
    R: RunRepository,
    U: UnitOfWork,
{
    async fn execute_retry(&self, command: RetryRunCommand) -> anyhow::Result<()> {
        self.execute(command).await
    }
}

pub struct ControlPlaneHandler {
    submit: Arc<dyn SubmitRunExecutor>,
    get: Arc<dyn GetRunExecutor>,
    list: Arc<dyn ListRunsExecutor>,
    cancel: Arc<dyn CancelRunExecutor>,
    retry: Arc<dyn RetryRunExecutor>,
}

impl ControlPlaneHandler {
    #[must_use]
    pub fn new<S, G, L, C, R>(submit: S, get: G, list: L, cancel: C, retry: R) -> Self
    where
        S: SubmitRunExecutor + 'static,
        G: GetRunExecutor + 'static,
        L: ListRunsExecutor + 'static,
        C: CancelRunExecutor + 'static,
        R: RetryRunExecutor + 'static,
    {
        Self {
            submit: Arc::new(submit),
            get: Arc::new(get),
            list: Arc::new(list),
            cancel: Arc::new(cancel),
            retry: Arc::new(retry),
        }
    }
}

#[tonic::async_trait]
impl ControlPlane for ControlPlaneHandler {
    type StreamRunStream = RunEventStream;

    async fn submit_run(
        &self,
        request: tonic::Request<SubmitRunRequest>,
    ) -> Result<tonic::Response<SubmitRunResponse>, tonic::Status> {
        let output = self
            .submit
            .execute_submit(SubmitRunCommand {
                idempotency_key: request.into_inner().idempotency_key,
            })
            .await
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        Ok(tonic::Response::new(SubmitRunResponse {
            run_id: output.run_id,
        }))
    }

    async fn get_run(
        &self,
        request: tonic::Request<GetRunRequest>,
    ) -> Result<tonic::Response<GetRunResponse>, tonic::Status> {
        let output = self
            .get
            .execute_get(GetRunCommand {
                run_id: RunId::new(request.into_inner().run_id),
            })
            .await
            .map_err(|error| tonic::Status::not_found(error.to_string()))?;
        Ok(tonic::Response::new(GetRunResponse {
            run_id: output.run_id,
            state: format!("{:?}", output.state),
        }))
    }

    async fn list_runs(
        &self,
        request: tonic::Request<ListRunsRequest>,
    ) -> Result<tonic::Response<ListRunsResponse>, tonic::Status> {
        let limit = request
            .into_inner()
            .limit
            .and_then(|value| usize::try_from(value).ok());
        let output = self
            .list
            .execute_list(ListRunsCommand { limit })
            .await
            .map_err(|error| tonic::Status::internal(error.to_string()))?;

        let runs = output
            .runs
            .into_iter()
            .map(|item| RunSummary {
                run_id: item.run_id,
                state: format!("{:?}", item.state),
            })
            .collect();

        Ok(tonic::Response::new(ListRunsResponse { runs }))
    }

    async fn cancel_run(
        &self,
        request: tonic::Request<CancelRunRequest>,
    ) -> Result<tonic::Response<CancelRunResponse>, tonic::Status> {
        self.cancel
            .execute_cancel(CancelRunCommand {
                run_id: RunId::new(request.into_inner().run_id),
            })
            .await
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        Ok(tonic::Response::new(CancelRunResponse {
            accepted: true,
            message: String::new(),
        }))
    }

    async fn retry_run(
        &self,
        request: tonic::Request<RetryRunRequest>,
    ) -> Result<tonic::Response<RetryRunResponse>, tonic::Status> {
        self.retry
            .execute_retry(RetryRunCommand {
                run_id: RunId::new(request.into_inner().run_id),
            })
            .await
            .map_err(|error| tonic::Status::failed_precondition(error.to_string()))?;
        Ok(tonic::Response::new(RetryRunResponse { accepted: true }))
    }

    async fn stream_run(
        &self,
        _request: tonic::Request<StreamRunRequest>,
    ) -> Result<tonic::Response<Self::StreamRunStream>, tonic::Status> {
        let stream = tokio_stream::iter(Vec::<Result<RunEvent, tonic::Status>>::new());
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}
