use std::sync::Arc;

use rapidbyte_controller::adapters::grpc::control_plane::ControlPlaneHandler;
use rapidbyte_controller::adapters::in_memory::repos::InMemoryRunRepository;
use rapidbyte_controller::adapters::in_memory::unit_of_work::InMemoryUnitOfWork;
use rapidbyte_controller::app::cancel_run::CancelRunUseCase;
use rapidbyte_controller::app::get_run::GetRunUseCase;
use rapidbyte_controller::app::list_runs::ListRunsUseCase;
use rapidbyte_controller::app::retry_run::RetryRunUseCase;
use rapidbyte_controller::app::submit_run::SubmitRunUseCase;
use rapidbyte_controller::domain::run::RunId;
use rapidbyte_controller::ports::clock::Clock;
use rapidbyte_controller::ports::event_bus::EventBus;
use rapidbyte_controller::ports::id_generator::IdGenerator;
use rapidbyte_controller::proto::rapidbyte::v2::control_plane_server::ControlPlane as _;
use rapidbyte_controller::proto::rapidbyte::v2::{GetRunRequest, SubmitRunRequest};

struct TestClock;

impl Clock for TestClock {}

struct TestEventBus;

impl EventBus for TestEventBus {}

struct TestIdGenerator;

impl IdGenerator for TestIdGenerator {
    fn new_run_id(&self) -> RunId {
        RunId::new("grpc-run-1")
    }
}

#[tokio::test]
async fn submit_and_get_run_roundtrip() {
    let repo = Arc::new(InMemoryRunRepository::default());
    let uow = Arc::new(InMemoryUnitOfWork::new(repo.clone()));
    let handler = ControlPlaneHandler::new(
        SubmitRunUseCase::new(
            repo.clone(),
            Arc::new(TestEventBus),
            Arc::new(TestClock),
            Arc::new(TestIdGenerator),
            uow.clone(),
        ),
        GetRunUseCase::new(repo.clone()),
        ListRunsUseCase::new(repo.clone()),
        CancelRunUseCase::new(repo.clone(), uow.clone()),
        RetryRunUseCase::new(repo.clone(), uow),
    );

    let submit = handler
        .submit_run(tonic::Request::new(SubmitRunRequest {
            idempotency_key: Some("grpc-key".to_owned()),
        }))
        .await
        .expect("submit should succeed")
        .into_inner();

    let get = handler
        .get_run(tonic::Request::new(GetRunRequest {
            run_id: submit.run_id.clone(),
        }))
        .await
        .expect("get should succeed")
        .into_inner();

    assert_eq!(submit.run_id, get.run_id);
}
