use rapidbyte_controller::adapters::grpc::control_plane::ControlPlaneHandler;
use rapidbyte_controller::proto::rapidbyte::v2::control_plane_server::ControlPlane as _;
use rapidbyte_controller::proto::rapidbyte::v2::run_event;
use rapidbyte_controller::proto::rapidbyte::v2::{
    GetRunRequest, StreamRunRequest, SubmitRunRequest,
};
use rapidbyte_controller::run_state::RunState;
use rapidbyte_controller::state::ControllerState;
use rapidbyte_controller::terminal::{finalize_terminal, TerminalOutcome};
use tokio_stream::StreamExt;

#[tokio::test]
async fn submit_and_get_run_roundtrip() {
    let state = ControllerState::new(b"grpc-test-signing-key");
    let handler = ControlPlaneHandler::new(state.clone());

    let submit = handler
        .submit_run(tonic::Request::new(SubmitRunRequest {
            idempotency_key: Some("grpc-key".to_owned()),
            pipeline_yaml_utf8: b"pipeline: grpc\nstate:\n  backend: postgres\n".to_vec(),
            execution: None,
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
    assert_eq!(get.state, "Pending");
    assert_eq!(state.tasks.read().await.all_tasks().len(), 1);
}

#[tokio::test]
async fn stream_run_emits_terminal_event_for_completed_run() {
    let state = ControllerState::new(b"grpc-test-signing-key");
    let handler = ControlPlaneHandler::new(state.clone());

    let run_id = {
        let mut runs = state.runs.write().await;
        let (run_id, _) = runs.create_run("run-1".to_owned(), "pipe".to_owned(), None);
        runs.transition(&run_id, RunState::Assigned).unwrap();
        runs.transition(&run_id, RunState::Running).unwrap();
        run_id
    };

    finalize_terminal(
        &state,
        &run_id,
        TerminalOutcome::Completed {
            metrics: rapidbyte_controller::run_state::RunMetrics {
                total_records: 10,
                total_bytes: 1024,
                elapsed_seconds: 1.5,
                cursors_advanced: 0,
            },
        },
    )
    .await;

    let mut stream = handler
        .stream_run(tonic::Request::new(StreamRunRequest {
            run_id: run_id.clone(),
        }))
        .await
        .expect("stream should open")
        .into_inner();

    let first = stream
        .next()
        .await
        .expect("terminal event should be emitted")
        .expect("event should be valid");

    assert_eq!(first.run_id, run_id);
    match first.event {
        Some(run_event::Event::Completed(completed)) => {
            assert_eq!(completed.total_records, 10);
            assert_eq!(completed.total_bytes, 1024);
        }
        other => panic!("expected completed event, got {other:?}"),
    }
}
