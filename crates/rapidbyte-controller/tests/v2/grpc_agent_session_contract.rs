use rapidbyte_controller::adapters::grpc::agent_bridge::AgentHandler;
use rapidbyte_controller::adapters::grpc::agent_session::AgentSessionHandler;
use rapidbyte_controller::proto::rapidbyte::v2::{
    agent_message, controller_message, AgentMessage, SessionHello, TaskCompletion, TaskOutcome,
};
use rapidbyte_controller::run_state::RunState;
use rapidbyte_controller::state::ControllerState;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

#[tokio::test]
async fn open_session_emits_session_accepted() {
    let state = ControllerState::new(b"test-key-for-v2-session-bridge");
    let handler = AgentSessionHandler::new(AgentHandler::new(state));

    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(AgentMessage {
        agent_id: "agent-1".to_owned(),
        payload: Some(agent_message::Payload::Hello(SessionHello {
            agent_id: "agent-1".to_owned(),
            max_tasks: 1,
            flight_advertise_endpoint: "127.0.0.1:9091".to_owned(),
            plugin_bundle_hash: String::new(),
            available_plugins: vec![],
            memory_bytes: 0,
        })),
    })
    .await
    .expect("send should succeed");
    drop(tx);

    let response = handler
        .open_session_from_stream(ReceiverStream::new(rx).map(Ok))
        .await
        .expect("open_session should succeed");

    let mut stream = Box::pin(response);
    let first = stream
        .next()
        .await
        .expect("stream should yield message")
        .expect("message should be ok");

    assert!(first.payload.is_some());
}

#[tokio::test]
async fn open_session_maps_cancelled_completion_to_cancelled_run_state() {
    let state = ControllerState::new(b"test-key-for-v2-session-bridge");
    let handler = AgentSessionHandler::new(AgentHandler::new(state.clone()));

    let run_id = {
        let mut runs = state.runs.write().await;
        let (run_id, _is_new) = runs.create_run("run-1".to_owned(), "pipe".to_owned(), None);
        run_id
    };

    {
        let mut tasks = state.tasks.write().await;
        let _task_id = tasks.enqueue(run_id.clone(), b"pipeline: test\n".to_vec(), false, None, 1);
    }

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tx.send(AgentMessage {
        agent_id: "agent-1".to_owned(),
        payload: Some(agent_message::Payload::Hello(SessionHello {
            agent_id: "agent-1".to_owned(),
            max_tasks: 1,
            flight_advertise_endpoint: "127.0.0.1:9091".to_owned(),
            plugin_bundle_hash: String::new(),
            available_plugins: vec![],
            memory_bytes: 0,
        })),
    })
    .await
    .expect("send should succeed");

    let response = handler
        .open_session_from_stream(ReceiverStream::new(rx).map(Ok))
        .await
        .expect("open_session should succeed");

    let mut stream = Box::pin(response);
    let accepted = stream
        .next()
        .await
        .expect("stream should yield accepted")
        .expect("accepted should be ok");
    let Some(controller_message::Payload::Accepted(accepted)) = accepted.payload else {
        panic!("expected accepted payload");
    };

    let assignment = stream
        .next()
        .await
        .expect("stream should yield assignment")
        .expect("assignment should be ok");
    let Some(controller_message::Payload::Assignment(assignment)) = assignment.payload else {
        panic!("expected assignment payload");
    };

    tx.send(AgentMessage {
        agent_id: accepted.agent_id,
        payload: Some(agent_message::Payload::Completion(TaskCompletion {
            task_id: assignment.task_id,
            lease_epoch: assignment.lease_epoch,
            success: false,
            records_processed: 0,
            bytes_processed: 0,
            elapsed_seconds: 0.2,
            cursors_advanced: 0,
            outcome: TaskOutcome::Cancelled as i32,
            error: None,
        })),
    })
    .await
    .expect("completion send should succeed");
    drop(tx);

    for _ in 0..50 {
        let current = {
            let runs = state.runs.read().await;
            runs.get_run(&run_id).map(|run| run.state)
        };
        if current == Some(RunState::Cancelled) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let final_state = {
        let runs = state.runs.read().await;
        runs.get_run(&run_id).map(|run| run.state)
    };
    assert_eq!(final_state, Some(RunState::Cancelled));
}
