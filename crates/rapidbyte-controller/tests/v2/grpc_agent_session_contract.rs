use rapidbyte_controller::adapters::grpc::agent_bridge::AgentHandler;
use rapidbyte_controller::adapters::grpc::agent_session::AgentSessionHandler;
use rapidbyte_controller::proto::rapidbyte::v2::{agent_message, AgentMessage, SessionHello};
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
