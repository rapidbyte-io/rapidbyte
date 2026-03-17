//! Integration tests for the v2 agent session flow.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use rapidbyte_agent::proto::rapidbyte::v2::agent_message;
use rapidbyte_agent::proto::rapidbyte::v2::agent_session_server::{
    AgentSession, AgentSessionServer,
};
use rapidbyte_agent::proto::rapidbyte::v2::controller_message;
use rapidbyte_agent::proto::rapidbyte::v2::{
    AgentMessage, ControllerMessage, Progress, SessionAccepted, SessionHello, TaskAssignment,
    TaskCompletion,
};
use rapidbyte_agent::worker::open_v2_session;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

type ControllerStream =
    Pin<Box<dyn Stream<Item = Result<ControllerMessage, tonic::Status>> + Send>>;

#[derive(Clone, Default)]
struct MockSessionServer {
    inbound_messages: Arc<Mutex<Vec<AgentMessage>>>,
}

#[tonic::async_trait]
impl AgentSession for MockSessionServer {
    type OpenSessionStream = ControllerStream;

    async fn open_session(
        &self,
        request: tonic::Request<tonic::Streaming<AgentMessage>>,
    ) -> Result<tonic::Response<Self::OpenSessionStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .next()
            .await
            .transpose()?
            .ok_or_else(|| tonic::Status::invalid_argument("expected hello"))?;
        self.inbound_messages.lock().unwrap().push(first);

        let observed = self.inbound_messages.clone();
        tokio::spawn(async move {
            while let Some(message) = inbound.next().await {
                if let Ok(message) = message {
                    observed.lock().unwrap().push(message);
                }
            }
        });

        let stream = tokio_stream::iter(vec![
            Ok(ControllerMessage {
                payload: Some(controller_message::Payload::Accepted(SessionAccepted {
                    session_id: "session-agent-test".to_owned(),
                    agent_id: "agent-test".to_owned(),
                    registry_url: String::new(),
                    registry_insecure: false,
                    trust_policy: "skip".to_owned(),
                    trusted_key_pems: vec![],
                })),
            }),
            Ok(ControllerMessage {
                payload: Some(controller_message::Payload::Assignment(TaskAssignment {
                    run_id: "run-1".to_owned(),
                    task_id: "task-1".to_owned(),
                    lease_epoch: 4,
                    attempt: 1,
                    pipeline_yaml_utf8: b"pipeline: test\n".to_vec(),
                    dry_run: false,
                    limit: None,
                    lease_expires_at: None,
                    execution: None,
                })),
            }),
        ]);

        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

#[tokio::test]
async fn session_flow_carries_assignment_progress_and_completion() {
    let server = MockSessionServer::default();
    let observed = server.inbound_messages.clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind should succeed");
    let addr = listener.local_addr().expect("local addr should resolve");

    let server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(AgentSessionServer::new(server))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("mock session server should run");
    });

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint should be valid")
        .connect()
        .await
        .expect("channel connect should succeed");

    let (mut session, _accepted) = open_v2_session(
        channel,
        SessionHello {
            agent_id: "agent-test".to_owned(),
            max_tasks: 1,
            flight_advertise_endpoint: "127.0.0.1:9091".to_owned(),
            plugin_bundle_hash: String::new(),
            available_plugins: vec![],
            memory_bytes: 0,
        },
        None,
    )
    .await
    .expect("session should open");
    let assignment = session
        .next_controller_message()
        .await
        .expect("receive should succeed")
        .expect("assignment should be present");
    assert!(matches!(
        assignment.payload,
        Some(controller_message::Payload::Assignment(_))
    ));

    session
        .send_message(AgentMessage {
            agent_id: "agent-test".to_owned(),
            payload: Some(agent_message::Payload::Progress(Progress {
                task_id: "task-1".to_owned(),
                lease_epoch: 4,
                records: 12,
            })),
        })
        .expect("progress message should send");
    session
        .send_message(AgentMessage {
            agent_id: "agent-test".to_owned(),
            payload: Some(agent_message::Payload::Completion(TaskCompletion {
                task_id: "task-1".to_owned(),
                lease_epoch: 4,
                success: true,
                records_processed: 12,
                bytes_processed: 1024,
                elapsed_seconds: 0.5,
                cursors_advanced: 1,
            })),
        })
        .expect("completion message should send");
    drop(session);

    for _ in 0..30 {
        if observed.lock().unwrap().len() >= 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let payloads = observed
        .lock()
        .unwrap()
        .iter()
        .filter_map(|message| message.payload.clone())
        .collect::<Vec<_>>();

    assert!(matches!(
        payloads.first(),
        Some(agent_message::Payload::Hello(SessionHello { .. }))
    ));
    assert!(payloads.iter().any(|payload| matches!(
        payload,
        agent_message::Payload::Progress(Progress { task_id, .. }) if task_id == "task-1"
    )));
    assert!(payloads.iter().any(|payload| matches!(
        payload,
        agent_message::Payload::Completion(TaskCompletion { task_id, success: true, .. }) if task_id == "task-1"
    )));

    server_task.abort();
}
