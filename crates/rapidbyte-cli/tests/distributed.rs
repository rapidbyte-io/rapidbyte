//! Integration test for the distributed pipeline flow.
//!
//! Starts a controller and agent in-process, submits a pipeline,
//! and verifies the coordination (submit -> assign -> execute -> report).

use std::sync::Arc;
use std::time::Duration;

use rapidbyte_agent::AgentConfig;
use rapidbyte_controller::proto::rapidbyte::v1::agent_service_client::AgentServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::pipeline_service_client::PipelineServiceClient;
use rapidbyte_controller::proto::rapidbyte::v1::{
    complete_task_request, poll_task_response, AgentCapabilities, CompleteTaskRequest,
    GetRunRequest, HeartbeatRequest, PollTaskRequest, RegisterRequest, RunState,
    SubmitPipelineRequest, TaskFailed, TaskHeartbeat,
};
use rapidbyte_controller::ControllerConfig;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

/// A minimal pipeline YAML with a deliberately unreachable Postgres state backend.
/// The agent will pick it up, fail fast during task execution, and report
/// the failure back — exercising the full distributed coordination path.
const TEST_PIPELINE_YAML: &str = r#"
version: "1.0"
pipeline: integration-test
source:
  use: test-nonexistent-source
  config: {}
  streams: []
destination:
  use: test-nonexistent-dest
  config: {}
state:
  backend: postgres
  connection: "host=127.0.0.1 port=1 dbname=rapidbyte_test_nonexistent connect_timeout=1"
"#;

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

struct TestMetadataSchema {
    admin_client: tokio_postgres::Client,
    connection_task: tokio::task::JoinHandle<()>,
    schema: String,
    scoped_url: String,
}

impl TestMetadataSchema {
    async fn create() -> Self {
        let admin_url = std::env::var("RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL")
            .expect("RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL must be set for this test");
        let (admin_client, connection) = tokio_postgres::connect(&admin_url, NoTls)
            .await
            .expect("admin connection should succeed");
        let connection_task = tokio::spawn(async move {
            connection
                .await
                .expect("admin connection task should stay healthy");
        });

        let schema = format!("cli_distributed_test_{}", uuid::Uuid::new_v4().simple());
        admin_client
            .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
            .await
            .expect("schema creation should succeed");

        // Build a scoped URL compatible with sqlx's PgConnectOptions parser.
        // sqlx supports options[key]=value in the query string.
        let sep = if admin_url.contains('?') { "&" } else { "?" };
        let scoped_url = format!("{admin_url}{sep}options[search_path]={schema}");

        Self {
            scoped_url,
            admin_client,
            connection_task,
            schema,
        }
    }

    async fn cleanup(self) {
        self.admin_client
            .batch_execute(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
            .await
            .expect("schema cleanup should succeed");
        self.connection_task.abort();
    }
}

async fn wait_for_controller(ctrl_url: &str) -> tonic::transport::Channel {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        match tonic::transport::Channel::from_shared(ctrl_url.to_string())
            .unwrap()
            .connect()
            .await
        {
            Ok(channel) => return channel,
            Err(error) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(?error, ctrl_url, "controller not ready yet");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("Failed to connect to controller: {error}"),
        }
    }
}

fn spawn_controller(
    ctrl_addr: String,
    metadata_database_url: String,
    signing_key: Vec<u8>,
    lease_check_interval: Duration,
    reconciliation_timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let config = ControllerConfig {
            listen_addr: ctrl_addr.parse().unwrap(),
            metadata_database_url: Some(metadata_database_url),
            auth: rapidbyte_controller::config::AuthConfig {
                allow_unauthenticated: true,
                signing_key,
                ..ControllerConfig::default().auth
            },
            timers: rapidbyte_controller::config::TimerConfig {
                agent_reap_interval: Duration::from_secs(60),
                agent_reap_timeout: Duration::from_secs(120),
                lease_check_interval,
                reconciliation_timeout,
                ..ControllerConfig::default().timers
            },
            ..Default::default()
        };
        let guard =
            Arc::new(rapidbyte_metrics::init("test-controller").expect("otel init should succeed"));
        rapidbyte_controller::run(config, guard, rapidbyte_secrets::SecretProviders::new())
            .await
            .expect("controller should stay running");
    })
}

async fn wait_for_run_state(
    client: &mut PipelineServiceClient<tonic::transport::Channel>,
    run_id: &str,
    expected_state: RunState,
    timeout: Duration,
) -> rapidbyte_controller::proto::rapidbyte::v1::GetRunResponse {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        assert!(
            tokio::time::Instant::now() <= deadline,
            "Timed out waiting for run {run_id} to reach state {:?}",
            expected_state
        );

        let response = client
            .get_run(GetRunRequest {
                run_id: run_id.to_string(),
            })
            .await
            .expect("GetRun failed")
            .into_inner();

        let run = response.run.as_ref().expect("GetRun missing run detail");
        if run.state == expected_state as i32 {
            return response;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "requires RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL"]
async fn distributed_submit_and_complete() {
    let _ = tracing_subscriber::fmt::try_init();
    let metadata = TestMetadataSchema::create().await;

    let ctrl_port = free_port();
    let ctrl_addr = format!("127.0.0.1:{ctrl_port}");
    let ctrl_url = format!("http://{ctrl_addr}");
    let signing_key = b"distributed-test-signing-key".to_vec();

    // Start controller
    let controller_task = spawn_controller(
        ctrl_addr.clone(),
        metadata.scoped_url.clone(),
        signing_key.clone(),
        Duration::from_secs(60),
        ControllerConfig::default().timers.reconciliation_timeout,
    );

    let controller_probe = wait_for_controller(&ctrl_url).await;
    drop(controller_probe);

    // Start agent
    let agent_ctrl_url = ctrl_url.clone();
    let agent_task = tokio::spawn(async move {
        let config = AgentConfig {
            controller_url: agent_ctrl_url,
            max_tasks: 1,
            heartbeat_interval: Duration::from_secs(5),
            poll_wait_seconds: 5,
            auth_token: None,
            controller_tls: None,
            metrics_listen: None,
            registry_url: None,
            registry_insecure: false,
            trust_policy: "skip".to_owned(),
            trusted_key_pems: vec![],
        };
        let guard =
            Arc::new(rapidbyte_metrics::init("test-agent").expect("otel init should succeed"));
        let (ctx, registration, adapters) = rapidbyte_agent::build_agent_context(&config, guard)
            .await
            .expect("agent context should build");
        let shutdown = CancellationToken::new();
        rapidbyte_agent::run_agent(&ctx, registration, &adapters, shutdown)
            .await
            .expect("agent should stay running");
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect as a pipeline client
    let channel = wait_for_controller(&ctrl_url).await;
    let mut client = PipelineServiceClient::new(channel);

    // Submit the pipeline
    let resp = client
        .submit_pipeline(SubmitPipelineRequest {
            pipeline_yaml: TEST_PIPELINE_YAML.to_string(),
            options: None,
            idempotency_key: String::new(),
        })
        .await
        .expect("SubmitPipeline failed");

    let run_id = resp.into_inner().run_id;
    assert!(!run_id.is_empty(), "run_id should not be empty");

    // Poll GetRun until terminal state. The agent will fail quickly when it
    // cannot open the configured Postgres state backend.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut final_state = 0i32;

    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("Timed out waiting for run to reach terminal state (last state={final_state})");
        }
        if controller_task.is_finished() {
            match controller_task.await {
                Ok(()) => panic!("controller exited unexpectedly"),
                Err(error) => panic!("controller task failed: {error}"),
            }
        }
        if agent_task.is_finished() {
            match agent_task.await {
                Ok(()) => panic!("agent exited unexpectedly"),
                Err(error) => panic!("agent task failed: {error}"),
            }
        }

        let run_resp = client
            .get_run(GetRunRequest {
                run_id: run_id.clone(),
            })
            .await
            .expect("GetRun failed")
            .into_inner();

        let run = run_resp.run.as_ref().expect("GetRun missing run detail");
        final_state = run.state;

        if final_state == RunState::Completed as i32
            || final_state == RunState::Failed as i32
            || final_state == RunState::Cancelled as i32
        {
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The run should have failed, proving the full
    // submit -> assign -> agent poll -> execute -> report flow works
    assert!(
        final_state == RunState::Failed as i32,
        "Expected run to fail (non-existent plugins), got state={final_state}"
    );

    controller_task.abort();
    agent_task.abort();
    metadata.cleanup().await;
}

#[tokio::test]
#[ignore = "requires RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL"]
async fn distributed_manual_agent_complete_task() {
    let _ = tracing_subscriber::fmt::try_init();
    let metadata = TestMetadataSchema::create().await;

    let ctrl_port = free_port();
    let ctrl_addr = format!("127.0.0.1:{ctrl_port}");
    let ctrl_url = format!("http://{ctrl_addr}");
    let signing_key = b"distributed-test-signing-key".to_vec();

    let controller_task = spawn_controller(
        ctrl_addr.clone(),
        metadata.scoped_url.clone(),
        signing_key,
        Duration::from_secs(60),
        Duration::from_secs(30),
    );

    let controller_probe = wait_for_controller(&ctrl_url).await;
    drop(controller_probe);

    let channel = wait_for_controller(&ctrl_url).await;
    let mut pipeline_client = PipelineServiceClient::new(channel.clone());
    let mut agent_client = AgentServiceClient::new(channel);

    let submit_response = pipeline_client
        .submit_pipeline(SubmitPipelineRequest {
            pipeline_yaml: TEST_PIPELINE_YAML.to_string(),
            options: None,
            idempotency_key: String::new(),
        })
        .await
        .expect("SubmitPipeline failed")
        .into_inner();
    let run_id = submit_response.run_id;

    let agent_id = uuid::Uuid::new_v4().to_string();
    let _register_response = agent_client
        .register(RegisterRequest {
            agent_id: agent_id.clone(),
            capabilities: Some(AgentCapabilities {
                plugins: vec![],
                max_concurrent_tasks: 1,
            }),
        })
        .await
        .expect("Register failed")
        .into_inner();

    let poll_response = agent_client
        .poll_task(PollTaskRequest {
            agent_id: agent_id.clone(),
        })
        .await
        .expect("PollTask failed")
        .into_inner();

    let assignment = match poll_response.result {
        Some(poll_task_response::Result::Assignment(task)) => task,
        other => panic!("expected task assignment, got {other:?}"),
    };
    assert_eq!(assignment.run_id, run_id);

    // Send a heartbeat with progress
    let _heartbeat_resp = agent_client
        .heartbeat(HeartbeatRequest {
            agent_id: agent_id.clone(),
            tasks: vec![TaskHeartbeat {
                task_id: assignment.task_id.clone(),
                lease_epoch: assignment.lease_epoch,
                progress_message: Some("running".into()),
                progress_pct: Some(50.0),
            }],
        })
        .await
        .expect("Heartbeat should succeed");

    // Wait for run to reach running state
    let _running_run = wait_for_run_state(
        &mut pipeline_client,
        &run_id,
        RunState::Running,
        Duration::from_secs(5),
    )
    .await;

    // Complete with failure
    let _completion = agent_client
        .complete_task(CompleteTaskRequest {
            agent_id,
            task_id: assignment.task_id,
            lease_epoch: assignment.lease_epoch,
            outcome: Some(complete_task_request::Outcome::Failed(TaskFailed {
                error_code: "TEST_EXECUTION_FAILED".into(),
                error_message: "manual test failure".into(),
                retryable: false,
                commit_state: 1, // BEFORE_COMMIT
            })),
        })
        .await
        .expect("CompleteTask should succeed")
        .into_inner();

    let failed_run = wait_for_run_state(
        &mut pipeline_client,
        &run_id,
        RunState::Failed,
        Duration::from_secs(5),
    )
    .await;
    let run = failed_run.run.expect("run detail missing");
    let error = run.error.expect("failed run should expose error");
    assert_eq!(error.code, "TEST_EXECUTION_FAILED");

    controller_task.abort();
    metadata.cleanup().await;
}
