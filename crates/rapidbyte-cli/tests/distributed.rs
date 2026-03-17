//! Integration tests for distributed CLI commands against the v2 control plane.

mod distributed_cli_contract {
    use std::pin::Pin;
    use std::process::Command;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use rapidbyte_controller::proto::rapidbyte::v2::control_plane_server::{
        ControlPlane, ControlPlaneServer,
    };
    use rapidbyte_controller::proto::rapidbyte::v2::{
        run_event, CancelRunRequest, CancelRunResponse, GetRunRequest, GetRunResponse,
        ListRunsRequest, ListRunsResponse, RetryRunRequest, RetryRunResponse, RunCompleted,
        RunEvent, RunSummary, StreamRunRequest, SubmitRunRequest, SubmitRunResponse,
    };
    use tempfile::NamedTempFile;
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_stream::Stream;

    type RunEventStream = Pin<Box<dyn Stream<Item = Result<RunEvent, tonic::Status>> + Send>>;

    #[derive(Clone, Default)]
    struct MockControlPlane {
        submitted: Arc<AtomicUsize>,
    }

    #[tonic::async_trait]
    impl ControlPlane for MockControlPlane {
        type StreamRunStream = RunEventStream;

        async fn submit_run(
            &self,
            _request: tonic::Request<SubmitRunRequest>,
        ) -> Result<tonic::Response<SubmitRunResponse>, tonic::Status> {
            let id = self.submitted.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(tonic::Response::new(SubmitRunResponse {
                run_id: format!("run-{id}"),
            }))
        }

        async fn get_run(
            &self,
            request: tonic::Request<GetRunRequest>,
        ) -> Result<tonic::Response<GetRunResponse>, tonic::Status> {
            Ok(tonic::Response::new(GetRunResponse {
                run_id: request.into_inner().run_id,
                state: "Completed".to_owned(),
            }))
        }

        async fn list_runs(
            &self,
            request: tonic::Request<ListRunsRequest>,
        ) -> Result<tonic::Response<ListRunsResponse>, tonic::Status> {
            let limit = request.into_inner().limit.unwrap_or(1).max(1) as usize;
            let mut runs = Vec::with_capacity(limit.min(3));
            for index in 1..=limit.min(3) {
                runs.push(RunSummary {
                    run_id: format!("run-{index}"),
                    state: "Accepted".to_owned(),
                });
            }
            Ok(tonic::Response::new(ListRunsResponse { runs }))
        }

        async fn cancel_run(
            &self,
            _request: tonic::Request<CancelRunRequest>,
        ) -> Result<tonic::Response<CancelRunResponse>, tonic::Status> {
            Ok(tonic::Response::new(CancelRunResponse {
                accepted: true,
                message: String::new(),
            }))
        }

        async fn retry_run(
            &self,
            _request: tonic::Request<RetryRunRequest>,
        ) -> Result<tonic::Response<RetryRunResponse>, tonic::Status> {
            Ok(tonic::Response::new(RetryRunResponse { accepted: true }))
        }

        async fn stream_run(
            &self,
            request: tonic::Request<StreamRunRequest>,
        ) -> Result<tonic::Response<Self::StreamRunStream>, tonic::Status> {
            let run_id = request.into_inner().run_id;
            let stream = tokio_stream::iter(vec![Ok(RunEvent {
                run_id,
                detail: "completed".to_owned(),
                event: Some(run_event::Event::Completed(RunCompleted {
                    total_records: 10,
                    total_bytes: 1024,
                    elapsed_seconds: 1.0,
                    cursors_advanced: 1,
                })),
            })]);
            Ok(tonic::Response::new(Box::pin(stream)))
        }
    }

    async fn spawn_mock_control_plane() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind should succeed");
        let addr = listener.local_addr().expect("local addr should resolve");
        let incoming = TcpListenerStream::new(listener);
        let server = Arc::new(MockControlPlane::default());
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ControlPlaneServer::new((*server).clone()))
                .serve_with_incoming(incoming)
                .await
                .expect("mock control plane should serve");
        });
        (format!("http://{addr}"), task)
    }

    fn run_cli(controller_url: &str, args: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_rapidbyte"))
            .arg("--controller")
            .arg(controller_url)
            .args(args)
            .output()
            .expect("rapidbyte CLI invocation should run")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn distributed_v2_status_watch_and_list_commands_succeed() {
        let (controller_url, server_task) = spawn_mock_control_plane().await;

        let status = run_cli(&controller_url, &["status", "run-1"]);
        assert!(
            status.status.success(),
            "status failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );

        let watch = run_cli(&controller_url, &["watch", "run-1"]);
        assert!(
            watch.status.success(),
            "watch failed: {}",
            String::from_utf8_lossy(&watch.stderr)
        );

        let list = run_cli(&controller_url, &["list-runs", "--limit", "2"]);
        assert!(
            list.status.success(),
            "list-runs failed: {}",
            String::from_utf8_lossy(&list.stderr)
        );

        server_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn distributed_v2_run_command_succeeds() {
        let (controller_url, server_task) = spawn_mock_control_plane().await;
        let pipeline = NamedTempFile::new().expect("temp pipeline should be created");
        std::fs::write(pipeline.path(), b"pipeline: test\n")
            .expect("temp pipeline should be writable");

        let run = run_cli(
            &controller_url,
            &[
                "run",
                pipeline
                    .path()
                    .to_str()
                    .expect("temp path should be valid utf-8"),
                "--dry-run",
            ],
        );
        assert!(
            run.status.success(),
            "run failed: {}",
            String::from_utf8_lossy(&run.stderr)
        );

        server_task.abort();
    }
}
