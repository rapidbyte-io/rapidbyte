//! `submit_pipeline` handler.

use opentelemetry::KeyValue;
use tonic::{Response, Status};

use crate::proto::rapidbyte::v1::{SubmitPipelineRequest, SubmitPipelineResponse};

pub(crate) async fn handle_submit(
    handler: &super::PipelineHandler,
    req: SubmitPipelineRequest,
) -> Result<Response<SubmitPipelineResponse>, Status> {
    // Parse YAML to validate and extract pipeline name
    let yaml_str = std::str::from_utf8(&req.pipeline_yaml_utf8)
        .map_err(|e| Status::invalid_argument(format!("Pipeline YAML is not valid UTF-8: {e}")))?;

    let config: serde_yaml::Value = serde_yaml::from_str(yaml_str)
        .map_err(|e| Status::invalid_argument(format!("Invalid YAML: {e}")))?;

    let pipeline_name = config
        .get("pipeline")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    // Reject SQLite backend (explicit or implicit) in distributed mode.
    // Pipelines without a state section or without state.backend default to
    // SQLite in the engine, which is a local file unreachable after reassignment.
    let backend = config
        .get("state")
        .and_then(|s| s.get("backend"))
        .and_then(|b| b.as_str());
    if backend != Some("postgres") {
        return Err(Status::invalid_argument(
            "Distributed mode requires state.backend: postgres. \
             SQLite (the default) is a local file and would be unreachable after agent reassignment.",
        ));
    }

    // Check idempotency
    let idempotency_key = if req.idempotency_key.is_empty() {
        None
    } else {
        Some(req.idempotency_key.clone())
    };

    let run_id = uuid::Uuid::new_v4().to_string();

    let (actual_run_id, is_new) = {
        let mut runs = handler.state.runs.write().await;
        runs.create_run(run_id, pipeline_name, idempotency_key)
    };

    if is_new {
        let dry_run = req.execution.as_ref().is_some_and(|e| e.dry_run);
        let limit = req.execution.as_ref().and_then(|e| e.limit);
        let task_id = {
            let mut tasks = handler.state.tasks.write().await;
            tasks.enqueue(
                actual_run_id.clone(),
                req.pipeline_yaml_utf8,
                dry_run,
                limit,
                1,
            )
        };
        let run_snapshot = {
            handler
                .state
                .runs
                .read()
                .await
                .get_run(&actual_run_id)
                .cloned()
                .expect("newly created run should exist")
        };
        let task_snapshot = {
            handler
                .state
                .tasks
                .read()
                .await
                .get(&task_id)
                .cloned()
                .expect("newly created task should exist")
        };

        tracing::info!(run_id = %actual_run_id, task_id, "Pipeline submitted");
        if let Err(error) = handler
            .state
            .create_run_with_task_records(&run_snapshot, &task_snapshot)
            .await
        {
            rollback_new_submission(handler, &actual_run_id, &task_id).await;
            return Err(Status::internal(error.to_string()));
        }
        handler.state.task_notify.notify_waiters();
        rapidbyte_metrics::instruments::controller::runs_submitted().add(
            1,
            &[KeyValue::new(rapidbyte_metrics::labels::STATUS, "accepted")],
        );
        rapidbyte_metrics::instruments::controller::active_runs().add(1, &[]);
    }

    Ok(Response::new(SubmitPipelineResponse {
        run_id: actual_run_id,
    }))
}

async fn rollback_new_submission(handler: &super::PipelineHandler, run_id: &str, task_id: &str) {
    {
        let mut tasks = handler.state.tasks.write().await;
        let _ = tasks.remove_task(task_id);
    }
    {
        let mut runs = handler.state.runs.write().await;
        let _ = runs.remove_run(run_id);
    }
}
