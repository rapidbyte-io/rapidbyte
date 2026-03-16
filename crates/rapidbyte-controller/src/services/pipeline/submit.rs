//! `submit_pipeline` handler.

use opentelemetry::KeyValue;
use tonic::{Response, Status};

use crate::proto::rapidbyte::v1::{SubmitPipelineRequest, SubmitPipelineResponse};

/// Validate pipeline YAML and extract the pipeline name.
#[allow(clippy::result_large_err)]
fn validate_pipeline_yaml(raw: &[u8]) -> Result<String, Status> {
    let yaml_str = std::str::from_utf8(raw)
        .map_err(|e| Status::invalid_argument(format!("Pipeline YAML is not valid UTF-8: {e}")))?;

    let config: serde_yaml::Value = serde_yaml::from_str(yaml_str)
        .map_err(|e| Status::invalid_argument(format!("Invalid YAML: {e}")))?;

    // Reject SQLite backend (explicit or implicit) in distributed mode.
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

    Ok(config
        .get("pipeline")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string())
}

pub(crate) async fn handle_submit(
    handler: &super::PipelineHandler,
    req: SubmitPipelineRequest,
) -> Result<Response<SubmitPipelineResponse>, Status> {
    let pipeline_name = validate_pipeline_yaml(&req.pipeline_yaml_utf8)?;

    // Check idempotency
    let idempotency_key = if req.idempotency_key.is_empty() {
        None
    } else {
        Some(req.idempotency_key.clone())
    };

    // Guard against concurrent submissions with the same idempotency key.
    // The pending set covers the window between in-memory create_run and
    // durable persistence — without it, a duplicate could observe the
    // idempotency index entry before the run+task are durably persisted.
    if let Some(key) = &idempotency_key {
        let pending = handler.state.pending_idempotency_keys.read().await;
        if pending.contains(key) {
            return Err(Status::aborted(
                "A submission with this idempotency key is already in progress. Retry shortly.",
            ));
        }
    }
    if let Some(key) = &idempotency_key {
        handler
            .state
            .pending_idempotency_keys
            .write()
            .await
            .insert(key.clone());
    }

    let run_id = uuid::Uuid::new_v4().to_string();

    let (actual_run_id, is_new) = {
        let mut runs = handler.state.runs.write().await;
        runs.create_run(run_id, pipeline_name, idempotency_key.clone())
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
            if let Some(key) = &idempotency_key {
                handler
                    .state
                    .pending_idempotency_keys
                    .write()
                    .await
                    .remove(key);
            }
            return Err(Status::internal(error.to_string()));
        }
        handler.state.task_notify.notify_waiters();
        rapidbyte_metrics::instruments::controller::runs_submitted().add(
            1,
            &[KeyValue::new(rapidbyte_metrics::labels::STATUS, "accepted")],
        );
        rapidbyte_metrics::instruments::controller::active_runs().add(1, &[]);
    }

    // Persistence succeeded (or deduped) — release the pending guard.
    if let Some(key) = &idempotency_key {
        handler
            .state
            .pending_idempotency_keys
            .write()
            .await
            .remove(key);
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
