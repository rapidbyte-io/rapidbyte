use crate::application::context::AppContext;
use crate::application::error::AppError;
use crate::domain::error::DomainError;
use crate::domain::event::DomainEvent;
use crate::domain::run::{Run, RunState};
use crate::domain::task::{Task, TaskOperation};

#[derive(Debug)]
pub struct SubmitResult {
    pub run_id: String,
    pub already_exists: bool,
}

/// Submit a new pipeline run.
///
/// If an idempotency key is provided and a run with that key already exists,
/// the existing run id is returned with `already_exists = true`.
///
/// # Errors
///
/// Returns `AppError::Domain` if the pipeline YAML is invalid, or a
/// repository / event-bus error on persistence failure.
pub async fn submit_pipeline(
    ctx: &AppContext,
    idempotency_key: Option<String>,
    pipeline_yaml: String,
    max_retries: u32,
    timeout_seconds: Option<u64>,
) -> Result<SubmitResult, AppError> {
    // 1. Idempotency check
    if let Some(ref key) = idempotency_key {
        if let Some(existing) = ctx.runs.find_by_idempotency_key(key).await? {
            return Ok(SubmitResult {
                run_id: existing.id().to_string(),
                already_exists: true,
            });
        }
    }

    // 2. Extract pipeline name
    let pipeline_name = rapidbyte_pipeline_config::extract_pipeline_name(&pipeline_yaml)
        .map_err(|e| AppError::Domain(DomainError::InvalidPipeline(e.to_string())))?;

    // 3-4. Generate IDs
    let run_id = uuid::Uuid::new_v4().to_string();
    let task_id = uuid::Uuid::new_v4().to_string();

    // 5. Get current time
    let now = ctx.clock.now();

    // 6. Clone idempotency key before it moves into Run
    let idempotency_key_clone = idempotency_key.clone();

    // 7. Create Run
    let run = Run::new(
        run_id.clone(),
        idempotency_key,
        pipeline_name,
        pipeline_yaml,
        max_retries,
        timeout_seconds,
        now,
    );

    // 8. Create Task
    let task = Task::new(task_id, run_id.clone(), 1, TaskOperation::Sync, now);

    // 9. Persist atomically
    match ctx.store.submit_run(&run, &task).await {
        Ok(()) => {}
        Err(e) => {
            // If this was a unique constraint violation on idempotency_key,
            // another request won the race -- return the existing run.
            if let Some(ref key) = idempotency_key_clone {
                if let Ok(Some(existing)) = ctx.runs.find_by_idempotency_key(key).await {
                    return Ok(SubmitResult {
                        run_id: existing.id().to_string(),
                        already_exists: true,
                    });
                }
            }
            return Err(e.into());
        }
    }

    // 10. Publish event
    ctx.event_bus
        .publish(DomainEvent::RunStateChanged {
            run_id: run_id.clone(),
            state: RunState::Pending,
            attempt: 1,
        })
        .await?;

    // 11. Return result
    Ok(SubmitResult {
        run_id,
        already_exists: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::testing::fake_context;

    #[tokio::test]
    async fn happy_path_creates_run_and_task() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";

        let result = submit_pipeline(&tc.ctx, None, yaml.to_string(), 2, Some(60))
            .await
            .unwrap();

        assert!(!result.already_exists);
        assert!(!result.run_id.is_empty());

        // Run should exist in storage
        let runs = tc.storage.runs.lock().unwrap();
        let run = runs.get(&result.run_id).unwrap();
        assert_eq!(run.state(), RunState::Pending);
        assert_eq!(run.pipeline_name(), "test-pipe");
        assert_eq!(run.max_retries(), 2);
        assert_eq!(run.timeout_seconds(), Some(60));

        // Task should exist in storage
        let tasks = tc.storage.tasks.lock().unwrap();
        assert_eq!(tasks.len(), 1);
        let task = tasks.values().next().unwrap();
        assert_eq!(task.run_id(), result.run_id);
        assert_eq!(task.attempt(), 1);
    }

    #[tokio::test]
    async fn idempotency_returns_existing_run() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let key = "idem-key-1".to_string();

        let first = submit_pipeline(&tc.ctx, Some(key.clone()), yaml.to_string(), 0, None)
            .await
            .unwrap();
        assert!(!first.already_exists);

        let second = submit_pipeline(&tc.ctx, Some(key.clone()), yaml.to_string(), 0, None)
            .await
            .unwrap();
        assert!(second.already_exists);
        assert_eq!(second.run_id, first.run_id);
    }

    #[tokio::test]
    async fn invalid_yaml_returns_error() {
        let tc = fake_context();
        let bad_yaml = "not: [valid: yaml: {{{}}}";

        let result = submit_pipeline(&tc.ctx, None, bad_yaml.to_string(), 0, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            AppError::Domain(DomainError::InvalidPipeline(_))
        ));
    }

    #[tokio::test]
    async fn event_published_on_submit() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";

        let result = submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();

        let events = tc.event_bus.published_events();
        assert_eq!(events.len(), 1);
        match &events[0] {
            DomainEvent::RunStateChanged {
                run_id,
                state,
                attempt,
            } => {
                assert_eq!(run_id, &result.run_id);
                assert_eq!(*state, RunState::Pending);
                assert_eq!(*attempt, 1);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn submit_with_max_retries_zero() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";

        let result = submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, None)
            .await
            .unwrap();

        let runs = tc.storage.runs.lock().unwrap();
        let run = runs.get(&result.run_id).unwrap();
        assert_eq!(run.max_retries(), 0);
    }

    #[tokio::test]
    async fn submit_with_timeout_seconds() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";

        let result = submit_pipeline(&tc.ctx, None, yaml.to_string(), 0, Some(60))
            .await
            .unwrap();

        let runs = tc.storage.runs.lock().unwrap();
        let run = runs.get(&result.run_id).unwrap();
        assert_eq!(run.timeout_seconds(), Some(60));
    }

    #[tokio::test]
    async fn submit_idempotency_sequential_dedup() {
        let tc = fake_context();
        let yaml = "pipeline: test-pipe\nversion: '1.0'";
        let key = "dedup-key".to_string();

        let first = submit_pipeline(&tc.ctx, Some(key.clone()), yaml.to_string(), 0, None)
            .await
            .unwrap();
        assert!(!first.already_exists);

        let second = submit_pipeline(&tc.ctx, Some(key.clone()), yaml.to_string(), 0, None)
            .await
            .unwrap();
        assert!(second.already_exists);
        assert_eq!(second.run_id, first.run_id);
    }
}
