use chrono::Utc;
use rapidbyte_controller::adapter::postgres::agent::PgAgentRepository;
use rapidbyte_controller::adapter::postgres::run::PgRunRepository;
use rapidbyte_controller::adapter::postgres::store::PgPipelineStore;
use rapidbyte_controller::adapter::postgres::task::PgTaskRepository;
use rapidbyte_controller::domain::agent::{Agent, AgentCapabilities};
use rapidbyte_controller::domain::lease::Lease;
use rapidbyte_controller::domain::ports::pipeline_store::PipelineStore;
use rapidbyte_controller::domain::ports::repository::{
    AgentRepository, RunRepository, TaskRepository,
};
use rapidbyte_controller::domain::run::{RunMetrics, RunState};
use rapidbyte_controller::domain::task::{Task, TaskOperation, TaskState};

use super::{
    sample_lease, sample_run, sample_run_with_key, sample_run_with_retries, sample_task,
    sample_task_with_attempt, setup_db,
};

#[tokio::test]
async fn assign_task_filters_by_supported_operations() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());
    let agent_repo = PgAgentRepository::new(pool);

    // Register agent that supports only Sync
    let agent = Agent::new(
        "agent-ops-1".to_string(),
        AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: 2,
            supported_operations: vec![TaskOperation::Sync],
        },
        Utc::now(),
    );
    agent_repo.save(&agent).await.unwrap();

    // Submit a teardown task
    let run = sample_run("run-ops-1");
    let task = Task::new(
        "task-ops-1".into(),
        "run-ops-1".into(),
        1,
        TaskOperation::Teardown,
        Utc::now(),
    );
    store.submit_run(&run, &task).await.unwrap();

    // Agent with Sync-only should NOT get the teardown task
    let lease = sample_lease(1);
    let result = store
        .assign_task("agent-ops-1", 2, lease.clone(), &[TaskOperation::Sync])
        .await
        .unwrap();
    assert!(
        result.is_none(),
        "sync-only agent should not get teardown task"
    );

    // Same agent with ALL ops should get it
    let result = store
        .assign_task("agent-ops-1", 2, lease, TaskOperation::ALL)
        .await
        .unwrap();
    assert!(result.is_some(), "all-ops agent should get teardown task");
}

#[tokio::test]
async fn supported_operations_round_trip() {
    let (pool, _container) = setup_db().await;
    let agent_repo = PgAgentRepository::new(pool);

    let agent = Agent::new(
        "agent-ops-rt".to_string(),
        AgentCapabilities {
            plugins: vec!["postgres".into()],
            max_concurrent_tasks: 4,
            supported_operations: vec![TaskOperation::Sync, TaskOperation::Teardown],
        },
        Utc::now(),
    );
    agent_repo.save(&agent).await.unwrap();

    // Read back and verify operations persisted
    let found = agent_repo
        .find_by_id("agent-ops-rt")
        .await
        .unwrap()
        .unwrap();
    let ops = &found.capabilities().supported_operations;
    assert_eq!(ops.len(), 2);
    assert!(ops.contains(&TaskOperation::Sync));
    assert!(ops.contains(&TaskOperation::Teardown));
}

#[tokio::test]
async fn submit_run_atomic() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());
    let run_repo = PgRunRepository::new(pool.clone());
    let task_repo = PgTaskRepository::new(pool);

    let run = sample_run("run-submit-1");
    let task = sample_task("task-submit-1", "run-submit-1");

    store.submit_run(&run, &task).await.unwrap();

    let found_run = run_repo.find_by_id("run-submit-1").await.unwrap();
    assert!(found_run.is_some());

    let found_task = task_repo.find_by_id("task-submit-1").await.unwrap();
    assert!(found_task.is_some());
}

#[tokio::test]
async fn submit_run_idempotency_violation_rolls_back() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());
    let run_repo = PgRunRepository::new(pool);

    // First submit succeeds
    let run1 = sample_run_with_key("run-idem-1", "dup-key");
    let task1 = sample_task("task-idem-1", "run-idem-1");
    store.submit_run(&run1, &task1).await.unwrap();

    // Second submit with same idempotency key (different run id) should fail
    let run2 = sample_run_with_key("run-idem-2", "dup-key");
    let task2 = sample_task("task-idem-2", "run-idem-2");
    let result = store.submit_run(&run2, &task2).await;
    assert!(result.is_err());

    // Verify the second run was NOT persisted (rolled back)
    let found = run_repo.find_by_id("run-idem-2").await.unwrap();
    assert!(found.is_none());
}

#[tokio::test]
async fn assign_task_happy_path() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());
    let agent_repo = PgAgentRepository::new(pool);

    let agent = Agent::new(
        "agent-assign".to_string(),
        AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: 2,
            supported_operations: vec![],
        },
        Utc::now(),
    );
    agent_repo.save(&agent).await.unwrap();

    let run = sample_run("run-assign-1");
    let task = sample_task("task-assign-1", "run-assign-1");
    store.submit_run(&run, &task).await.unwrap();

    let lease = sample_lease(1);
    let result = store
        .assign_task("agent-assign", 2, lease, &[])
        .await
        .unwrap();
    assert!(result.is_some());

    let (assigned_task, assigned_run) = result.unwrap();
    assert_eq!(assigned_task.state(), TaskState::Running);
    assert_eq!(assigned_task.agent_id(), Some("agent-assign"));
    assert_eq!(assigned_run.state(), RunState::Running);
}

#[tokio::test]
async fn assign_task_respects_capacity() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());
    let agent_repo = PgAgentRepository::new(pool);

    // Agent with max_concurrent_tasks = 1
    let agent = Agent::new(
        "agent-cap".to_string(),
        AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: 1,
            supported_operations: vec![],
        },
        Utc::now(),
    );
    agent_repo.save(&agent).await.unwrap();

    // Submit 2 runs
    let run1 = sample_run("run-cap-1");
    let task1 = sample_task("task-cap-1", "run-cap-1");
    store.submit_run(&run1, &task1).await.unwrap();

    let run2 = sample_run("run-cap-2");
    let task2 = sample_task("task-cap-2", "run-cap-2");
    store.submit_run(&run2, &task2).await.unwrap();

    // First assign should succeed
    let lease1 = sample_lease(10);
    let result1 = store
        .assign_task("agent-cap", 1, lease1, &[])
        .await
        .unwrap();
    assert!(result1.is_some());

    // Second assign should return None (capacity exceeded)
    let lease2 = sample_lease(11);
    let result2 = store
        .assign_task("agent-cap", 1, lease2, &[])
        .await
        .unwrap();
    assert!(result2.is_none());
}

#[tokio::test]
async fn assign_task_concurrent_cancelled_run_rolls_back() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());
    let run_repo = PgRunRepository::new(pool.clone());

    let run = sample_run("run-cancel-assign");
    let task = sample_task("task-cancel-assign", "run-cancel-assign");
    store.submit_run(&run, &task).await.unwrap();

    // Directly cancel the run in the database (simulating concurrent cancellation)
    let mut cancelled_run = run_repo
        .find_by_id("run-cancel-assign")
        .await
        .unwrap()
        .unwrap();
    cancelled_run.cancel().unwrap();
    run_repo.save(&cancelled_run).await.unwrap();

    // assign_task should return None because the run is no longer pending
    let lease = sample_lease(20);
    let result = store.assign_task("agent-x", 10, lease, &[]).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn assign_task_skip_locked() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());

    // Submit 2 independent runs with tasks
    let run1 = sample_run("run-skip-1");
    let task1 = sample_task("task-skip-1", "run-skip-1");
    store.submit_run(&run1, &task1).await.unwrap();

    let run2 = sample_run("run-skip-2");
    let task2 = sample_task("task-skip-2", "run-skip-2");
    store.submit_run(&run2, &task2).await.unwrap();

    // Assign concurrently with 2 different agents
    let lease1 = Lease::new(30, Utc::now() + chrono::Duration::seconds(300));
    let lease2 = Lease::new(31, Utc::now() + chrono::Duration::seconds(300));

    let (r1, r2) = tokio::join!(
        store.assign_task("agent-a", 10, lease1, &[]),
        store.assign_task("agent-b", 10, lease2, &[]),
    );

    let assigned1 = r1.unwrap();
    let assigned2 = r2.unwrap();

    // Both should succeed with different tasks
    assert!(assigned1.is_some());
    assert!(assigned2.is_some());

    let (t1, _) = assigned1.unwrap();
    let (t2, _) = assigned2.unwrap();
    assert_ne!(t1.id(), t2.id());
}

#[tokio::test]
async fn complete_run_atomic() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());
    let run_repo = PgRunRepository::new(pool.clone());
    let task_repo = PgTaskRepository::new(pool);

    let run = sample_run("run-complete-1");
    let task = sample_task("task-complete-1", "run-complete-1");
    store.submit_run(&run, &task).await.unwrap();

    // Assign
    let lease = sample_lease(40);
    let (mut assigned_task, mut assigned_run) = store
        .assign_task("agent-c", 5, lease, &[])
        .await
        .unwrap()
        .unwrap();

    // Complete
    assigned_task.complete().unwrap();
    assigned_run
        .complete(RunMetrics {
            rows_read: 100,
            rows_written: 50,
            bytes_read: 1024,
            bytes_written: 512,
            duration_ms: 5000,
        })
        .unwrap();

    store
        .complete_run(&assigned_task, &assigned_run)
        .await
        .unwrap();

    let found_run = run_repo
        .find_by_id("run-complete-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found_run.state(), RunState::Completed);
    assert!(found_run.metrics().is_some());

    let found_task = task_repo
        .find_by_id(assigned_task.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found_task.state(), TaskState::Completed);
}

#[tokio::test]
async fn fail_and_retry_creates_new_task() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());
    let run_repo = PgRunRepository::new(pool.clone());
    let task_repo = PgTaskRepository::new(pool);

    let run = sample_run_with_retries("run-retry-1", 2);
    let task = sample_task("task-retry-1", "run-retry-1");
    store.submit_run(&run, &task).await.unwrap();

    // Assign
    let lease = sample_lease(50);
    let (mut assigned_task, mut assigned_run) = store
        .assign_task("agent-d", 5, lease, &[])
        .await
        .unwrap()
        .unwrap();

    // Fail the task
    assigned_task.fail().unwrap();
    // Retry the run (goes back to Pending with incremented attempt)
    let new_attempt = assigned_run.retry().unwrap();

    // Create new task for the retry
    let new_task = sample_task_with_attempt("task-retry-1-new", "run-retry-1", new_attempt);

    store
        .fail_and_retry(&assigned_task, &assigned_run, &new_task)
        .await
        .unwrap();

    // Verify: failed task exists
    let found_failed = task_repo
        .find_by_id(assigned_task.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found_failed.state(), TaskState::Failed);

    // Verify: new pending task exists
    let found_new = task_repo
        .find_by_id("task-retry-1-new")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found_new.state(), TaskState::Pending);
    assert_eq!(found_new.attempt(), new_attempt);

    // Verify: run is back to Pending
    let found_run = run_repo.find_by_id("run-retry-1").await.unwrap().unwrap();
    assert_eq!(found_run.state(), RunState::Pending);
    assert_eq!(found_run.current_attempt(), new_attempt);
}

#[tokio::test]
async fn cancel_pending_run_only_if_still_pending() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());
    let run_repo = PgRunRepository::new(pool.clone());

    let run = sample_run("run-cancel-pend");
    let task = sample_task("task-cancel-pend", "run-cancel-pend");
    store.submit_run(&run, &task).await.unwrap();

    // Cancel the pending run
    let mut cancel_run = run_repo
        .find_by_id("run-cancel-pend")
        .await
        .unwrap()
        .unwrap();
    cancel_run.cancel().unwrap();
    let mut cancel_task = sample_task("task-cancel-pend", "run-cancel-pend");
    cancel_task.cancel_pending().unwrap();

    store
        .cancel_pending_run(&cancel_run, &cancel_task)
        .await
        .unwrap();

    // Verify cancelled
    let found_run = run_repo
        .find_by_id("run-cancel-pend")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found_run.state(), RunState::Cancelled);

    // Second cancel on already-cancelled should fail
    // Submit a new pending run for the second attempt
    let run2 = sample_run("run-cancel-pend-2");
    let task2 = sample_task("task-cancel-pend-2", "run-cancel-pend-2");
    store.submit_run(&run2, &task2).await.unwrap();

    // First, cancel it
    let mut cr2 = run_repo
        .find_by_id("run-cancel-pend-2")
        .await
        .unwrap()
        .unwrap();
    cr2.cancel().unwrap();
    let mut ct2 = sample_task("task-cancel-pend-2", "run-cancel-pend-2");
    ct2.cancel_pending().unwrap();
    store.cancel_pending_run(&cr2, &ct2).await.unwrap();

    // Now try to cancel again -- the run is already cancelled so DB won't match "pending"
    let result = store.cancel_pending_run(&cr2, &ct2).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn timeout_and_retry_with_none_new_task() {
    let (pool, _container) = setup_db().await;
    let store = PgPipelineStore::new(pool.clone());
    let run_repo = PgRunRepository::new(pool.clone());
    let task_repo = PgTaskRepository::new(pool);

    let run = sample_run("run-timeout-1");
    let task = sample_task("task-timeout-1", "run-timeout-1");
    store.submit_run(&run, &task).await.unwrap();

    // Assign
    let lease = sample_lease(60);
    let (mut assigned_task, mut assigned_run) = store
        .assign_task("agent-e", 5, lease, &[])
        .await
        .unwrap()
        .unwrap();

    // Timeout (no retry — new_task is None)
    assigned_task.timeout().unwrap();
    assigned_run
        .fail(rapidbyte_controller::domain::run::RunError {
            code: "TIMEOUT".to_string(),
            message: "task timed out".to_string(),
        })
        .unwrap();

    store
        .timeout_and_retry(&assigned_task, &assigned_run, None)
        .await
        .unwrap();

    // Verify: run is Failed
    let found_run = run_repo.find_by_id("run-timeout-1").await.unwrap().unwrap();
    assert_eq!(found_run.state(), RunState::Failed);
    assert_eq!(found_run.error().unwrap().code, "TIMEOUT");

    // Verify: task is TimedOut
    let found_task = task_repo
        .find_by_id(assigned_task.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found_task.state(), TaskState::TimedOut);

    // Verify: no new task created
    let all_tasks = task_repo.find_by_run_id("run-timeout-1").await.unwrap();
    assert_eq!(all_tasks.len(), 1);
}
