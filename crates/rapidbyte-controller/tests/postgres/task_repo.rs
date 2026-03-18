use chrono::Utc;
use rapidbyte_controller::adapter::postgres::run::PgRunRepository;
use rapidbyte_controller::adapter::postgres::task::PgTaskRepository;
use rapidbyte_controller::domain::lease::Lease;
use rapidbyte_controller::domain::ports::repository::{RunRepository, TaskRepository};
use rapidbyte_controller::domain::task::TaskState;

use super::{sample_run, sample_task, sample_task_with_attempt, setup_db};

#[tokio::test]
async fn save_and_find_by_id() {
    let (pool, _container) = setup_db().await;
    let run_repo = PgRunRepository::new(pool.clone());
    let task_repo = PgTaskRepository::new(pool);

    let run = sample_run("run-t1");
    run_repo.save(&run).await.unwrap();

    let task = sample_task("task-1", "run-t1");
    task_repo.save(&task).await.unwrap();

    let found = task_repo.find_by_id("task-1").await.unwrap().unwrap();
    assert_eq!(found.id(), "task-1");
    assert_eq!(found.run_id(), "run-t1");
    assert_eq!(found.attempt(), 1);
    assert_eq!(found.state(), TaskState::Pending);
    assert!(found.agent_id().is_none());
    assert!(found.lease().is_none());
}

#[tokio::test]
async fn find_by_run_id() {
    let (pool, _container) = setup_db().await;
    let run_repo = PgRunRepository::new(pool.clone());
    let task_repo = PgTaskRepository::new(pool);

    let run = sample_run("run-t2");
    run_repo.save(&run).await.unwrap();

    for i in 1..=3 {
        let task = sample_task_with_attempt(&format!("task-2-{i}"), "run-t2", i);
        task_repo.save(&task).await.unwrap();
    }

    let tasks = task_repo.find_by_run_id("run-t2").await.unwrap();
    assert_eq!(tasks.len(), 3);
    // Should be ordered by attempt ASC
    assert_eq!(tasks[0].attempt(), 1);
    assert_eq!(tasks[1].attempt(), 2);
    assert_eq!(tasks[2].attempt(), 3);
}

#[tokio::test]
async fn find_running_by_agent_id() {
    let (pool, _container) = setup_db().await;
    let run_repo = PgRunRepository::new(pool.clone());
    let task_repo = PgTaskRepository::new(pool);

    let run = sample_run("run-t3");
    run_repo.save(&run).await.unwrap();

    // Task assigned to agent-A, Running
    let mut task1 = sample_task_with_attempt("task-3-1", "run-t3", 1);
    let lease = Lease::new(1, Utc::now() + chrono::Duration::seconds(300));
    task1.assign("agent-A".to_string(), lease).unwrap();
    task_repo.save(&task1).await.unwrap();

    // Task assigned to agent-B, Running (different agent)
    let run2 = sample_run("run-t3b");
    run_repo.save(&run2).await.unwrap();
    let mut task2 = sample_task("task-3-2", "run-t3b");
    let lease2 = Lease::new(2, Utc::now() + chrono::Duration::seconds(300));
    task2.assign("agent-B".to_string(), lease2).unwrap();
    task_repo.save(&task2).await.unwrap();

    // Task assigned to agent-A but Completed
    let run3 = sample_run("run-t3c");
    run_repo.save(&run3).await.unwrap();
    let mut task3 = sample_task("task-3-3", "run-t3c");
    let lease3 = Lease::new(3, Utc::now() + chrono::Duration::seconds(300));
    task3.assign("agent-A".to_string(), lease3).unwrap();
    task3.complete().unwrap();
    task_repo.save(&task3).await.unwrap();

    let running = task_repo.find_running_by_agent_id("agent-A").await.unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].id(), "task-3-1");
}

#[tokio::test]
async fn find_expired_leases() {
    let (pool, _container) = setup_db().await;
    let run_repo = PgRunRepository::new(pool.clone());
    let task_repo = PgTaskRepository::new(pool);

    let run = sample_run("run-t4");
    run_repo.save(&run).await.unwrap();

    // Expired lease (past)
    let mut task1 = sample_task_with_attempt("task-4-1", "run-t4", 1);
    let expired_lease = Lease::new(1, Utc::now() - chrono::Duration::seconds(60));
    task1.assign("agent-X".to_string(), expired_lease).unwrap();
    task_repo.save(&task1).await.unwrap();

    // Future lease (not expired)
    let run2 = sample_run("run-t4b");
    run_repo.save(&run2).await.unwrap();
    let mut task2 = sample_task("task-4-2", "run-t4b");
    let future_lease = Lease::new(2, Utc::now() + chrono::Duration::seconds(300));
    task2.assign("agent-Y".to_string(), future_lease).unwrap();
    task_repo.save(&task2).await.unwrap();

    let expired = task_repo.find_expired_leases(Utc::now()).await.unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].id(), "task-4-1");
}

#[tokio::test]
async fn next_lease_epoch_increments() {
    let (pool, _container) = setup_db().await;
    let task_repo = PgTaskRepository::new(pool);

    let e1 = task_repo.next_lease_epoch().await.unwrap();
    let e2 = task_repo.next_lease_epoch().await.unwrap();
    let e3 = task_repo.next_lease_epoch().await.unwrap();

    assert!(e1 < e2);
    assert!(e2 < e3);
}

#[tokio::test]
async fn unique_run_attempt_constraint() {
    let (pool, _container) = setup_db().await;
    let run_repo = PgRunRepository::new(pool.clone());
    let task_repo = PgTaskRepository::new(pool);

    let run = sample_run("run-t5");
    run_repo.save(&run).await.unwrap();

    let task1 = sample_task_with_attempt("task-5-1", "run-t5", 1);
    task_repo.save(&task1).await.unwrap();

    // Same run_id + attempt=1 but different task id => should fail UNIQUE(run_id, attempt)
    let task2 = sample_task_with_attempt("task-5-2", "run-t5", 1);
    let result = task_repo.save(&task2).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn save_with_no_lease() {
    let (pool, _container) = setup_db().await;
    let run_repo = PgRunRepository::new(pool.clone());
    let task_repo = PgTaskRepository::new(pool);

    let run = sample_run("run-t6");
    run_repo.save(&run).await.unwrap();

    let task = sample_task("task-6", "run-t6");
    task_repo.save(&task).await.unwrap();

    let found = task_repo.find_by_id("task-6").await.unwrap().unwrap();
    assert!(found.agent_id().is_none());
    assert!(found.lease().is_none());
    assert_eq!(found.state(), TaskState::Pending);
}
