use std::env;

use rapidbyte_controller::adapters::postgres::{PostgresRunRepository, PostgresUnitOfWork};
use rapidbyte_controller::domain::run::{Run, RunId, RunState};
use rapidbyte_controller::ports::repositories::RunRepository;
use rapidbyte_controller::ports::unit_of_work::UnitOfWork;
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore = "requires RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL"]
async fn postgres_run_repository_roundtrip_and_transaction_rollback() {
    let admin_url = env::var("RAPIDBYTE_CONTROLLER_METADATA_TEST_DATABASE_URL")
        .expect("test database URL env var must be set");
    let (admin_client, admin_connection) = tokio_postgres::connect(&admin_url, NoTls)
        .await
        .expect("admin connection should succeed");
    tokio::spawn(async move {
        admin_connection
            .await
            .expect("admin connection task should stay healthy");
    });

    let schema = format!("controller_v2_repo_test_{}", uuid::Uuid::new_v4().simple());
    admin_client
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .expect("schema creation should succeed");

    let scoped_url = format!("{admin_url} options='-c search_path={schema}'");
    let repo = PostgresRunRepository::connect(&scoped_url)
        .await
        .expect("postgres run repository should connect");
    let uow = PostgresUnitOfWork::new(repo.shared_client());

    let mut first = Run::new(RunId::new("run-a"));
    first
        .transition(RunState::Queued)
        .expect("queued transition should be valid");
    let created = repo
        .create_or_get_by_idempotency(first, Some("idem-1"))
        .await
        .expect("run should be inserted");

    let mut duplicate = Run::new(RunId::new("run-b"));
    duplicate
        .transition(RunState::Queued)
        .expect("queued transition should be valid");
    let dedup = repo
        .create_or_get_by_idempotency(duplicate, Some("idem-1"))
        .await
        .expect("dedup should return existing run");
    assert_eq!(created.run.id.as_str(), dedup.run.id.as_str());

    let listed = repo.list(10).await.expect("list should succeed");
    assert_eq!(listed.len(), 1);

    repo.mark_cancelled(&RunId::new(created.run.id.as_str().to_owned()))
        .await
        .expect("cancel should persist");
    let cancelled = repo
        .get(&RunId::new(created.run.id.as_str().to_owned()))
        .await
        .expect("get should succeed")
        .expect("run should exist");
    assert_eq!(cancelled.run.state, RunState::Cancelled);

    let rollback_id = RunId::new("rollback-run");
    let rollback: anyhow::Result<()> = uow
        .in_transaction({
            let repo = repo.clone();
            let rollback_id = rollback_id.clone();
            move || async move {
                let mut rollback_run = Run::new(rollback_id.clone());
                rollback_run
                    .transition(RunState::Queued)
                    .expect("queued transition should be valid");
                repo.create_or_get_by_idempotency(rollback_run, Some("idem-rollback"))
                    .await?;
                anyhow::bail!("force rollback")
            }
        })
        .await;
    assert!(rollback.is_err());

    let rolled_back = repo
        .get(&rollback_id)
        .await
        .expect("get after rollback should succeed");
    assert!(rolled_back.is_none());

    admin_client
        .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .expect("schema cleanup should succeed");
}
