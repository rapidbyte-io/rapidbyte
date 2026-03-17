use std::sync::Arc;

use rapidbyte_controller::adapters::in_memory::repos::InMemoryRunRepository;
use rapidbyte_controller::adapters::in_memory::unit_of_work::InMemoryUnitOfWork;
use rapidbyte_controller::domain::run::{Run, RunId, RunState};
use rapidbyte_controller::ports::repositories::RunRepository;
use rapidbyte_controller::ports::unit_of_work::UnitOfWork;

#[tokio::test]
async fn unit_of_work_rolls_back_run_insert_on_error() {
    let repo = Arc::new(InMemoryRunRepository::default());
    let uow = InMemoryUnitOfWork::new(repo.clone());

    let run_id = RunId::new("run-rollback");
    let result: anyhow::Result<()> = uow
        .in_transaction({
            let repo = repo.clone();
            let run_id = run_id.clone();
            move || async move {
                let mut run = Run::new(run_id.clone());
                run.transition(RunState::Queued)?;
                repo.create_or_get_by_idempotency(run, Some("rollback-key"))
                    .await?;
                anyhow::bail!("simulate failure")
            }
        })
        .await;

    assert!(result.is_err());
    assert!(repo
        .get(&run_id)
        .await
        .expect("get should succeed")
        .is_none());
}
