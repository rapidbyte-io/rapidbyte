use rapidbyte_controller::adapter::postgres::run::PgRunRepository;
use rapidbyte_controller::domain::ports::repository::{Pagination, RunFilter, RunRepository};
use rapidbyte_controller::domain::run::RunState;

use super::{sample_run, sample_run_with_key, setup_db};

#[tokio::test]
async fn save_and_find_by_id() {
    let (pool, _container) = setup_db().await;
    let repo = PgRunRepository::new(pool);

    let run = sample_run("run-1");
    repo.save(&run).await.unwrap();

    let found = repo.find_by_id("run-1").await.unwrap().unwrap();
    assert_eq!(found.id(), "run-1");
    assert_eq!(found.pipeline_name(), "test-pipe");
    assert_eq!(found.pipeline_yaml(), "pipeline: test\nversion: '1.0'");
    assert_eq!(found.state(), RunState::Pending);
    assert_eq!(found.current_attempt(), 1);
    assert_eq!(found.max_retries(), 0);
    assert!(found.idempotency_key().is_none());
    assert!(found.error().is_none());
    assert!(found.metrics().is_none());
}

#[tokio::test]
async fn save_upsert_updates_existing() {
    let (pool, _container) = setup_db().await;
    let repo = PgRunRepository::new(pool);

    let mut run = sample_run("run-upsert");
    repo.save(&run).await.unwrap();

    run.start().unwrap();
    repo.save(&run).await.unwrap();

    let found = repo.find_by_id("run-upsert").await.unwrap().unwrap();
    assert_eq!(found.state(), RunState::Running);
}

#[tokio::test]
async fn find_by_idempotency_key() {
    let (pool, _container) = setup_db().await;
    let repo = PgRunRepository::new(pool);

    let run = sample_run_with_key("run-idem", "my-key");
    repo.save(&run).await.unwrap();

    let found = repo
        .find_by_idempotency_key("my-key")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.id(), "run-idem");
    assert_eq!(found.idempotency_key(), Some("my-key"));
}

#[tokio::test]
async fn find_by_idempotency_key_not_found() {
    let (pool, _container) = setup_db().await;
    let repo = PgRunRepository::new(pool);

    let found = repo
        .find_by_idempotency_key("nonexistent-key")
        .await
        .unwrap();
    assert!(found.is_none());
}

#[tokio::test]
async fn idempotency_key_unique_constraint() {
    let (pool, _container) = setup_db().await;
    let repo = PgRunRepository::new(pool);

    let run1 = sample_run_with_key("run-dup-1", "dup-key");
    repo.save(&run1).await.unwrap();

    let run2 = sample_run_with_key("run-dup-2", "dup-key");
    let result = repo.save(&run2).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn list_with_state_filter() {
    let (pool, _container) = setup_db().await;
    let repo = PgRunRepository::new(pool);

    // Save 3 runs, all initially pending
    let run1 = sample_run("run-filter-1");
    let run2 = sample_run("run-filter-2");
    let mut run3 = sample_run("run-filter-3");

    repo.save(&run1).await.unwrap();
    repo.save(&run2).await.unwrap();

    // Transition run3 to Running before saving
    run3.start().unwrap();
    repo.save(&run3).await.unwrap();

    let page = repo
        .list(
            RunFilter {
                state: Some(RunState::Pending),
            },
            Pagination {
                page_size: 10,
                page_token: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(page.runs.len(), 2);
    for r in &page.runs {
        assert_eq!(r.state(), RunState::Pending);
    }
}

#[tokio::test]
async fn list_pagination_cursor() {
    let (pool, _container) = setup_db().await;
    let repo = PgRunRepository::new(pool);

    for i in 0..5 {
        let run = sample_run(&format!("run-page-{i}"));
        repo.save(&run).await.unwrap();
    }

    let page1 = repo
        .list(
            RunFilter { state: None },
            Pagination {
                page_size: 2,
                page_token: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(page1.runs.len(), 2);
    assert!(page1.next_page_token.is_some());

    let page2 = repo
        .list(
            RunFilter { state: None },
            Pagination {
                page_size: 2,
                page_token: page1.next_page_token,
            },
        )
        .await
        .unwrap();

    assert_eq!(page2.runs.len(), 2);
    // Pages should not overlap
    assert_ne!(page1.runs[0].id(), page2.runs[0].id());
    assert_ne!(page1.runs[1].id(), page2.runs[0].id());
}

#[tokio::test]
async fn list_default_page_size() {
    let (pool, _container) = setup_db().await;
    let repo = PgRunRepository::new(pool);

    for i in 0..25 {
        let run = sample_run(&format!("run-default-{i:02}"));
        repo.save(&run).await.unwrap();
    }

    let page = repo
        .list(
            RunFilter { state: None },
            Pagination {
                page_size: 0,
                page_token: None,
            },
        )
        .await
        .unwrap();

    // Default page size is 20
    assert_eq!(page.runs.len(), 20);
    assert!(page.next_page_token.is_some());
}
