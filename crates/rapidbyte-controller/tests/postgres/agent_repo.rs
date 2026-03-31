use chrono::{Duration, Utc};
use rapidbyte_controller::adapter::postgres::agent::PgAgentRepository;
use rapidbyte_controller::domain::agent::{Agent, AgentCapabilities};
use rapidbyte_controller::domain::ports::repository::AgentRepository;

use super::{sample_agent, setup_db};

#[tokio::test]
async fn save_and_find_by_id() {
    let (pool, _container) = setup_db().await;
    let repo = PgAgentRepository::new(pool);

    let agent = sample_agent("agent-1");
    repo.save(&agent).await.unwrap();

    let found = repo.find_by_id("agent-1").await.unwrap().unwrap();
    assert_eq!(found.id(), "agent-1");
    assert_eq!(found.capabilities().plugins, vec!["pg".to_string()]);
    assert_eq!(found.capabilities().max_concurrent_tasks, 2);
}

#[tokio::test]
async fn save_upsert() {
    let (pool, _container) = setup_db().await;
    let repo = PgAgentRepository::new(pool);

    let agent = sample_agent("agent-upsert");
    repo.save(&agent).await.unwrap();

    // Update with different capabilities
    let updated_agent = Agent::new(
        "agent-upsert".to_string(),
        AgentCapabilities {
            plugins: vec!["pg".to_string(), "mysql".to_string()],
            max_concurrent_tasks: 5,
            supported_operations: vec![],
        },
        Utc::now(),
    );
    repo.save(&updated_agent).await.unwrap();

    let found = repo.find_by_id("agent-upsert").await.unwrap().unwrap();
    assert_eq!(found.capabilities().max_concurrent_tasks, 5);
    assert_eq!(found.capabilities().plugins.len(), 2);
}

#[tokio::test]
async fn delete_agent() {
    let (pool, _container) = setup_db().await;
    let repo = PgAgentRepository::new(pool);

    let agent = sample_agent("agent-del");
    repo.save(&agent).await.unwrap();

    repo.delete("agent-del").await.unwrap();

    let found = repo.find_by_id("agent-del").await.unwrap();
    assert!(found.is_none());
}

#[tokio::test]
async fn find_stale() {
    let (pool, _container) = setup_db().await;
    let repo = PgAgentRepository::new(pool);

    let now = Utc::now();

    // Agent that was seen recently (not stale)
    let fresh_agent = Agent::new(
        "agent-fresh".to_string(),
        AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: 1,
            supported_operations: vec![],
        },
        now,
    );
    repo.save(&fresh_agent).await.unwrap();

    // Agent that was last seen long ago (stale)
    let stale_time = now - Duration::seconds(600);
    let stale_agent = Agent::from_row(
        "agent-stale".to_string(),
        AgentCapabilities {
            plugins: vec![],
            max_concurrent_tasks: 1,
            supported_operations: vec![],
        },
        stale_time,
        stale_time,
    );
    repo.save(&stale_agent).await.unwrap();

    let stale = repo.find_stale(Duration::seconds(300), now).await.unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].id(), "agent-stale");
}

#[tokio::test]
async fn delete_nonexistent_is_noop() {
    let (pool, _container) = setup_db().await;
    let repo = PgAgentRepository::new(pool);

    // Should not error
    let result = repo.delete("nonexistent-agent").await;
    assert!(result.is_ok());
}
