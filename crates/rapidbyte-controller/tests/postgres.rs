#![cfg(feature = "integration")]

mod postgres {
    pub mod agent_repo;
    pub mod event_bus;
    pub mod pipeline_store;
    pub mod run_repo;
    pub mod task_repo;

    use chrono::Utc;
    use sqlx::PgPool;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

    use rapidbyte_controller::domain::agent::{Agent, AgentCapabilities};
    use rapidbyte_controller::domain::lease::Lease;
    use rapidbyte_controller::domain::run::Run;
    use rapidbyte_controller::domain::task::Task;

    pub async fn setup_db() -> (PgPool, testcontainers::ContainerAsync<Postgres>) {
        let container = Postgres::default().start().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
        let pool = PgPool::connect(&url).await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        (pool, container)
    }

    pub fn sample_run(id: &str) -> Run {
        Run::new(
            id.to_string(),
            None,
            "test-pipe".to_string(),
            "pipeline: test\nversion: '1.0'".to_string(),
            0,
            None,
            Utc::now(),
        )
    }

    pub fn sample_run_with_key(id: &str, key: &str) -> Run {
        Run::new(
            id.to_string(),
            Some(key.to_string()),
            "test-pipe".to_string(),
            "yaml".to_string(),
            0,
            None,
            Utc::now(),
        )
    }

    pub fn sample_run_with_retries(id: &str, max_retries: u32) -> Run {
        Run::new(
            id.to_string(),
            None,
            "test-pipe".to_string(),
            "yaml".to_string(),
            max_retries,
            None,
            Utc::now(),
        )
    }

    pub fn sample_task(id: &str, run_id: &str) -> Task {
        Task::new(id.to_string(), run_id.to_string(), 1, Utc::now())
    }

    pub fn sample_task_with_attempt(id: &str, run_id: &str, attempt: u32) -> Task {
        Task::new(id.to_string(), run_id.to_string(), attempt, Utc::now())
    }

    pub fn sample_agent(id: &str) -> Agent {
        Agent::new(
            id.to_string(),
            AgentCapabilities {
                plugins: vec!["pg".to_string()],
                max_concurrent_tasks: 2,
            },
            Utc::now(),
        )
    }

    pub fn sample_lease(epoch: u64) -> Lease {
        Lease::new(epoch, Utc::now() + chrono::Duration::seconds(300))
    }
}
