use anyhow::Result;
use once_cell::sync::OnceCell;
use testcontainers::clients;
use testcontainers::core::WaitFor;
use testcontainers::GenericImage;

static SHARED_PORT: OnceCell<u16> = OnceCell::new();

pub fn shared_postgres_port() -> Result<u16> {
    SHARED_PORT
        .get_or_try_init(|| {
            let docker: &'static clients::Cli = Box::leak(Box::new(clients::Cli::default()));
            let image = GenericImage::new("postgres", "16-alpine")
                .with_env_var("POSTGRES_USER", "postgres")
                .with_env_var("POSTGRES_PASSWORD", "postgres")
                .with_env_var("POSTGRES_DB", "postgres")
                .with_exposed_port(5432)
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ));
            let args = vec![
                "-c".to_string(),
                "wal_level=logical".to_string(),
                "-c".to_string(),
                "max_replication_slots=16".to_string(),
                "-c".to_string(),
                "max_wal_senders=16".to_string(),
            ];
            let node = Box::leak(Box::new(docker.run((image, args))));
            let port = node.get_host_port_ipv4(5432);
            Ok(port)
        })
        .copied()
}
