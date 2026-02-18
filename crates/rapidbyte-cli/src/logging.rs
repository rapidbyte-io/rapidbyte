use tracing_subscriber::EnvFilter;

/// Initialize structured logging with tracing-subscriber.
///
/// Uses the `RUST_LOG` env var if set, otherwise falls back to the provided level.
pub fn init(log_level: &str) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();
}
