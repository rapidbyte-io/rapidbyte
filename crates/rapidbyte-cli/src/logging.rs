//! Structured logging initialization via tracing-subscriber.

use crate::Verbosity;
use tracing_subscriber::EnvFilter;

fn default_filter_for_env(
    verbosity: Verbosity,
    log_level: &str,
    rust_log: Option<&str>,
) -> EnvFilter {
    if let Some(value) = rust_log {
        return EnvFilter::try_new(value).unwrap_or_else(|_| EnvFilter::new(log_level));
    }

    match verbosity {
        Verbosity::Diagnostic => EnvFilter::new("debug"),
        Verbosity::Verbose => EnvFilter::new("info"),
        Verbosity::Quiet | Verbosity::Default => {
            if log_level == "info" {
                EnvFilter::new("off")
            } else {
                EnvFilter::new(log_level)
            }
        }
    }
}

fn default_filter(verbosity: Verbosity, log_level: &str) -> EnvFilter {
    default_filter_for_env(
        verbosity,
        log_level,
        std::env::var("RUST_LOG").ok().as_deref(),
    )
}

/// Initialize structured logging with tracing-subscriber.
///
/// By default, `-v` enables `info` tracing and `-vv` enables `debug` tracing.
/// `RUST_LOG` overrides the derived verbosity-based filter when set.
pub fn init(verbosity: Verbosity, log_level: &str) {
    let env_filter = default_filter(verbosity, log_level);

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_verbosity_suppresses_default_info_logs() {
        assert_eq!(
            default_filter_for_env(Verbosity::Default, "info", None).to_string(),
            "off"
        );
    }

    #[test]
    fn verbose_enables_info_logs() {
        assert_eq!(
            default_filter_for_env(Verbosity::Verbose, "info", None).to_string(),
            "info"
        );
    }

    #[test]
    fn diagnostic_enables_debug_logs() {
        assert_eq!(
            default_filter_for_env(Verbosity::Diagnostic, "info", None).to_string(),
            "debug"
        );
    }

    #[test]
    fn explicit_log_level_is_used_without_verbose_flags() {
        assert_eq!(
            default_filter_for_env(Verbosity::Default, "warn", None).to_string(),
            "warn"
        );
    }

    #[test]
    fn rust_log_overrides_verbosity_defaults() {
        assert_eq!(
            default_filter_for_env(Verbosity::Verbose, "info", Some("trace")).to_string(),
            "trace"
        );
    }
}
