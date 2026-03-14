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
///
/// When `otel_guard` is `Some`, a `tracing_opentelemetry` layer is added so
/// that spans are exported via the OpenTelemetry tracer pipeline.
pub fn init(
    verbosity: Verbosity,
    log_level: &str,
    otel_guard: Option<&rapidbyte_metrics::OtelGuard>,
) {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::prelude::*;

    let env_filter = default_filter(verbosity, log_level);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_writer(std::io::stderr);

    if let Some(guard) = otel_guard {
        let otel_layer =
            tracing_opentelemetry::layer().with_tracer(guard.tracer_provider().tracer("rapidbyte"));
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(otel_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    }
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
