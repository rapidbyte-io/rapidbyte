//! OpenTelemetry-based observability for Rapidbyte.
//!
//! Call [`init`] at process start to configure the metrics and tracing pipeline.
//! The returned [`OtelGuard`] flushes pending exports on drop.
//!
//! | Module       | Responsibility |
//! |--------------|----------------|
//! | `cache`      | DashMap instrument cache for dynamic plugin metrics |
//! | `grpc_layer` | Tower layer for gRPC RED metrics |
//! | `instruments`| OnceLock instrument accessors (pipeline, host, plugin, ...) |
//! | `labels`     | Bounded label keys and parsing |
//! | `snapshot`   | InMemoryMetricReader and PipelineResult bridge |
//! | `views`      | Histogram bucket configuration |

#![warn(clippy::pedantic)]

pub mod cache;
pub mod grpc_layer;
pub mod instruments;
pub mod labels;
pub mod snapshot;
pub mod views;

use anyhow::Result;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;

/// Guard that owns the OpenTelemetry providers, snapshot reader, and flushes on drop.
pub struct OtelGuard {
    tracer_provider: SdkTracerProvider,
    meter_provider: SdkMeterProvider,
    prometheus_registry: prometheus::Registry,
    snapshot_reader: snapshot::SnapshotReader,
}

impl OtelGuard {
    #[must_use]
    pub fn tracer_provider(&self) -> &SdkTracerProvider {
        &self.tracer_provider
    }

    #[must_use]
    pub fn meter_provider(&self) -> &SdkMeterProvider {
        &self.meter_provider
    }

    #[must_use]
    pub fn snapshot_reader(&self) -> &snapshot::SnapshotReader {
        &self.snapshot_reader
    }

    /// Render Prometheus text exposition format.
    #[must_use]
    pub fn prometheus_text(&self) -> String {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.prometheus_registry.gather();
        encoder
            .encode_to_string(&metric_families)
            .unwrap_or_default()
    }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(e) = self.meter_provider.shutdown() {
            tracing::warn!("failed to shutdown meter provider: {e}");
        }
        if let Err(e) = self.tracer_provider.shutdown() {
            tracing::warn!("failed to shutdown tracer provider: {e}");
        }
    }
}

/// Initialize OpenTelemetry metrics + tracing pipeline.
///
/// Sets the global meter provider so that instrument accessors in [`instruments`]
/// work correctly. Must be called before pipeline execution.
///
/// Reads `OTEL_EXPORTER_OTLP_ENDPOINT` to optionally enable OTLP export.
///
/// # Errors
///
/// Returns an error if the Prometheus exporter or OTLP exporter fails to initialize.
pub fn init(service_name: &str) -> Result<OtelGuard> {
    let resource = Resource::builder()
        .with_service_name(service_name.to_owned())
        .build();

    // Prometheus exporter
    let prometheus_registry = prometheus::Registry::new();
    let prometheus_exporter = opentelemetry_prometheus::exporter()
        .with_registry(prometheus_registry.clone())
        .build()?;

    // Create snapshot reader for PipelineResult bridge (CLI display)
    let snapshot_reader = snapshot::SnapshotReader::new();
    let snapshot_periodic_reader = snapshot_reader.build_reader();

    let mut meter_builder = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(prometheus_exporter)
        .with_reader(snapshot_periodic_reader);

    // Optional OTLP metric exporter
    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let otlp_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .build()?;
        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(otlp_exporter).build();
        meter_builder = meter_builder.with_reader(reader);
    }

    let meter_provider = meter_builder.build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    // Traces
    let tracer_provider = if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
        let otlp_span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .build()?;
        SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(otlp_span_exporter)
            .build()
    } else {
        SdkTracerProvider::builder().with_resource(resource).build()
    };

    Ok(OtelGuard {
        tracer_provider,
        meter_provider,
        prometheus_registry,
        snapshot_reader,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: init() sets a process-global meter provider. Tests that call init()
    // must run sequentially. Use a single test that exercises both code paths,
    // or use #[serial] from serial_test crate if splitting.
    #[test]
    fn init_returns_guard_and_prometheus_text_does_not_panic() {
        // This test does NOT set OTEL_EXPORTER_OTLP_ENDPOINT,
        // so only the Prometheus exporter is active (no OTLP).
        let guard = init("test-service").expect("init should succeed");
        // prometheus_text() should not panic
        let _text = guard.prometheus_text();
        drop(guard);
    }
}
