//! Outbound port for metrics infrastructure.

/// Provides `OTel` handles needed by the executor adapter.
pub trait MetricsProvider: Send + Sync {
    /// Snapshot reader for forwarding to the engine.
    fn snapshot_reader(&self) -> &rapidbyte_metrics::snapshot::SnapshotReader;

    /// Meter provider for `OTel` instrumentation.
    fn meter_provider(&self) -> &opentelemetry_sdk::metrics::SdkMeterProvider;
}
