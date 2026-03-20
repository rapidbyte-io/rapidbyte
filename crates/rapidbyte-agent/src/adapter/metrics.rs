//! `OTel` metrics adapter.

use std::sync::Arc;

use crate::domain::ports::metrics::MetricsProvider;

/// Wraps the `OTel` guard's metric handles.
///
/// Holds an `Arc<OtelGuard>` so that the `SnapshotReader` (not `Clone`) and the
/// `SdkMeterProvider` can be accessed by reference through the port trait.
pub struct OtelMetricsProvider {
    guard: Arc<rapidbyte_metrics::OtelGuard>,
}

impl OtelMetricsProvider {
    #[must_use]
    pub fn new(guard: Arc<rapidbyte_metrics::OtelGuard>) -> Self {
        Self { guard }
    }
}

impl MetricsProvider for OtelMetricsProvider {
    fn snapshot_reader(&self) -> &rapidbyte_metrics::snapshot::SnapshotReader {
        self.guard.snapshot_reader()
    }

    fn meter_provider(&self) -> &opentelemetry_sdk::metrics::SdkMeterProvider {
        self.guard.meter_provider()
    }
}
