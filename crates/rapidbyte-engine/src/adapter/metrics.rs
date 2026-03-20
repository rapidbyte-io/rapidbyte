//! [`MetricsSnapshot`] adapter backed by the OpenTelemetry in-memory exporter.
//!
//! Wraps [`rapidbyte_metrics::snapshot::SnapshotReader`] to flush the meter
//! provider and return a [`PipelineMetricsSnapshot`] on demand.

use opentelemetry_sdk::metrics::SdkMeterProvider;
use rapidbyte_metrics::snapshot::{PipelineMetricsSnapshot, SnapshotReader};

use crate::domain::ports::MetricsSnapshot;

/// Metrics snapshot adapter that flushes the `OTel` meter provider and reads
/// aggregated counters/histograms from the in-memory snapshot exporter.
pub struct OtelMetricsSnapshot {
    reader: SnapshotReader,
    meter_provider: SdkMeterProvider,
    pipeline_name: String,
}

impl OtelMetricsSnapshot {
    /// Create a new adapter.
    ///
    /// * `reader` -- the [`SnapshotReader`] registered on the meter provider.
    /// * `meter_provider` -- used to force-flush pending metrics before reading.
    /// * `pipeline_name` -- label filter so only metrics for this pipeline are
    ///   included in the snapshot.
    #[must_use]
    pub fn new(
        reader: SnapshotReader,
        meter_provider: SdkMeterProvider,
        pipeline_name: String,
    ) -> Self {
        Self {
            reader,
            meter_provider,
            pipeline_name,
        }
    }
}

impl MetricsSnapshot for OtelMetricsSnapshot {
    fn snapshot_for_run(&self) -> PipelineMetricsSnapshot {
        self.reader
            .flush_and_snapshot(&self.meter_provider, &self.pipeline_name)
    }
}
