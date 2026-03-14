//! Shared test utilities for `OTel` metric testing.

use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
};

use crate::snapshot::SnapshotReader;

/// Create a test meter provider wired to a [`SnapshotReader`].
#[must_use]
pub fn snapshot_test_provider() -> (SdkMeterProvider, SnapshotReader) {
    let reader = SnapshotReader::new();
    let provider = SdkMeterProvider::builder()
        .with_reader(reader.build_reader())
        .build();
    (provider, reader)
}

/// Create a test meter provider wired to an in-memory exporter for raw metric inspection.
#[must_use]
pub fn exporter_test_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporterBuilder::new().build();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    (provider, exporter)
}
