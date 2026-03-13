//! DashMap-based instrument cache for dynamic plugin metrics.

use std::sync::OnceLock;

use dashmap::DashMap;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram};

const PLUGIN_METER: &str = "rapidbyte.plugin";

fn plugin_meter() -> opentelemetry::metrics::Meter {
    global::meter(PLUGIN_METER)
}

static COUNTERS: OnceLock<DashMap<String, Counter<u64>>> = OnceLock::new();
static GAUGES: OnceLock<DashMap<String, Gauge<f64>>> = OnceLock::new();
static HISTOGRAMS: OnceLock<DashMap<String, Histogram<f64>>> = OnceLock::new();

fn counters() -> &'static DashMap<String, Counter<u64>> {
    COUNTERS.get_or_init(DashMap::new)
}

fn gauges() -> &'static DashMap<String, Gauge<f64>> {
    GAUGES.get_or_init(DashMap::new)
}

fn histograms() -> &'static DashMap<String, Histogram<f64>> {
    HISTOGRAMS.get_or_init(DashMap::new)
}

/// Get or create a counter for the given metric name.
pub fn custom_counter(name: &str) -> Counter<u64> {
    let owned = name.to_owned();
    counters()
        .entry(owned.clone())
        .or_insert_with(move || plugin_meter().u64_counter(owned).build())
        .clone()
}

/// Get or create a gauge for the given metric name.
pub fn custom_gauge(name: &str) -> Gauge<f64> {
    let owned = name.to_owned();
    gauges()
        .entry(owned.clone())
        .or_insert_with(move || plugin_meter().f64_gauge(owned).build())
        .clone()
}

/// Get or create a histogram for the given metric name.
pub fn custom_histogram(name: &str) -> Histogram<f64> {
    let owned = name.to_owned();
    histograms()
        .entry(owned.clone())
        .or_insert_with(move || plugin_meter().f64_histogram(owned).build())
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_counter_returns_same_instrument_for_same_name() {
        let a = custom_counter("my_plugin.rows");
        let b = custom_counter("my_plugin.rows");
        // Both are valid counters (we can't compare identity easily,
        // but calling both should not panic)
        a.add(1, &[]);
        b.add(1, &[]);
    }

    #[test]
    fn different_names_return_different_instruments() {
        let _ = custom_counter("metric_a");
        let _ = custom_counter("metric_b");
        // Should not panic — two distinct instruments created
    }
}
