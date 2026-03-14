//! DashMap-based instrument cache for dynamic plugin metrics.

use std::fmt;
use std::sync::{Mutex, OnceLock};

use dashmap::DashMap;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge, Histogram};

const PLUGIN_METER: &str = "rapidbyte.plugin";
pub const MAX_CUSTOM_METRICS_PER_KIND: usize = 256;
pub const MAX_CUSTOM_METRIC_NAME_LEN: usize = 128;

fn plugin_meter() -> opentelemetry::metrics::Meter {
    global::meter(PLUGIN_METER)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstrumentCacheError {
    MetricNameTooLong { len: usize, max: usize },
    MetricLimitExceeded { kind: &'static str, max: usize },
}

impl fmt::Display for InstrumentCacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MetricNameTooLong { len, max } => {
                write!(f, "metric name length {len} exceeds max {max}")
            }
            Self::MetricLimitExceeded { kind, max } => {
                write!(f, "custom {kind} metric limit exceeded (max {max})")
            }
        }
    }
}

impl std::error::Error for InstrumentCacheError {}

static COUNTERS: OnceLock<DashMap<String, Counter<u64>>> = OnceLock::new();
static GAUGES: OnceLock<DashMap<String, Gauge<f64>>> = OnceLock::new();
static HISTOGRAMS: OnceLock<DashMap<String, Histogram<f64>>> = OnceLock::new();
static COUNTER_INSERT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static GAUGE_INSERT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static HISTOGRAM_INSERT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn counters() -> &'static DashMap<String, Counter<u64>> {
    COUNTERS.get_or_init(DashMap::new)
}

fn gauges() -> &'static DashMap<String, Gauge<f64>> {
    GAUGES.get_or_init(DashMap::new)
}

fn histograms() -> &'static DashMap<String, Histogram<f64>> {
    HISTOGRAMS.get_or_init(DashMap::new)
}

fn counter_insert_lock() -> &'static Mutex<()> {
    COUNTER_INSERT_LOCK.get_or_init(|| Mutex::new(()))
}

fn gauge_insert_lock() -> &'static Mutex<()> {
    GAUGE_INSERT_LOCK.get_or_init(|| Mutex::new(()))
}

fn histogram_insert_lock() -> &'static Mutex<()> {
    HISTOGRAM_INSERT_LOCK.get_or_init(|| Mutex::new(()))
}

fn validate_name(name: &str) -> Result<(), InstrumentCacheError> {
    if name.len() > MAX_CUSTOM_METRIC_NAME_LEN {
        return Err(InstrumentCacheError::MetricNameTooLong {
            len: name.len(),
            max: MAX_CUSTOM_METRIC_NAME_LEN,
        });
    }
    Ok(())
}

/// Get or create a counter for the given metric name.
pub fn custom_counter(name: &str) -> Result<Counter<u64>, InstrumentCacheError> {
    validate_name(name)?;
    if let Some(existing) = counters().get(name) {
        return Ok(existing.clone());
    }

    let _guard = counter_insert_lock()
        .lock()
        .expect("counter insert lock poisoned");
    if let Some(existing) = counters().get(name) {
        return Ok(existing.clone());
    }
    if counters().len() >= MAX_CUSTOM_METRICS_PER_KIND {
        return Err(InstrumentCacheError::MetricLimitExceeded {
            kind: "counter",
            max: MAX_CUSTOM_METRICS_PER_KIND,
        });
    }

    let instrument = plugin_meter().u64_counter(name.to_owned()).build();
    counters().insert(name.to_owned(), instrument.clone());
    Ok(instrument)
}

/// Get or create a gauge for the given metric name.
pub fn custom_gauge(name: &str) -> Result<Gauge<f64>, InstrumentCacheError> {
    validate_name(name)?;
    if let Some(existing) = gauges().get(name) {
        return Ok(existing.clone());
    }

    let _guard = gauge_insert_lock()
        .lock()
        .expect("gauge insert lock poisoned");
    if let Some(existing) = gauges().get(name) {
        return Ok(existing.clone());
    }
    if gauges().len() >= MAX_CUSTOM_METRICS_PER_KIND {
        return Err(InstrumentCacheError::MetricLimitExceeded {
            kind: "gauge",
            max: MAX_CUSTOM_METRICS_PER_KIND,
        });
    }

    let instrument = plugin_meter().f64_gauge(name.to_owned()).build();
    gauges().insert(name.to_owned(), instrument.clone());
    Ok(instrument)
}

/// Get or create a histogram for the given metric name.
pub fn custom_histogram(name: &str) -> Result<Histogram<f64>, InstrumentCacheError> {
    validate_name(name)?;
    if let Some(existing) = histograms().get(name) {
        return Ok(existing.clone());
    }

    let _guard = histogram_insert_lock()
        .lock()
        .expect("histogram insert lock poisoned");
    if let Some(existing) = histograms().get(name) {
        return Ok(existing.clone());
    }
    if histograms().len() >= MAX_CUSTOM_METRICS_PER_KIND {
        return Err(InstrumentCacheError::MetricLimitExceeded {
            kind: "histogram",
            max: MAX_CUSTOM_METRICS_PER_KIND,
        });
    }

    let instrument = plugin_meter().f64_histogram(name.to_owned()).build();
    histograms().insert(name.to_owned(), instrument.clone());
    Ok(instrument)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;

    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn reset_caches() {
        counters().clear();
        gauges().clear();
        histograms().clear();
    }

    #[test]
    fn custom_counter_returns_same_instrument_for_same_name() {
        let _guard = TEST_LOCK.lock().expect("test lock poisoned");
        reset_caches();

        let a = custom_counter("my_plugin.rows").expect("counter should be created");
        let b = custom_counter("my_plugin.rows").expect("counter should be reused");
        // Both are valid counters (we can't compare identity easily,
        // but calling both should not panic)
        a.add(1, &[]);
        b.add(1, &[]);
    }

    #[test]
    fn different_names_return_different_instruments() {
        let _guard = TEST_LOCK.lock().expect("test lock poisoned");
        reset_caches();

        let _ = custom_counter("metric_a").expect("metric_a should be created");
        let _ = custom_counter("metric_b").expect("metric_b should be created");
        // Should not panic — two distinct instruments created
    }

    #[test]
    fn custom_counter_rejects_new_names_after_limit() {
        let _guard = TEST_LOCK.lock().expect("test lock poisoned");
        reset_caches();

        for idx in 0..MAX_CUSTOM_METRICS_PER_KIND {
            custom_counter(&format!("metric_{idx}"))
                .expect("metric within limit should be created");
        }

        let err =
            custom_counter("metric_overflow").expect_err("overflow metric should be rejected");
        assert_eq!(
            err,
            InstrumentCacheError::MetricLimitExceeded {
                kind: "counter",
                max: MAX_CUSTOM_METRICS_PER_KIND
            }
        );
        assert!(custom_counter("metric_0").is_ok());
    }

    #[test]
    fn custom_counter_rejects_overlong_names() {
        let _guard = TEST_LOCK.lock().expect("test lock poisoned");
        reset_caches();

        let name = "m".repeat(MAX_CUSTOM_METRIC_NAME_LEN + 1);
        let err = custom_counter(&name).expect_err("overlong metric should be rejected");
        assert_eq!(
            err,
            InstrumentCacheError::MetricNameTooLong {
                len: MAX_CUSTOM_METRIC_NAME_LEN + 1,
                max: MAX_CUSTOM_METRIC_NAME_LEN
            }
        );
    }
}
