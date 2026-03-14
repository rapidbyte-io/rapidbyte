//! Histogram bucket configuration for different duration scales.

use opentelemetry_sdk::metrics::{Aggregation, Instrument, Stream};

/// Sub-millisecond operations: compress, decompress, `emit_batch`, `next_batch`.
#[must_use]
pub fn fast_duration_buckets() -> Vec<f64> {
    vec![0.000_1, 0.000_5, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
}

/// Normal operations: plugin connect, query, flush, commit, module load.
#[must_use]
pub fn normal_duration_buckets() -> Vec<f64> {
    vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
}

/// Long operations: pipeline duration, task duration.
#[must_use]
pub fn slow_duration_buckets() -> Vec<f64> {
    vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]
}

/// Type alias for the view callback returned by [`bucket_view`].
pub type ViewFn = Box<dyn Fn(&Instrument) -> Option<Stream> + Send + Sync>;

/// Create a view that applies custom buckets to instruments matching a name prefix.
#[must_use]
pub fn bucket_view(prefix: &'static str, buckets: Vec<f64>) -> ViewFn {
    Box::new(move |instrument: &Instrument| {
        if instrument.name.starts_with(prefix) {
            let stream = Stream::new().aggregation(Aggregation::ExplicitBucketHistogram {
                boundaries: buckets.clone(),
                record_min_max: true,
            });
            Some(stream)
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_buckets_are_sub_millisecond_focused() {
        let buckets = fast_duration_buckets();
        assert!(buckets[0] < 0.001, "first bucket should be sub-ms");
        assert!(buckets.last().unwrap() <= &1.0);
    }

    #[test]
    fn slow_buckets_cover_minutes() {
        let buckets = slow_duration_buckets();
        assert!(buckets.last().unwrap() >= &300.0);
    }
}
