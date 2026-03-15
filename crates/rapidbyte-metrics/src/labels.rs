//! Bounded label keys and parsing.

use opentelemetry::KeyValue;
use std::collections::HashSet;
use std::sync::LazyLock;

pub const PIPELINE: &str = "pipeline";
pub const STREAM: &str = "stream";
pub const SOURCE: &str = "source";
pub const DESTINATION: &str = "destination";
pub const SHARD: &str = "shard";
pub const STATUS: &str = "status";
pub const METHOD: &str = "method";
pub const REASON: &str = "reason";
pub const PLUGIN: &str = "plugin";
pub const RUN: &str = "run";
pub const AGENT_ID: &str = "agent_id";
pub const RULE: &str = "rule";
pub const FIELD: &str = "field";

// ── Status label values ─────────────────────────────────────────────────────

/// Metric status value for successful operations.
pub const STATUS_OK: &str = "ok";
/// Metric status value for failed operations.
pub const STATUS_ERROR: &str = "error";
/// Metric status value for cancelled operations.
pub const STATUS_CANCELLED: &str = "cancelled";

static ALLOWED_KEYS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        PIPELINE,
        STREAM,
        SOURCE,
        DESTINATION,
        SHARD,
        STATUS,
        METHOD,
        REASON,
        PLUGIN,
        RUN,
        AGENT_ID,
        RULE,
        FIELD,
    ])
});

/// Parse a JSON label string, keeping only keys in the bounded set.
/// Unknown keys are silently dropped. Malformed JSON returns empty.
#[must_use]
pub fn parse_bounded_labels(json: &str) -> Vec<KeyValue> {
    let Ok(map) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(json) else {
        tracing::debug!("malformed metric labels JSON: {json}");
        return Vec::new();
    };

    map.into_iter()
        .filter_map(|(k, v)| {
            if ALLOWED_KEYS.contains(k.as_str()) {
                let val = match v {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                };
                Some(KeyValue::new(k, val))
            } else {
                tracing::debug!("dropping unknown metric label key: {k}");
                None
            }
        })
        .collect()
}

/// Pre-built label set for a specific metric scope (pipeline + plugin + stream + run + shard).
/// Avoids JSON parsing and allocation on every metric emission for built-in metrics.
#[derive(Clone)]
pub struct ScopeLabels {
    labels: Vec<KeyValue>,
}

impl ScopeLabels {
    /// Build scope labels for a specific pipeline/plugin/stream context.
    #[must_use]
    pub fn new(
        pipeline: &str,
        plugin: &str,
        stream: &str,
        run: Option<&str>,
        shard: usize,
    ) -> Self {
        let mut labels = vec![
            KeyValue::new(PIPELINE, pipeline.to_owned()),
            KeyValue::new(PLUGIN, plugin.to_owned()),
            KeyValue::new(STREAM, stream.to_owned()),
        ];
        if let Some(run) = run {
            labels.push(KeyValue::new(RUN, run.to_owned()));
        }
        if shard > 0 {
            labels.push(KeyValue::new(SHARD, shard.to_string()));
        }
        Self { labels }
    }

    /// Return the pre-built labels as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[KeyValue] {
        &self.labels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_known_labels() {
        let json = r#"{"pipeline":"sync","stream":"users"}"#;
        let labels = parse_bounded_labels(json);
        assert_eq!(labels.len(), 2);
        assert!(labels.iter().any(|kv| kv.key.as_str() == "pipeline"));
        assert!(labels.iter().any(|kv| kv.key.as_str() == "stream"));
    }

    #[test]
    fn unknown_labels_are_dropped() {
        let json = r#"{"pipeline":"sync","unknown_key":"val"}"#;
        let labels = parse_bounded_labels(json);
        assert_eq!(labels.len(), 1);
        assert!(labels.iter().any(|kv| kv.key.as_str() == "pipeline"));
    }

    #[test]
    fn malformed_json_returns_empty() {
        let labels = parse_bounded_labels("not json");
        assert!(labels.is_empty());
    }

    #[test]
    fn validation_labels_rule_and_field_are_allowed() {
        let json = r#"{"rule":"not_null","field":"email","pipeline":"sync"}"#;
        let labels = parse_bounded_labels(json);
        assert_eq!(labels.len(), 3);
        assert!(labels.iter().any(|kv| kv.key.as_str() == "rule"));
        assert!(labels.iter().any(|kv| kv.key.as_str() == "field"));
        assert!(labels.iter().any(|kv| kv.key.as_str() == "pipeline"));
    }
}
