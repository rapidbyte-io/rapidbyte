#![cfg_attr(not(test), allow(dead_code))]

#[cfg(test)]
mod tests {
    use serde_json::Value as JsonValue;

    use crate::runner::{materialize_artifact, RunResult};

    #[test]
    fn canonical_metrics_are_present_in_emitted_artifacts() {
        let artifact = materialize_artifact(RunResult::success(
            "pr",
            "pr_smoke_pipeline",
            JsonValue::Null,
            1_000,
        ))
        .expect("artifact");

        for key in [
            "duration_secs",
            "records_per_sec",
            "mb_per_sec",
            "cpu_secs",
            "peak_rss_mb",
            "batch_count",
        ] {
            assert!(
                artifact.canonical_metrics.get(key).is_some(),
                "missing canonical metric {key}"
            );
        }
    }

    #[test]
    fn connector_specific_metrics_attach_without_breaking_schema() {
        let artifact = materialize_artifact(RunResult::success(
            "pr",
            "pr_smoke_pipeline",
            serde_json::json!({
                "destination": {
                    "insert_statement_count": 12
                }
            }),
            1_000,
        ))
        .expect("artifact");

        assert_eq!(
            artifact.connector_metrics["destination"]["insert_statement_count"],
            serde_json::json!(12)
        );
    }
}
