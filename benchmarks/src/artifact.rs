#![cfg_attr(not(test), allow(dead_code))]

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkArtifact {
    pub schema_version: u32,
    pub suite_id: String,
    pub scenario_id: String,
    pub git_sha: String,
    pub build_mode: String,
    pub canonical_metrics: JsonValue,
    pub connector_metrics: JsonValue,
    pub correctness: ArtifactCorrectness,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactCorrectness {
    pub passed: bool,
    pub validator: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_serializes_required_top_level_fields() {
        let artifact = BenchmarkArtifact {
            schema_version: 1,
            suite_id: "pr".to_string(),
            scenario_id: "pr_smoke_pipeline".to_string(),
            git_sha: "abc1234".to_string(),
            build_mode: "release".to_string(),
            canonical_metrics: serde_json::json!({
                "duration_secs": 1.23,
                "records_per_sec": 1000.0,
            }),
            connector_metrics: serde_json::json!({
                "source": {
                    "pages": 4
                }
            }),
            correctness: ArtifactCorrectness {
                passed: true,
                validator: "row_count".to_string(),
            },
        };

        let json = serde_json::to_value(&artifact).expect("serialize artifact");
        let object = json.as_object().expect("top-level object");

        for key in [
            "schema_version",
            "suite_id",
            "scenario_id",
            "git_sha",
            "build_mode",
            "canonical_metrics",
            "connector_metrics",
            "correctness",
        ] {
            assert!(object.contains_key(key), "missing key {key}");
        }
    }
}
