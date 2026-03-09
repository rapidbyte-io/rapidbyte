#![cfg_attr(not(test), allow(dead_code))]

use std::fs::{self, File};
use std::path::Path;

use anyhow::{bail, Result};
use serde_json::{Map, Value as JsonValue};

use crate::artifact::{ArtifactCorrectness, BenchmarkArtifact};
use crate::output::write_artifact_json;
use crate::scenario::{filter_scenarios, ScenarioManifest};
use crate::workload::resolve_workload_plan;

#[derive(Debug, Clone)]
pub struct RunResult {
    suite_id: String,
    scenario_id: String,
    connector_metrics: JsonValue,
    records_written: u64,
    correctness_assertions_present: bool,
}

impl RunResult {
    pub fn success(
        suite_id: impl Into<String>,
        scenario_id: impl Into<String>,
        connector_metrics: JsonValue,
        records_written: u64,
    ) -> Self {
        Self {
            suite_id: suite_id.into(),
            scenario_id: scenario_id.into(),
            connector_metrics,
            records_written,
            correctness_assertions_present: true,
        }
    }

    pub fn without_assertions(suite_id: impl Into<String>, scenario_id: impl Into<String>) -> Self {
        Self {
            suite_id: suite_id.into(),
            scenario_id: scenario_id.into(),
            connector_metrics: JsonValue::Object(Map::new()),
            records_written: 0,
            correctness_assertions_present: false,
        }
    }
}

pub fn materialize_artifact(result: RunResult) -> Result<BenchmarkArtifact> {
    if !result.correctness_assertions_present {
        bail!(
            "benchmark run for scenario {} is missing correctness assertions",
            result.scenario_id
        );
    }

    let duration_secs = 1.0;
    let canonical_metrics = serde_json::json!({
        "duration_secs": duration_secs,
        "records_per_sec": result.records_written as f64 / duration_secs,
        "mb_per_sec": (result.records_written as f64 * 128.0) / 1024.0 / 1024.0 / duration_secs,
        "cpu_secs": 0.5,
        "peak_rss_mb": 64.0,
        "batch_count": 1,
    });

    Ok(BenchmarkArtifact {
        schema_version: 1,
        suite_id: result.suite_id,
        scenario_id: result.scenario_id,
        git_sha: "unknown".to_string(),
        hardware_class: "local-dev".to_string(),
        build_mode: "debug".to_string(),
        execution_flags: serde_json::json!({
            "synthetic": true,
            "aot": false,
        }),
        canonical_metrics,
        connector_metrics: result.connector_metrics,
        correctness: ArtifactCorrectness {
            passed: true,
            validator: "row_count".to_string(),
        },
    })
}

pub fn emit_scenario_artifacts(
    scenarios: &[ScenarioManifest],
    suite: Option<&str>,
    output_path: &Path,
) -> Result<usize> {
    let filtered = filter_scenarios(scenarios, suite, &[]);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = File::create(output_path)?;
    for scenario in &filtered {
        let workload = resolve_workload_plan(scenario)?;
        let artifact = materialize_artifact(RunResult::success(
            scenario.suite.clone(),
            scenario.id.clone(),
            serde_json::json!({
                "workload_family": format!("{:?}", scenario.workload.family),
            }),
            workload.rows,
        ))?;
        write_artifact_json(&mut file, &artifact)?;
    }

    Ok(filtered.len())
}
