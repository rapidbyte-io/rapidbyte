import tempfile
import unittest
from pathlib import Path

from benchmarks.analysis.compare import (
    compare_artifact_sets,
    compare_against_baseline,
    load_artifacts,
)


class CompareArtifactsTest(unittest.TestCase):
    def write_jsonl(self, rows):
        temp = tempfile.NamedTemporaryFile("w", delete=False, suffix=".jsonl")
        for row in rows:
            temp.write(__import__("json").dumps(row) + "\n")
        temp.close()
        return Path(temp.name)

    def test_baseline_matching_uses_scenario_and_hardware_identity(self):
        baseline = self.write_jsonl(
            [
                {
                    "scenario_id": "pr_smoke_pipeline",
                    "suite_id": "pr",
                    "hardware_class": "ci-small",
                    "build_mode": "release",
                    "execution_flags": {"aot": True},
                    "canonical_metrics": {"records_per_sec": 1000.0, "duration_secs": 1.0},
                    "correctness": {"passed": True},
                }
            ]
        )
        candidate = self.write_jsonl(
            [
                {
                    "scenario_id": "pr_smoke_pipeline",
                    "suite_id": "pr",
                    "hardware_class": "ci-small",
                    "build_mode": "release",
                    "execution_flags": {"aot": True},
                    "canonical_metrics": {"records_per_sec": 950.0, "duration_secs": 1.05},
                    "correctness": {"passed": True},
                }
            ]
        )

        result = compare_artifact_sets(load_artifacts(baseline), load_artifacts(candidate))
        self.assertEqual(len(result["comparisons"]), 1)
        self.assertEqual(result["comparisons"][0]["scenario_id"], "pr_smoke_pipeline")

    def test_regression_thresholds_apply_to_throughput_and_latency(self):
        baseline_rows = [
            {
                "scenario_id": "pr_smoke_pipeline",
                "suite_id": "pr",
                "hardware_class": "ci-small",
                "build_mode": "release",
                "execution_flags": {"aot": True},
                "canonical_metrics": {"records_per_sec": 1000.0, "duration_secs": 1.0},
                "correctness": {"passed": True},
            }
        ] * 3
        candidate_rows = [
            {
                "scenario_id": "pr_smoke_pipeline",
                "suite_id": "pr",
                "hardware_class": "ci-small",
                "build_mode": "release",
                "execution_flags": {"aot": True},
                "canonical_metrics": {"records_per_sec": 850.0, "duration_secs": 1.2},
                "correctness": {"passed": True},
            }
        ] * 3

        result = compare_against_baseline(
            baseline_rows,
            candidate_rows,
            thresholds={"throughput_drop_pct": 10.0, "latency_increase_pct": 15.0},
        )
        comparison = result["comparisons"][0]
        self.assertEqual(comparison["status"], "regressed")
        self.assertIn("throughput", comparison["reasons"][0])

    def test_low_sample_runs_report_insufficient_confidence(self):
        baseline_rows = [
            {
                "scenario_id": "pr_smoke_pipeline",
                "suite_id": "pr",
                "hardware_class": "ci-small",
                "build_mode": "release",
                "execution_flags": {"aot": True},
                "canonical_metrics": {"records_per_sec": 1000.0, "duration_secs": 1.0},
                "correctness": {"passed": True},
            }
        ]
        candidate_rows = [
            {
                "scenario_id": "pr_smoke_pipeline",
                "suite_id": "pr",
                "hardware_class": "ci-small",
                "build_mode": "release",
                "execution_flags": {"aot": True},
                "canonical_metrics": {"records_per_sec": 900.0, "duration_secs": 1.1},
                "correctness": {"passed": True},
            }
        ]

        result = compare_against_baseline(
            baseline_rows,
            candidate_rows,
            thresholds={"throughput_drop_pct": 10.0, "latency_increase_pct": 15.0},
            min_samples=3,
        )
        self.assertEqual(result["comparisons"][0]["status"], "insufficient_confidence")


if __name__ == "__main__":
    unittest.main()
