import json
import argparse
import statistics
from collections import defaultdict
from pathlib import Path


DEFAULT_THRESHOLDS = {
    "throughput_drop_pct": 10.0,
    "latency_increase_pct": 15.0,
}


def load_artifacts(path):
    rows = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def compare_artifact_sets(baseline_rows, candidate_rows):
    return compare_against_baseline(baseline_rows, candidate_rows, DEFAULT_THRESHOLDS)


def compare_against_baseline(
    baseline_rows,
    candidate_rows,
    thresholds=None,
    min_samples=3,
):
    thresholds = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    baseline_groups = _group_artifacts(baseline_rows)
    candidate_groups = _group_artifacts(candidate_rows)
    comparisons = []

    for identity, candidates in sorted(candidate_groups.items()):
        baselines = baseline_groups.get(identity)
        scenario_id = identity[0]
        if not baselines:
            comparisons.append(
                {
                    "scenario_id": scenario_id,
                    "status": "missing_baseline",
                    "reasons": ["no matching baseline artifacts"],
                }
            )
            continue

        if len(candidates) < min_samples or len(baselines) < min_samples:
            comparisons.append(
                {
                    "scenario_id": scenario_id,
                    "status": "insufficient_confidence",
                    "reasons": [
                        f"baseline_samples={len(baselines)} candidate_samples={len(candidates)}",
                    ],
                }
            )
            continue

        comparison = _compare_group(identity, baselines, candidates, thresholds)
        comparisons.append(comparison)

    return {"comparisons": comparisons}


def _group_artifacts(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[_identity(row)].append(row)
    return grouped


def _identity(row):
    return (
        row["scenario_id"],
        row["suite_id"],
        row.get("hardware_class", "unknown"),
        row.get("build_mode", "debug"),
        json.dumps(row.get("execution_flags", {}), sort_keys=True),
    )


def _compare_group(identity, baselines, candidates, thresholds):
    baseline_throughput = _median_metric(baselines, "records_per_sec")
    candidate_throughput = _median_metric(candidates, "records_per_sec")
    baseline_latency = _median_metric(baselines, "duration_secs")
    candidate_latency = _median_metric(candidates, "duration_secs")

    throughput_drop_pct = 0.0
    if baseline_throughput > 0:
        throughput_drop_pct = ((baseline_throughput - candidate_throughput) / baseline_throughput) * 100.0

    latency_increase_pct = 0.0
    if baseline_latency > 0:
        latency_increase_pct = ((candidate_latency - baseline_latency) / baseline_latency) * 100.0

    reasons = []
    if throughput_drop_pct > thresholds["throughput_drop_pct"]:
        reasons.append(
            f"throughput dropped {throughput_drop_pct:.1f}% ({candidate_throughput:.1f} vs {baseline_throughput:.1f})"
        )
    if latency_increase_pct > thresholds["latency_increase_pct"]:
        reasons.append(
            f"latency increased {latency_increase_pct:.1f}% ({candidate_latency:.3f}s vs {baseline_latency:.3f}s)"
        )
    if any(not row.get("correctness", {}).get("passed", False) for row in candidates):
        reasons.append("correctness failed")

    return {
        "scenario_id": identity[0],
        "status": "regressed" if reasons else "ok",
        "reasons": reasons,
        "baseline_samples": len(baselines),
        "candidate_samples": len(candidates),
        "baseline_throughput": baseline_throughput,
        "candidate_throughput": candidate_throughput,
        "baseline_latency": baseline_latency,
        "candidate_latency": candidate_latency,
    }


def _median_metric(rows, metric_name):
    values = [row["canonical_metrics"][metric_name] for row in rows]
    return statistics.median(values)


def render_markdown(report):
    lines = ["# Benchmark Comparison", ""]
    for comparison in report["comparisons"]:
        lines.append(f"- `{comparison['scenario_id']}`: {comparison['status']}")
        for reason in comparison.get("reasons", []):
            lines.append(f"  - {reason}")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Compare benchmark artifact sets")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--min-samples", type=int, default=3)
    parser.add_argument("--throughput-drop-pct", type=float, default=DEFAULT_THRESHOLDS["throughput_drop_pct"])
    parser.add_argument("--latency-increase-pct", type=float, default=DEFAULT_THRESHOLDS["latency_increase_pct"])
    args = parser.parse_args()

    report = compare_against_baseline(
        load_artifacts(args.baseline),
        load_artifacts(args.candidate),
        thresholds={
            "throughput_drop_pct": args.throughput_drop_pct,
            "latency_increase_pct": args.latency_increase_pct,
        },
        min_samples=args.min_samples,
    )
    print(render_markdown(report), end="")


if __name__ == "__main__":
    main()
