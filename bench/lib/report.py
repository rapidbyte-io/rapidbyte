#!/usr/bin/env python3
"""Criterion-style statistical report for benchmark results.

Usage:
    python3 report.py <rows> <profile> <mode1:file1> [mode2:file2] ...

Each mode argument is "mode_name:results_file_path".
Produces formatted output with mean +/- std dev, confidence intervals,
throughput in rows/s and MB/s, and speedup ratios vs the baseline (first mode).
"""

import json
import math
import sys
from collections import OrderedDict


def load_results(path: str) -> list[dict]:
    results = []
    try:
        for line in open(path):
            line = line.strip()
            if line:
                results.append(json.loads(line))
    except FileNotFoundError:
        pass
    return results


def stats(values: list[float]) -> dict:
    """Compute mean, std dev, min, max, and 95% confidence interval."""
    n = len(values)
    if n == 0:
        return {"mean": 0, "std": 0, "min": 0, "max": 0, "ci_lo": 0, "ci_hi": 0, "n": 0}
    mean = sum(values) / n
    if n > 1:
        variance = sum((x - mean) ** 2 for x in values) / (n - 1)
        std = math.sqrt(variance)
        # 95% CI using t-distribution approximation (t ~ 2.0 for small n)
        t_val = {2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365}.get(n, 1.96)
        margin = t_val * std / math.sqrt(n)
    else:
        std = 0
        margin = 0
    return {
        "mean": mean,
        "std": std,
        "min": min(values),
        "max": max(values),
        "ci_lo": mean - margin,
        "ci_hi": mean + margin,
        "n": n,
    }


def fmt_ci(s: dict, unit: str = "s") -> str:
    """Format as criterion-style: [lo mean hi]"""
    if s["n"] == 0:
        return "no data"
    if unit == "ms":
        return f'[{s["ci_lo"]:.1f} {s["mean"]:.1f} {s["ci_hi"]:.1f}] ms'
    return f'[{s["ci_lo"]:.4f} {s["mean"]:.4f} {s["ci_hi"]:.4f}] {unit}'


def fmt_metric_value(val: float, unit: str) -> str:
    if unit == "ms":
        return f"{val:.1f}{unit}"
    if unit == "%":
        return f"{val:.2f}{unit}"
    if unit in {"MB", "cores"}:
        return f"{val:.2f} {unit}"
    if unit == "count":
        return f"{val:.1f}"
    return f"{val:.4f}{unit}"


def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <rows> <profile> <mode:file> ...", file=sys.stderr)
        sys.exit(1)

    rows = int(sys.argv[1])
    profile = sys.argv[2]

    # Parse mode:file pairs (preserving order)
    modes: OrderedDict[str, list[dict]] = OrderedDict()
    for arg in sys.argv[3:]:
        mode, path = arg.split(":", 1)
        modes[mode] = load_results(path)

    # Filter out empty modes
    modes = OrderedDict((m, r) for m, r in modes.items() if r)

    if not modes:
        print("  No results collected")
        sys.exit(1)

    mode_names = list(modes.keys())
    baseline = mode_names[0]

    # Get bytes_read from first available result
    first_results = next(iter(modes.values()))
    ref = first_results[0]
    bytes_read = ref.get("bytes_read", 0)
    avg_row_bytes = bytes_read // rows if rows > 0 else 0

    # ── Header ────────────────────────────────────────────────────
    samples_str = ", ".join(
        f"{len(r)} {m.upper()}" for m, r in modes.items()
    )
    print(f"  Profile:     {profile} ({avg_row_bytes} B/row)")
    print(f"  Dataset:     {rows:,} rows, {bytes_read / 1048576:.2f} MB")
    print(f"  Samples:     {samples_str}")
    print()

    # ── Criterion-style per-mode output ───────────────────────────
    for label, results in modes.items():
        durations = [r["duration_secs"] for r in results]
        s = stats(durations)
        rps_vals = [rows / d for d in durations if d > 0]
        rps = stats(rps_vals)
        mbps_vals = [bytes_read / d / 1048576 for d in durations if d > 0]
        mbps = stats(mbps_vals)

        print(f"  connector-postgres/{label}/{rows}")
        print(f"                        time:   {fmt_ci(s)}")
        print(f"                        thrpt:  [{rps['ci_lo']:,.0f} {rps['mean']:,.0f} {rps['ci_hi']:,.0f}] rows/s")
        print(f"                                [{mbps['ci_lo']:.2f} {mbps['mean']:.2f} {mbps['ci_hi']:.2f}] MB/s")
        print()

    # ── Speedup comparisons vs baseline ──────────────────────────
    if len(modes) > 1:
        baseline_avg = stats([r["duration_secs"] for r in modes[baseline]])["mean"]
        for mode in mode_names[1:]:
            mode_avg = stats([r["duration_secs"] for r in modes[mode]])["mean"]
            if mode_avg > 0.001 and baseline_avg > 0.001:
                ratio = baseline_avg / mode_avg
                if ratio >= 1.0:
                    print(f"  {mode.upper()} vs {baseline.upper()}:  {ratio:.2f}x faster")
                else:
                    print(f"  {mode.upper()} vs {baseline.upper()}:  {1/ratio:.2f}x slower")
        print()

    # ── Detailed metrics table ────────────────────────────────────
    # Dynamic column widths based on mode count
    col_w = 12
    hdr_parts = ["  {:<22s}"] + ["{:>" + str(col_w) + "s}"] * len(modes) + ["{:>8s}"]
    hdr = "  ".join(hdr_parts)
    headers = ["Metric (mean)"] + [m.upper() for m in mode_names] + ["vs " + baseline.upper()]
    print(hdr.format(*headers))
    print(hdr.format("-" * 22, *(["-" * col_w] * len(modes)), "-" * 8))

    metrics = [
        ("Total duration", "duration_secs", "s"),
        ("Dest duration", "dest_duration_secs", "s"),
        ("  Connect", "dest_connect_secs", "s"),
        ("  Flush", "dest_flush_secs", "s"),
        ("  Arrow decode", "dest_arrow_decode_secs", "s"),
        ("  Commit", "dest_commit_secs", "s"),
        ("  VM setup", "dest_vm_setup_secs", "s"),
        ("  Recv loop", "dest_recv_secs", "s"),
        ("  WASM overhead", "wasm_overhead_secs", "s"),
        ("Source duration", "source_duration_secs", "s"),
        ("  Connect", "source_connect_secs", "s"),
        ("  Query", "source_query_secs", "s"),
        ("  Fetch", "source_fetch_secs", "s"),
        ("  Arrow encode", "source_arrow_encode_secs", "s"),
        ("Source module load", "source_module_load_ms", "ms"),
        ("Dest module load", "dest_module_load_ms", "ms"),
        ("CPU cores (avg)", "cpu_cores_mean", "cores"),
        ("CPU cores (peak)", "cpu_cores_max", "cores"),
        ("CPU total util (avg)", "cpu_total_util_pct_mean", "%"),
        ("CPU total util (peak)", "cpu_total_util_pct_max", "%"),
        ("RSS memory (avg)", "mem_rss_mb_mean", "MB"),
        ("RSS memory (peak)", "mem_rss_mb_max", "MB"),
        ("Resource samples", "resource_samples", "count"),
    ]

    for label, key, unit in metrics:
        vals = []
        for mode in mode_names:
            results = modes[mode]
            s = stats([r.get(key, 0) for r in results])
            vals.append((s, fmt_metric_value(s["mean"], unit) if s["n"] > 0 else "-"))

        b_mean = vals[0][0]["mean"]
        # Speedup column: last mode vs baseline
        last_mean = vals[-1][0]["mean"]
        speedup = f'{b_mean/last_mean:.1f}x' if last_mean > 0.001 else "-"

        row = [label] + [v[1] for v in vals] + [speedup]
        print(hdr.format(*row))

    # ── Throughput summary ────────────────────────────────────────
    print()
    thrpt_vals = []
    for mode in mode_names:
        results = modes[mode]
        valid = [r for r in results if r["duration_secs"] > 0]
        rps = sum(rows / r["duration_secs"] for r in valid) / len(valid) if valid else 0
        mbps = sum(bytes_read / r["duration_secs"] / 1048576 for r in valid) / len(valid) if valid else 0
        thrpt_vals.append((rps, mbps))

    b_rps, b_mbps = thrpt_vals[0]
    last_rps, last_mbps = thrpt_vals[-1]
    rps_su = f"{last_rps/b_rps:.1f}x" if b_rps > 0 else "-"
    mbps_su = f"{last_mbps/b_mbps:.1f}x" if b_mbps > 0 else "-"

    row_rps = ["Throughput (rows/s)"] + [f"{t[0]:,.0f}" for t in thrpt_vals] + [rps_su]
    row_mbps = ["Throughput (MB/s)"] + [f"{t[1]:.2f}" for t in thrpt_vals] + [mbps_su]
    print(hdr.format(*row_rps))
    print(hdr.format(*row_mbps))


if __name__ == "__main__":
    main()
