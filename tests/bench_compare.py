#!/usr/bin/env python3
"""Compare benchmark results across runs.

Usage:
    python3 tests/bench_compare.py                    # Compare last 2 runs
    python3 tests/bench_compare.py --last 5           # Show last 5 runs
    python3 tests/bench_compare.py --sha abc123 def456  # Compare specific commits
"""
import argparse
import json
import sys
from pathlib import Path
from collections import defaultdict

RESULTS_FILE = Path(__file__).parent.parent / "target" / "bench_results" / "results.jsonl"

def load_results():
    if not RESULTS_FILE.exists():
        print("No benchmark results found.")
        print("Run a benchmark first:  just bench-connector-postgres")
        print("Then compare runs:      just bench-compare")
        sys.exit(1)
    results = []
    for line in RESULTS_FILE.read_text().splitlines():
        if line.strip():
            results.append(json.loads(line))
    return results

def group_by_run(results):
    """Group results by (git_sha, mode, bench_rows)."""
    groups = defaultdict(list)
    for r in results:
        key = (r.get("git_sha", "?"), r.get("mode", "?"), r.get("bench_rows", 0))
        groups[key].append(r)
    return groups

def avg(results, key):
    vals = [r.get(key, 0) for r in results]
    return sum(vals) / len(vals) if vals else 0

def main():
    parser = argparse.ArgumentParser(description="Compare benchmark results across runs")
    parser.add_argument("--last", type=int, default=2, help="Compare last N runs")
    parser.add_argument("--sha", nargs="+", help="Compare specific git SHAs")
    parser.add_argument("--rows", type=int, help="Filter by row count")
    args = parser.parse_args()

    results = load_results()
    if args.rows:
        results = [r for r in results if r.get("bench_rows") == args.rows]

    if args.sha:
        results = [r for r in results if r.get("git_sha") in args.sha]

    groups = group_by_run(results)
    if not groups:
        print("No matching results found.")
        return

    # Get unique SHAs in order
    seen = []
    for r in results:
        sha = r.get("git_sha", "?")
        if sha not in seen:
            seen.append(sha)

    shas = seen[-args.last:] if not args.sha else args.sha

    metrics = [
        ("Duration (s)", "duration_secs"),
        ("Source (s)", "source_duration_secs"),
        ("Dest (s)", "dest_duration_secs"),
        ("  Flush (s)", "dest_flush_secs"),
        ("  Commit (s)", "dest_commit_secs"),
        ("  WASM overhead (s)", "wasm_overhead_secs"),
        ("  VM setup (s)", "dest_vm_setup_secs"),
        ("  Recv loop (s)", "dest_recv_secs"),
        ("Source load (ms)", "source_module_load_ms"),
        ("Dest load (ms)", "dest_module_load_ms"),
    ]

    for mode in ["insert", "copy"]:
        print(f"\n{'=' * 60}")
        print(f"  Mode: {mode.upper()}")
        print(f"{'=' * 60}")

        header = f"  {'Metric':<22s}"
        for sha in shas:
            header += f"  {sha:>10s}"
        if len(shas) == 2:
            header += f"  {'Change':>10s}"
        print(header)
        print(f"  {'-' * 22}" + f"  {'-' * 10}" * len(shas) + ("  " + "-" * 10 if len(shas) == 2 else ""))

        for label, key in metrics:
            line = f"  {label:<22s}"
            vals = []
            for sha in shas:
                matching = [r for r in results if r.get("git_sha") == sha and r.get("mode") == mode]
                v = avg(matching, key)
                vals.append(v)
                line += f"  {v:>10.3f}"
            if len(vals) == 2 and vals[0] > 0:
                pct = (vals[1] - vals[0]) / vals[0] * 100
                sign = "+" if pct > 0 else ""
                line += f"  {sign}{pct:.1f}%"
            print(line)

if __name__ == "__main__":
    main()
