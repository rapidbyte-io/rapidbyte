#!/usr/bin/env python3
"""Criterion-style statistical report for benchmark results.

Usage:
    python3 report.py <insert_results.jsonl> <copy_results.jsonl> <rows>

Produces formatted output with mean +/- std dev, confidence intervals,
throughput in rows/s and MB/s, and speedup ratios.
"""

import json
import math
import sys


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


def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <insert_file> <copy_file> <rows>", file=sys.stderr)
        sys.exit(1)

    insert_results = load_results(sys.argv[1])
    copy_results = load_results(sys.argv[2])
    rows = int(sys.argv[3])
    profile = sys.argv[4] if len(sys.argv) > 4 else "unknown"

    if not insert_results and not copy_results:
        print("  No results collected")
        sys.exit(1)

    ref = insert_results[0] if insert_results else copy_results[0]
    bytes_read = ref.get("bytes_read", 0)
    avg_row_bytes = bytes_read // rows if rows > 0 else 0

    # ── Header ────────────────────────────────────────────────────
    print(f"  Profile:     {profile} ({avg_row_bytes} B/row)")
    print(f"  Dataset:     {rows:,} rows, {bytes_read / 1048576:.2f} MB")
    print(f"  Samples:     {len(insert_results)} INSERT, {len(copy_results)} COPY")
    print()

    # ── Criterion-style per-mode output ───────────────────────────
    for label, results in [("INSERT", insert_results), ("COPY", copy_results)]:
        if not results:
            continue
        durations = [r["duration_secs"] for r in results]
        s = stats(durations)
        rps_vals = [rows / d for d in durations if d > 0]
        rps = stats(rps_vals)
        mbps_vals = [bytes_read / d / 1048576 for d in durations if d > 0]
        mbps = stats(mbps_vals)

        print(f"  connector-postgres/{label.lower()}/{rows}")
        print(f"                        time:   {fmt_ci(s)}")
        print(f"                        thrpt:  [{rps['ci_lo']:,.0f} {rps['mean']:,.0f} {rps['ci_hi']:,.0f}] rows/s")
        print(f"                                [{mbps['ci_lo']:.2f} {mbps['mean']:.2f} {mbps['ci_hi']:.2f}] MB/s")
        print()

    # ── Speedup comparison ────────────────────────────────────────
    if insert_results and copy_results:
        i_avg = stats([r["duration_secs"] for r in insert_results])["mean"]
        c_avg = stats([r["duration_secs"] for r in copy_results])["mean"]
        if c_avg > 0.001 and i_avg > 0.001:
            ratio = i_avg / c_avg
            if ratio >= 1.0:
                print(f"  COPY vs INSERT:  {ratio:.2f}x faster")
            else:
                print(f"  COPY vs INSERT:  {1/ratio:.2f}x slower")
            print()

    # ── Detailed metrics table ────────────────────────────────────
    hdr = "  {:<22s}  {:>12s}  {:>12s}  {:>8s}"
    print(hdr.format("Metric (mean)", "INSERT", "COPY", "Speedup"))
    print(hdr.format("-" * 22, "-" * 12, "-" * 12, "-" * 8))

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
    ]

    for label, key, unit in metrics:
        i_s = stats([r.get(key, 0) for r in insert_results]) if insert_results else stats([])
        c_s = stats([r.get(key, 0) for r in copy_results]) if copy_results else stats([])
        speedup = f'{i_s["mean"]/c_s["mean"]:.1f}x' if c_s["mean"] > 0.001 else "-"
        i_val = f'{i_s["mean"]:.4f}{unit}' if i_s["n"] > 0 else "-"
        c_val = f'{c_s["mean"]:.4f}{unit}' if c_s["n"] > 0 else "-"
        print(hdr.format(label, i_val, c_val, speedup))

    # ── Throughput summary ────────────────────────────────────────
    print()
    if insert_results:
        valid_i = [r for r in insert_results if r["duration_secs"] > 0]
        i_rps = sum(rows / r["duration_secs"] for r in valid_i) / len(valid_i) if valid_i else 0
        i_mbps = sum(bytes_read / r["duration_secs"] / 1048576 for r in valid_i) / len(valid_i) if valid_i else 0
    else:
        i_rps = i_mbps = 0

    if copy_results:
        valid_c = [r for r in copy_results if r["duration_secs"] > 0]
        c_rps = sum(rows / r["duration_secs"] for r in valid_c) / len(valid_c) if valid_c else 0
        c_mbps = sum(bytes_read / r["duration_secs"] / 1048576 for r in valid_c) / len(valid_c) if valid_c else 0
    else:
        c_rps = c_mbps = 0

    rps_su = f"{c_rps/i_rps:.1f}x" if i_rps > 0 else "-"
    mbps_su = f"{c_mbps/i_mbps:.1f}x" if i_mbps > 0 else "-"
    print(hdr.format("Throughput (rows/s)", f"{i_rps:,.0f}", f"{c_rps:,.0f}", rps_su))
    print(hdr.format("Throughput (MB/s)", f"{i_mbps:.2f}", f"{c_mbps:.2f}", mbps_su))


if __name__ == "__main__":
    main()
