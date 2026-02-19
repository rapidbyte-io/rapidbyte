# Baseline Benchmark Results

Pre-optimization baseline captured before implementing changes from `docs/PERFORMANCE_OPTIMIZATIONS.md`.

## Environment

| Property | Value |
|---|---|
| Git SHA | `4dc4c72` |
| Git branch | `perf` |
| Date | 2026-02-19 |
| OS | Darwin 24.6.0 (macOS) |
| Build mode | release |
| AOT compiled | yes |
| Iterations | 3 per mode |
| PostgreSQL | Docker local (port 5433) |

## 10,000 Rows (0.89 MB, 93 B/row)

| Metric (avg) | INSERT | COPY | Speedup |
|---|---|---|---|
| **Total duration** | 0.155s | 0.093s | 1.7x |
| **Dest duration** | 0.120s | 0.083s | 1.4x |
| - Connect | 0.012s | 0.010s | 1.3x |
| - Flush | 0.092s | 0.048s | 1.9x |
| - Arrow decode | 0.000s | 0.000s | - |
| - Commit | 0.002s | 0.013s | 0.1x |
| - VM setup | 0.001s | 0.000s | - |
| - Recv loop | 0.119s | 0.083s | 1.4x |
| - WASM overhead | 0.000s | 0.000s | - |
| **Source duration** | 0.047s | 0.040s | 1.2x |
| - Connect | 0.012s | 0.009s | 1.3x |
| - Query | 0.012s | 0.009s | 1.3x |
| - Fetch | 0.020s | 0.020s | 1.0x |
| - Arrow encode | 0.002s | 0.002s | 1.0x |
| **Source module load** | 6.000ms | 1.000ms | 6.0x |
| **Dest module load** | 5.333ms | 1.667ms | 3.2x |
| **Throughput (rows/s)** | 66,340 | 110,873 | 1.7x |
| **Throughput (MB/s)** | 5.94 | 9.92 | 1.7x |

### Per-Iteration Detail (10K)

| Metric | Mode | Iter 1 | Iter 2 | Iter 3 | Min | Avg | Max |
|---|---|---|---|---|---|---|---|
| Total (s) | INSERT | 0.1796 | 0.1203 | 0.1661 | 0.1203 | 0.1553 | 0.1796 |
| Total (s) | COPY | 0.1151 | 0.0793 | 0.0836 | 0.0793 | 0.0927 | 0.1151 |
| Dest flush (s) | INSERT | 0.1070 | 0.0850 | 0.0846 | 0.0846 | 0.0922 | 0.1070 |
| Dest flush (s) | COPY | 0.0484 | 0.0486 | 0.0479 | 0.0479 | 0.0483 | 0.0486 |
| Source fetch (s) | INSERT | 0.0216 | 0.0198 | 0.0179 | 0.0179 | 0.0198 | 0.0216 |
| Source fetch (s) | COPY | 0.0188 | 0.0183 | 0.0220 | 0.0183 | 0.0197 | 0.0220 |
| Arrow encode (s) | INSERT | 0.0022 | 0.0023 | 0.0022 | 0.0022 | 0.0022 | 0.0023 |
| Arrow encode (s) | COPY | 0.0022 | 0.0021 | 0.0022 | 0.0021 | 0.0022 | 0.0022 |

## 100,000 Rows (9.04 MB, 94 B/row)

| Metric (avg) | INSERT | COPY | Speedup |
|---|---|---|---|
| **Total duration** | 1.544s | 0.572s | 2.7x |
| **Dest duration** | 1.517s | 0.559s | 2.7x |
| - Connect | 0.017s | 0.013s | 1.4x |
| - Flush | 1.479s | 0.509s | 2.9x |
| - Arrow decode | 0.003s | 0.003s | 1.2x |
| - Commit | 0.003s | 0.021s | 0.1x |
| - VM setup | 0.002s | 0.001s | - |
| - Recv loop | 1.515s | 0.558s | 2.7x |
| - WASM overhead | 0.000s | 0.000s | - |
| **Source duration** | 0.542s | 0.402s | 1.3x |
| - Connect | 0.018s | 0.013s | 1.4x |
| - Query | 0.016s | 0.014s | 1.2x |
| - Fetch | 0.499s | 0.372s | 1.3x |
| - Arrow encode | 0.047s | 0.033s | 1.4x |
| **Source module load** | 8.333ms | 2.333ms | 3.6x |
| **Dest module load** | 8.333ms | 2.333ms | 3.6x |
| **Throughput (rows/s)** | 66,096 | 176,238 | 2.7x |
| **Throughput (MB/s)** | 5.98 | 15.94 | 2.7x |

### Per-Iteration Detail (100K)

| Metric | Mode | Iter 1 | Iter 2 | Iter 3 | Min | Avg | Max |
|---|---|---|---|---|---|---|---|
| Total (s) | INSERT | 1.8617 | 1.4441 | 1.3276 | 1.3276 | 1.5444 | 1.8617 |
| Total (s) | COPY | 0.5735 | 0.5092 | 0.6331 | 0.5092 | 0.5719 | 0.6331 |
| Dest flush (s) | INSERT | 1.7678 | 1.4011 | 1.2674 | 1.2674 | 1.4788 | 1.7678 |
| Dest flush (s) | COPY | 0.4810 | 0.4612 | 0.5849 | 0.4612 | 0.5091 | 0.5849 |
| Source fetch (s) | INSERT | 0.6000 | 0.5242 | 0.3718 | 0.3718 | 0.4987 | 0.6000 |
| Source fetch (s) | COPY | 0.3757 | 0.3745 | 0.3669 | 0.3669 | 0.3724 | 0.3757 |
| Arrow encode (s) | INSERT | 0.0696 | 0.0354 | 0.0356 | 0.0354 | 0.0469 | 0.0696 |
| Arrow encode (s) | COPY | 0.0376 | 0.0311 | 0.0315 | 0.0311 | 0.0334 | 0.0376 |

## Throughput Summary

| Dataset | Mode | Rows/s | MB/s |
|---|---|---|---|
| 10K | INSERT | 66,340 | 5.94 |
| 10K | COPY | 110,873 | 9.92 |
| 100K | INSERT | 66,096 | 5.98 |
| 100K | COPY | 176,238 | 15.94 |

## Derived Allocation Estimates

Estimated from code analysis + benchmark data. These are the optimization targets from `docs/PERFORMANCE_OPTIMIZATIONS.md`.

### Destination INSERT (per pipeline run)

| Allocation source | 10K rows | 100K rows | Notes |
|---|---|---|---|
| `format_sql_value()` String allocs | 70,000 | 700,000 | `records_read x 7 cols`, each creates a heap String |
| `downcast_ref()` calls | 70,000 | 700,000 | Same count, per-cell TypeId comparison |
| SQL buffer reallocations | ~80 | ~800 | ~8 reallocs/chunk x 10/100 chunks (chunk_size=1000) |
| Upsert clause rebuilds | 10 | 100 | 1 per chunk (upsert mode only) |

### Source (per pipeline run)

| Allocation source | 10K rows | 100K rows | Notes |
|---|---|---|---|
| Intermediate `Vec<Option<T>>` | 7 | 70 | `batch_count x col_count` |
| Text column double-Vec | 7 | 70 | `Vec<Option<String>>` + `Vec<Option<&str>>` per text col per batch |
| PG FETCH round-trips | 10 | 100 | `records_read / FETCH_CHUNK(1000)` |
| Cursor type-check error allocs | 20,000 | 200,000 | `2 x records_read` (incremental mode only) |

### Key Bottleneck Hierarchy (100K INSERT)

```
Total: 1.544s
├── Dest: 1.517s (98.2%)
│   ├── Flush (INSERT execution): 1.479s (95.7%)  ← PRIMARY BOTTLENECK
│   │   ├── 700,000 String allocs (7 cols x 100,000 rows)
│   │   ├── 700,000 downcast_ref() calls
│   │   └── ~800 SQL buffer reallocations
│   ├── Connect: 0.017s
│   ├── Commit: 0.003s
│   └── Arrow decode: 0.003s
├── Source: 0.542s (35.1%, overlapped)
│   ├── Fetch: 0.499s (100 PG round-trips)
│   ├── Arrow encode: 0.047s
│   ├── Query: 0.016s
│   └── Connect: 0.018s
└── Module load: ~17ms
```

Note: Source and dest run in parallel on separate threads. Total wall time is max(source, dest), not the sum.

## Data Files

- `baseline_10k.jsonl` — 6 entries (3 INSERT + 3 COPY), machine-readable
- `baseline_100k.jsonl` — 6 entries (3 INSERT + 3 COPY), machine-readable
