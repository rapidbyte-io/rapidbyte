# Runtime Autotune Performance Results (2026-02-28)

## Scope

Validation run for default-on in-run autotune plumbing changes:

- engine stream runtime override propagation
- source postgres partition strategy override precedence
- destination postgres COPY flush override precedence and guardrails

## Benchmark Matrix Executed

Commands run:

- `just bench postgres --profile small --iters 3`
- `just bench postgres --profile medium --iters 3`
- `just bench postgres --profile large --iters 3`

## Throughput Summary (COPY mode)

| Profile | Rows | Dataset size | Median duration | Rows/s | MB/s |
|---|---:|---:|---:|---:|---:|
| small | 100,000 | 37.46 MB | 0.9653s | 104,094 | 38.99 |
| medium | 50,000 | 80.47 MB | 1.2097s | 41,350 | 66.54 |
| large | 10,000 | 658.29 MB | 2.0949s | 4,774 | 314.26 |

## Selected Runtime Decisions

- Autotune remains default-on at config level (`resources.autotune.enabled: true` by default).
- Manual pins are propagated as stream-level runtime overrides.
- Source connector uses `StreamContext.partition_strategy` before env fallback.
- Destination connector uses `StreamContext.copy_flush_bytes_override` before connector config/adaptive fallback.
- Engine execution parallelism now honors stream-level effective parallelism rather than legacy config-only resolution.

## Probe Overhead

Current implementation stage: override resolution + precedence plumbing.

- No multi-candidate probe loop is active yet.
- Probe-specific overhead is therefore `0` in this revision.

## Regressions / Notes

- No correctness regressions observed in targeted connector and e2e coverage.
- During native-target connector testing, `dest-postgres` had a pre-existing unit-test compile issue (`TypedCol` missing `Debug` for `expect_err`); fixed by deriving `Debug`.
- Bench harness currently reports default-on runs; side-by-side benchmark automation for `autotune.enabled=false` and pinned-control sweeps is a recommended follow-up enhancement.
