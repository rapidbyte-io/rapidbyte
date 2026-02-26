# Heuristic Parallelism Design (Static `auto`, Correctness-First)

## Goal

Replace the current underutilizing default (`parallelism = 1`) with a deterministic `auto` heuristic that improves throughput/core utilization while preserving current correctness guarantees.

## Scope

- Design a static `auto` calculation done once at run start.
- Keep explicit numeric override behavior.
- Keep CDC/incremental and replace-mode semantics unchanged.

## Non-Goals

- No runtime adaptive control loop in this phase.
- No retry policy behavior changes.
- No destination architecture fallback paths.

## Configuration Model

`resources.parallelism` becomes a tagged value:

- `auto`
- positive integer (`>= 1`)

Default value when omitted: `auto`.

## Deterministic Heuristic

Compute `effective_parallelism` once before stream context fanout.

### Inputs

- `available_cores = std::thread::available_parallelism().unwrap_or(1)`
- `cap = min(available_cores, 16)`
- `transform_count = config.transforms.len()`
- source connector id (`source-postgres` partitioning support check)
- per-stream sync mode and destination write mode

### Eligibility for Fanout

A stream is partition-eligible only if all are true:

1. source connector is `source-postgres`
2. stream `sync_mode == full_refresh`
3. destination `write_mode != replace`

Let:

- `eligible_streams = count(partition-eligible streams)`

### Formula

1. If `eligible_streams == 0`, then `base_target = 1`.
2. Else compute a bounded per-stream shard budget:
   - `max_shard_depth = 4`
   - `per_stream_budget = max(1, cap / eligible_streams)`
   - `per_stream_target = min(max_shard_depth, per_stream_budget)`
   - `base_target = min(cap, eligible_streams * per_stream_target)`
3. `transform_factor = if transform_count == 0 {
     1.0
   } else {
     max(0.6, 1.0 / (1.0 + 0.15 * transform_count as f64))
   }`
4. `raw_target = round(base_target as f64 * transform_factor) as u32`
5. `effective_parallelism = clamp(max(1, raw_target), 1, cap)`

For explicit numeric configuration:

- `effective_parallelism = max(1, configured_value)`

## Behavior by Workload Type

- Full-refresh append/upsert with source-postgres: fanout enabled up to `effective_parallelism`.
- Incremental/CDC: no new sharding behavior (single stream context behavior preserved).
- Replace mode: existing non-sharded path preserved.
- Non-source-postgres source connectors: no partition fanout from this heuristic (parallelism still limits worker semaphore where applicable).

## Correctness Invariants

Must remain true after introducing `auto`:

1. Retry gate remains `is_retryable && safe_to_retry` only.
2. Post-commit unsafe errors are never retried.
3. Deterministic first-error cancellation semantics unchanged.
4. Leader-prepared/worker-write-only destination sequencing unchanged.

## Telemetry Additions

Add run-level fields to engine result / CLI JSON output:

- `parallelism_mode` (`manual|auto`)
- `parallelism_effective`
- `parallelism_cap`
- `parallelism_available_cores`
- `parallelism_eligible_streams`
- `parallelism_transform_count`

These fields make benchmark acceptance analysis reproducible.

## Logging

Emit a startup log event at run begin:

- pipeline name
- mode (`auto`/`manual`)
- chosen effective parallelism
- cap, cores, eligible streams, transform count
- explanation tag (`"static_heuristic_v1"`)

## Validation Matrix

### Unit/Config Validation

- parse accepts `parallelism: auto`
- parse accepts positive integer
- parse rejects `parallelism: 0`
- default omitted parallelism resolves to `auto`

### Orchestrator Semantics

- full-refresh + eligible source fans out to `effective_parallelism`
- incremental/CDC does not fan out regardless of auto value
- replace mode does not fan out
- manual numeric override still honored

### Correctness Regressions

- commit-state retry safety tests unchanged and passing
- cancellation determinism tests unchanged and passing

### Performance Gate

Against current baseline (`parallelism` omitted old behavior effectively 1):

- medium profile: measurable throughput uplift target >= +20%
- large profile: measurable throughput uplift target >= +10%
- no increase in unsafe retry incidents

## Rollout

1. Land static `auto` + telemetry.
2. Run full correctness suite first.
3. Run benchmark matrix (`small/medium/large`, `iters >= 3`).
4. Re-baseline docs/bench artifacts after results.

## Calibration Notes (2026-02-26)

The constants above are intentionally conservative and based on local benchmark artifacts:

- Single-stream medium COPY sweep in `/tmp/rapidbyte_ab_medium.jsonl` showed no utilization gain from raising parallelism to 4 and often worse mean latency.
  - baseline (`parallelism=1`): mean `2.62s`
  - `parallelism=4`: mean `2.98s`
- Recent profile runs in `/tmp/bench_*_20260226.log` also show high first-iteration variance (AOT/module warmup effects), so we avoid aggressive per-stream fanout by default.

Therefore:

- `max_shard_depth=4` is selected for v1 static auto.
- transform penalty now scales with transform count and has a safety floor at `0.6` to avoid collapsing parallelism too aggressively.

Example transform factors:

- 0 transforms: `1.00`
- 1 transform: `~0.87`
- 2 transforms: `~0.77`
- 4 transforms: `~0.63`
- 8+ transforms: floored at `0.60`

These values should be revisited after collecting stable multi-iteration telemetry with the new `parallelism_mode` fields.

## Future Path

Use the static `auto` telemetry data to calibrate and optionally introduce `adaptive` later as opt-in, not default.

---

This design intentionally chooses deterministic behavior first to maximize confidence in correctness and benchmark reproducibility.
