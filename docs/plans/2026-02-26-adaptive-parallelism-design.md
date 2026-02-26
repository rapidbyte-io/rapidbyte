# Adaptive Parallelism Design (Correctness-First)

> Draft focused on safety and measurable outcomes. This is design-only (no implementation in this document).

## Goal

Increase throughput and core utilization automatically while preserving destination correctness guarantees (commit-state-aware retries, deterministic cancellation, no CDC/incremental behavior changes).

## Non-Goals

- No behavior changes to CDC/incremental execution in this phase.
- No fallback to legacy destination architecture.
- No adaptive control of retry policy itself.

## Constraints

- Current execution architecture is stream/shard task fanout with semaphore-bounded concurrency.
- Destination correctness depends on commit-state classification (`safe_to_retry` gating already implemented).
- Existing benchmark + JSON metrics path is the acceptance source of truth.

## Mode Model

Add a new mode set for `resources.parallelism`:

- `manual(n)` where `n >= 1`
- `auto` (deterministic static heuristic)
- `adaptive` (dynamic concurrency control)

Default recommendation for rollout:

- Keep default as `auto`.
- `adaptive` is explicit opt-in until hard correctness/perf gates are met.

## Eligibility Rules (Hard Safety Gates)

Adaptive control is only active when all are true:

1. Source connector supports partitioned full-refresh fanout (`source-postgres` today).
2. Stream sync mode is `full_refresh`.
3. Destination write mode is not `replace` for mid-run scaling decisions.
4. Pipeline is not in dry-run.
5. No active safety freeze condition.

If any condition is false, controller degrades to static `auto` result.

## Safety Freeze Conditions

When any of these occur, adaptive adjustments stop immediately (hold current concurrency):

1. Any destination error with `commit_state != None` or `safe_to_retry == false`.
2. Retry attempt in progress for the run.
3. Deterministic cancellation path activated due to first worker failure.
4. Replace-mode lifecycle stage not finalized.

Once frozen, no further scale-up/scale-down in the same run.

## Control Objective

Maximize useful destination worker activity while preventing overload:

- Increase if workers are mostly active and CPU headroom exists.
- Decrease if workers are mostly waiting, skew is severe, or error pressure rises.

The controller adjusts only future task starts; it does not preempt running workers.

## Signals

Use only existing/plumbable signals already in engine/CLI metrics:

- `process_cpu_pct_available_cores`
- `worker_dest_active_secs`
- `worker_dest_recv_secs`
- `worker_records_skew_ratio`
- `retry_count`
- per-worker error events/classification

Derived metrics per control window:

- `active_ratio = worker_dest_active_secs / max(worker_dest_total_secs, epsilon)`
- `wait_ratio = worker_dest_recv_secs / max(worker_dest_total_secs, epsilon)`
- `cpu_ratio = process_cpu_pct_available_cores / 100.0`

## Control Loop

### Windowing

- Tick every `T = 3s` with `min_samples = 2` windows before first adjustment.
- Use EWMA smoothing (`alpha = 0.35`) for `active_ratio`, `wait_ratio`, `cpu_ratio`.
- Cooldown `C = 2 ticks` after each adjustment to avoid oscillation.

### Bounds

- `min_parallelism = 1`
- `max_parallelism = min(available_cores, 16)`
- Initial value for adaptive mode starts from deterministic `auto`.

### Adjustment Step

- Step size is `1` worker per adjustment.
- No multi-step jumps in v1.

### Scale-Up Rule

Scale up by 1 when all are true:

1. `active_ratio_ewma >= 0.70`
2. `cpu_ratio_ewma <= 0.85`
3. `wait_ratio_ewma <= 0.25`
4. `worker_records_skew_ratio <= 2.0` (or unavailable)
5. No retries/errors in the last 2 windows

### Scale-Down Rule

Scale down by 1 when any are true:

1. `wait_ratio_ewma >= 0.55` and `active_ratio_ewma <= 0.40`
2. `cpu_ratio_ewma >= 0.95` with no throughput gain over 2 windows
3. `worker_records_skew_ratio >= 4.0`
4. any retryable error appears (pre-freeze cautionary downshift)

### Tie-Break Rule

If both up/down conditions trigger, prefer downshift (safety bias).

## Concurrency Actuation Model

Implementation-friendly approach for current semaphore architecture:

- Maintain `desired_parallelism` and `effective_parallelism`.
- Scale up: `semaphore.add_permits(1)` and increment effective.
- Scale down: acquire one permit into `held_permits` pool (never released until scale-up); this reduces future starts without killing active tasks.
- Never block worker hot path for controller actions.

## Correctness Invariants

Must hold for every adaptive run:

1. Retry eligibility remains `is_retryable && safe_to_retry` only.
2. First worker failure still deterministically cancels siblings.
3. Post-commit-unsafe errors are never retried, regardless of concurrency state.
4. Replace lifecycle ownership remains single-owner and unaffected by controller actions.

## Observability Additions

Emit run-level telemetry fields:

- `parallelism_mode` (`manual|auto|adaptive`)
- `parallelism_initial`
- `parallelism_effective_max`
- `parallelism_effective_min`
- `parallelism_adjustments_up`
- `parallelism_adjustments_down`
- `parallelism_safety_freeze` (`bool`)
- `parallelism_freeze_reason` (enum string)

Emit time-series events to tracing logs:

- `adaptive_tick`: smoothed metrics + decision
- `adaptive_adjust`: direction, old/new
- `adaptive_freeze`: reason

## Rollout Plan

1. Ship behind explicit `adaptive` mode only.
2. Run correctness matrix first:
   - full-refresh append/upsert with partition fanout
   - injected pre-commit and post-commit destination failures
   - deterministic cancellation tests
3. Run benchmark matrix (`small/medium/large`, `iters >= 3`) comparing `auto` vs `adaptive`.
4. Promote only if all gates pass.

## Acceptance Gates

All must pass:

1. Zero regressions in correctness tests covering retry safety and cancellation semantics.
2. Zero unsafe retries in post-commit scenarios.
3. Throughput uplift vs `auto` baseline:
   - `medium`: >= +10%
   - `large`: >= +5%
4. CPU utilization improvement in destination write window without increased error rate.
5. No oscillation pathologies:
   - adjustment count bounded (for example <= 8 per run on medium profile)

## Failure Handling

If controller encounters internal error:

1. Log `adaptive_controller_error`.
2. Freeze adjustments and continue at current effective parallelism.
3. Do not alter retry/cancellation behavior.

## Open Questions

1. Should transform-heavy pipelines have a stricter max cap than 16?
2. Should skew use bytes skew in addition to records skew?
3. Should adaptive be disabled when stream count is 1 (no fanout opportunity)?
4. Should we require a minimum row/byte threshold before controller activates?

## Recommendation

Implement deterministic `auto` first as default and land adaptive as opt-in with the safety-freeze model above. This preserves correctness guarantees while enabling measurable, controlled progress toward higher core utilization.
