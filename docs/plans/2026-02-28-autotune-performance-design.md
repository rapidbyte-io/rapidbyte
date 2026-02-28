# Runtime Autotune For Source/Destination Throughput

> **Status:** Approved design
> **Scope:** Rapidbyte engine orchestration + source/destination connector runtime knobs
> **Decision:** Implement a phase-based in-run autotuner (probe -> steady), enabled by default

## Goals

- Maximize end-to-end throughput automatically with minimal user configuration.
- Keep behavior safe and predictable using hard guardrails only.
- Preserve output correctness (no semantic changes to write mode or checkpoint semantics).
- Avoid tech debt by replacing superseded paths, not layering permanent parallel logic.

## Non-Goals

- Cross-run or historical learning in v1.
- Mid-run rollback/freeze controller complexity.
- Tuning write semantics (append/replace/upsert), retry budgets, or schema policies.

## Product Constraints (Approved)

1. Autotune is on by default.
2. Users can manually pin/tune specific knobs when needed.
3. Legacy code paths replaced by autotuner flow must be removed in the same delivery cycle.

## Architecture Decision

Use a **phase-based in-run autotuner**:

- **Probe phase:** evaluate small candidate sets over short bounded windows.
- **Steady phase:** select winner and lock settings for remainder of stream execution.

This captures most gains of adaptation while avoiding continuous-loop oscillation risk.

## Tuned Knobs (v1)

- Effective stream parallelism.
- Source postgres full-refresh partition mode: `mod` vs `range`.
- Destination postgres COPY flush threshold (`copy_flush_bytes`).

## Manual Override Model

Precedence for each knob:

1. Explicit user pin (highest)
2. Autotuner decision
3. Built-in defaults (lowest)

If a knob is explicitly set by user config, autotuner does not modify that knob.

## Eligibility Rules

- Tuning activates for streams where tuned knobs are meaningful.
- Skip tuning for tiny streams (probe overhead not worth it).
- Skip unsupported combinations (for example, non-postgres source for partition mode selection).
- Emit explicit `autotune-skipped` reason telemetry.

## Candidate Generation

### Parallelism

- Base: current auto/manual-resolved `P0` (before autotune override).
- Candidate set: `{max(1, floor(P0/2)), P0, min(cap, round(P0*1.5))}`.
- Dedupe + clamp to guardrails.

### Source Partition Mode (postgres full refresh only)

- Candidate set: `{mod, range}`.
- Skip `range` if prerequisites are unavailable.

### Destination COPY Flush Bytes

- Candidate set: `{4 MiB, 8 MiB, 16 MiB}`.
- Hard-clamp to safe bounds (`1 MiB..32 MiB`) in controller.

## Probe Budget And Windows

- Max probe rows: `min(200_000, 15% of estimated stream rows)` when estimate exists.
- Max probe wall time per stream: `30s`.
- Minimum windows per candidate: 2.
- Early-stop if leader remains >8% ahead with stable variance.

## Objective Function

Primary objective:

- Maximize `bytes_per_sec`.

Secondary tie-breakers:

- Higher `rows_per_sec`.
- Lower `dest_flush_secs / duration_secs`.
- Better CPU efficiency (`throughput / cpu_core_usage`).

Candidate invalidation (hard guardrails only):

- Out-of-bound memory behavior.
- Invalid/unsupported runtime transitions.
- Error or retry behavior beyond strict safety threshold.

## Safety Guardrails

- Never exceed orchestrator worker-cap safety budget.
- Never set illegal or semantically unsafe values.
- Never mutate write semantics or checkpoint semantics.
- If controller faults: fallback to baseline resolved values and continue run.

## Telemetry And Explainability

Emit structured per-stream decision artifacts:

- `autotune-decision`: candidates, sampled metrics, selected winner, probe cost.
- `autotune-skipped`: reason + pinned knobs.

Use existing timing counters and process metrics where available.

## Integration Points

- Engine orchestrator: introduce `AutotuneController` in stream execution path.
- Source postgres connector: accept effective partition mode via stream context override (env remains debug fallback).
- Destination postgres connector: accept effective COPY flush threshold via execution override.

## Code Hygiene Requirement (No New Tech Debt)

- Replace obsolete heuristic-only decision branches after parity verification.
- Keep one canonical decision flow for each tuned knob.
- Remove temporary compatibility scaffolding before completion unless explicitly accepted as short-lived with follow-up task.

## Verification Strategy

- Unit tests for candidate generation, clamping, scoring, and winner selection.
- Integration tests for probe->steady correctness and output equivalence.
- Bench validation on small/medium/large profiles with confidence windows.
- Fault-injection coverage: missing metrics, unsupported knobs, controller failure -> safe fallback.

## Risks And Mitigations

- Probe overhead on short runs: bounded by row/time budget and skip rules.
- Noisy measurements: EWMA smoothing + minimum windows + confidence gap.
- Hidden semantic drift: explicit non-goal + equivalence tests against fixed baseline.

## Rollout

- Enabled by default.
- Manual pins always honored.
- Fast-disable escape hatch retained for emergency operations.
