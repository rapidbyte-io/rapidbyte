# Distributed Benchmark Summary Design

## Context

The new distributed benchmark path for `pg_dest_copy_release_distributed` is working correctly: the benchmark runner launches a controller and agent, submits the pipeline through the controller, and records distributed runtime metadata in the benchmark artifact. However, the current `bench-summary` output is misleading for distributed artifacts because it reports `records written: 0` and `bytes written: 0` even when correctness details show the expected row counts.

The root cause is representation mismatch. Distributed benchmark artifacts currently carry correct top-level totals such as `records_written` and `bytes_written`, but their `stream_metrics` array is empty. The summary renderer computes written rows and bytes only from `connector_metrics.stream_metrics`, so distributed artifacts display zero totals despite valid throughput, latency, and correctness.

## Goals

- Make `bench-summary` report correct `records written` and `bytes written` for distributed artifacts.
- Make distributed benchmark execution explicit in the summary output.
- Preserve compatibility with existing local artifacts and already-written distributed artifacts.
- Avoid changing the benchmark artifact schema in this pass.

## Non-Goals

- Do not redesign the benchmark artifact schema.
- Do not add placeholder distributed timing metrics that are not currently tracked.
- Do not change local benchmark output beyond any necessary internal refactoring.

## Approaches Considered

### 1. Fix summary rendering only

Update the summary code to:
- fall back to top-level totals when `stream_metrics` is absent or empty
- render a compact distributed section whenever the artifact indicates distributed execution

Pros:
- Smallest change
- Fixes old and new artifacts immediately
- Keeps benchmark runner and CLI payload stable

Cons:
- Leaves the underlying artifact shape asymmetric between local and distributed runs

### 2. Normalize distributed artifacts to include stream metrics

Change distributed benchmark JSON emission so it populates `stream_metrics` like local runs do, then keep summary logic unchanged.

Pros:
- Unifies artifact shape

Cons:
- More invasive
- Does not fix already-produced artifacts
- Still benefits from a distributed summary section, so it is not sufficient by itself

### 3. Change both artifact emission and summary

Normalize distributed artifacts and also enhance the summary.

Pros:
- Most consistent long term

Cons:
- More moving parts than needed for the immediate user-facing problem

## Recommendation

Choose approach 1 for this pass.

It directly addresses the current confusion with the least churn. The benchmark artifact already contains enough information to compute correct totals and to prove the run used controller and agent components. The main issue is that the summary does not consume that information correctly.

## Proposed Design

### Summary totals fallback

The summary logic should compute `records written` and `bytes written` using this order:

1. Prefer the sum of `connector_metrics.stream_metrics[*].records_written` / `bytes_written` when present and non-empty.
2. Fall back to top-level totals derived from the benchmark payload representation already stored in the artifact.
3. Never silently print zero because one representation is absent.

For current distributed artifacts, the natural fallback source is the row-count correctness details:
- `correctness.details.actual_records_written`
- `correctness.details.actual_records_read`

For bytes, the canonical source already exists:
- `canonical_metrics.mb_per_sec`
- `canonical_metrics.duration_secs`

But to avoid reconstructing bytes from rates, the better approach is to capture top-level totals already present in the artifact model if available. If that is not available in the summary model today, this pass should add the minimal extraction needed from existing artifact JSON structures without changing the on-disk schema.

### Distributed summary section

When `execution_flags.execution_mode == "distributed"`, render a compact distributed block in the normal summary output:

- `execution mode: distributed`
- `agent count: <n>` when available
- `controller url: <url>` when available
- `flight endpoint: <url>` when available

This section should be additive and should not affect local summaries.

### Error handling

- If distributed metadata exists but one field is missing, skip that field rather than failing the entire summary.
- If fallback totals cannot be derived, keep the field omitted rather than printing a misleading zero.
- Maintain current error behavior for malformed artifact files.

## Testing Strategy

Add focused summary tests that cover:

1. Distributed artifact with empty `stream_metrics`
   - summary reports correct written rows/bytes
   - summary renders distributed metadata lines

2. Local artifact with populated `stream_metrics`
   - summary output remains unchanged

3. Distributed artifact with partial metadata
   - summary still renders the fields that exist

## Future Work

Once the runtime starts recording trustworthy distributed lifecycle timings, the summary can grow a second-stage distributed metrics block with values like:
- `submit_to_terminal_secs`
- `queue_wait_secs`
- `execution_secs`

That should happen only after the artifact path records those timings explicitly.
