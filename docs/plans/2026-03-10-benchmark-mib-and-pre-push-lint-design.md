# Benchmark MiB Label And Pre-Push Lint Design

## Summary

Make the benchmark summary output honest about binary byte units by renaming the
rendered bandwidth label from `MB/sec` to `MiB/sec`, and tighten the local
developer workflow so `pre-push` runs `just lint` and blocks pushes on clippy
failures that CI would reject anyway.

## Goals

- Align the summary label with the existing binary unit calculation
  (`bytes / 1024 / 1024`).
- Catch `cargo clippy --workspace --all-targets -- -D warnings` failures before
  code reaches CI.
- Preserve the current lightweight `pre-commit` workflow.
- Keep the implementation minimal and local to the existing benchmark summary
  and Git hook surfaces.

## Non-Goals

- Renaming the stored canonical metric key `mb_per_sec`.
- Changing benchmark artifact schema or benchmark math.
- Moving heavy test execution into Git hooks.
- Replacing `just ci` as the full pre-PR gate.

## Current Problems

- The summary currently prints `MB/sec` even though the reported bandwidth is
  computed in binary mebibytes.
- CI caught a clippy warning in `benchmarks/src/summary.rs` that local hooks did
  not catch because `pre-push` currently only runs formatting.
- The docs describe hooks as formatting-only, which matches current behavior but
  leaves a predictable class of CI failure uncovered.

## Alternatives Considered

### 1. Recommended: Rename the label, keep the metric key, and run full lint in `pre-push`

Update the rendered summary and docs to say `MiB/sec`, leave the artifact field
name as `mb_per_sec`, and make `pre-push` run `just lint` after its formatting
step.

Pros:
- Honest user-facing unit label with no schema churn.
- The exact CI clippy command is enforced locally before push.
- Minimal change to existing workflows.

Cons:
- `pre-push` becomes meaningfully slower than formatting-only.

### 2. Rename the metric schema field too

Change `mb_per_sec` to `mib_per_sec` throughout the artifact format and all
consumers.

Pros:
- Perfect naming consistency.

Cons:
- Unnecessary schema churn for a presentation issue.
- Breaks compatibility with existing artifacts and analysis code.

### 3. Keep hooks light and rely on contributor discipline

Fix the summary label, but keep `pre-push` formatting-only and expect people to
run `just lint` manually.

Pros:
- Fastest hooks.

Cons:
- Repeats the exact failure mode that just escaped to CI.

## Recommended Design

### Benchmark Summary

Change only the rendered terminology:

- `bandwidth: ... MiB/sec median (...)`

Keep:

- `canonical_metrics.mb_per_sec`
- the existing binary-unit calculation
- the current grouping and summary behavior

Update tests and user-facing docs to use `MiB/sec` wherever they refer to the
rendered summary.

### Hook Workflow

Keep the current split of responsibilities:

- `pre-commit`: formatting-only, auto-fix, abort if it changed staged files
- `pre-push`: formatting guardrail plus `just lint`
- `just ci`: full local readiness gate

Recommended `pre-push` flow:

1. Resolve repo root.
2. Exit early if there is no `Cargo.toml`.
3. Run the existing formatting path.
4. If formatting changed files, stage them, print instructions, and abort.
5. If formatting is clean, run `just lint`.
6. Abort the push on any lint failure.

This preserves the existing auto-fix ergonomics while adding the missing clippy
gate.

## Change Scope

### Modified files

- `benchmarks/src/summary.rs`
- `.githooks/pre-push`
- `README.md`
- `CONTRIBUTING.md`
- `docs/BENCHMARKING.md`

## Verification Strategy

- targeted summary test for the `MiB/sec` label
- `cargo test -p rapidbyte-benchmarks summary::...`
- `bash -n .githooks/pre-push`
- run `.githooks/pre-push` on a clean tree and confirm it executes `just lint`
- run `just lint`

## Outcome

After this change, benchmark summaries will present the correct binary unit
label, and pushes will fail locally on the same clippy warnings that CI would
reject.
