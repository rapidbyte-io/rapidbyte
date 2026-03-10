# Manual Benchmark CI Design

## Summary

Stop running the benchmark PR workflow automatically on every pull request.
Keep the benchmark workflow available as a manual `workflow_dispatch` action
for maintainers, and treat `just bench-pr` as the primary local path for
performance investigation until dedicated benchmark runners exist.

## Goals

- Remove slow, noisy benchmark work from default PR CI.
- Preserve the benchmark workflow for manual maintainer use.
- Keep local benchmark commands as the main path for performance validation.
- Align repository docs and status signals with the new manual-only policy.

## Non-Goals

- Deleting the benchmark workflow entirely.
- Removing `just bench-pr` or the benchmark platform itself.
- Replacing benchmarking with a new CI system in this slice.
- Reintroducing automatic PR benchmark gating on GitHub-hosted runners.

## Alternatives Considered

### 1. Recommended: keep the workflow, manual-only

Remove the `pull_request` trigger from `.github/workflows/bench-pr.yml` and
leave only `workflow_dispatch`.

Pros:
- Zero automatic PR CI cost.
- Maintainers can still run the workflow when needed.
- Leaves a clean path to re-enable broader automation later on dedicated
  runners.

Cons:
- Perf regressions are no longer surfaced automatically in normal PR CI.

### 2. Label-gated or conditional PR workflow

Keep the PR trigger but require a label or conditional guard.

Pros:
- Some on-demand PR integration remains available from the PR surface.

Cons:
- Still invites accidental use on GitHub-hosted runners.
- Keeps benchmark CI visible as if it were a normal PR check.

### 3. Remove the workflow entirely

Delete `.github/workflows/bench-pr.yml` and rely only on local execution.

Pros:
- Simplest operational model.

Cons:
- Loses the future manual GitHub path the team still wants to keep.

## Recommended Design

### Workflow Policy

Update `.github/workflows/bench-pr.yml` so it triggers only on
`workflow_dispatch`.

This preserves the benchmark workflow but makes its cost an explicit maintainer
choice rather than part of the normal PR path. No benchmark job should run on
`pull_request`.

### Local-First Benchmarking

Keep `just bench-pr` as the standard path for local performance work. The local
benchmark command remains the documented way to:

- run the PR smoke benchmark suite
- compare candidate results against the checked-in `main` baseline
- investigate performance regressions before or during review

### Repository Signals

Update repo messaging so it does not imply benchmark CI is part of normal PR
validation:

- remove the benchmark workflow badge from `README.md`
- update contributor docs to describe `just bench-pr` as a local perf tool and
  the GitHub workflow as manual-only
- update benchmark PR suite docs so they no longer imply routine PR CI

This avoids training contributors to expect automatic benchmark coverage when
that is no longer true.

## Change Scope

### Modified files

- `.github/workflows/bench-pr.yml`
- `README.md`
- `CONTRIBUTING.md`
- `benchmarks/scenarios/pr/README.md`

## Error Handling

- Manual workflow runs should continue to fail normally when the benchmark
  environment or baseline comparison fails.
- Normal pull requests should no longer spend time or fail on benchmark CI.

## Verification Strategy

- read the workflow to confirm `workflow_dispatch` remains and `pull_request`
  is removed
- verify docs consistently describe benchmarking as local/manual-only
- run malformed-diff checks after edits
- if desired, run `python3 -c 'import yaml; ...'` or equivalent local YAML parse
  sanity check on `.github/workflows/bench-pr.yml`

## Outcome

After this change:

- default PR CI is faster and less noisy
- performance benchmarking remains available locally and via manual GitHub
  dispatch
- repository docs match the actual benchmark policy
