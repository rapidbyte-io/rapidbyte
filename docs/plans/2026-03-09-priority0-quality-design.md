# Priority 0 External Readiness Design

## Problem

`PLAN.md` identifies four issues that block credible external testing:

1. The documented lint gate is red.
2. The repository has no CI workflows enforcing the published checks.
3. The SDK conformance surface still contains TODO placeholders for advertised feature contracts.
4. The scaffold command generates misleading placeholder plugins that silently succeed.

These problems are related, but they should not be solved as one monolithic patch. The right design is a single implementation tranche split into three ordered slices with a green baseline established first.

## Goals

- Restore a trustworthy local quality baseline.
- Encode that baseline in GitHub Actions.
- Replace conformance TODOs with executable contract helpers.
- Make scaffolded plugins explicitly incomplete instead of deceptively "working."

## Non-Goals

- No unsafe/lifecycle hardening in this tranche.
- No Docker-backed runtime e2e in CI yet; compile coverage is enough for Priority 0.
- No broad contributor-doc rewrite beyond minimal updates needed to reflect the new CI entrypoint.

## Design

### Slice A: Baseline Enforcement

Fix the current `clippy -D warnings` failures in `rapidbyte-state` with the smallest reasonable code changes, then add a canonical `just ci` command and a GitHub Actions workflow that runs:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`
- `cargo test --manifest-path tests/e2e/Cargo.toml --no-run`

This slice intentionally stops at e2e compilation for CI. It is enough to make the repository externally credible without taking on container orchestration risk in the first pass.

### Slice B: Executable Conformance Contracts

The current `rapidbyte-sdk::conformance` API has the right intent but no real behavior. The fix is to evolve the harness trait from a setup shell into an execution contract.

The conformance module will add explicit harness methods for:

- running a partitioned read and returning observed row IDs
- running a CDC capture flow and returning observed change IDs

The exported helper functions will stop carrying TODOs and instead assert:

- partitioned read covers all seeded rows exactly once
- CDC captures the mutations applied during the test flow

The first proof layer will be fake-harness unit tests inside `rapidbyte-sdk` so the contract is deterministic and cheap to run. Real plugin-backed integration can follow on top of the same API.

### Slice C: Honest Scaffolding

The scaffold command should generate code that compiles cleanly but fails loudly when unimplemented behavior is invoked.

The generated plugin templates will change in three ways:

- neutralize the default config template so it does not imply a specific backend port or working connection string
- return typed `PluginError` failures from validation/read/write/discover stubs instead of fake success/empty output
- add tests that generate scaffolds into a temp directory and assert the resulting files contain the intended guardrails

This preserves ergonomics while removing the current risk that contributors cargo-cult a broken placeholder into production code.

## Testing Strategy

### Slice A

- targeted crate tests for the clippy-fix sites if needed
- full `cargo clippy --workspace --all-targets -- -D warnings`
- full `cargo test --workspace --all-targets`
- `cargo test --manifest-path tests/e2e/Cargo.toml --no-run`

### Slice B

- new fake-harness tests in `crates/rapidbyte-sdk/src/conformance.rs`
- assert the helpers fail correctly on duplicates/missing rows/missing CDC events

### Slice C

- new scaffold tests in `crates/rapidbyte-cli/src/commands/scaffold.rs`
- generate source and destination plugins into temp directories
- assert generated files compile-shaped content and explicit unimplemented errors

## Risks And Controls

### Risk: CI codifies a flaky command set

Control: use commands already validated locally in the new worktree before writing the workflow.

### Risk: Conformance API grows too abstract

Control: keep the harness narrowly shaped around the two missing contracts only.

### Risk: Scaffold cleanup becomes product design churn

Control: keep generated stubs minimal, compile-safe, and intentionally failing. Do not add speculative helper layers.

## Outcome

After this tranche, Rapidbyte should have:

- a green enforced baseline
- a visible CI workflow
- executable contract coverage for the currently incomplete SDK conformance helpers
- scaffold output that is safe for external contributors to copy as a starting point
