# Workspace Test Assurance Design

Date: 2026-02-26
Status: Approved

## Context

Rapidbyte already has meaningful tests across unit, integration, and end-to-end (E2E) layers, but coverage guarantees are currently implicit. We want a maximum-assurance strategy where every feature, configuration setting, and operational knob is explicitly mapped to tests and validated by automation.

The project currently exposes a `just test` command and `just e2e` command, with broad scenario coverage in `tests/connectors/postgres/scenarios/`. We will standardize command ergonomics and enforce coverage completeness with a machine-checkable manifest.

## Goals

- Provide canonical, predictable test entrypoints:
  - `just test`
  - `just test-unit`
  - `just test-integration`
  - `just test-e2e`
- Enforce explicit feature/knob-to-test mappings for all supported functionality.
- Make `just test` a future CI-ready gate that fails on missing mappings or invalid references.
- Preserve backward compatibility with existing `just e2e` usage during rollout.

## Non-Goals

- Replacing existing E2E architecture or scenario scripts.
- Defining detailed CI pipeline configuration in this phase.
- Redesigning crate-specific test internals beyond what is needed to support mapping and validation.

## Decision Summary

Use a manifest-driven coverage matrix with validation (Option 2):

1. Add top-level test command taxonomy to `Justfile`.
2. Add `tests/coverage-matrix.yaml` as source of truth for feature/knob coverage.
3. Add a validation script (`scripts/validate-test-coverage.sh`) executed by `just test` before running test suites.
4. Backfill matrix entries for existing supported behavior and move to strict validation.

## Command Architecture

### Canonical commands

- `just test-unit`
  - Runs fast/unit-oriented Rust tests for workspace crates and connector crates.
- `just test-integration`
  - Runs integration suites (crate `tests/` targets and integration-focused async tests).
- `just test-e2e`
  - Runs shell harness currently in `tests/e2e.sh`.
- `just test`
  - Runs: coverage validation -> unit -> integration -> E2E.

### Compatibility

- Keep `just e2e` as an alias to `just test-e2e` for a transitional period.

## Coverage Matrix Contract

Introduce `tests/coverage-matrix.yaml` with records shaped like:

- `id`: stable feature/knob identifier (e.g. `compression.lz4`, `source.incremental.cursor_state`).
- `component`: owning subsystem or connector.
- `description`: short behavior statement.
- `required_layers`: one or more of `unit`, `integration`, `e2e`.
- `tests`:
  - `unit`: list of rust test references.
  - `integration`: list of rust integration test references.
  - `e2e`: list of scenario script references.

This gives explicit proof that each feature has expected test depth.

## Validator Behavior

Add `scripts/validate-test-coverage.sh` to enforce:

- Required fields present per matrix entry.
- No duplicate `id` values.
- `required_layers` values are valid (`unit`, `integration`, `e2e`).
- Every required layer has at least one mapped test.
- Referenced test artifacts exist:
  - Rust test references are discoverable in code.
  - E2E scenarios map to real files under `tests/connectors/*/scenarios/`.

Output must be actionable and fail-fast.

Example failures:

- `missing layer mapping: source.incremental.cursor_state requires integration`
- `unknown e2e scenario: postgres/scenarios/99_missing.sh`

## Layer Semantics

- `unit`
  - Pure logic and isolated behavior (encoders, type maps, query builders, config parsing helpers).
- `integration`
  - Multi-module behavior in-process (runner/orchestrator interactions, state backend behavior, config + runtime coupling).
- `e2e`
  - Real pipeline execution through connector binaries and test databases in scripted scenarios.

This prevents overloading E2E for logic-level concerns and keeps assurance balanced.

## Rollout Plan

1. Add commands and matrix schema.
2. Add validator and wire it to `just test`.
3. Populate matrix with current known features/config knobs.
4. Fill gaps by adding missing tests where required layers are absent.
5. Enable strict mode and treat validation failures as blocking.

## Risks And Mitigations

- Risk: Matrix drift as new features are added.
  - Mitigation: `just test` always validates matrix first.
- Risk: False confidence from weak mapping quality.
  - Mitigation: Require layer semantics and concrete test identifiers, not broad file-level tags.
- Risk: Slower local runs for developers.
  - Mitigation: Keep granular commands (`test-unit`, `test-integration`, `test-e2e`) for targeted iteration.

## Success Criteria

- `just test`, `just test-unit`, `just test-integration`, `just test-e2e` all exist and are documented.
- Coverage validator fails on missing/invalid mappings.
- Every currently supported feature/knob has at least one matrix entry and required layer mappings.
- Missing coverage is visible as concrete actionable failures rather than implicit assumptions.
