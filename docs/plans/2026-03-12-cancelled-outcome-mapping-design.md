# Cancelled Outcome Mapping Design

## Problem

The engine reports safe pre-commit cancellation as `PluginError("CANCELLED", ...)`, but the agent executor currently maps all plugin errors to `TaskOutcomeKind::Failed`. That means an active `CancelRun` delivered after execution starts can surface as `FAILED` instead of `CANCELLED`, even though the controller already has a distinct cancelled completion path.

## Decision

Interpret engine cancellation as a true cancelled task outcome only when it is explicitly safe and pre-commit:

- `PipelineError::Plugin` with `code == "CANCELLED"`
- `commit_state == BeforeCommit`

That case maps to `TaskOutcomeKind::Cancelled`.

All other plugin errors remain `TaskOutcomeKind::Failed`, including:

- post-commit failures
- non-cancel plugin errors
- ambiguous/unknown commit states

## Testing

- Add an agent executor regression proving a pre-commit engine cancellation becomes `TaskOutcomeKind::Cancelled`.
- Add a controller completion regression proving a cancelling run completed with `TaskOutcome::Cancelled` ends in `Cancelled`.

## Scope

This is a narrow behavior correction in executor mapping and test coverage. It does not change controller cancellation delivery, engine commit-state rules, or post-commit outcome preservation.
