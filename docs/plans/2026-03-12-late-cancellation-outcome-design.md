# Late Cancellation Outcome Design

## Problem

`run_pipeline()` currently checks the cancellation token after `execute_streams(...)` returns and before dry-run/result finalization. That creates a bad timing window: destination work may already be finished, but a late cancel can still replace the real outcome with a synthetic `CANCELLED` error marked `before_commit`.

This is wrong for both correctness and retry semantics. A real successful run can be downgraded to a cancellation, and a real post-commit failure can be misclassified as safe to retry.

## Decision

Once stream execution has completed, the engine must preserve the real outcome. Cancellation is only honored at safe pre-execution and pre-write boundaries.

Concretely:

- Keep cancellation checks before execution starts.
- Keep cancellation checks before destination preflight and inside long-running execution loops where the engine can still stop safely.
- Do not abort after `execute_streams(...)` has returned successfully.
- Continue through dry-run assembly or `finalize_run(...)` and return the real outcome from that path.

## Testing

Add a regression around the post-stream boundary so a cancelled token no longer causes a synthetic failure once stream execution has already completed.

## Scope

This is a narrow orchestrator fix. It does not change the broader cooperative cancellation model or the agent/controller cancellation protocol.
