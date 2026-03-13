# Controller Background Durability Design

**Date:** 2026-03-13

**Problem**

The controller’s background maintenance loops still have two durability gaps:

1. lease-expiry and reconciliation-timeout handlers mutate run/task state in memory first and only attempt durable writes afterward; on persistence failure they log and skip publish, leaving the live process in a terminal state while Postgres still contains non-terminal metadata
2. expired preview cleanup removes previews from memory first and attempts durable deletion once; if the delete fails, there is no retry path because the preview has already been forgotten locally

This is inconsistent with the rest of the branch, where foreground state transitions are being made rollback-safe or transactional.

## Decision Summary

Make background timeout transitions rollback-safe and make preview cleanup converge through retry.

Specifically:

- timeout handlers should snapshot previous in-memory state, apply the terminal transition, attempt durable persistence, and restore memory if any durable write fails
- preview cleanup should track failed durable preview deletions in a retry set and retry them on subsequent cleanup ticks until the durable rows are gone

## Design

### Timeout Handler Rollback

`handle_expired_lease` and `sweep_reconciliation_timeouts` should follow the same rule as service handlers:

1. snapshot the previous run/task records that will be mutated
2. apply the terminal in-memory transition
3. persist the task and run durable metadata
4. if either write fails, restore the previous run/task snapshots in memory
5. only publish terminal events after durable success

For lease expiry:

- snapshot the task before `expire_leases` or immediately before the terminal outcome handling uses it
- snapshot the run before transitioning to `TimedOut` or `RecoveryFailed`

For reconciliation timeout:

- snapshot the current run before transition
- snapshot the current task before `mark_timed_out`

If persistence fails, the controller should remain in the pre-timeout state and the next background tick can try again cleanly. That prevents live-memory/durable split-brain.

### Preview Delete Retry

Preview expiry should be one-way from the API perspective, but durable cleanup still needs eventual convergence.

Add an in-memory retry set of expired preview run IDs that still need durable deletion. Cleanup should:

1. collect newly expired preview run IDs and remove them from memory
2. union those IDs into the retry set
3. attempt durable delete for every ID in the retry set
4. remove IDs from the retry set only when the durable delete succeeds

This gives these properties:

- expired previews are not served again after memory expiry
- transient Postgres delete failures do not leak durable preview rows forever
- repeated cleanup ticks converge without needing the preview entry itself to remain in memory

### Scope Boundary

Do not redesign the entire lease-expiry architecture into a job queue in this patch. The branch already has the needed ingredients for a contained fix:

- previous-snapshot rollback helpers
- durable delete helpers
- background loops with periodic retries

The right move now is to make those background loops obey the same durability contract as the foreground handlers.

## Testing Strategy

Add tests before implementation:

- a timeout test proving lease-expiry state rolls back in memory when task or run persistence fails
- a reconciliation-timeout test proving run/task state rolls back in memory when durable persistence fails
- a preview cleanup test proving failed durable deletes are retried and eventually removed from the retry set

Verification after implementation:

- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p rapidbyte-controller`
- `cargo test -p rapidbyte-cli`
- ignored Postgres-backed metadata and restart tests

## Rollout

Ship this in the branch before merge. These paths are part of the durable controller-state feature surface, and leaving them best-effort would preserve a live split-brain failure mode after the rest of the branch has already raised the durability bar.
