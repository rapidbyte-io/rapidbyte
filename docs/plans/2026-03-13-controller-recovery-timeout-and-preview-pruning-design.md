# Controller Recovery Timeout And Preview Pruning Design

**Date:** 2026-03-13

**Problem**

The persistent-controller branch still has two operational durability gaps:

1. inflight runs are normalized to `Reconciling` only in memory on startup, so `recovery_started_at` is reset on every restart and a repeatedly restarted controller can postpone `RECOVERY_TIMEOUT` indefinitely
2. preview expiry cleanup removes expired entries only from the in-memory `PreviewStore`, while `controller_previews` rows remain in Postgres indefinitely

Both problems undercut the branch’s core promise that durable metadata is the source of operational truth after restart.

## Decision Summary

Persist startup reconciliation state the first time inflight work is normalized, and make preview cleanup remove expired durable rows as well as in-memory entries.

The guiding rule is simple:

- if restart recovery depends on a timestamp, that timestamp must live in durable metadata
- if preview discovery metadata is durably persisted, expiry must be durably enforced too

## Design

### Durable Reconciling Normalization

`ControllerState::from_metadata_store` currently loads a durable snapshot and calls `normalize_recovery_snapshot`, which rewrites assigned/running runs to `Reconciling` and stamps `recovery_started_at` in memory only. That makes timeout accounting restart-sensitive.

The fix should move startup normalization into a durable reconciliation pass:

1. load the snapshot
2. identify inflight tasks and their owning runs
3. for runs that were durably `Assigned` or `Running`, convert them to `Reconciling`
4. only set `recovery_started_at` if it is currently absent
5. persist those normalized run/task records before serving traffic
6. build in-memory state from the normalized snapshot

This preserves the original recovery start time across repeated restarts instead of refreshing it on each boot.

Important detail:

- a run already durably marked `Reconciling` must keep its existing `recovery_started_at`
- only the first restart normalization should stamp the timestamp

### Store Support For Startup Recovery Repair

The cleanest place for this is the metadata store, not the service layer. Add a startup repair method that rewrites the snapshot before the controller builds `ControllerState`.

Recommended shape:

- `repair_reconciling_snapshot() -> MetadataSnapshot`

That method should:

- load the current durable snapshot
- apply the same inflight-run normalization rules
- persist any changed run rows inside the store layer
- return the repaired snapshot

Keeping this in the store avoids duplicating boot-time persistence logic in `server.rs` or `state.rs`.

### Durable Preview Pruning

The preview cleanup task currently calls `PreviewStore::cleanup_expired()`, which drops expired entries from memory and logs the count. Durable preview rows survive forever.

The cleanup flow should become:

1. identify expired preview run IDs in memory
2. remove those entries from the in-memory store
3. delete the corresponding `controller_previews` rows from durable metadata
4. log both success and any durable deletion failures

If durable deletion fails, do not restore the expired preview to memory. Expiry semantics should stay conservative: once expired locally, the preview should no longer be served even if durable cleanup needs another pass.

This means the cleanup task should operate on explicit expired run IDs, not only a count.

### Scope Boundary

Do not broaden this patch into unrelated build-system changes. I checked the current branch against `origin/main`, and the `Justfile` plugin build section is not part of this PR’s effective diff. Treat the plugin-wrapper concern as separate work unless the user expands scope.

## Testing Strategy

Add tests before implementation:

- a controller-state/store test proving a run normalized to `Reconciling` keeps its original `recovery_started_at` across repeated snapshot reloads
- a restart-style test proving repeated startup normalization does not postpone `RECOVERY_TIMEOUT`
- a preview cleanup test proving expired preview rows are removed from durable metadata as well as memory

Verification after implementation:

- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p rapidbyte-controller`
- `cargo test -p rapidbyte-cli`
- ignored Postgres-backed metadata and restart tests

## Rollout

Ship both fixes in this branch before merge. These are direct follow-through items for the durable controller-state feature and belong with the restart recovery work, not as post-merge debt.
