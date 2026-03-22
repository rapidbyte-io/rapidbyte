# Remove Dry Run And Collapse SDK Surface Design

## Goal

Remove `dry_run` from the product, protocol, and plugin SDK entirely, remove preview/replay flow that only exists for `dry_run`, and collapse redundant public `_with_` APIs on the plugin-facing surface so there is one clear canonical API path.

## Scope

This is a clean cut with no backward compatibility.

Included:
- WIT and protocol request changes
- SDK typed input changes
- macro-generated plugin entrypoint changes
- engine/CLI/runtime request plumbing
- preview/dry-run product flow removal
- public/plugin-facing `_with_` API cleanup
- scaffold/docs/test updates

Excluded:
- unrelated internal/test helper renames such as `with_context`, `manifest_with_schema`, or local builder-style helpers
- plugin-specific behavior redesign beyond removing now-dead dry-run branches

## Current Problems

### 1. `dry_run` is a dead or misleading execution mode

The codebase still exposes `dry_run` through CLI, engine, WIT, runtime bindings, and plugin SDK inputs. That creates several problems:

- plugins must carry mode branches that are not part of normal execution
- protocol requests contain fields that do not belong in the core connector contract
- engine/controller/preview paths keep extra complexity to support a separate result shape
- review fixes have to keep re-threading `dry_run` through partitioned and CDC flows

This is the wrong shape for a clean capability-oriented plugin contract.

### 2. Public SDK naming has redundant parallel APIs

The public surface currently has several “simple” and “with_*” variants for what should be a single operation:

- `Reader::next_batch` vs `Reader::next_batch_with_decode_timing`
- `Metrics::counter` vs `counter_with_labels`
- `Emit::batch_for_stream` instead of a single explicit batch API that always takes metadata
- typed input constructors split across `new`, `with_dry_run`, and `with_capabilities`
- `ValidateInput::new(...).with_stream_name(...)`

This adds surface area without adding real capability. The richer form should be the default form.

## Recommended Design

### Protocol

Remove `dry_run` from:

- `rapidbyte:plugin@8.0.0` `run-request`
- `ApplyRequest` in `rapidbyte-types`
- any runtime or engine request types that mirror those request records

Because this is a breaking contract change, the WIT/package and protocol version should bump again when the implementation lands.

### Product Flow

Delete the product feature, not just the protocol field:

- remove CLI `check --dry-run`
- remove preview/replay paths and preview-only reporting
- remove engine branches that only exist to support dry-run execution or preview materialization
- keep normal check/discover/apply/run/teardown behavior only

If preview is reintroduced later, it should come back as an explicit product feature with its own clean contract, not as an execution flag smeared through every request shape.

### Canonical Public SDK API

#### Batch emit

Use one canonical emit method:

```rust
emit.batch(&batch, &metadata)?;
```

`BatchMetadata` always travels with the batch. There is no “simple emit” that silently manufactures the metadata shape and no parallel `emit_batch_with_metadata`.

#### Batch read

Use one canonical read method:

```rust
let next = reader.next_batch(max_bytes)?;
```

It returns the richer decoded result including metadata/timing. If a caller only wants schema and batches, it can ignore the extra fields locally. The richer result should not require a second `_with_` method.

#### Metrics

Use one canonical metric API:

```rust
metrics.counter("rows_read", 10, &[("phase", "scan")])?;
metrics.gauge("lag_seconds", 1.2, &[])?;
metrics.histogram("batch_ms", 8.4, &[("stream", "users")])?;
```

Labels are always accepted. Empty labels are just `&[]`.

#### Validate input

Use one constructor:

```rust
ValidateInput::new(upstream, stream_name)
```

There is no builder-style `with_stream_name`.

#### Typed inputs

For typed inputs, keep only two constructor styles where needed:

- `new(...)` for explicit construction
- `host(...)` for default host/runtime capabilities

That replaces `with_capabilities`, `with_dry_run`, and similar redundant public constructors.

### Macro And Session Behavior

The macro-generated entrypoints should also finish the session fix:

- `open()` must allocate and store a real session id
- lifecycle/run/apply/teardown entrypoints must reject stale or mismatched `input.session`

This is not directly part of the dry-run removal, but it is still outstanding public-surface debt in the same macro layer and should land in this cleanup pass.

## File-Level Impact

### WIT / protocol / types

- `wit/rapidbyte-plugin.wit`
- `crates/rapidbyte-types/src/run.rs`
- `crates/rapidbyte-types/src/lifecycle.rs`
- `crates/rapidbyte-types/src/wire.rs`
- `crates/rapidbyte-runtime/src/bindings.rs`

### SDK / macros

- `crates/rapidbyte-sdk/src/input.rs`
- `crates/rapidbyte-sdk/src/capabilities.rs`
- `crates/rapidbyte-sdk/src/host_ffi.rs`
- `crates/rapidbyte-sdk/src/testing.rs`
- `crates/rapidbyte-sdk/src/context.rs` where legacy compatibility wrappers still mirror the public surface
- `crates/rapidbyte-sdk/macros/src/plugin.rs`

### Engine / CLI / runtime

- `crates/rapidbyte-engine/src/adapter/wasm_runner.rs`
- `crates/rapidbyte-engine/src/domain/ports/runner.rs`
- `crates/rapidbyte-engine/src/application/run.rs`
- `crates/rapidbyte-cli/src/main.rs`
- `crates/rapidbyte-cli/src/commands/check.rs`
- any preview-specific flow still used by controller/agent-facing execution

### Plugins / scaffold / docs

- `plugins/sources/postgres/src/main.rs`
- `plugins/destinations/postgres/src/apply.rs`
- `plugins/transforms/sql/src/*` if public SDK names change
- `plugins/transforms/validate/src/*` if public SDK names change
- `crates/rapidbyte-cli/src/commands/scaffold.rs`
- `README.md`
- `docs/PLUGIN_DEV.md`
- other product docs that still advertise dry-run/preview

## Error Handling

This change should not alter retry semantics. It removes a mode and surface area, but normal validation, apply, and run failures should keep their existing categories and commit-state behavior.

The one notable protocol behavior change is that stale session ids should become explicit plugin errors instead of being silently accepted by macro-generated glue.

## Testing

Required coverage:

- WIT/runtime binding tests for request-shape removal of `dry_run`
- macro tests for generated `host(...)`/`new(...)` constructor usage and session validation
- engine tests that no longer expect preview/dry-run behavior
- CLI tests that `check --dry-run` is gone
- SDK tests for canonical batch/metric/input APIs
- plugin compile/use-site tests after constructor renames
- workspace clippy/test pass

## Recommendation

Implement this as one focused breaking slice:

1. remove `dry_run` and preview from protocol/product
2. collapse the public/plugin-facing `_with_` sprawl to canonical names
3. finish the macro session-validation debt while touching that layer

That gives a smaller, clearer v2 surface instead of preserving transitional shapes.
