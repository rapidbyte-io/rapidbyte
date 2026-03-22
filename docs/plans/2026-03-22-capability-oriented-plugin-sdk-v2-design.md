# Capability-Oriented Plugin SDK V2 Design

## Summary

Replace the current `Context`-centric plugin contract with a clean-cut v2
protocol built around typed lifecycle inputs and generic host capabilities.
Bias the design toward Rust plugin-author ergonomics while keeping the WIT
surface connector-agnostic and free of hidden implementation policy.

## Goals

- Make plugin method signatures small, typed, and legible.
- Remove the god-object role of `rapidbyte_sdk::Context`.
- Keep WIT generic across sources, destinations, and transforms.
- Keep connector implementation details out of the protocol and core SDK.
- Let plugin instances own long-lived resources such as pools, caches, and
  clients.
- Make cancellation, checkpointing, state, logging, metrics, and batch IO
  explicit capabilities rather than ambient powers.
- Make advanced behavior opt-in through feature traits rather than unused
  parameters on core traits.
- Optimize first for Rust author DX without contorting the protocol for
  hypothetical non-Rust SDKs.

## Non-Goals

- Backward compatibility with the current v7 trait signatures or `Context`
  shape.
- Encoding SQL, HTTP, CDC, retry, pooling, or partitioning logic into WIT.
- Making raw WIT itself pleasant to hand-author in every language.
- Designing a complete non-Rust SDK story now.
- Hiding orchestration or durability semantics behind SDK convenience helpers.

## Why Change

The current contract is generic in theory but operationally blurry in practice.
Too much meaning is hidden in `Context`, which mixes logging, metrics, batch
IO, state, checkpointing, cancellation, and stream identity behind one broad
API. This causes three long-term problems:

- plugin methods receive powers they do not need, which creates unused
  parameters and weakens API clarity
- plugin authors have to remember what `Context` can do instead of seeing it in
  the method signature
- testing is harder because the whole execution surface is bundled together

The PostgreSQL connector work also showed a deeper issue: plugin-scope,
run-scope, and stream-scope concerns are not modeled explicitly. That leads to
confusion about where prerequisites, apply/setup work, cancellation, and shared
resources should live.

## Alternatives Considered

### 1. Conservative v1.5 wrappers on top of `Context`

Keep the current trait signatures and add nicer Rust wrappers around `Context`.

Pros:
- low migration cost
- minimal WIT churn

Cons:
- preserves the main structural problem
- keeps lifecycle scope blurry
- does not solve the unused-parameter pattern cleanly

### 2. Recommended: capability-oriented v2

Redesign the protocol around typed lifecycle inputs and explicit generic
capabilities, then provide a Rust-first ergonomic SDK on top.

Pros:
- clean method signatures
- clear lifecycle scope
- connector-agnostic protocol
- excellent Rust DX without hiding behavior

Cons:
- deliberate migration cost
- requires coordinated WIT, SDK, macro, runtime, and plugin changes

### 3. SDK-heavy framework v2

Push more behavior into SDK abstractions such as prerequisite runners,
connection lifecycle management, buffering, retry loops, or partition
orchestration.

Pros:
- very high short-term convenience

Cons:
- hides behavior in SDK helpers
- makes the protocol feel generic while Rust behavior becomes opinionated
- risks freezing the wrong abstractions

## Recommended Design

### Protocol Philosophy

V2 should be a capability protocol, not a connector framework.

The WIT layer defines:

- lifecycle operations
- typed lifecycle inputs
- generic capability handles/interfaces
- feature negotiation
- error and durability semantics

The connector defines:

- client choice
- connection lifecycle and pooling
- retry strategy
- batching strategy
- partition strategy
- schema/query logic
- caches and in-memory state

The Rust SDK defines:

- ergonomic wrappers over protocol capabilities
- typed input structs
- report builders
- testing fakes/harnesses
- proc-macro registration glue

### Layering

#### Layer 1: Stable Generic WIT

The protocol should be explicit and mechanical. It should not know about
databases, files, HTTP, or streaming engines as product concepts. It should
only know about plugin lifecycle and generic capabilities exposed by the host.

Examples of appropriate protocol concepts:

- batch emission / batch reading
- checkpoint transaction management
- state store access
- logging
- metrics
- cancellation
- optional network/socket access

Examples of inappropriate protocol concepts:

- connection pooling
- SQL DDL helpers
- built-in retry classes for connector operations
- CDC cursors
- partition algorithms

#### Layer 2: Rust-First Ergonomic SDK

The Rust SDK is where DX becomes pleasant. It should expose typed lifecycle
inputs and small wrappers over capabilities, while remaining transparent about
behavior.

The SDK may provide:

- `ReadInput`, `WriteInput`, `DiscoverInput`, and similar typed structs
- `Cancellation`, `BatchEmitter`, `BatchReader`, `CheckpointManager`,
  `StateStore`, `Logger`, `Metrics`, and `NetworkAccess` wrappers
- helpers for building prerequisite and validation reports
- test fakes for emit/read/checkpoint/state/cancellation/metrics

The SDK should not silently add:

- retries
- buffering policy
- connection lifecycle policy
- checkpoint cadence
- schema evolution behavior

#### Layer 3: Connector Implementation

Connectors own all concrete operational decisions. They may build pools or
clients in `init`, cache metadata on the plugin instance, and implement their
own concurrency, partitioning, and recovery logic using the generic
capabilities provided by the host.

### Lifecycle Model

The lifecycle should model scope explicitly.

#### Plugin scope

- `init`
- shared config parsing
- shared long-lived resource construction

#### Connector-wide checks

- `prerequisites`
- connectivity
- auth
- version/capability probes

#### Planning/setup

- `discover`
- `validate`
- `apply`

#### Data plane

- `read`
- `write`
- `transform`

#### Cleanup

- `close`
- `teardown`

Each lifecycle method should receive only the scope and capabilities it needs.

### Typed Lifecycle Inputs

The core Rust-facing inputs should be:

- `InitInput`
- `PrerequisitesInput`
- `DiscoverInput`
- `ValidateInput`
- `ApplyInput`
- `ReadInput`
- `WriteInput`
- `TransformInput`
- `CloseInput`
- `TeardownInput`

These inputs should contain only:

- operation scope data
- generic host capabilities relevant to that operation

They should not contain connector-specific policy or implementation choices.

### Capability Model

Instead of a single broad `Context`, operations should receive small, typed
capability wrappers. The recommended capability set is:

- `Logger`
- `Cancellation`
- `Metrics`
- `StateStore`
- `CheckpointManager`
- `BatchEmitter`
- `BatchReader`
- `NetworkAccess`

Connectors can compose these however they want. For example, a PostgreSQL
source can build a pool in `init` using `NetworkAccess`, then use
`BatchEmitter`, `Cancellation`, and `CheckpointManager` during `read`.

### Feature Traits

Keep the core traits small. Advanced behavior should be opt-in through feature
traits. Recommended features:

- `CdcSource`
- `PartitionPlanner`
- `PartitionReader`
- `BulkDestination`
- `MultiStreamSource`
- `MultiStreamCdcSource`

This removes the need for unused parameters on core trait methods. A connector
that does not support partitioning never sees partition inputs.

### Error Model

Retain explicit protocol-level error categories and durability state.

The SDK should help authors construct correct `PluginError` values, but should
not reinterpret them. In particular:

- config/auth/permission/schema/data remain operator-facing failures
- transient network and transient DB remain retry-friendly failures
- `cancelled` remains first-class
- commit/durability boundaries remain explicit for read/write flows

### Execution Model

The host owns orchestration. The plugin owns execution strategy within an
operation.

This means:

- the host decides when lifecycle operations run
- the plugin decides how to use its clients, pools, and concurrency
- advanced fanout such as partition planning is explicit, not hidden runtime
  magic

### Testing Model

Rust DX should improve materially through small, fakeable capabilities.

The SDK should ship test harnesses for:

- emitted batches
- consumed batches
- checkpoint transactions
- state store
- cancellation
- logs and metrics

That makes connector logic easy to unit test without standing up the full host
or a full WIT round-trip.

## Clean-Cut Migration Stance

This should be a clean cut, not a compatibility-preserving refactor.

Rules:

- no attempt to preserve current v7 trait signatures
- no `Context` compatibility adapter in the new core API
- no dual authoring model kept alive indefinitely
- no hidden legacy behavior in macros to make old plugins appear to work

If temporary migration glue is needed internally during implementation, it
should be treated as throwaway scaffolding with a clear deletion step, not as a
product contract.

## Proposed Rust Authoring Shape

The desired ergonomics are conceptually:

```rust
#[rapidbyte_sdk::plugin(source)]
pub struct MySource {
    client: MyClient,
}

impl Source for MySource {
    type Config = Config;

    async fn init(config: Self::Config, init: InitInput<'_>) -> Result<Self, PluginError> {
        config.validate()?;
        let client = MyClient::new(&config, init.network()).await?;
        Ok(Self { client })
    }

    async fn prerequisites(&self, input: PrerequisitesInput<'_>) -> Result<PrerequisitesReport, PluginError> {
        input
            .checks()
            .check("connectivity", async { self.client.check_connectivity().await })
            .check("auth", async { self.client.check_auth().await })
            .run()
            .await
    }

    async fn read(&self, input: ReadInput<'_>) -> Result<ReadSummary, PluginError> {
        let mut cp = input.checkpoints().begin_source()?;
        while let Some(batch) = self.client.next_batch(input.stream()).await? {
            input.emit().batch(&batch)?;
            input.cancel().check()?;
        }
        cp.commit_progress(...)?;
        Ok(ReadSummary::default())
    }
}
```

The important point is not the exact syntax. The point is that the method
signature communicates the available powers directly.

## Initial Change Scope

The first v2 implementation slice should touch:

- `wit/rapidbyte-plugin.wit`
- `crates/rapidbyte-runtime/src/bindings.rs`
- `crates/rapidbyte-sdk/src/plugin.rs`
- `crates/rapidbyte-sdk/src/context.rs`
- `crates/rapidbyte-sdk/macros/src/plugin.rs`
- one representative source plugin
- one representative destination plugin
- runtime/engine integration tests

Use those migrations to validate the API before rolling it across all plugins.

## Outcome

After this redesign, Rapidbyte will have a plugin contract that is simpler to
read, easier to test, and more honest about scope and capabilities. The
protocol will remain connector-agnostic, while the Rust SDK will deliver the
simple and powerful DX you want without hiding concrete implementation policy in
the SDK.
