# WIT Protocol v7 Design

**Date:** 2026-03-20
**Status:** Draft
**Scope:** Complete redesign of the Rapidbyte plugin protocol (WIT, SDK traits, host orchestration)

## Motivation

Protocol v6 has served well but has accumulated gaps identified through production use and comparison with Estuary Flow's connector protocol:

1. **No schema negotiation** — source/destination schema mismatches discovered at runtime, not at check time
2. **No pre-flight validation** — CDC prerequisites (wal_level, replication role, publication) checked implicitly in `run()`, producing cryptic errors
3. **No DDL phase** — destinations create tables implicitly during `run()`, no way to preview or audit DDL
4. **No multi-stream support** — CDC creates one replication slot per stream, doesn't scale to 50+ tables
5. **No cooperative cancellation** — host can only kill WASM via epoch timeout, no graceful shutdown
6. **Checkpoint/cursor coupling** — cursor state and progress markers tangled, no atomic commit guarantees
7. **Batch transport lacks metadata** — no stream tagging, schema fingerprinting, or sequence numbers
8. **No resource teardown** — replication slots and staging tables orphaned when pipelines are deleted

v7 is a clean break. All plugins must be rebuilt against the new SDK. No v6 compatibility bridge.

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Migration strategy | Clean break, no v6 bridge | Only 3 plugins exist, all controlled internally. One protocol, one code path. |
| Stream model | Hybrid single/multi-stream | Single-stream is simpler for batch; multi-stream is essential for CDC at scale. Default single, opt-in multi via manifest capability. |
| Lifecycle phases | Full: spec → open → prerequisites → discover → validate → apply → run → close → teardown | All phases exist in WIT. SDK provides default no-op impls. Plugin authors override what they need. |
| Batch transport | Metadata-enriched frames | Every batch carries stream index, schema fingerprint, sequence number, compression hint. SDK builds metadata automatically. |
| Schema negotiation | Full pipeline contract | Source declares output schema. Transforms declare output given input. Destinations declare field constraints. Host reconciles end-to-end at check time. |
| Checkpoint model | Transactional | Plugin opens checkpoint txn, attaches cursor updates + state mutations, commits atomically. Exactly-once by construction. |
| Error model | Per-stream partial failure | Multi-stream plugins report errors for individual streams while continuing others. Single-stream plugins unaffected. |
| Cancellation | Cooperative polling | `is-cancelled()` host import. SDK also checks automatically in `emit_batch`. |
| Protocol architecture | Evolved WIT (synchronous function calls) | Host drives lifecycle. Type safety from component model. SDK hides WIT complexity behind Rust traits. |

## WIT Protocol Definition

### Package Declaration

```wit
package rapidbyte:plugin@7.0.0;
```

### Types Interface

```wit
interface types {
    // ── Handles ──
    type session = u64;
    type frame-handle = u64;
    type socket-handle = u64;
    type checkpoint-txn = u64;

    // ── Plugin identity ──
    enum plugin-kind { source, destination, transform }

    // ── Lifecycle phases ──
    enum run-phase { read, write, transform }

    // ── Sync modes ──
    enum sync-mode { full-refresh, incremental, cdc }

    // ── Write modes ──
    enum write-mode { append, replace, upsert }

    // ── Validation ──
    enum validation-status { success, failed, warning }

    // ── Schema negotiation ──
    enum field-constraint {
        field-required,
        field-optional,
        field-forbidden,
        field-recommended,
        type-incompatible,
    }

    record field-requirement {
        field-name: string,
        constraint: field-constraint,
        reason: string,
        accepted-types: option<list<string>>,
    }

    record schema-field {
        name: string,
        arrow-type: string,
        nullable: bool,
        is-primary-key: bool,
        is-generated: bool,
        is-partition-key: bool,
        default-value: option<string>,
    }

    record stream-schema {
        fields: list<schema-field>,
        primary-key: list<string>,
        partition-keys: list<string>,
        source-defined-cursor: option<string>,
        schema-id: option<string>,
    }

    // ── Error model ──
    enum error-category {
        config, auth, permission, rate-limit,
        transient-network, transient-db,
        data, schema, internal, frame,
        cancelled,
    }

    enum error-scope { per-stream, per-batch, per-record }

    enum backoff-class { fast, normal, slow }

    enum commit-state {
        before-commit,
        after-commit-unknown,
        after-commit-confirmed,
    }

    record plugin-error {
        category: error-category,
        scope: error-scope,
        code: string,
        message: string,
        retryable: bool,
        retry-after-ms: option<u64>,
        backoff-class: backoff-class,
        safe-to-retry: bool,
        commit-state: option<commit-state>,
        details-json: option<string>,
    }

    // ── Stream configuration ──
    enum partition-strategy { mod-hash, range }

    record stream-limits {
        max-batch-bytes: u64,
        max-record-bytes: u64,
        max-inflight-batches: u32,
        max-parallel-requests: u32,
        checkpoint-interval-bytes: u64,
        checkpoint-interval-rows: u64,
        checkpoint-interval-seconds: u64,
        max-records: option<u64>,
    }

    enum data-error-policy { fail, skip, dlq }

    enum column-policy { add, ignore, fail }
    enum type-change-policy { coerce, fail, null-out }
    enum nullability-policy { allow, fail }

    record schema-evolution-policy {
        new-column: column-policy,
        removed-column: column-policy,
        type-change: type-change-policy,
        nullability-change: nullability-policy,
    }

    record stream-policies {
        on-data-error: data-error-policy,
        schema-evolution: schema-evolution-policy,
    }

    record cursor-info {
        cursor-field: string,
        tie-breaker-field: option<string>,
        cursor-type: string,
        last-value-json: option<string>,
    }

    record stream-context {
        stream-index: u32,
        stream-name: string,
        source-stream-name: option<string>,
        schema: stream-schema,
        sync-mode: sync-mode,
        cursor-info: option<cursor-info>,
        limits: stream-limits,
        policies: stream-policies,
        write-mode: option<write-mode>,
        selected-columns: option<list<string>>,
        partition-key: option<string>,
        partition-count: option<u32>,
        partition-index: option<u32>,
        partition-strategy: option<partition-strategy>,
    }

    // ── Batch metadata ──
    record batch-metadata {
        stream-index: u32,
        schema-fingerprint: option<string>,
        sequence-number: u64,
        compression: option<string>,
        record-count: u32,
        byte-count: u64,
    }

    // ── Checkpoint (transactional) ──
    enum checkpoint-kind { source, destination, transform }

    record cursor-update {
        stream-name: string,
        cursor-field: string,
        cursor-value-json: string,
    }

    record state-mutation {
        scope: state-scope,
        key: string,
        value: string,
    }

    enum state-scope { pipeline, per-stream, plugin-instance }

    // ── Run request / results ──
    record run-request {
        streams: list<stream-context>,
        dry-run: bool,
    }

    record stream-result {
        stream-index: u32,
        stream-name: string,
        outcome-json: string,
        succeeded: bool,
    }

    record run-summary {
        results: list<stream-result>,
    }

    // ── Discovery ──
    record discovered-stream {
        name: string,
        schema: stream-schema,
        supported-sync-modes: list<sync-mode>,
        default-cursor-field: option<string>,
        estimated-row-count: option<u64>,
        metadata-json: option<string>,
    }

    // ── Validation ──
    record validation-report {
        status: validation-status,
        message: string,
        warnings: list<string>,
        output-schema: option<stream-schema>,
        field-requirements: option<list<field-requirement>>,
    }

    // ── Prerequisites ──
    enum prerequisite-severity { error, warning, info }

    record prerequisite-check {
        name: string,
        passed: bool,
        severity: prerequisite-severity,
        message: string,
        fix-hint: option<string>,
    }

    record prerequisites-report {
        passed: bool,
        checks: list<prerequisite-check>,
    }

    // ── Spec ──
    record plugin-spec {
        protocol-version: u32,
        config-schema-json: string,
        resource-schema-json: option<string>,
        documentation-url: option<string>,
        features: list<string>,
        supported-sync-modes: list<sync-mode>,
        supported-write-modes: option<list<write-mode>>,
    }

    // ── Apply ──
    record apply-request {
        streams: list<stream-context>,
        dry-run: bool,
    }

    record apply-action {
        stream-name: string,
        description: string,
        ddl-executed: option<string>,
    }

    record apply-report {
        actions: list<apply-action>,
    }

    // ── Teardown ──
    record teardown-request {
        streams: list<string>,
        reason: string,
    }

    record teardown-report {
        actions: list<string>,
    }

    // ── Socket I/O ──
    variant socket-read-result {
        data(list<u8>),
        eof,
        would-block,
    }

    variant socket-write-result {
        written(u64),
        would-block,
    }
}
```

### Host Imports

```wit
interface host {
    use types.{
        plugin-error,
        batch-metadata,
        frame-handle,
        socket-handle,
        socket-read-result,
        socket-write-result,
        checkpoint-txn,
        checkpoint-kind,
        cursor-update,
        state-mutation,
        state-scope,
    };

    // ── Batch transport (V4 — metadata-enriched) ──

    frame-new: func(capacity: u64) -> result<frame-handle, plugin-error>;
    frame-write: func(handle: frame-handle, chunk: list<u8>) -> result<u64, plugin-error>;
    frame-seal: func(handle: frame-handle) -> result<_, plugin-error>;
    frame-len: func(handle: frame-handle) -> result<u64, plugin-error>;
    frame-read: func(handle: frame-handle, offset: u64, len: u64) -> result<list<u8>, plugin-error>;
    frame-drop: func(handle: frame-handle);

    emit-batch: func(handle: frame-handle, metadata: batch-metadata) -> result<_, plugin-error>;
    next-batch: func() -> result<option<frame-handle>, plugin-error>;
    next-batch-metadata: func(handle: frame-handle) -> result<batch-metadata, plugin-error>;

    // ── Transactional checkpoints ──

    checkpoint-begin: func(kind: checkpoint-kind) -> result<checkpoint-txn, plugin-error>;
    checkpoint-set-cursor: func(txn: checkpoint-txn, cursor: cursor-update) -> result<_, plugin-error>;
    checkpoint-set-state: func(txn: checkpoint-txn, mutation: state-mutation) -> result<_, plugin-error>;
    checkpoint-commit: func(
        txn: checkpoint-txn,
        records-processed: u64,
        bytes-processed: u64,
    ) -> result<_, plugin-error>;
    checkpoint-abort: func(txn: checkpoint-txn);

    // ── Non-transactional state ──

    state-get: func(scope: state-scope, key: string) -> result<option<string>, plugin-error>;
    state-put: func(scope: state-scope, key: string, val: string) -> result<_, plugin-error>;

    // ── Cancellation ──

    is-cancelled: func() -> bool;

    // ── Per-stream error reporting ──

    stream-error: func(stream-index: u32, error: plugin-error) -> result<_, plugin-error>;

    // ── Logging ──

    log: func(level: u32, msg: string);

    // ── Metrics ──

    counter-add: func(name: string, value: u64, labels-json: string) -> result<_, plugin-error>;
    gauge-set: func(name: string, value: f64, labels-json: string) -> result<_, plugin-error>;
    histogram-record: func(name: string, value: f64, labels-json: string) -> result<_, plugin-error>;

    // ── Dead-letter queue ──

    emit-dlq-record: func(
        stream-name: string,
        record-json: string,
        error-message: string,
        error-category: string,
    ) -> result<_, plugin-error>;

    // ── Network (ACL-gated) ──

    connect-tcp: func(host: string, port: u16) -> result<socket-handle, plugin-error>;
    socket-read: func(handle: socket-handle, len: u64) -> result<socket-read-result, plugin-error>;
    socket-write: func(handle: socket-handle, data: list<u8>) -> result<socket-write-result, plugin-error>;
    socket-close: func(handle: socket-handle);
}
```

### Plugin Exports

```wit
interface source {
    use types.{
        plugin-error, plugin-spec, session,
        run-request, run-summary, discovered-stream,
        validation-report, prerequisites-report,
        apply-report, apply-request,
        teardown-request, teardown-report, stream-schema,
    };

    spec: func() -> result<plugin-spec, plugin-error>;
    open: func(config-json: string) -> result<session, plugin-error>;
    prerequisites: func(session: session) -> result<prerequisites-report, plugin-error>;
    discover: func(session: session) -> result<list<discovered-stream>, plugin-error>;
    validate: func(session: session, stream-schema: option<stream-schema>) -> result<validation-report, plugin-error>;
    apply: func(session: session, request: apply-request) -> result<apply-report, plugin-error>;
    run: func(session: session, request: run-request) -> result<run-summary, plugin-error>;
    close: func(session: session) -> result<_, plugin-error>;
    teardown: func(session: session, request: teardown-request) -> result<teardown-report, plugin-error>;
}

interface destination {
    use types.{
        plugin-error, plugin-spec, session,
        run-request, run-summary,
        validation-report, prerequisites-report,
        apply-report, apply-request,
        teardown-request, teardown-report, stream-schema,
    };

    spec: func() -> result<plugin-spec, plugin-error>;
    open: func(config-json: string) -> result<session, plugin-error>;
    prerequisites: func(session: session) -> result<prerequisites-report, plugin-error>;
    validate: func(session: session, stream-schema: option<stream-schema>) -> result<validation-report, plugin-error>;
    apply: func(session: session, request: apply-request) -> result<apply-report, plugin-error>;
    run: func(session: session, request: run-request) -> result<run-summary, plugin-error>;
    close: func(session: session) -> result<_, plugin-error>;
    teardown: func(session: session, request: teardown-request) -> result<teardown-report, plugin-error>;
}

interface transform {
    use types.{
        plugin-error, plugin-spec, session,
        run-request, run-summary,
        validation-report, stream-schema,
    };

    spec: func() -> result<plugin-spec, plugin-error>;
    open: func(config-json: string) -> result<session, plugin-error>;
    validate: func(session: session, stream-schema: option<stream-schema>) -> result<validation-report, plugin-error>;
    run: func(session: session, request: run-request) -> result<run-summary, plugin-error>;
    close: func(session: session) -> result<_, plugin-error>;
}
```

### Worlds

```wit
world rapidbyte-host {
    import host;
}

world rapidbyte-source {
    import host;
    export source;
}

world rapidbyte-destination {
    import host;
    export destination;
}

world rapidbyte-transform {
    import host;
    export transform;
}
```

## SDK Trait Design

### Core Traits

Plugin authors implement these. All new lifecycle methods have default no-op implementations — the minimum to implement a working plugin is the same as v6: `init`, `discover` (source only), and `read`/`write`/`transform`.

```rust
#[async_trait]
pub trait Source: Sized + Send {
    type Config: DeserializeOwned + Send;

    fn spec() -> PluginSpec { PluginSpec::from_manifest() }

    async fn init(config: Self::Config) -> Result<Self, PluginError>;

    async fn prerequisites(&self, ctx: &Context) -> Result<PrerequisitesReport, PluginError> {
        Ok(PrerequisitesReport::passed())
    }

    async fn discover(&self, ctx: &Context) -> Result<Vec<DiscoveredStream>, PluginError>;

    async fn validate(
        &self, ctx: &Context, upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Validation not implemented"))
    }

    async fn apply(
        &self, ctx: &Context, request: ApplyRequest,
    ) -> Result<ApplyReport, PluginError> {
        Ok(ApplyReport::noop())
    }

    async fn read(
        &self, ctx: &Context, stream: StreamContext,
    ) -> Result<ReadSummary, PluginError>;

    async fn close(&self, ctx: &Context) -> Result<(), PluginError> {
        Ok(())
    }

    async fn teardown(
        &self, ctx: &Context, request: TeardownRequest,
    ) -> Result<TeardownReport, PluginError> {
        Ok(TeardownReport::noop())
    }
}

#[async_trait]
pub trait Destination: Sized + Send {
    type Config: DeserializeOwned + Send;

    fn spec() -> PluginSpec { PluginSpec::from_manifest() }
    async fn init(config: Self::Config) -> Result<Self, PluginError>;

    async fn prerequisites(&self, ctx: &Context) -> Result<PrerequisitesReport, PluginError> {
        Ok(PrerequisitesReport::passed())
    }

    async fn validate(
        &self, ctx: &Context, upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Validation not implemented"))
    }

    async fn apply(
        &self, ctx: &Context, request: ApplyRequest,
    ) -> Result<ApplyReport, PluginError> {
        Ok(ApplyReport::noop())
    }

    async fn write(
        &self, ctx: &Context, stream: StreamContext,
    ) -> Result<WriteSummary, PluginError>;

    async fn close(&self, ctx: &Context) -> Result<(), PluginError> {
        Ok(())
    }

    async fn teardown(
        &self, ctx: &Context, request: TeardownRequest,
    ) -> Result<TeardownReport, PluginError> {
        Ok(TeardownReport::noop())
    }
}

#[async_trait]
pub trait Transform: Sized + Send {
    type Config: DeserializeOwned + Send;

    fn spec() -> PluginSpec { PluginSpec::from_manifest() }
    async fn init(config: Self::Config) -> Result<Self, PluginError>;

    async fn validate(
        &self, ctx: &Context, upstream: Option<&StreamSchema>,
    ) -> Result<ValidationReport, PluginError> {
        Ok(ValidationReport::success("Validation not implemented"))
    }

    async fn transform(
        &self, ctx: &Context, stream: StreamContext,
    ) -> Result<TransformSummary, PluginError>;

    async fn close(&self, ctx: &Context) -> Result<(), PluginError> {
        Ok(())
    }
}
```

### Feature Traits (opt-in capabilities)

```rust
#[async_trait]
pub trait MultiStreamSource: Source {
    async fn read_streams(
        &self, ctx: &Context, streams: Vec<StreamContext>,
    ) -> Result<RunSummary, PluginError>;
}

#[async_trait]
pub trait CdcSource: Source {
    async fn read_changes(
        &self, ctx: &Context, stream: StreamContext,
    ) -> Result<ReadSummary, PluginError>;
}

#[async_trait]
pub trait MultiStreamCdcSource: MultiStreamSource + CdcSource {
    async fn read_all_changes(
        &self, ctx: &Context, streams: Vec<StreamContext>,
    ) -> Result<RunSummary, PluginError>;
}

#[async_trait]
pub trait PartitionedSource: Source {
    async fn read_partition(
        &self, ctx: &Context, stream: StreamContext,
    ) -> Result<ReadSummary, PluginError>;
}

#[async_trait]
pub trait BulkDestination: Destination {
    async fn write_bulk(
        &self, ctx: &Context, stream: StreamContext,
    ) -> Result<WriteSummary, PluginError>;
}
```

### Context API

```rust
impl Context {
    // ── Identity ──
    pub fn plugin_id(&self) -> &str;
    pub fn stream_name(&self) -> &str;
    pub fn with_stream(&self, name: &str) -> Context;

    // ── Batch emission (source/transform) ──
    pub fn emit_batch(&self, batch: &RecordBatch) -> Result<(), PluginError>;
    pub fn emit_batch_for_stream(
        &self, stream_index: u32, batch: &RecordBatch,
    ) -> Result<(), PluginError>;

    // ── Batch consumption (destination/transform) ──
    pub fn next_batch(&self) -> Result<Option<IncomingBatch>, PluginError>;

    // ── Checkpoints (simple path) ──
    pub fn checkpoint(&self, cp: &Checkpoint) -> Result<(), PluginError>;

    // ── Checkpoints (transactional path) ──
    pub fn begin_checkpoint(
        &self, kind: CheckpointKind,
    ) -> Result<CheckpointTxn, PluginError>;

    // ── Non-progress state ──
    pub fn state_get(
        &self, scope: StateScope, key: &str,
    ) -> Result<Option<String>, PluginError>;
    pub fn state_put(
        &self, scope: StateScope, key: &str, value: &str,
    ) -> Result<(), PluginError>;

    // ── Cancellation ──
    pub fn is_cancelled(&self) -> bool;
    pub fn check_cancelled(&self) -> Result<(), PluginError>;

    // ── Stream errors (multi-stream) ──
    pub fn stream_error(
        &self, stream_index: u32, error: PluginError,
    ) -> Result<(), PluginError>;

    // ── Logging ──
    pub fn log(&self, level: LogLevel, msg: &str);

    // ── Metrics ──
    pub fn counter(&self, name: &str, value: u64) -> Result<(), PluginError>;
    pub fn counter_with_labels(
        &self, name: &str, value: u64, labels: &[(&str, &str)],
    ) -> Result<(), PluginError>;
    pub fn gauge(&self, name: &str, value: f64) -> Result<(), PluginError>;
    pub fn gauge_with_labels(
        &self, name: &str, value: f64, labels: &[(&str, &str)],
    ) -> Result<(), PluginError>;
    pub fn histogram(&self, name: &str, value: f64) -> Result<(), PluginError>;
    pub fn histogram_with_labels(
        &self, name: &str, value: f64, labels: &[(&str, &str)],
    ) -> Result<(), PluginError>;

    // ── DLQ ──
    pub fn emit_dlq_record(
        &self, record_json: &str, error_msg: &str, category: ErrorCategory,
    ) -> Result<(), PluginError>;
}
```

### Checkpoint Builder

```rust
pub struct CheckpointTxn { /* host handle */ }

impl CheckpointTxn {
    pub fn set_cursor(
        &mut self, stream: &str, field: &str, value: CursorValue,
    ) -> Result<(), PluginError>;

    pub fn set_state(
        &mut self, scope: StateScope, key: &str, value: &str,
    ) -> Result<(), PluginError>;

    pub fn commit(self, records: u64, bytes: u64) -> Result<(), PluginError>;

    pub fn abort(self);
}

impl Drop for CheckpointTxn {
    fn drop(&mut self) { /* abort if not committed */ }
}
```

### Incoming Batch (for destinations/transforms)

```rust
pub struct IncomingBatch {
    pub schema: Arc<Schema>,
    pub batches: Vec<RecordBatch>,
    pub metadata: BatchMetadata,
}

impl IncomingBatch {
    pub fn stream_index(&self) -> u32;
    pub fn sequence_number(&self) -> u64;
    pub fn schema_changed(&self) -> bool;
}
```

### Macro Dispatch

The `#[plugin(source, features = [...])]` macro generates WIT glue that routes `run()` calls:

1. If `multi-stream` + `cdc` declared AND request has multiple CDC streams → `read_all_changes()`
2. If `cdc` declared AND sync_mode is CDC → `read_changes()`
3. If `multi-stream` declared AND request has multiple streams → `read_streams()`
4. If `partitioned-read` AND partition coords present → `read_partition()`
5. Otherwise → `read()`

## Host-Side Orchestration

### Pipeline Check Flow (`rapidbyte check`)

```
1. Load plugin WASMs, read manifests (permissions, limits, protocol version == 7)
2. For each plugin: call spec() → collect config schemas, validate pipeline YAML
3. For each plugin: call open(config_json) → store session handles
4. For each plugin: call prerequisites(session)
   ├─ Source: wal_level? replication role? slot? publication?
   ├─ Destination: write permissions? table exists?
   └─ If any severity=error check failed: print fix hints, exit 1
5. Source: call discover(session) → list<discovered-stream> with rich schemas
6. Schema negotiation chain:
   │  for each stream in pipeline:
   │    schema = source_schema[stream]
   │    for each transform in chain:
   │      report = transform.validate(session, Some(schema))
   │      schema = report.output_schema
   │    report = destination.validate(session, Some(schema))
   │    reconcile(schema, report.field_requirements):
   │      FIELD_REQUIRED + missing → error
   │      FIELD_FORBIDDEN + present → error
   │      TYPE_INCOMPATIBLE → error with accepted_types hint
   │      FIELD_RECOMMENDED + missing → warning
   │      FIELD_OPTIONAL → pass
7. Print validation summary
8. close(session) for all plugins
```

### Pipeline Apply Flow (`rapidbyte check --apply`)

```
1-7. Same as check flow (must pass first)
8. destination.apply(session, streams, dry_run=false) → DDL actions
9. source.apply(session, streams, dry_run=false) → create repl slots, publications
10. close(session) for all plugins
```

### Pipeline Run Flow (`rapidbyte run`)

```
1. spec() + open() + prerequisites()
2. Load cursors from state backend → inject into stream_context.cursor_info
3. Build stream contexts with limits, policies, schema, cursor
4. Determine dispatch strategy:
   │  if source has "multi-stream" + "cdc" AND multiple CDC streams:
   │    → bundle all CDC streams into one run_request
   │  else:
   │    → one run_request per stream, parallel tasks bounded by max_concurrent
5. Set up channel topology:
   │  Source → mpsc channel → Transform chain → mpsc channel → Destination
   │  (one channel per stream, batches routed by stream_index)
6. Monitor execution:
   │  - Correlate source + destination checkpoint transactions
   │  - On matched pair: persist cursor atomically to state backend
   │  - Propagate cancel_requested to is_cancelled()
   │  - Collect stream_error() calls → mark streams as failed
7. Collect per-stream results from run_summary
8. Per-stream retry:
   │  - Failed + retryable: decrement retry budget, apply backoff, re-run stream only
   │  - Failed + not retryable: mark failed, continue others
9. close(session) for all plugins
```

### Checkpoint Correlation

```
Source emits batch B1 for stream "users":
  emit-batch(frame, metadata{stream_index: 0, sequence: 1})

Source commits checkpoint:
  txn = checkpoint-begin(Source)
  checkpoint-set-cursor(txn, {stream: "users", field: "id", value: "5000"})
  checkpoint-commit(txn, records: 5000, bytes: 2MB)
  → Host records: source checkpoint S1 = {users: id=5000, seq=1}

Destination processes B1, commits checkpoint:
  txn = checkpoint-begin(Destination)
  checkpoint-commit(txn, records: 5000, bytes: 2MB)
  → Host records: destination checkpoint D1 = {seq=1}

Host correlates S1 + D1:
  Sequence numbers match → data landed
  Atomically persist cursor(users) = id=5000 to state backend
```

### Multi-Stream CDC Dispatch

```
Pipeline: source=postgres(CDC), streams=[users, orders, payments, products, events]
Source manifest: features = ["multi-stream", "cdc"]

Host builds one RunRequest with all 5 streams.
Plugin opens ONE replication slot, ONE publication covering all tables.
Plugin decodes pgoutput, emits batches tagged by stream_index.
Plugin checkpoints with cursors for ALL streams atomically.

Host creates 5 destination channels, routes batches by stream_index.

If "payments" fails:
  Plugin calls stream_error(2, PluginError::schema(...))
  Plugin continues processing streams 0, 1, 3, 4
  RunSummary: 4 succeeded, 1 failed
  Host retries "payments" separately if retryable
```

### Protocol Version Enforcement

```
Host loads plugin → reads manifest → checks protocol_version

if protocol_version != 7:
  reject with error:
  "Plugin {id} uses protocol v{N}, but this runtime requires v7.
   Rebuild the plugin against rapidbyte-sdk 7.x"
```

No v6 compatibility bridge. All plugins must be rebuilt for v7.

## DX Comparison: v6 vs v7

| Aspect | v6 | v7 |
|---|---|---|
| Minimum methods to implement | 3 (init, discover, read) | 3 (init, discover, read) |
| Default impls provided | validate, close | spec, prerequisites, validate, apply, close, teardown |
| Schema discovery | `Vec<Stream>` with basic columns | `Vec<DiscoveredStream>` with PKs, cursors, estimates |
| Schema negotiation | None | Full pipeline validation at check time |
| Checkpoint | Single fire-and-forget call | Simple one-liner OR transactional builder |
| Multi-stream CDC | Not possible | Opt-in trait, macro routes automatically |
| Cancellation | Not available | `ctx.check_cancelled()?` |
| Pre-flight checks | Manual in validate | Dedicated `prerequisites()` with fix hints |
| DDL / setup | Implicit in run | Explicit `apply()` with dry-run |
| Resource cleanup | Implicit in close | Explicit `teardown()` with reason |
| Batch metadata | None | Stream index, schema fingerprint, sequence, compression |
| Partial failure | Whole invocation fails | Per-stream error reporting |

## Changes from v6

### Removed

- `state-cas` host import (replaced by transactional checkpoints)
- `checkpoint(kind, payload-json)` host import (replaced by checkpoint-begin/set-cursor/set-state/commit/abort)
- Protocol version negotiation at runtime (hard reject if not v7)
- `SchemaHint::ArrowIpc` variant (replaced by `stream-schema` with typed fields)

### Added

- **WIT exports:** `spec`, `prerequisites`, `apply`, `teardown`
- **WIT imports:** `checkpoint-begin/set-cursor/set-state/commit/abort`, `is-cancelled`, `stream-error`, `next-batch-metadata`
- **Types:** `stream-schema`, `schema-field`, `field-requirement`, `field-constraint`, `batch-metadata`, `prerequisites-report`, `prerequisite-check`, `apply-request/report/action`, `teardown-request/report`, `plugin-spec`, `run-request` (multi-stream), `stream-result`, `run-summary`, `cursor-update`, `state-mutation`, `checkpoint-txn`
- **SDK traits:** `MultiStreamSource`, `MultiStreamCdcSource`, `BulkDestination`
- **Error category:** `cancelled`

### Modified

- `emit-batch` now requires `batch-metadata` parameter
- `validate` receives optional upstream `stream-schema`
- `validation-report` carries `output-schema` and `field-requirements`
- `run` receives `run-request` with `list<stream-context>` (multi-stream)
- `run` returns `run-summary` with `list<stream-result>` (per-stream outcomes)
- Transform interface is smaller (no prerequisites, apply, teardown)
