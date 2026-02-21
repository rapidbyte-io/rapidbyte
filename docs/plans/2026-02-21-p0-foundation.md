# P0 Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the Rapidbyte engine production-viable by implementing the five P0 items from IMPROVEMENTS.md: Pipeline Parallelism, CDC, Schema Evolution Enforcement, Dead Letter Queue, and Projection Pushdown.

**Architecture:** Each feature is an independent work stream. They share no code dependencies and can be implemented in any order. The plan is ordered by impact: Projection Pushdown and Schema Evolution are the simplest wins; Parallelism and CDC are the largest; DLQ sits in between.

**Tech Stack:** Rust, Wasmtime v41, Arrow IPC, tokio, serde_yaml, rusqlite

---

## Task 1: Projection Pushdown

Add column selection to pipeline YAML and pass it through to source connectors so they only read requested columns.

**Files:**
- Modify: `crates/rapidbyte-core/src/pipeline/types.rs` (StreamConfig struct, line 26)
- Modify: `crates/rapidbyte-sdk/src/protocol.rs` (StreamContext struct, line 196)
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs` (stream context building, ~line 274)
- Modify: `connectors/source-postgres/src/reader.rs` (SQL query building)
- Test: `crates/rapidbyte-core/src/pipeline/types.rs` (existing test module)
- Test: `crates/rapidbyte-sdk/src/protocol.rs` (existing test module)

**Step 1: Add `columns` field to `StreamConfig`**

In `crates/rapidbyte-core/src/pipeline/types.rs`, add an optional columns field:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: String,
    pub sync_mode: String,
    pub cursor_field: Option<String>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
}
```

**Step 2: Write test for YAML parsing with columns**

Add to the test module in `types.rs`:

```rust
#[test]
fn test_deserialize_stream_with_columns() {
    let yaml = r#"
version: "1"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
      columns:
        - id
        - name
        - email
destination:
  use: dest-postgres
  config: {}
  write_mode: append
"#;
    let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(
        config.source.streams[0].columns,
        Some(vec!["id".into(), "name".into(), "email".into()])
    );
}

#[test]
fn test_deserialize_stream_without_columns_defaults_none() {
    let yaml = r#"
version: "1"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config: {}
  write_mode: append
"#;
    let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(config.source.streams[0].columns.is_none());
}
```

Run: `cargo test -p rapidbyte-core test_deserialize_stream_with_columns test_deserialize_stream_without_columns`

**Step 3: Add `selected_columns` to `StreamContext`**

In `crates/rapidbyte-sdk/src/protocol.rs`, extend StreamContext:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamContext {
    pub stream_name: String,
    pub schema: SchemaHint,
    pub sync_mode: SyncMode,
    pub cursor_info: Option<CursorInfo>,
    pub limits: StreamLimits,
    pub policies: StreamPolicies,
    pub write_mode: Option<WriteMode>,
    #[serde(default)]
    pub selected_columns: Option<Vec<String>>,
}
```

**Step 4: Write test for StreamContext roundtrip with selected_columns**

Add to the test module in `protocol.rs`:

```rust
#[test]
fn test_stream_context_with_selected_columns_roundtrip() {
    let ctx = StreamContext {
        stream_name: "users".to_string(),
        schema: SchemaHint::Columns(vec![]),
        sync_mode: SyncMode::FullRefresh,
        cursor_info: None,
        limits: StreamLimits::default(),
        policies: StreamPolicies::default(),
        write_mode: Some(WriteMode::Append),
        selected_columns: Some(vec!["id".into(), "name".into()]),
    };
    let json = serde_json::to_string(&ctx).unwrap();
    let back: StreamContext = serde_json::from_str(&json).unwrap();
    assert_eq!(ctx, back);
}

#[test]
fn test_stream_context_backwards_compat_no_selected_columns() {
    // Old JSON without selected_columns should deserialize with None
    let json = r#"{"stream_name":"users","schema":{"columns":[]},"sync_mode":"full_refresh","cursor_info":null,"limits":{"max_batch_bytes":67108864,"max_record_bytes":16777216,"max_inflight_batches":16,"max_parallel_requests":1,"checkpoint_interval_bytes":67108864,"checkpoint_interval_rows":0,"checkpoint_interval_seconds":0},"policies":{"on_data_error":"fail","schema_evolution":{"new_column":"add","removed_column":"ignore","type_change":"fail","nullability_change":"allow"}},"write_mode":"append"}"#;
    let ctx: StreamContext = serde_json::from_str(json).unwrap();
    assert!(ctx.selected_columns.is_none());
}
```

Run: `cargo test -p rapidbyte-sdk test_stream_context_with_selected_columns test_stream_context_backwards_compat`

**Step 5: Wire columns through the orchestrator**

In `crates/rapidbyte-core/src/engine/orchestrator.rs`, where `StreamContext` is built (~line 274), pass through the columns from pipeline config:

```rust
StreamContext {
    stream_name: s.name.clone(),
    schema: SchemaHint::Columns(vec![]),
    sync_mode,
    cursor_info,
    limits: limits.clone(),
    policies: StreamPolicies {
        on_data_error,
        ..StreamPolicies::default()
    },
    write_mode: Some(write_mode),
    selected_columns: s.columns.clone(),
}
```

**Step 6: Update source-postgres to use selected_columns in SQL query**

In `connectors/source-postgres/src/reader.rs`, where the SELECT query is built, use the column list if provided:

```rust
let col_list = match &ctx.selected_columns {
    Some(cols) if !cols.is_empty() => cols.join(", "),
    _ => "*".to_string(),
};
// Use col_list in the query: SELECT {col_list} FROM {schema}.{table}
```

Also filter the Arrow schema to only include selected columns (so downstream stages see the reduced schema).

**Step 7: Update existing `test_stream_context_roundtrip` test**

The existing test in `protocol.rs` needs the new field added. Set `selected_columns: None` to match existing behavior.

**Step 8: Run all tests**

```bash
cargo test --workspace
```

**Step 9: Commit**

```bash
git add -A && git commit -m "feat: add projection pushdown (column selection per stream)"
```

---

## Task 2: Schema Evolution Enforcement

Expose schema evolution policies in pipeline YAML so users can control how the destination handles schema drift. The dest-postgres connector already implements all the enforcement logic — this task just wires the config through.

**Files:**
- Modify: `crates/rapidbyte-core/src/pipeline/types.rs` (DestinationConfig struct, line 40)
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs` (policy parsing, ~line 227)
- Test: `crates/rapidbyte-core/src/pipeline/types.rs` (test module)

**Step 1: Add schema_evolution fields to DestinationConfig**

In `crates/rapidbyte-core/src/pipeline/types.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationConfig {
    #[serde(rename = "use")]
    pub use_ref: String,
    pub config: serde_json::Value,
    pub write_mode: String,
    #[serde(default)]
    pub primary_key: Vec<String>,
    #[serde(default = "default_on_data_error")]
    pub on_data_error: String,
    #[serde(default)]
    pub schema_evolution: Option<SchemaEvolutionConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEvolutionConfig {
    #[serde(default = "default_new_column")]
    pub new_column: String,
    #[serde(default = "default_removed_column")]
    pub removed_column: String,
    #[serde(default = "default_type_change")]
    pub type_change: String,
    #[serde(default = "default_nullability_change")]
    pub nullability_change: String,
}

fn default_new_column() -> String { "add".to_string() }
fn default_removed_column() -> String { "ignore".to_string() }
fn default_type_change() -> String { "fail".to_string() }
fn default_nullability_change() -> String { "allow".to_string() }
```

**Step 2: Write tests for YAML parsing**

```rust
#[test]
fn test_deserialize_schema_evolution() {
    let yaml = r#"
version: "1"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config: {}
  write_mode: append
  schema_evolution:
    new_column: ignore
    type_change: coerce
"#;
    let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
    let se = config.destination.schema_evolution.unwrap();
    assert_eq!(se.new_column, "ignore");
    assert_eq!(se.type_change, "coerce");
    assert_eq!(se.removed_column, "ignore"); // default
    assert_eq!(se.nullability_change, "allow"); // default
}

#[test]
fn test_schema_evolution_defaults_to_none() {
    let yaml = r#"
version: "1"
pipeline: test
source:
  use: source-postgres
  config: {}
  streams:
    - name: users
      sync_mode: full_refresh
destination:
  use: dest-postgres
  config: {}
  write_mode: append
"#;
    let config: PipelineConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(config.destination.schema_evolution.is_none());
}
```

Run: `cargo test -p rapidbyte-core test_deserialize_schema_evolution test_schema_evolution_defaults`

**Step 3: Parse schema evolution config in orchestrator**

In `crates/rapidbyte-core/src/engine/orchestrator.rs`, where `StreamPolicies` is built (~line 280), parse the schema evolution config:

```rust
let schema_evolution = match &config.destination.schema_evolution {
    Some(se) => SchemaEvolutionPolicy {
        new_column: match se.new_column.as_str() {
            "ignore" => ColumnPolicy::Ignore,
            "fail" => ColumnPolicy::Fail,
            _ => ColumnPolicy::Add,
        },
        removed_column: match se.removed_column.as_str() {
            "fail" => ColumnPolicy::Fail,
            "add" => ColumnPolicy::Add,
            _ => ColumnPolicy::Ignore,
        },
        type_change: match se.type_change.as_str() {
            "coerce" => TypeChangePolicy::Coerce,
            "null" => TypeChangePolicy::Null,
            _ => TypeChangePolicy::Fail,
        },
        nullability_change: match se.nullability_change.as_str() {
            "fail" => NullabilityPolicy::Fail,
            _ => NullabilityPolicy::Allow,
        },
    },
    None => SchemaEvolutionPolicy::default(),
};

// Then use in StreamContext:
policies: StreamPolicies {
    on_data_error,
    schema_evolution,
},
```

**Step 4: Run all tests**

```bash
cargo test --workspace
```

**Step 5: Commit**

```bash
git add -A && git commit -m "feat: expose schema evolution policies in pipeline YAML"
```

---

## Task 3: Dead Letter Queue (DLQ)

Route failed records to a separate DLQ table with error metadata instead of silently dropping them. This requires a new host import, changes to the destination connector, and a DLQ writer in the orchestrator.

**Files:**
- Modify: `crates/rapidbyte-sdk/src/protocol.rs` (add DlqRecord struct)
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs` (add `emit-dlq-batch` host import)
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs` (spawn DLQ writer, add DLQ channel)
- Modify: `crates/rapidbyte-core/src/pipeline/types.rs` (add DLQ config)
- Modify: `connectors/dest-postgres/src/batch.rs` (emit DLQ records on per-row failure)
- Create: `crates/rapidbyte-core/src/engine/dlq.rs` (DLQ writer logic)
- Test: Unit tests for DLQ record serialization and DLQ writer

**Step 1: Define DLQ record type in protocol**

In `crates/rapidbyte-sdk/src/protocol.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DlqRecord {
    pub stream_name: String,
    pub record_json: String,
    pub error_message: String,
    pub error_category: String,
    pub failed_at: String, // ISO 8601 timestamp
}
```

**Step 2: Add DLQ config to pipeline YAML**

In `crates/rapidbyte-core/src/pipeline/types.rs`, add to DestinationConfig:

```rust
#[serde(default)]
pub dlq_table: Option<String>, // e.g., "_rapidbyte_dlq"
```

When `on_data_error: dlq` is set and `dlq_table` is None, default to `_rapidbyte_dlq`.

**Step 3: Add `emit-dlq-batch` host import**

In `crates/rapidbyte-core/src/runtime/component_runtime.rs`, register a new host function alongside `emit-batch`:

```rust
// Host import: emit-dlq-batch(records_json: string) -> result<_, connector-error>
// Accepts a JSON array of DlqRecord, sends to DLQ channel
```

The DLQ channel is a separate `mpsc::Sender<String>` stored in `ComponentHostState`.

**Step 4: Create DLQ writer module**

Create `crates/rapidbyte-core/src/engine/dlq.rs`:
- Receives DLQ records from channel
- Creates/ensures DLQ table exists in destination (columns: id, stream_name, record_json, error_message, error_category, failed_at, pipeline_name, run_id)
- Inserts records using the destination's connection (or a separate connection)
- For the initial implementation, write DLQ records to the state backend (SQLite) since it's always available

**Step 5: Wire DLQ channel in orchestrator**

In `orchestrator.rs`:
- Create DLQ channel alongside batch channels
- Pass DLQ sender to destination's `ComponentHostState`
- Spawn DLQ writer task
- Wait for DLQ writer after destination completes
- Log DLQ summary (records routed)

**Step 6: Update dest-postgres per-row fallback**

In `connectors/dest-postgres/src/batch.rs`, in the `write_rows_individually` function (~line 533), when a row fails and policy is DLQ:
- Serialize the failed row as JSON
- Call `emit-dlq-batch` host import with the DlqRecord
- Continue processing remaining rows

**Step 7: Write tests**

- DlqRecord serialization roundtrip
- DLQ channel: send records, verify received
- DLQ writer: mock destination, verify table creation and inserts

**Step 8: Run all tests**

```bash
cargo test --workspace
```

**Step 9: Commit**

```bash
git add -A && git commit -m "feat: add Dead Letter Queue for failed records"
```

---

## Task 4: Pipeline Parallelism

Run multiple streams concurrently instead of sequentially. Each stream gets its own WASM instance and channel pair.

**Files:**
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs` (parallel stream spawning)
- Modify: `crates/rapidbyte-core/src/engine/runner.rs` (per-stream runner functions)
- Modify: `crates/rapidbyte-core/src/runtime/component_runtime.rs` (per-stream host state)
- Modify: `crates/rapidbyte-core/src/engine/checkpoint.rs` (parallel checkpoint collection)
- Test: `crates/rapidbyte-core/src/engine/` (orchestrator + checkpoint tests)

**Step 1: Make `current_stream` per-task instead of shared**

In `crates/rapidbyte-core/src/runtime/component_runtime.rs`, change `current_stream` from `Arc<Mutex<String>>` to a plain `String` owned by each task. Each WASM instance gets its own `ComponentHostState` with its own `current_stream` set at construction time. This removes the mutex contention.

**Step 2: Refactor checkpoint collection for per-stream isolation**

Currently checkpoints are collected into shared `Arc<Mutex<Vec<Checkpoint>>>`. Change to per-stream collection:
- Each stream task has its own `Vec<Checkpoint>` (no Arc<Mutex>)
- After all stream tasks complete, aggregate checkpoints for correlation

**Step 3: Refactor runner to support per-stream execution**

In `crates/rapidbyte-core/src/engine/runner.rs`, the current `run_source` iterates all streams in a for loop. Refactor to:

```rust
pub fn run_source_stream(
    module: &LoadedComponent,
    stream_ctx: StreamContext,
    sender: mpsc::Sender<Frame>,
    // ... per-stream host state fields
) -> Result<(ReadSummary, Vec<Checkpoint>)>
```

Same pattern for `run_destination_stream` and `run_transform_stream`.

**Step 4: Implement parallel stream spawning in orchestrator**

In `orchestrator.rs`, replace the single source/dest task with a fan-out pattern:

```rust
let parallelism = config.resources.parallelism as usize;
let semaphore = Arc::new(Semaphore::new(parallelism));

let mut stream_tasks = Vec::new();
for stream_ctx in stream_ctxs {
    let permit = semaphore.clone().acquire_owned().await?;
    let module = source_module.clone(); // Arc<Engine> + Arc<Component>

    let (batch_tx, batch_rx) = mpsc::channel(channel_capacity);

    // Spawn source + dest pair for this stream
    let source_task = tokio::task::spawn_blocking(move || {
        let result = run_source_stream(&module, stream_ctx, batch_tx, ...);
        drop(permit); // Release semaphore slot
        result
    });

    let dest_task = tokio::task::spawn_blocking(move || {
        run_destination_stream(&dest_module, stream_ctx, batch_rx, ...)
    });

    stream_tasks.push((source_task, dest_task));
}

// Wait for all stream tasks
for (src, dst) in stream_tasks {
    let (read_summary, src_checkpoints) = src.await??;
    let (write_summary, dst_checkpoints) = dst.await??;
    all_source_checkpoints.extend(src_checkpoints);
    all_dest_checkpoints.extend(dst_checkpoints);
}
```

Key design decisions:
- **Semaphore** bounds max concurrent streams to `config.resources.parallelism`
- Each stream gets its own channel pair (no shared channels)
- Each stream gets its own WASM Store (Engine + Component are shared via Arc)
- Transforms: if transforms exist, each stream gets its own transform chain
- Checkpoint correlation runs after all streams complete (unchanged)

**Step 5: Handle transforms in parallel**

If transforms exist, each stream gets its own transform pipeline:
- source → transform_1 → transform_2 → ... → destination (per stream)
- Each transform gets a new WASM instance (Store is per-task)

**Step 6: Update timing aggregation**

Timing metrics (`HostTimings`) are currently `Arc<Mutex<HostTimings>>`. With per-stream execution, collect per-stream timings and aggregate after all streams complete. Change to per-task owned timings, then merge.

**Step 7: Write tests**

- Test parallel execution with 2 streams (verify both complete)
- Test semaphore bounds (parallelism=1 should still work sequentially)
- Test checkpoint correlation with parallel checkpoints
- Test that existing sequential behavior (parallelism=1) is preserved

**Step 8: Run all tests**

```bash
cargo test --workspace
```

**Step 9: Commit**

```bash
git add -A && git commit -m "feat: add per-stream pipeline parallelism"
```

---

## Task 5: CDC (Change Data Capture)

Add Postgres logical replication (pgoutput) support to source-postgres. This is the largest task — it adds a new sync mode that streams WAL changes in real-time.

**Files:**
- Modify: `connectors/source-postgres/src/reader.rs` (add CDC read path)
- Create: `connectors/source-postgres/src/cdc.rs` (logical replication client)
- Modify: `connectors/source-postgres/src/lib.rs` (register CDC module)
- Modify: `connectors/source-postgres/src/schema.rs` (CDC-specific discovery)
- Modify: `crates/rapidbyte-sdk/src/protocol.rs` (CDC-specific checkpoint fields)
- Test: CDC message parsing, LSN tracking

**Step 1: Understand the CDC protocol requirements**

Postgres logical replication uses:
- A replication slot (created once, tracks WAL position)
- The `pgoutput` plugin (built-in since PG 10)
- A replication connection (`replication=database` in connection string)
- Streaming protocol: BEGIN, RELATION, INSERT/UPDATE/DELETE, COMMIT messages
- LSN (Log Sequence Number) as the cursor

**Step 2: Add CDC checkpoint fields to protocol**

In `crates/rapidbyte-sdk/src/protocol.rs`, add a new CursorType variant:

```rust
pub enum CursorType {
    Int64,
    Utf8,
    TimestampMillis,
    TimestampMicros,
    Decimal,
    Json,
    Lsn, // NEW: Postgres Log Sequence Number (u64)
}
```

And CursorValue:

```rust
pub enum CursorValue {
    // ... existing variants
    Lsn(String), // e.g., "0/16B3748"
}
```

**Step 3: Create CDC module**

Create `connectors/source-postgres/src/cdc.rs`:

```rust
pub struct CdcReader {
    client: Client,
    slot_name: String,
    publication_name: String,
    start_lsn: Option<String>,
}

impl CdcReader {
    /// Create replication slot and publication if they don't exist.
    pub async fn setup(&self) -> Result<(), String> { ... }

    /// Start streaming changes from the given LSN (or slot's confirmed_flush_lsn).
    /// Emits Arrow batches via emit_batch() host import.
    /// Emits checkpoints with LSN cursor.
    pub async fn stream_changes(
        &self,
        stream_name: &str,
        schema: &[ColumnSchema],
    ) -> Result<ReadSummary, String> { ... }
}
```

The CDC reader:
1. Opens a replication connection
2. Creates a replication slot + publication (idempotent)
3. Starts `START_REPLICATION` from the last committed LSN (from checkpoint) or slot creation point
4. Parses pgoutput messages (BEGIN, RELATION, INSERT, UPDATE, DELETE, COMMIT)
5. Converts rows to Arrow RecordBatches
6. Emits batches via `emit_batch()` host import
7. On COMMIT, emits checkpoint with the commit LSN
8. Handles keepalive messages (responds with standby status)

**Step 4: Wire CDC into the read path**

In `connectors/source-postgres/src/reader.rs`, branch on sync_mode:

```rust
match ctx.sync_mode {
    SyncMode::FullRefresh | SyncMode::Incremental => {
        // Existing query-based read
    }
    SyncMode::Cdc => {
        let cdc = CdcReader::new(client, slot_name, publication_name, start_lsn);
        cdc.setup().await?;
        cdc.stream_changes(&ctx.stream_name, &schema).await?;
    }
}
```

**Step 5: Add CDC config options**

In the source-postgres connector config (manifest.json schema):
- `replication_slot`: slot name (default: `rapidbyte_{pipeline_name}`)
- `publication_name`: publication name (default: `rapidbyte_{pipeline_name}`)

**Step 6: Handle CDC-specific concerns**

- **DELETE operations**: Emit a delete marker in the Arrow batch (add `_rb_op` column with values: insert/update/delete)
- **Schema discovery for CDC**: Query `pg_publication_tables` to list tables in the publication
- **Keepalive**: Respond to server keepalive messages to prevent timeout
- **Graceful stop**: On pipeline shutdown, flush pending changes and confirm the LSN

**Step 7: Write tests**

- pgoutput message parsing (unit test with fixture bytes)
- LSN cursor roundtrip (serialization)
- CDC checkpoint with LSN value
- Integration test: requires PG with `wal_level=logical` (add to e2e.sh)

**Step 8: Update e2e.sh**

Add a CDC test case to `tests/e2e.sh`:
- Configure PG with `wal_level=logical`
- Create a publication
- Run pipeline with `sync_mode: cdc`
- INSERT/UPDATE/DELETE rows
- Verify all changes captured

**Step 9: Run all tests**

```bash
cargo test --workspace
cd connectors/source-postgres && cargo build --target wasm32-wasip2
```

**Step 10: Commit**

```bash
git add -A && git commit -m "feat: add CDC (logical replication) support for source-postgres"
```

---

## Dependency Order

These tasks have no hard dependencies on each other. Recommended execution order based on complexity (simplest first):

1. **Task 1: Projection Pushdown** — Small, well-scoped. Touches config + SDK + one connector.
2. **Task 2: Schema Evolution** — Small, mostly config wiring. Dest-postgres already does the work.
3. **Task 3: DLQ** — Medium. New host import + new module + connector changes.
4. **Task 4: Parallelism** — Large. Refactors orchestrator + runner + host state.
5. **Task 5: CDC** — Largest. New connector module, new protocol, new e2e test infrastructure.

Tasks 1-3 can be dispatched to parallel agents. Tasks 4-5 benefit from sequential execution due to their scope.
