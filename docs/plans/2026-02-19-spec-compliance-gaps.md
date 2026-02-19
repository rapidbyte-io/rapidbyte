# Spec Compliance Gaps Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the two remaining spec compliance gaps (commit_state handling and connector metrics), and clean up related tech debt.

**Architecture:** Fix dest-postgres to use correct CommitState on COMMIT failures, add commit_state-aware logging in orchestrator retry loop, and add `host_ffi::metric()` calls to both connectors for real-time progress tracking per spec § Standard Metrics.

**Tech Stack:** Rust, rapidbyte-sdk, rapidbyte-core, source-postgres, dest-postgres

---

## Audit Findings Summary

The original audit reported 5 "partially implemented" features. Research revealed **3 are already fully implemented**:

| Feature | Status | Evidence |
|---------|--------|----------|
| Feature::ExactlyOnce | **Already done** | `dest-postgres/src/main.rs:155` — `vec![Feature::ExactlyOnce]` |
| Feature::BulkLoadCopy | **Already done** | `dest-postgres/src/main.rs:156-158` — conditionally added when `load_method == "copy"` |
| WASI sandbox enforcement | **Already done** | `vm_factory.rs:create_secure_wasi_module()` — env filtering, fs preopens, deny-by-default |

The **2 real gaps** to address:

1. **commit_state handling**: Dest connector uses `BeforeCommit` even for COMMIT failures (should be `AfterCommitUnknown`); host ignores `commit_state` entirely.
2. **host_metric()**: FFI registered and SDK wrapper exists, but no connector calls it — real-time metrics are missing per spec § Standard Metrics.

---

### Task 1: Fix commit_state in dest-postgres and add host-side handling

**Files:**
- Modify: `connectors/dest-postgres/src/main.rs:333-340`
- Modify: `crates/rapidbyte-core/src/engine/errors.rs:42-58`
- Modify: `crates/rapidbyte-core/src/engine/orchestrator.rs:35-63`
- Test: `crates/rapidbyte-core/src/engine/errors.rs` (existing test module)

**Step 1: Write the failing test for commit_state extraction**

Add to `crates/rapidbyte-core/src/engine/errors.rs` test module:

```rust
#[test]
fn test_pipeline_error_commit_state_extraction() {
    use rapidbyte_sdk::errors::CommitState;

    let err = PipelineError::Connector(
        ConnectorError::transient_db("COMMIT_FAILED", "timeout")
            .with_commit_state(CommitState::AfterCommitUnknown),
    );
    let ce = err.as_connector_error().unwrap();
    assert_eq!(ce.commit_state, Some(CommitState::AfterCommitUnknown));
}

#[test]
fn test_pipeline_error_commit_unknown_is_retryable_but_unsafe() {
    use rapidbyte_sdk::errors::CommitState;

    let err = PipelineError::Connector(
        ConnectorError::transient_db("COMMIT_FAILED", "timeout")
            .with_commit_state(CommitState::AfterCommitUnknown),
    );
    // The error is marked retryable by the connector...
    assert!(err.is_retryable());
    // ...but commit_state signals danger
    let ce = err.as_connector_error().unwrap();
    assert_eq!(ce.commit_state, Some(CommitState::AfterCommitUnknown));
    assert!(!ce.safe_to_retry);
}
```

**Step 2: Run test to verify it passes (these test existing functionality)**

Run: `cargo test --workspace -- test_pipeline_error_commit_state`
Expected: PASS (commit_state is already stored, we're just testing the accessor)

**Step 3: Fix dest-postgres to use AfterCommitUnknown for COMMIT failures**

In `connectors/dest-postgres/src/main.rs`, change the COMMIT error path (line 336-340) from:
```rust
Err(e) => {
    return make_err_response(
        ConnectorError::transient_db("COMMIT_FAILED", e)
            .with_commit_state(CommitState::BeforeCommit),
    );
}
```
to:
```rust
Err(e) => {
    return make_err_response(
        ConnectorError::transient_db("COMMIT_FAILED", e)
            .with_commit_state(CommitState::AfterCommitUnknown),
    );
}
```

This is correct per spec: when COMMIT fails, we don't know if the server-side commit actually succeeded (network timeout, connection lost). The response may have been lost even though the commit completed.

**Step 4: Add commit_state-aware logging to orchestrator retry loop**

In `crates/rapidbyte-core/src/engine/orchestrator.rs`, enhance the retry loop (`run_pipeline`) to log commit_state when present. Change the retry branch (lines 37-49) from:

```rust
Err(ref err) if err.is_retryable() && attempt <= max_retries => {
    if let Some(connector_err) = err.as_connector_error() {
        let delay = compute_backoff(connector_err, attempt);
        tracing::warn!(
            attempt,
            max_retries,
            delay_ms = delay.as_millis() as u64,
            category = %connector_err.category,
            code = %connector_err.code,
            "Retryable error, will retry"
        );
        std::thread::sleep(delay);
    }
    continue;
}
```

to:

```rust
Err(ref err) if err.is_retryable() && attempt <= max_retries => {
    if let Some(connector_err) = err.as_connector_error() {
        let delay = compute_backoff(connector_err, attempt);

        // Per spec § Delivery: log commit_state for observability
        if let Some(ref cs) = connector_err.commit_state {
            tracing::warn!(
                attempt,
                max_retries,
                delay_ms = delay.as_millis() as u64,
                category = %connector_err.category,
                code = %connector_err.code,
                commit_state = ?cs,
                safe_to_retry = connector_err.safe_to_retry,
                "Retryable error with commit_state, will retry"
            );
        } else {
            tracing::warn!(
                attempt,
                max_retries,
                delay_ms = delay.as_millis() as u64,
                category = %connector_err.category,
                code = %connector_err.code,
                "Retryable error, will retry"
            );
        }

        std::thread::sleep(delay);
    }
    continue;
}
```

Also add commit_state to the final error path (lines 52-60):

```rust
Err(err) => {
    if let Some(connector_err) = err.as_connector_error() {
        if err.is_retryable() {
            tracing::error!(
                attempt,
                max_retries,
                commit_state = ?connector_err.commit_state,
                "Max retries exhausted, failing pipeline"
            );
        } else if connector_err.commit_state.is_some() {
            tracing::error!(
                category = %connector_err.category,
                code = %connector_err.code,
                commit_state = ?connector_err.commit_state,
                "Non-retryable error with commit_state"
            );
        }
    } else if err.is_retryable() {
        tracing::error!(
            attempt,
            max_retries,
            "Max retries exhausted, failing pipeline"
        );
    }
    return Err(err);
}
```

**Step 5: Run tests**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 6: Build dest-postgres connector**

Run: `cd connectors/dest-postgres && cargo build --target wasm32-wasip1 --release`
Expected: Build succeeds

**Step 7: Commit**

```bash
git add crates/rapidbyte-core/src/engine/errors.rs \
       crates/rapidbyte-core/src/engine/orchestrator.rs \
       connectors/dest-postgres/src/main.rs
git commit -m "fix(core,dest): handle commit_state per spec — AfterCommitUnknown on COMMIT failure, commit_state-aware retry logging"
```

---

### Task 2: Add host_ffi::metric() calls to connectors per spec § Standard Metrics

**Files:**
- Modify: `connectors/source-postgres/src/source.rs:286-385`
- Modify: `connectors/dest-postgres/src/sink.rs:240-253`
- Test: Build both connectors successfully

Per spec § Standard Metrics, connectors must emit via host functions: `records_read`, `records_written`, `bytes_read`, `bytes_written`, `batches_emitted`, `batches_written`, `checkpoint_count`.

Currently, these values are tracked internally and returned in ReadSummary/WriteSummary at the end. Adding `host_ffi::metric()` calls enables real-time progress monitoring during long-running pipelines.

**Step 1: Add periodic metric emission to source-postgres**

In `connectors/source-postgres/src/source.rs`, after each successful `host_ffi::emit_batch()` call (there are two: the mid-loop flush at ~line 239 and the should_emit flush at ~line 303), emit metrics. Add a helper at the top of the `read_stream` function and call it after each emit:

Add this import at the top of the file:
```rust
use rapidbyte_sdk::protocol::{Metric, MetricValue};
```

Then after each successful `emit_batch` call, add metric emission. Specifically, after each block that increments `total_records` and `batches_emitted`, add:

```rust
// Emit real-time metrics per spec § Standard Metrics
let _ = host_ffi::metric("source-postgres", &ctx.stream_name, &Metric {
    name: "records_read".to_string(),
    value: MetricValue::Counter(total_records),
    labels: vec![],
});
let _ = host_ffi::metric("source-postgres", &ctx.stream_name, &Metric {
    name: "bytes_read".to_string(),
    value: MetricValue::Counter(total_bytes),
    labels: vec![],
});
```

To avoid excessive metric calls, only emit after each batch (not each row). There are two batch emission points — add the metric calls after both.

**Step 2: Add per-batch metric emission to dest-postgres**

In `connectors/dest-postgres/src/sink.rs`, in the `process_batch` method, after stats accumulation (line ~248, after `self.batches_written += 1;`), add:

```rust
// Emit real-time metrics per spec § Standard Metrics
let _ = host_ffi::metric("dest-postgres", &self.stream_name, &Metric {
    name: "records_written".to_string(),
    value: MetricValue::Counter(self.total_rows),
    labels: vec![],
});
let _ = host_ffi::metric("dest-postgres", &self.stream_name, &Metric {
    name: "bytes_written".to_string(),
    value: MetricValue::Counter(self.total_bytes),
    labels: vec![],
});
```

Add the import at the top of `sink.rs`:
```rust
use rapidbyte_sdk::protocol::{Metric, MetricValue};
```

**Step 3: Build both connectors**

Run:
```bash
cd connectors/source-postgres && cargo build --target wasm32-wasip1 --release
cd connectors/dest-postgres && cargo build --target wasm32-wasip1 --release
```
Expected: Both build successfully

**Step 4: Run host tests (ensure no SDK breakage)**

Run: `cargo test --workspace`
Expected: All pass

**Step 5: Commit**

```bash
git add connectors/source-postgres/src/source.rs \
       connectors/dest-postgres/src/sink.rs
git commit -m "feat(connectors): emit real-time metrics via host_ffi::metric() per spec § Standard Metrics"
```

---

### Task 3: Verification

**Step 1: Run all host tests**

Run: `cargo test --workspace`
Expected: All pass, no regressions

**Step 2: Run clippy**

Run: `cargo clippy --workspace -- -D warnings`
Expected: No warnings

**Step 3: Build both connectors**

Run:
```bash
cd connectors/source-postgres && cargo build --target wasm32-wasip1 --release
cd connectors/dest-postgres && cargo build --target wasm32-wasip1 --release
```
Expected: Both build cleanly

**Step 4: Verify spec compliance matrix**

After these changes, the full compliance status:

| Feature | Status |
|---------|--------|
| Feature::ExactlyOnce | Done (was already done) |
| Feature::BulkLoadCopy | Done (was already done) |
| WASI sandbox enforcement | Done (was already done) |
| after_commit_unknown | **Done** (Task 1) |
| host_metric() | **Done** (Task 2) |
