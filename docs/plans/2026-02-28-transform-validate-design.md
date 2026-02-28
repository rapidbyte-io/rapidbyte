# Transform Validate Design

## Goal

Implement a new `transform-validate` connector that enforces row-level data contracts in-flight on Arrow batches, with behavior controlled by pipeline `on_data_error` policy (`fail`, `skip`, `dlq`).

## Scope

### In scope (MVP)

- New connector crate at `connectors/transform-validate`
- Rule support:
  - `assert_not_null`
  - `assert_regex`
  - `assert_range`
  - `assert_unique` (per-batch scope)
- Rule config forms:
  - repeated entries
  - shorthand multi-field entries
  - map shorthand for regex rules
- Keep all failure reasons per row
- Policy-aware handling based on `StreamContext.policies.on_data_error`
- Unit tests + E2E coverage

### Out of scope (MVP)

- Stream-wide uniqueness across batches
- Per-rule severity overrides
- Alternate null semantics for regex/range
- Specialized metric schema for validation failures

## Architecture

Mirror existing transform connector structure from `transform-sql`:

- `connectors/transform-validate/src/main.rs`
  - `#[connector(transform)]`
  - `init`, `validate`, `transform`
- `connectors/transform-validate/src/config.rs`
  - `ConfigSchema`-derived types for rules and shorthand forms
- `connectors/transform-validate/src/transform.rs`
  - batch read/evaluate/split/emit loop

Connector protocol metadata will advertise `ProtocolVersion::V4` and default stream limits from SDK defaults.

## Config Model

Accepted YAML examples:

```yaml
transforms:
  - use: transform-validate
    config:
      rules:
        - assert_not_null: user_id
        - assert_not_null: [email, account_id]
        - assert_regex:
            email: "^.+@.+\\..+$"
            phone: "^\\+?[0-9-]+$"
        - assert_range: { field: age, min: 0, max: 150 }
        - assert_unique: [order_id, external_id]
```

Internal config representation will normalize shorthand into a flat validated rule list.

## Rule Semantics

- Missing field in schema: rule fails for all rows.
- `assert_not_null(field)`: null fails.
- `assert_regex(field, pattern)`: null fails; non-string values fail.
- `assert_range(field, min?, max?)`: null fails; non-numeric values fail; bounds are inclusive.
- `assert_unique(field)`: duplicate values within a single batch fail. Null is treated as a value, so duplicate nulls fail.

`assert_unique` is explicitly batch-scoped in MVP.

## Runtime Flow

Per input batch:

1. Read next batch from host (`ctx.next_batch`).
2. Evaluate all rules for each row and collect `Vec<RuleFailure>`.
3. Partition row indices into valid and invalid sets.
4. Emit filtered valid rows downstream via `ctx.emit_batch`.
5. Apply `on_data_error` policy to invalid rows:
   - `fail`: return `ConnectorError::data` immediately with summary.
   - `skip`: drop invalid rows and continue.
   - `dlq`: emit one DLQ record per invalid row, then continue.

For DLQ rows:

- `record_json`: full row as JSON
- `error_message`: concatenation of all failed rule reasons for the row
- `error_category`: `ErrorCategory::Data`

## Error Handling

- Config-time validation failures (`validate`) return `ValidationStatus::Failed` with actionable messages:
  - invalid regex
  - malformed rule object
  - invalid range bounds (`min > max`)
  - empty field names
  - empty `rules`
- Runtime validation failures are non-retryable data errors.

## Testing Strategy

### Unit tests

- Config parsing and shorthand normalization
- Rule evaluation correctness for each rule type
- Failure aggregation includes all reasons
- Duplicate semantics for `assert_unique` including null behavior

### E2E tests

- Add connector build to e2e harness
- Add validation pipeline scenario with mixed-validity data:
  - `fail` stops pipeline
  - `skip` writes only valid rows
  - `dlq` continues and writes valid rows

## Trade-offs

- Batch-scoped uniqueness keeps memory bounded and implementation simple.
- Collecting all row failures improves operator debugging at the cost of larger DLQ payloads.

## Follow-ups

- Optional `assert_unique` stream scope backed by bounded state
- Optional failure-cap config (for example `max_failures_per_row`)
- Dedicated validation metrics schema
