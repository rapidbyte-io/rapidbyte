# WIT Protocol v7 — Phase 1: Protocol Foundation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the v7 WIT protocol definition and Rust type foundation that all subsequent phases build on.

**Architecture:** Replace the v6 WIT file with v7, add new types to `rapidbyte-types`, update SDK traits and Context API. This phase produces a compilable foundation — the WIT source of truth, the Rust type representations, and the plugin-author-facing SDK interfaces. No runtime/codegen changes yet.

**Tech Stack:** WIT (WASI Component Model), Rust, serde, Arrow types

**Spec:** `docs/superpowers/specs/2026-03-20-wit-protocol-v7-design.md`

**Phase sequence:** This is Phase 1 of 4. Subsequent phases:
- Phase 2: Runtime & Codegen (bindings, host_state, proc macro)
- Phase 3: Orchestration (engine check/apply/run flows)
- Phase 4: Plugin Migration (rebuild all 3 plugins)

---

### Task 1: Update protocol version and feature flags in wire.rs

**Files:**
- Modify: `crates/rapidbyte-types/src/wire.rs`

- [ ] **Step 1: Write failing tests for V7 protocol version and new features**

Add to the existing test module in `wire.rs`:

```rust
#[test]
fn protocol_version_v7_is_current() {
    assert_eq!(ProtocolVersion::current(), ProtocolVersion::V7);
    assert_eq!(serde_json::to_string(&ProtocolVersion::V7).unwrap(), "\"7\"");
}

#[test]
fn v7_protocol_version_roundtrip() {
    let v: ProtocolVersion = serde_json::from_str("\"7\"").unwrap();
    assert_eq!(v, ProtocolVersion::V7);
}

#[test]
fn feature_multi_stream_serde() {
    let f = Feature::MultiStream;
    assert_eq!(serde_json::to_string(&f).unwrap(), "\"multi_stream\"");
    let back: Feature = serde_json::from_str("\"multi_stream\"").unwrap();
    assert_eq!(back, Feature::MultiStream);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-types -- wire::tests`
Expected: FAIL — `V7` and `MultiStream` not defined

- [ ] **Step 3: Add V7 variant and new features**

In `wire.rs`, update `ProtocolVersion`:

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolVersion {
    #[serde(rename = "5")]
    V5,
    #[serde(rename = "6")]
    V6,
    #[default]
    #[serde(rename = "7")]
    V7,
}

impl ProtocolVersion {
    #[must_use]
    pub const fn current() -> Self {
        Self::V7
    }
}
```

Update `Feature` enum — add new v7 features:

```rust
pub enum Feature {
    Cdc,
    Stateful,
    ExactlyOnce,
    SchemaAutoMigrate,
    #[serde(alias = "bulk_load_copy")]
    BulkLoad,
    PartitionedRead,
    /// Source can handle multiple streams in a single run invocation.
    MultiStream,
    /// Multi-stream CDC: one replication slot, many tables.
    MultiStreamCdc,
}
```

- [ ] **Step 4: Run tests — new tests should pass, old V6-as-current test will fail**

Run: `cargo test -p rapidbyte-types -- wire::tests`
Expected: New V7 tests PASS, `protocol_version_default_is_v6` FAILS (expected — fixing next)

- [ ] **Step 5: Fix the old V6-as-current test**

The test `protocol_version_default_is_v6` now fails because V7 is current. Update it:

```rust
#[test]
fn protocol_version_default_is_v7() {
    let v = ProtocolVersion::default();
    assert_eq!(v, ProtocolVersion::current());
    assert_eq!(serde_json::to_string(&v).unwrap(), "\"7\"");
}
```

- [ ] **Step 6: Run full types crate test suite**

Run: `cargo test -p rapidbyte-types`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add crates/rapidbyte-types/src/wire.rs
git commit -m "feat(types): add protocol V7 and multi-stream feature flags"
```

---

### Task 2: Add v7 schema types (SchemaField, StreamSchema, FieldConstraint)

**Files:**
- Create: `crates/rapidbyte-types/src/schema.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs`

- [ ] **Step 1: Write failing tests for new schema types**

Create `crates/rapidbyte-types/src/schema.rs` with tests only:

```rust
//! Schema negotiation types for v7 protocol.
//!
//! [`StreamSchema`] describes the shape of data a plugin produces or consumes.
//! [`FieldRequirement`] lets destinations declare per-field constraints for
//! host-side schema reconciliation at check time.

use serde::{Deserialize, Serialize};

// Types will go here

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_field_roundtrip() {
        let field = SchemaField {
            name: "id".into(),
            arrow_type: "int64".into(),
            nullable: false,
            is_primary_key: true,
            is_generated: false,
            is_partition_key: false,
            default_value: None,
        };
        let json = serde_json::to_string(&field).unwrap();
        let back: SchemaField = serde_json::from_str(&json).unwrap();
        assert_eq!(field, back);
    }

    #[test]
    fn stream_schema_with_primary_key() {
        let schema = StreamSchema {
            fields: vec![
                SchemaField::new("id", "int64", false).with_primary_key(true),
                SchemaField::new("name", "utf8", true),
            ],
            primary_key: vec!["id".into()],
            partition_keys: vec![],
            source_defined_cursor: Some("id".into()),
            schema_id: None,
        };
        assert_eq!(schema.primary_key, vec!["id"]);
        assert_eq!(schema.fields[0].is_primary_key, true);
    }

    #[test]
    fn field_constraint_serde() {
        let c = FieldConstraint::FieldRequired;
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(json, "\"field_required\"");
    }

    #[test]
    fn field_requirement_roundtrip() {
        let req = FieldRequirement {
            field_name: "email".into(),
            constraint: FieldConstraint::FieldForbidden,
            reason: "reserved column name".into(),
            accepted_types: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: FieldRequirement = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn field_requirement_with_accepted_types() {
        let req = FieldRequirement {
            field_name: "amount".into(),
            constraint: FieldConstraint::TypeIncompatible,
            reason: "float not supported".into(),
            accepted_types: Some(vec!["int64".into(), "utf8".into()]),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["accepted_types"], serde_json::json!(["int64", "utf8"]));
    }

    #[test]
    fn schema_field_builder_chain() {
        let f = SchemaField::new("ts", "timestamp_micros", false)
            .with_primary_key(false)
            .with_generated(true);
        assert!(f.is_generated);
        assert!(!f.is_primary_key);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-types -- schema::tests`
Expected: FAIL — types not defined

- [ ] **Step 3: Implement schema types**

Add the type definitions above the test module in `schema.rs`:

```rust
use serde::{Deserialize, Serialize};

/// Per-field constraint declared by a destination during schema negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldConstraint {
    /// Destination must receive this field.
    FieldRequired,
    /// Destination can accept this field.
    FieldOptional,
    /// Destination cannot handle this field.
    FieldForbidden,
    /// Not required but improves performance (e.g., sort key).
    FieldRecommended,
    /// Field exists but type cannot be reconciled.
    TypeIncompatible,
}

/// A destination's requirement for a specific field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldRequirement {
    /// Name of the field this requirement applies to.
    pub field_name: String,
    /// Constraint type.
    pub constraint: FieldConstraint,
    /// Human-readable explanation of the constraint.
    pub reason: String,
    /// Arrow type names the destination can accept for this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_types: Option<Vec<String>>,
}

/// A single field in a stream schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
    /// Column name.
    pub name: String,
    /// Arrow canonical type name (e.g., "int64", "utf8", "timestamp_micros").
    pub arrow_type: String,
    /// Whether the column permits null values.
    pub nullable: bool,
    /// Whether this field is part of the primary key.
    #[serde(default)]
    pub is_primary_key: bool,
    /// Whether this is a generated/computed column (read-only).
    #[serde(default)]
    pub is_generated: bool,
    /// Whether this field is a partition key.
    #[serde(default)]
    pub is_partition_key: bool,
    /// Default value expression (if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
}

impl SchemaField {
    /// Create a new schema field with required properties.
    #[must_use]
    pub fn new(name: &str, arrow_type: &str, nullable: bool) -> Self {
        Self {
            name: name.to_string(),
            arrow_type: arrow_type.to_string(),
            nullable,
            is_primary_key: false,
            is_generated: false,
            is_partition_key: false,
            default_value: None,
        }
    }

    /// Mark this field as a primary key component.
    #[must_use]
    pub fn with_primary_key(mut self, is_pk: bool) -> Self {
        self.is_primary_key = is_pk;
        self
    }

    /// Mark this field as generated/computed.
    #[must_use]
    pub fn with_generated(mut self, is_generated: bool) -> Self {
        self.is_generated = is_generated;
        self
    }
}

/// Complete schema for a stream, with metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSchema {
    /// Ordered list of fields.
    pub fields: Vec<SchemaField>,
    /// Primary key column names (ordered).
    #[serde(default)]
    pub primary_key: Vec<String>,
    /// Partition key column names.
    #[serde(default)]
    pub partition_keys: Vec<String>,
    /// Source-defined cursor column for incremental sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_defined_cursor: Option<String>,
    /// Fingerprint for change detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<String>,
}
```

- [ ] **Step 4: Register module in lib.rs**

Add `pub mod schema;` to `crates/rapidbyte-types/src/lib.rs` and add schema types to the prelude:

```rust
pub use crate::schema::{FieldConstraint, FieldRequirement, SchemaField, StreamSchema};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p rapidbyte-types -- schema::tests`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-types/src/schema.rs crates/rapidbyte-types/src/lib.rs
git commit -m "feat(types): add v7 schema negotiation types"
```

---

### Task 3: Add v7 discovery types (DiscoveredStream, PluginSpec)

**Files:**
- Create: `crates/rapidbyte-types/src/discovery.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Create `crates/rapidbyte-types/src/discovery.rs` with tests for `DiscoveredStream` and `PluginSpec`:

```rust
//! Discovery and spec types for v7 protocol.
//!
//! [`DiscoveredStream`] is returned by source plugins during discovery.
//! [`PluginSpec`] is returned by the sessionless `spec()` export.

use crate::schema::StreamSchema;
use crate::wire::{SyncMode, WriteMode};
use serde::{Deserialize, Serialize};

// Types will go here

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaField;

    #[test]
    fn discovered_stream_roundtrip() {
        let ds = DiscoveredStream {
            name: "public.users".into(),
            schema: StreamSchema {
                fields: vec![SchemaField::new("id", "int64", false).with_primary_key(true)],
                primary_key: vec!["id".into()],
                partition_keys: vec![],
                source_defined_cursor: Some("id".into()),
                schema_id: None,
            },
            supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Incremental],
            default_cursor_field: Some("id".into()),
            estimated_row_count: Some(50_000),
            metadata_json: None,
        };
        let json = serde_json::to_string(&ds).unwrap();
        let back: DiscoveredStream = serde_json::from_str(&json).unwrap();
        assert_eq!(ds, back);
    }

    #[test]
    fn discovered_stream_optional_fields_skipped() {
        let ds = DiscoveredStream {
            name: "events".into(),
            schema: StreamSchema {
                fields: vec![],
                primary_key: vec![],
                partition_keys: vec![],
                source_defined_cursor: None,
                schema_id: None,
            },
            supported_sync_modes: vec![SyncMode::FullRefresh],
            default_cursor_field: None,
            estimated_row_count: None,
            metadata_json: None,
        };
        let json = serde_json::to_value(&ds).unwrap();
        assert!(json.get("estimated_row_count").is_none());
        assert!(json.get("metadata_json").is_none());
    }

    #[test]
    fn plugin_spec_source_roundtrip() {
        let spec = PluginSpec {
            protocol_version: 7,
            config_schema_json: r#"{"type":"object"}"#.into(),
            resource_schema_json: None,
            documentation_url: Some("https://docs.example.com".into()),
            features: vec!["cdc".into(), "multi_stream".into()],
            supported_sync_modes: vec![SyncMode::FullRefresh, SyncMode::Cdc],
            supported_write_modes: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: PluginSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn plugin_spec_destination_with_write_modes() {
        let spec = PluginSpec {
            protocol_version: 7,
            config_schema_json: r#"{"type":"object"}"#.into(),
            resource_schema_json: None,
            documentation_url: None,
            features: vec!["bulk_load".into()],
            supported_sync_modes: vec![],
            supported_write_modes: Some(vec![WriteMode::Append, WriteMode::Upsert {
                primary_key: vec!["id".into()],
            }]),
        };
        let json = serde_json::to_value(&spec).unwrap();
        assert!(json["supported_write_modes"].is_array());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-types -- discovery::tests`
Expected: FAIL — types not defined

- [ ] **Step 3: Implement DiscoveredStream and PluginSpec**

Add above the test module:

```rust
/// A stream discovered by a source plugin with rich metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredStream {
    /// Fully-qualified stream name (e.g., "public.users").
    pub name: String,
    /// Full schema with typed fields, PKs, partition keys.
    pub schema: StreamSchema,
    /// Sync modes this stream supports.
    pub supported_sync_modes: Vec<SyncMode>,
    /// Recommended cursor field for incremental sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_cursor_field: Option<String>,
    /// Estimated number of rows (from pg_stat / equivalent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_row_count: Option<u64>,
    /// Plugin-specific metadata as JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_json: Option<String>,
}

/// Plugin self-description returned by the sessionless `spec()` export.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginSpec {
    /// Protocol version (must be 7).
    pub protocol_version: u32,
    /// JSON Schema for plugin configuration.
    pub config_schema_json: String,
    /// JSON Schema for per-stream resource configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_schema_json: Option<String>,
    /// URL to plugin documentation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation_url: Option<String>,
    /// Feature flag names (e.g., "cdc", "multi_stream").
    #[serde(default)]
    pub features: Vec<String>,
    /// Supported sync modes (source/transform).
    #[serde(default)]
    pub supported_sync_modes: Vec<SyncMode>,
    /// Supported write modes (destination only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported_write_modes: Option<Vec<WriteMode>>,
}
```

- [ ] **Step 4: Register module in lib.rs and add to prelude**

```rust
pub mod discovery;
```

Prelude additions:
```rust
pub use crate::discovery::{DiscoveredStream, PluginSpec};
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p rapidbyte-types -- discovery::tests`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-types/src/discovery.rs crates/rapidbyte-types/src/lib.rs
git commit -m "feat(types): add v7 discovery and plugin spec types"
```

---

### Task 4: Add v7 validation types (ValidationReport, PrerequisitesReport)

**Files:**
- Create: `crates/rapidbyte-types/src/validation.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Create `crates/rapidbyte-types/src/validation.rs`:

```rust
//! Validation and prerequisites types for v7 protocol.
//!
//! [`ValidationReport`] is the v7 replacement for `ValidationResult`,
//! carrying optional output schema and field requirements for schema negotiation.
//! [`PrerequisitesReport`] carries pre-flight check results with fix hints.

use crate::schema::{FieldRequirement, StreamSchema};
use serde::{Deserialize, Serialize};

// Types will go here

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldConstraint, FieldRequirement, SchemaField, StreamSchema};

    #[test]
    fn validation_report_success() {
        let r = ValidationReport::success("Connected to db");
        assert_eq!(r.status, ValidationStatus::Success);
        assert!(r.output_schema.is_none());
        assert!(r.field_requirements.is_none());
    }

    #[test]
    fn validation_report_with_output_schema() {
        let schema = StreamSchema {
            fields: vec![SchemaField::new("id", "int64", false)],
            primary_key: vec!["id".into()],
            partition_keys: vec![],
            source_defined_cursor: None,
            schema_id: None,
        };
        let r = ValidationReport::success("OK").with_output_schema(schema.clone());
        assert_eq!(r.output_schema.unwrap(), schema);
    }

    #[test]
    fn validation_report_with_field_requirements() {
        let reqs = vec![FieldRequirement {
            field_name: "id".into(),
            constraint: FieldConstraint::FieldRequired,
            reason: "primary key".into(),
            accepted_types: None,
        }];
        let r = ValidationReport::success("OK").with_field_requirements(reqs.clone());
        assert_eq!(r.field_requirements.unwrap().len(), 1);
    }

    #[test]
    fn prerequisites_report_all_passed() {
        let r = PrerequisitesReport::from_checks(vec![
            PrerequisiteCheck::passed("wal_level", "wal_level is logical"),
            PrerequisiteCheck::passed("replication_role", "user has REPLICATION"),
        ]);
        assert!(r.passed);
        assert_eq!(r.checks.len(), 2);
    }

    #[test]
    fn prerequisites_report_with_failure() {
        let r = PrerequisitesReport::from_checks(vec![
            PrerequisiteCheck::passed("version", "PostgreSQL 15.2"),
            PrerequisiteCheck::error(
                "wal_level",
                "wal_level is 'replica', expected 'logical'",
            )
            .with_fix_hint("ALTER SYSTEM SET wal_level = logical; -- then restart"),
        ]);
        assert!(!r.passed);
        assert!(!r.checks[1].passed);
        assert!(r.checks[1].fix_hint.is_some());
    }

    #[test]
    fn prerequisites_report_empty_passes() {
        let r = PrerequisitesReport::passed();
        assert!(r.passed);
        assert!(r.checks.is_empty());
    }

    #[test]
    fn validation_report_roundtrip() {
        let r = ValidationReport {
            status: ValidationStatus::Warning,
            message: "deprecated config".into(),
            warnings: vec!["use 'host' instead of 'hostname'".into()],
            output_schema: None,
            field_requirements: None,
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: ValidationReport = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-types -- validation::tests`
Expected: FAIL

- [ ] **Step 3: Implement validation types**

Add above the test module:

```rust
/// Validation status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    Success,
    Failed,
    Warning,
}

/// Result of a plugin's `validate()` call in v7.
///
/// Carries optional schema information for pipeline-wide negotiation:
/// - `output_schema`: what this stage produces (meaningful for sources, transforms)
/// - `field_requirements`: what this stage needs (meaningful for destinations)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationReport {
    pub status: ValidationStatus,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<StreamSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_requirements: Option<Vec<FieldRequirement>>,
}

impl ValidationReport {
    #[must_use]
    pub fn success(message: &str) -> Self {
        Self {
            status: ValidationStatus::Success,
            message: message.to_string(),
            warnings: Vec::new(),
            output_schema: None,
            field_requirements: None,
        }
    }

    #[must_use]
    pub fn failed(message: &str) -> Self {
        Self {
            status: ValidationStatus::Failed,
            message: message.to_string(),
            warnings: Vec::new(),
            output_schema: None,
            field_requirements: None,
        }
    }

    #[must_use]
    pub fn with_output_schema(mut self, schema: StreamSchema) -> Self {
        self.output_schema = Some(schema);
        self
    }

    #[must_use]
    pub fn with_field_requirements(mut self, reqs: Vec<FieldRequirement>) -> Self {
        self.field_requirements = Some(reqs);
        self
    }
}

/// Severity of a prerequisite check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrerequisiteSeverity {
    Error,
    Warning,
    Info,
}

/// A single pre-flight check result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrerequisiteCheck {
    pub name: String,
    pub passed: bool,
    pub severity: PrerequisiteSeverity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fix_hint: Option<String>,
}

impl PrerequisiteCheck {
    #[must_use]
    pub fn passed(name: &str, message: &str) -> Self {
        Self {
            name: name.to_string(),
            passed: true,
            severity: PrerequisiteSeverity::Info,
            message: message.to_string(),
            fix_hint: None,
        }
    }

    #[must_use]
    pub fn error(name: &str, message: &str) -> Self {
        Self {
            name: name.to_string(),
            passed: false,
            severity: PrerequisiteSeverity::Error,
            message: message.to_string(),
            fix_hint: None,
        }
    }

    #[must_use]
    pub fn with_fix_hint(mut self, hint: &str) -> Self {
        self.fix_hint = Some(hint.to_string());
        self
    }
}

/// Aggregate result of all pre-flight checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrerequisitesReport {
    pub passed: bool,
    pub checks: Vec<PrerequisiteCheck>,
}

impl PrerequisitesReport {
    /// Create a report that passes with no checks.
    #[must_use]
    pub fn passed() -> Self {
        Self {
            passed: true,
            checks: Vec::new(),
        }
    }

    /// Create a report from a list of checks.
    /// `passed` is true only if all severity=error checks passed.
    #[must_use]
    pub fn from_checks(checks: Vec<PrerequisiteCheck>) -> Self {
        let passed = checks
            .iter()
            .filter(|c| c.severity == PrerequisiteSeverity::Error)
            .all(|c| c.passed);
        Self { passed, checks }
    }
}
```

- [ ] **Step 4: Register module in lib.rs and add to prelude**

```rust
pub mod validation;
```

Prelude additions:
```rust
pub use crate::validation::{
    PrerequisiteCheck, PrerequisiteSeverity, PrerequisitesReport, ValidationReport,
    ValidationStatus,
};
```

Note: remove the old `ValidationResult` and `ValidationStatus` from the `error.rs` prelude re-export — these are now in `validation.rs`. Keep `ValidationResult` in `error.rs` as a deprecated alias if needed for compilation, or remove it entirely since this is a clean break.

- [ ] **Step 5: Run tests**

Run: `cargo test -p rapidbyte-types -- validation::tests`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-types/src/validation.rs crates/rapidbyte-types/src/lib.rs
git commit -m "feat(types): add v7 validation and prerequisites types"
```

---

### Task 5: Add v7 apply and teardown types

**Files:**
- Create: `crates/rapidbyte-types/src/lifecycle.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Create `crates/rapidbyte-types/src/lifecycle.rs`:

```rust
//! Apply and teardown lifecycle types for v7 protocol.

use crate::stream::StreamContext;
use serde::{Deserialize, Serialize};

// Types will go here

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_report_noop() {
        let r = ApplyReport::noop();
        assert!(r.actions.is_empty());
    }

    #[test]
    fn apply_action_with_ddl() {
        let action = ApplyAction {
            stream_name: "public.users".into(),
            description: "Created table public.users".into(),
            ddl_executed: Some("CREATE TABLE public.users (id BIGINT PRIMARY KEY)".into()),
        };
        let json = serde_json::to_value(&action).unwrap();
        assert!(json["ddl_executed"].is_string());
    }

    #[test]
    fn apply_request_dry_run() {
        let req = ApplyRequest {
            streams: vec![],
            dry_run: true,
        };
        assert!(req.dry_run);
    }

    #[test]
    fn teardown_request_roundtrip() {
        let req = TeardownRequest {
            streams: vec!["users".into(), "orders".into()],
            reason: "pipeline_deleted".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: TeardownRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn teardown_report_noop() {
        let r = TeardownReport::noop();
        assert!(r.actions.is_empty());
    }

    #[test]
    fn teardown_report_with_actions() {
        let r = TeardownReport {
            actions: vec!["Dropped replication slot rapidbyte_users".into()],
        };
        assert_eq!(r.actions.len(), 1);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-types -- lifecycle::tests`
Expected: FAIL

- [ ] **Step 3: Implement apply and teardown types**

```rust
/// Request to apply DDL/setup before data flows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApplyRequest {
    /// Streams to prepare.
    pub streams: Vec<StreamContext>,
    /// If true, show what would be done without executing.
    pub dry_run: bool,
}

/// A single DDL action taken during apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyAction {
    /// Stream this action applies to.
    pub stream_name: String,
    /// Human-readable description.
    pub description: String,
    /// Actual DDL statement executed (for audit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ddl_executed: Option<String>,
}

/// Result of an apply operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyReport {
    pub actions: Vec<ApplyAction>,
}

impl ApplyReport {
    #[must_use]
    pub fn noop() -> Self {
        Self {
            actions: Vec::new(),
        }
    }
}

/// Request to tear down persistent resources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeardownRequest {
    /// Stream names to clean up.
    pub streams: Vec<String>,
    /// Reason for teardown: "pipeline_deleted", "reconfigure", "stream_removed".
    pub reason: String,
}

/// Result of a teardown operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeardownReport {
    pub actions: Vec<String>,
}

impl TeardownReport {
    #[must_use]
    pub fn noop() -> Self {
        Self {
            actions: Vec::new(),
        }
    }
}
```

- [ ] **Step 4: Register module in lib.rs and add to prelude**

- [ ] **Step 5: Run tests**

Run: `cargo test -p rapidbyte-types -- lifecycle::tests`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-types/src/lifecycle.rs crates/rapidbyte-types/src/lib.rs
git commit -m "feat(types): add v7 apply and teardown lifecycle types"
```

---

### Task 6: Add v7 batch metadata and run result types

**Files:**
- Create: `crates/rapidbyte-types/src/batch.rs`
- Create: `crates/rapidbyte-types/src/run.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs`

- [ ] **Step 1: Write failing tests for BatchMetadata**

Create `crates/rapidbyte-types/src/batch.rs`:

```rust
//! Batch metadata for v7 frame transport.

use serde::{Deserialize, Serialize};

// Types will go here

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_metadata_roundtrip() {
        let m = BatchMetadata {
            stream_index: 0,
            schema_fingerprint: Some("abc123".into()),
            sequence_number: 42,
            compression: Some("lz4".into()),
            record_count: 1000,
            byte_count: 65536,
        };
        let json = serde_json::to_string(&m).unwrap();
        let back: BatchMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn batch_metadata_minimal() {
        let m = BatchMetadata {
            stream_index: 0,
            schema_fingerprint: None,
            sequence_number: 0,
            compression: None,
            record_count: 0,
            byte_count: 0,
        };
        let json = serde_json::to_value(&m).unwrap();
        assert!(json.get("schema_fingerprint").is_none());
    }
}
```

- [ ] **Step 2: Write failing tests for RunSummary / StreamResult**

Create `crates/rapidbyte-types/src/run.rs`:

```rust
//! Run request and per-stream result types for v7 protocol.

use crate::stream::StreamContext;
use serde::{Deserialize, Serialize};

// Types will go here

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_result_success() {
        let r = StreamResult {
            stream_index: 0,
            stream_name: "users".into(),
            outcome_json: r#"{"records_read":1000}"#.into(),
            succeeded: true,
        };
        assert!(r.succeeded);
    }

    #[test]
    fn stream_result_failure() {
        let r = StreamResult {
            stream_index: 2,
            stream_name: "payments".into(),
            outcome_json: r#"{"code":"SCHEMA_MISMATCH"}"#.into(),
            succeeded: false,
        };
        assert!(!r.succeeded);
    }

    #[test]
    fn run_summary_mixed_results() {
        let summary = RunSummary {
            results: vec![
                StreamResult {
                    stream_index: 0,
                    stream_name: "users".into(),
                    outcome_json: "{}".into(),
                    succeeded: true,
                },
                StreamResult {
                    stream_index: 1,
                    stream_name: "orders".into(),
                    outcome_json: "{}".into(),
                    succeeded: false,
                },
            ],
        };
        assert_eq!(summary.succeeded_count(), 1);
        assert_eq!(summary.failed_count(), 1);
    }

    #[test]
    fn run_request_single_stream() {
        let req = RunRequest {
            streams: vec![],
            dry_run: false,
        };
        assert!(!req.dry_run);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-types -- batch::tests run::tests`
Expected: FAIL

- [ ] **Step 4: Implement BatchMetadata**

In `batch.rs`:

```rust
/// Metadata attached to every emitted batch in v7.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchMetadata {
    /// Which stream this batch belongs to (index into RunRequest.streams).
    pub stream_index: u32,
    /// Schema fingerprint for change detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_fingerprint: Option<String>,
    /// Monotonic sequence number per stream.
    pub sequence_number: u64,
    /// Compression codec applied ("lz4", "zstd", or None).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    /// Number of records in this batch.
    pub record_count: u32,
    /// Total byte size of the batch payload.
    pub byte_count: u64,
}
```

- [ ] **Step 5: Implement RunRequest, StreamResult, RunSummary**

In `run.rs`:

```rust
/// Request to execute a pipeline run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRequest {
    /// Streams to process (1 for single-stream, N for multi-stream).
    pub streams: Vec<StreamContext>,
    /// If true, validate without processing data.
    pub dry_run: bool,
}

/// Outcome for a single stream within a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamResult {
    /// Index into the RunRequest.streams list.
    pub stream_index: u32,
    /// Stream name.
    pub stream_name: String,
    /// JSON-encoded ReadSummary/WriteSummary/TransformSummary or error.
    pub outcome_json: String,
    /// Whether this stream completed successfully.
    pub succeeded: bool,
}

/// Aggregate result of a run across all streams.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSummary {
    pub results: Vec<StreamResult>,
}

impl RunSummary {
    /// Count of streams that succeeded.
    #[must_use]
    pub fn succeeded_count(&self) -> usize {
        self.results.iter().filter(|r| r.succeeded).count()
    }

    /// Count of streams that failed.
    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.results.iter().filter(|r| !r.succeeded).count()
    }
}
```

- [ ] **Step 6: Register modules in lib.rs and add to prelude**

- [ ] **Step 7: Run tests**

Run: `cargo test -p rapidbyte-types -- batch::tests run::tests`
Expected: ALL PASS

- [ ] **Step 8: Commit**

```bash
git add crates/rapidbyte-types/src/batch.rs crates/rapidbyte-types/src/run.rs crates/rapidbyte-types/src/lib.rs
git commit -m "feat(types): add v7 batch metadata and run result types"
```

---

### Task 7: Update StreamContext to v7 shape and resolve ValidationStatus collision

**Files:**
- Modify: `crates/rapidbyte-types/src/stream.rs`
- Modify: `crates/rapidbyte-types/src/error.rs`
- Modify: `crates/rapidbyte-types/src/lib.rs`

This task addresses two critical issues:
1. `StreamContext` must use the new `StreamSchema` instead of `SchemaHint` and gain a `stream_index` field
2. The old `ValidationStatus` / `ValidationResult` in `error.rs` conflicts with the new `ValidationStatus` / `ValidationReport` in `validation.rs`

- [ ] **Step 1: Write failing test for stream_index on StreamContext**

Add to existing test module in `stream.rs`:

```rust
#[test]
fn stream_context_has_stream_index() {
    let ctx = StreamContext {
        stream_index: 3,
        stream_name: "users".into(),
        // ... (use existing test construction pattern with remaining fields)
    };
    assert_eq!(ctx.stream_index, 3);
}
```

- [ ] **Step 2: Update StreamContext**

In `stream.rs`:
- Add `pub stream_index: u32` as the first field in `StreamContext`
- Change `schema: SchemaHint` to `schema: StreamSchema` (import from `crate::schema::StreamSchema`)
- Update `PartitionStrategy::Mod` to `PartitionStrategy::ModHash` to match the WIT spec's `mod-hash`
- Update `CursorInfo` import or note: the `last_value: Option<CursorValue>` field stays as the rich typed enum for SDK ergonomics. The WIT uses `last-value-json: option<string>` — the Phase 2 bindings layer will handle the JSON serialization bridge. Document this with a comment.
- Update all existing test constructions in `stream.rs` to include the new `stream_index` field and use `StreamSchema` instead of `SchemaHint`

- [ ] **Step 3: Remove old ValidationStatus and ValidationResult from error.rs**

In `error.rs`:
- Delete the `ValidationStatus` enum (now lives in `validation.rs`)
- Delete the `ValidationResult` struct (replaced by `ValidationReport` in `validation.rs`)
- Remove their re-exports from the prelude in `lib.rs`
- Keep all other error types (`PluginError`, `ErrorCategory`, etc.) unchanged

- [ ] **Step 4: Update prelude in lib.rs**

Replace old imports:
```rust
// Remove:
pub use crate::error::{ValidationResult, ValidationStatus};
// Already added in Task 4:
pub use crate::validation::{ValidationReport, ValidationStatus};
```

Also update the `SchemaHint` re-export — remove it from the prelude if `StreamSchema` is now the primary type:
```rust
// Remove:
pub use crate::catalog::SchemaHint;
// The new prelude uses:
pub use crate::schema::StreamSchema;
```

- [ ] **Step 5: Fix compilation errors throughout types crate**

Other files in the crate that import `ValidationResult`, `ValidationStatus`, or `SchemaHint` will need updating. Run `cargo check -p rapidbyte-types` and fix imports iteratively.

- [ ] **Step 6: Run full test suite**

Run: `cargo test -p rapidbyte-types`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add crates/rapidbyte-types/
git commit -m "feat(types): update StreamContext to v7 shape, resolve ValidationStatus collision"
```

**Design notes for executing agent:**
- `PluginSpec.features` uses `Vec<String>` intentionally — this matches the WIT `list<string>` wire format and allows new features to be added without enum changes. The typed `Feature` enum in `wire.rs` is for compile-time validation in the manifest; `PluginSpec` is for runtime introspection.
- `CursorInfo.last_value` stays as `Option<CursorValue>` (not `Option<String>`) for SDK ergonomics. The Phase 2 bindings layer serializes to/from JSON at the WIT boundary.
- `CheckpointKind::Dest` vs WIT's `destination` naming will be reconciled in Phase 2 bindings.
- `PartitionStrategy::Mod` is renamed to `PartitionStrategy::ModHash` to match the WIT spec's `mod-hash`.

---

### Task 8: Add v7 transactional checkpoint types (CursorUpdate, StateMutation)

**Files:**
- Modify: `crates/rapidbyte-types/src/checkpoint.rs`

- [ ] **Step 1: Write failing tests for new checkpoint types**

Add to existing test module in `checkpoint.rs`:

```rust
#[test]
fn cursor_update_roundtrip() {
    let cu = CursorUpdate {
        stream_name: "users".into(),
        cursor_field: "id".into(),
        cursor_value_json: "5000".into(),
    };
    let json = serde_json::to_string(&cu).unwrap();
    let back: CursorUpdate = serde_json::from_str(&json).unwrap();
    assert_eq!(cu, back);
}

#[test]
fn state_mutation_roundtrip() {
    let sm = StateMutation {
        scope: StateScope::Stream,
        key: "last_xid".into(),
        value: "42".into(),
    };
    let json = serde_json::to_string(&sm).unwrap();
    let back: StateMutation = serde_json::from_str(&json).unwrap();
    assert_eq!(sm, back);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p rapidbyte-types -- checkpoint::tests`
Expected: FAIL — `CursorUpdate` and `StateMutation` not defined

- [ ] **Step 3: Add CursorUpdate and StateMutation**

Add to `checkpoint.rs`:

```rust
/// Cursor position update within a transactional checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorUpdate {
    /// Stream this cursor update applies to.
    pub stream_name: String,
    /// Cursor column name.
    pub cursor_field: String,
    /// JSON-encoded cursor value.
    pub cursor_value_json: String,
}

/// State mutation within a transactional checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateMutation {
    /// State scope (Pipeline, Stream, PluginInstance).
    pub scope: StateScope,
    /// State key.
    pub key: String,
    /// State value.
    pub value: String,
}
```

- [ ] **Step 4: Add to prelude**

```rust
pub use crate::checkpoint::{CursorUpdate, StateMutation};
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p rapidbyte-types -- checkpoint::tests`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add crates/rapidbyte-types/src/checkpoint.rs crates/rapidbyte-types/src/lib.rs
git commit -m "feat(types): add v7 transactional checkpoint types"
```

---

### Task 9: Add cancelled error category

**Files:**
- Modify: `crates/rapidbyte-types/src/error.rs`

- [ ] **Step 1: Write failing test**

Add to test module in `error.rs`:

```rust
#[test]
fn cancelled_error_category() {
    let err = PluginError::cancelled("USER_CANCEL", "Pipeline cancelled by user");
    assert_eq!(err.category, ErrorCategory::Cancelled);
    assert!(!err.retryable);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rapidbyte-types -- error::tests::cancelled`
Expected: FAIL

- [ ] **Step 3: Add Cancelled variant and constructor**

Add `Cancelled` to `ErrorCategory` enum:

```rust
/// Cooperative cancellation (plugin detected is_cancelled).
Cancelled,
```

Add constructor to `PluginError` impl:

```rust
/// Create a cancellation error.
#[must_use]
pub fn cancelled(code: &str, message: impl Into<String>) -> Self {
    Self {
        category: ErrorCategory::Cancelled,
        scope: ErrorScope::Stream,
        code: code.to_string(),
        message: message.into(),
        retryable: false,
        retry_after_ms: None,
        backoff_class: BackoffClass::Normal,
        safe_to_retry: false,
        commit_state: None,
        details: None,
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p rapidbyte-types -- error::tests`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add crates/rapidbyte-types/src/error.rs
git commit -m "feat(types): add Cancelled error category for v7 cooperative cancellation"
```

---

### Task 10: Write the v7 WIT file

**Files:**
- Modify: `wit/rapidbyte-plugin.wit`

- [ ] **Step 1: Replace the WIT file with v7 protocol**

Replace the entire contents of `wit/rapidbyte-plugin.wit` with the v7 WIT protocol definition from the spec document (`docs/superpowers/specs/2026-03-20-wit-protocol-v7-design.md`, sections "Types Interface", "Host Imports", "Plugin Exports", "Worlds").

Copy verbatim from the spec — the types interface, host interface, source/destination/transform interfaces, and world definitions.

- [ ] **Step 2: Verify WIT syntax**

Run: `wasm-tools component wit wit/rapidbyte-plugin.wit 2>&1 || echo "WIT validation not available — manual review"`

If `wasm-tools` is installed, it validates WIT syntax. If not, manually verify the file matches the spec.

- [ ] **Step 3: Commit**

```bash
git add wit/rapidbyte-plugin.wit
git commit -m "feat(wit): replace v6 protocol with v7 WIT definition"
```

---

### Task 11: Run full types crate test suite and fix any compilation issues

**Files:**
- Modify: `crates/rapidbyte-types/src/lib.rs` (prelude cleanup)
- Possibly modify: any file with compilation errors from the v6→v7 type changes

- [ ] **Step 1: Run full test suite**

Run: `cargo test -p rapidbyte-types`

- [ ] **Step 2: Fix any compilation errors**

Common issues:
- Old `ValidationResult` / `ValidationStatus` imports from `error.rs` may conflict with new `validation.rs` exports
- If `ValidationResult` was used elsewhere, update imports to use the new `ValidationReport`
- Ensure the prelude doesn't re-export conflicting names

- [ ] **Step 3: Run workspace-wide check (types only — runtime/engine will break, that's expected)**

Run: `cargo check -p rapidbyte-types`
Expected: PASS (types crate has no internal deps on runtime/engine)

- [ ] **Step 4: Commit any fixes**

```bash
git add -A crates/rapidbyte-types/
git commit -m "fix(types): resolve v7 type migration compilation issues"
```

---

### Task 12: Verify types crate is self-consistent

**Files:** None (verification only)

- [ ] **Step 1: Run full test suite with all features**

Run: `cargo test -p rapidbyte-types --all-features`
Expected: ALL PASS

- [ ] **Step 2: Run clippy**

Run: `cargo clippy -p rapidbyte-types -- -D warnings`
Expected: No warnings

- [ ] **Step 3: Verify documentation compiles**

Run: `cargo doc -p rapidbyte-types --no-deps`
Expected: PASS

Phase 1 complete. The types crate and WIT file now define the v7 protocol foundation. Phase 2 (Runtime & Codegen) builds on this to make the protocol executable.
