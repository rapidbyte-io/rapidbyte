//! Shared Rapidbyte protocol, manifest, and error types.
//!
//! Dependency-boundary-safe for both host runtime and WASI plugin targets.
//! All types use serde for serialization across the host/guest boundary.
//!
//! | Module         | Responsibility |
//! |----------------|----------------|
//! | `arrow`        | Arrow data type mappings |
//! | `catalog`      | Catalog, stream, and column schema definitions |
//! | `checkpoint`   | Checkpoint and state scope types |
//! | `compression`  | Compression codec enum |
//! | `cursor`       | Cursor info and value types for incremental sync |
//! | `discovery`    | Discovered streams and plugin spec types (v7) |
//! | `envelope`     | DLQ record and payload envelope types |
//! | `error`        | `PluginError`, `ValidationResult`, error categories |
//! | `lifecycle`    | Apply and teardown lifecycle types (v7) |
//! | `manifest`     | Plugin manifest and permission types |
//! | `metric`       | Metric, summary types (read/write/transform) |
//! | `schema`       | Schema negotiation types (v7 protocol) |
//! | `state`        | Run state, pipeline ID, cursor state types |
//! | `state_backend`| `StateBackend` trait (storage contract) |
//! | `state_error`  | `StateError` type for backend operations |
//! | `stream`       | Stream context, limits, policies |
//! | `validation`   | Validation report and prerequisites types (v7) |
//! | `wire`         | Wire protocol enums (sync mode, write mode, role) |

#![warn(clippy::pedantic)]

pub mod arrow;
pub mod catalog;
pub mod checkpoint;
pub mod compression;
pub mod cursor;
pub mod discovery;
pub mod envelope;
pub mod error;
pub mod format;
pub mod lifecycle;
pub mod manifest;
pub mod metric;
pub mod schema;
pub mod state;
pub mod state_backend;
pub mod state_error;
pub mod stream;
pub mod validation;
pub mod wire;

/// Common imports for typical usage.
///
/// ```
/// use rapidbyte_types::prelude::*;
/// ```
pub mod prelude {
    pub use crate::arrow::ArrowDataType;
    pub use crate::catalog::{Catalog, ColumnSchema, SchemaHint, Stream};
    pub use crate::checkpoint::{Checkpoint, CheckpointKind, StateScope};
    pub use crate::compression::CompressionCodec;
    pub use crate::cursor::{CursorInfo, CursorType, CursorValue};
    pub use crate::discovery::{DiscoveredStream, PluginSpec};
    pub use crate::envelope::{DlqRecord, PayloadEnvelope, Timestamp};
    pub use crate::error::{
        BackoffClass, CommitState, ErrorCategory, ErrorScope, PluginError, ValidationResult,
        ValidationStatus,
    };
    pub use crate::lifecycle::{
        ApplyAction, ApplyReport, ApplyRequest, TeardownReport, TeardownRequest,
    };
    pub use crate::manifest::PluginManifest;
    pub use crate::metric::{ReadSummary, TransformSummary, WriteSummary};
    pub use crate::schema::{FieldConstraint, FieldRequirement, SchemaField, StreamSchema};
    pub use crate::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
    pub use crate::state_backend::{noop_state_backend, NoopStateBackend, StateBackend};
    pub use crate::state_error::StateError;
    pub use crate::stream::{StreamContext, StreamLimits, StreamPolicies};
    // NOTE: `validation::ValidationStatus` is intentionally excluded to avoid
    // collision with `error::ValidationStatus`. Resolved in Task 7.
    pub use crate::validation::{
        PrerequisiteCheck, PrerequisiteSeverity, PrerequisitesReport, ValidationReport,
    };
    pub use crate::wire::{Feature, PluginInfo, PluginKind, ProtocolVersion, SyncMode, WriteMode};
}
