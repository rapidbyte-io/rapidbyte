//! Shared Rapidbyte protocol, manifest, and error types.
//!
//! Dependency-boundary-safe for both host runtime and WASI connector targets.
//! All types use serde for serialization across the host/guest boundary.

#![warn(clippy::pedantic)]

pub mod arrow;
pub mod catalog;
pub mod checkpoint;
pub mod compression;
pub mod cursor;
pub mod envelope;
pub mod error;
pub mod manifest;
pub mod metric;
pub mod state;
pub mod stream;
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
    pub use crate::envelope::{DlqRecord, PayloadEnvelope, Timestamp};
    pub use crate::error::{
        BackoffClass, CommitState, ConnectorError, ErrorCategory, ErrorScope, ValidationResult,
        ValidationStatus,
    };
    pub use crate::manifest::ConnectorManifest;
    pub use crate::metric::{Metric, MetricValue, ReadSummary, TransformSummary, WriteSummary};
    pub use crate::state::{CursorState, PipelineId, RunStats, RunStatus, StreamName};
    pub use crate::stream::{StreamContext, StreamLimits, StreamPolicies};
    pub use crate::wire::{
        ConnectorInfo, ConnectorRole, Feature, ProtocolVersion, SyncMode, WriteMode,
    };
}
