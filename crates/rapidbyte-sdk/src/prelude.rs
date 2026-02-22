//! Convenience re-exports for connector authors.
//!
//! ```ignore
//! use rapidbyte_sdk::prelude::*;
//! ```

pub use crate::connector::{Destination, Source, Transform};
pub use crate::errors::{
    BackoffClass, CommitState, ConnectorError, ConnectorResult, ErrorCategory, ErrorScope,
    ValidationResult, ValidationStatus,
};
pub use crate::host_ffi;
pub use crate::host_tcp::HostTcpStream;
pub use crate::protocol::{
    ArrowDataType, Catalog, Checkpoint, CheckpointKind, ColumnSchema, ConnectorInfo, ConnectorRole,
    CursorInfo, CursorValue, Feature, Metric, MetricValue, ProtocolVersion, ReadPerf, ReadSummary,
    SchemaHint, StateScope, Stream, StreamContext, StreamLimits, SyncMode, TransformSummary,
    WriteMode, WritePerf, WriteSummary,
};
pub use crate::arrow::{self, build_arrow_schema, decode_ipc, encode_ipc, arrow_data_type};
pub use crate::arrow::arrow_reexport;
