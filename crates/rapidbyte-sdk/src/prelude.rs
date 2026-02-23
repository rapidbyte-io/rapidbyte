//! Convenience re-exports for connector authors.
//!
//! ```ignore
//! use rapidbyte_sdk::prelude::*;
//! ```

// Connector traits
pub use crate::connector::{Destination, Source, Transform};

// Context and logging
pub use crate::context::{Context, LogLevel};

// Errors
pub use crate::errors::{
    CommitState, ConnectorError, ConnectorResult, ValidationResult, ValidationStatus,
};

// Protocol types — lifecycle
pub use crate::protocol::{
    ConnectorInfo, Feature, ProtocolVersion, DEFAULT_MAX_BATCH_BYTES,
};

// Protocol types — streams and catalog
pub use crate::protocol::{
    Catalog, ColumnSchema, CursorInfo, CursorValue, Stream, StreamContext, StreamLimits, SyncMode,
    WriteMode,
};

// Protocol types — data flow
pub use crate::protocol::{
    ArrowDataType, Checkpoint, CheckpointKind, Metric, MetricValue,
};

// Protocol types — summaries
pub use crate::protocol::{
    ReadPerf, ReadSummary, TransformSummary, WritePerf, WriteSummary,
};

// Arrow helpers
pub use crate::arrow::{self, arrow_data_type, build_arrow_schema, decode_ipc, encode_ipc};

// Host interop
pub use crate::host_ffi;
pub use crate::host_tcp::HostTcpStream;
