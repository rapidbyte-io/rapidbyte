//! Convenience re-exports for connector authors.
//!
//! ```ignore
//! use rapidbyte_sdk::prelude::*;
//! ```

pub use crate::connector::{DestinationConnector, SourceConnector, TransformConnector};
pub use crate::errors::{ConnectorError, ValidationResult, ValidationStatus};
pub use crate::host_ffi;
pub use crate::host_tcp::HostTcpStream;
pub use crate::protocol::{
    Catalog, Checkpoint, CheckpointKind, CursorInfo, CursorValue, Feature, Metric, MetricValue,
    ConnectorInfo, ReadPerf, ReadSummary, SchemaHint, StreamContext, StreamLimits, SyncMode,
    TransformSummary, WriteMode, WritePerf, WriteSummary,
};
