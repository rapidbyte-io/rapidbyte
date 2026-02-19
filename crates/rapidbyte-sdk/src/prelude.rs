//! Convenience re-exports for connector authors.
//!
//! ```ignore
//! use rapidbyte_sdk::prelude::*;
//! ```

pub use crate::connector::{DestinationConnector, SourceConnector};
pub use crate::errors::{ConnectorError, ValidationResult, ValidationStatus};
pub use crate::host_ffi;
pub use crate::protocol::{
    Catalog, Checkpoint, CheckpointKind, ConfigBlob, CursorInfo, CursorValue, Feature, Metric,
    MetricValue, OpenContext, OpenInfo, ReadPerf, ReadSummary, SchemaHint, StreamContext,
    StreamLimits, SyncMode, WriteMode, WritePerf, WriteSummary,
};
