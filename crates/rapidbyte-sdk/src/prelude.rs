//! Convenience re-exports for plugin authors.
//!
//! ```ignore
//! use rapidbyte_sdk::prelude::*;
//! ```

// Plugin traits
pub use crate::input::{
    ApplyInput, BulkWriteInput, CdcReadInput, CloseInput, DiscoverInput, InitInput,
    MultiStreamReadInput, PartitionedReadInput, PrerequisitesInput, ReadInput, TeardownInput,
    TransformInput, ValidateInput, WriteInput,
};
pub use crate::plugin::{Destination, Source, Transform};

// Feature traits
pub use crate::features::{
    BulkDestination, CdcSource, MultiStreamCdcSource, MultiStreamSource, PartitionedSource,
};
pub use crate::stream::{CdcResumeToken, PartitionCoordinates};

// Context and logging
pub use crate::capabilities::{Cancel, Checkpoints, Emit, Log, Metrics, Network, Reader, State};
pub use crate::context::{Context, LogLevel};

// Errors
pub use crate::error::{CommitState, PluginError};
pub use crate::lifecycle::{
    ApplyAction, ApplyReport, ApplyRequest, TeardownReport, TeardownRequest,
};
pub use crate::validation::{PrerequisitesReport, ValidationReport, ValidationStatus};

// Protocol types — lifecycle
pub use crate::wire::{Feature, ProtocolVersion};

// Protocol types — streams and discovery
pub use crate::context::CheckpointTxn;
pub use crate::cursor::{CursorInfo, CursorValue};
pub use crate::discovery::{DiscoveredStream, PluginSpec};
pub use crate::schema::{SchemaField, StreamSchema};
pub use crate::stream::{StreamContext, StreamLimits};
pub use crate::wire::{SyncMode, WriteMode};

// Protocol types — data flow
pub use crate::arrow_types::ArrowDataType;
pub use crate::checkpoint::{Checkpoint, CheckpointKind};

// Protocol types — summaries
pub use crate::metric::{ReadPerf, ReadSummary, TransformSummary, WritePerf, WriteSummary};

// Arrow helpers
pub use crate::arrow::{self, arrow_data_type, build_arrow_schema, decode_ipc, encode_ipc};

// Host interop
pub use crate::host_ffi;
pub use crate::host_tcp::HostTcpStream;
