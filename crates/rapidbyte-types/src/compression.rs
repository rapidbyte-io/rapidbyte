//! IPC channel compression codec selection.

use serde::{Deserialize, Serialize};

/// IPC channel compression codec.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionCodec {
    Lz4,
    Zstd,
}
