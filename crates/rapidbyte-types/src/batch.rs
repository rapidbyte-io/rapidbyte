//! Batch metadata for Arrow IPC frame transport.
//!
//! [`BatchMetadata`] accompanies each Arrow IPC batch flowing through the
//! pipeline, carrying stream identity, sequencing, compression, and size
//! information used by the orchestrator and destination plugins.

use serde::{Deserialize, Serialize};

/// Metadata attached to an Arrow IPC batch in the frame transport layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchMetadata {
    /// Index of the stream this batch belongs to.
    pub stream_index: u32,
    /// Optional fingerprint of the Arrow schema (e.g. SHA-256 of IPC schema bytes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_fingerprint: Option<String>,
    /// Monotonically increasing sequence number within the stream.
    pub sequence_number: u64,
    /// Compression codec applied to the batch payload (e.g. "lz4", "zstd").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,
    /// Number of records in this batch.
    pub record_count: u32,
    /// Total byte size of the serialized batch payload.
    pub byte_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_metadata_roundtrip() {
        let meta = BatchMetadata {
            stream_index: 3,
            schema_fingerprint: Some("abc123".into()),
            sequence_number: 42,
            compression: Some("lz4".into()),
            record_count: 1000,
            byte_count: 65536,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: BatchMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, back);
    }

    #[test]
    fn batch_metadata_minimal() {
        let meta = BatchMetadata {
            stream_index: 0,
            schema_fingerprint: None,
            sequence_number: 1,
            compression: None,
            record_count: 10,
            byte_count: 256,
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert!(!json.contains("schema_fingerprint"));
        assert!(!json.contains("compression"));
        // Verify it round-trips cleanly.
        let back: BatchMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, back);
    }
}
