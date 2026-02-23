//! Payload envelope and dead-letter queue types.
//!
//! [`PayloadEnvelope`] wraps protocol messages (checkpoints, metrics)
//! with routing metadata. [`DlqRecord`] captures records that failed
//! processing for later inspection.

use crate::error::ErrorCategory;
use crate::wire::ProtocolVersion;
use serde::{Deserialize, Serialize};
use std::fmt;

/// ISO-8601 formatted timestamp string.
///
/// Thin wrapper providing type clarity without requiring a datetime
/// library dependency. No format validation — callers are trusted to
/// provide valid ISO-8601 strings.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(String);

impl Timestamp {
    /// Create a new timestamp from an ISO-8601 string.
    #[must_use]
    pub fn new(iso8601: impl Into<String>) -> Self {
        Self(iso8601.into())
    }

    /// Returns the underlying string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Record that failed processing, routed to the dead-letter queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqRecord {
    /// Stream the record belongs to.
    pub stream_name: String,
    /// JSON-serialized record content.
    pub record_json: String,
    /// Human-readable error description.
    pub error_message: String,
    /// Error classification.
    pub error_category: ErrorCategory,
    /// When the failure occurred.
    pub failed_at: Timestamp,
}

/// Envelope wrapping a protocol payload with routing metadata.
///
/// Used to frame checkpoints, metrics, and other messages exchanged
/// between connectors and the host runtime.
///
/// The `payload` field is a distinct JSON key (not flattened) for
/// unambiguous serialization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PayloadEnvelope<T> {
    /// Protocol version for this message.
    pub protocol_version: ProtocolVersion,
    /// Connector that produced this message.
    pub connector_id: String,
    /// Stream this message relates to.
    pub stream_name: String,
    /// The wrapped payload.
    pub payload: T,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_transparent_serde() {
        let ts = Timestamp::new("2026-01-15T10:30:00Z");
        let json = serde_json::to_string(&ts).unwrap();
        assert_eq!(json, "\"2026-01-15T10:30:00Z\"");
        let back: Timestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(ts, back);
    }

    #[test]
    fn payload_envelope_explicit_payload_field() {
        let env = PayloadEnvelope {
            protocol_version: ProtocolVersion::V2,
            connector_id: "rapidbyte/source-postgres".into(),
            stream_name: "public.users".into(),
            payload: serde_json::json!({"id": 1}),
        };
        let json = serde_json::to_value(&env).unwrap();
        // payload is an explicit key, not flattened
        assert!(json.get("payload").is_some());
        assert_eq!(json["payload"]["id"], 1);
    }

    #[test]
    fn dlq_record_roundtrip() {
        let rec = DlqRecord {
            stream_name: "users".into(),
            record_json: r#"{"id": 1, "name": null}"#.into(),
            error_message: "null name not allowed".into(),
            error_category: ErrorCategory::Data,
            failed_at: Timestamp::new("2026-01-15T10:30:00Z"),
        };
        let json = serde_json::to_string(&rec).unwrap();
        let back: DlqRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(rec, back);
    }
}
