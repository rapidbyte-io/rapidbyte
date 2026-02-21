use serde::{Deserialize, Serialize};

use crate::errors::ErrorCategory;

use super::ProtocolVersion;

/// Timestamp serialized as ISO-8601 text.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Iso8601Timestamp(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DlqRecord {
    pub stream_name: String,
    pub record_json: String,
    pub error_message: String,
    pub error_category: ErrorCategory,
    pub failed_at: Iso8601Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PayloadEnvelope<T> {
    pub protocol_version: ProtocolVersion,
    pub connector_id: String,
    pub stream_name: String,
    #[serde(flatten)]
    pub payload: T,
}
