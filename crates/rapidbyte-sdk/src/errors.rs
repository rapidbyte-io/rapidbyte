use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationStatus {
    Success,
    Failed,
    Warning,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ValidationResult {
    pub status: ValidationStatus,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectorError {
    pub code: String,
    pub message: String,
}

/// Result type returned by connector protocol functions.
/// Serialized as JSON for host-guest transport.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ConnectorResult<T> {
    Ok { data: T },
    Err { error: ConnectorError },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_result_roundtrip() {
        let result = ValidationResult {
            status: ValidationStatus::Success,
            message: "Connection successful".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        let back: ValidationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(result, back);
    }

    #[test]
    fn test_connector_result_ok() {
        let result: ConnectorResult<String> = ConnectorResult::Ok {
            data: "hello".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"status\":\"ok\""));
        let back: ConnectorResult<String> = serde_json::from_str(&json).unwrap();
        assert_eq!(result, back);
    }

    #[test]
    fn test_connector_result_err() {
        let result: ConnectorResult<String> = ConnectorResult::Err {
            error: ConnectorError {
                code: "CONNECTION_FAILED".to_string(),
                message: "Could not connect to database".to_string(),
            },
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"status\":\"err\""));
        let back: ConnectorResult<String> = serde_json::from_str(&json).unwrap();
        assert_eq!(result, back);
    }
}
