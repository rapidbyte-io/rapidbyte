use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use wasmedge_sdk::vm::SyncInst;
use wasmedge_sdk::{params, Vm};

use rapidbyte_sdk::errors::{ConnectorResult, ValidationResult};
use rapidbyte_sdk::protocol::{
    Catalog, OpenContext, OpenInfo, ReadSummary, StreamContext, WriteSummary,
};

use super::memory_protocol;
use crate::engine::errors::PipelineError;

/// High-level typed wrapper around raw Wasm VM calls.
///
/// Each method follows the memory protocol:
/// 1. Serialize input as JSON
/// 2. Write to guest memory via `rb_allocate`
/// 3. Call the guest function
/// 4. Read JSON result from guest memory
/// 5. Deserialize into typed response
///
/// The lifetime `'a` is tied to the import objects that the VM borrows.
pub struct ConnectorHandle<'a> {
    vm: Vm<'a, dyn SyncInst>,
}

impl<'a> ConnectorHandle<'a> {
    pub fn new(vm: Vm<'a, dyn SyncInst>) -> Self {
        Self { vm }
    }

    /// Validate the connector's configuration and connectivity.
    pub fn validate(&mut self, config: &serde_json::Value) -> Result<ValidationResult> {
        let result: ConnectorResult<ValidationResult> =
            memory_protocol::call_with_json(&mut self.vm, "rb_validate", config)
                .context("Failed to call rb_validate")?;

        match result {
            ConnectorResult::Ok { data } => Ok(data),
            ConnectorResult::Err { error } => {
                anyhow::bail!(
                    "Connector validate failed: [{}] {} ({})",
                    error.category,
                    error.message,
                    error.code
                )
            }
        }
    }

    /// Discover the connector's catalog (available streams and schemas).
    pub fn discover(&mut self) -> Result<Catalog> {
        let empty = serde_json::json!({});
        let result: ConnectorResult<Catalog> =
            memory_protocol::call_with_json(&mut self.vm, "rb_discover", &empty)
                .context("Failed to call rb_discover")?;

        match result {
            ConnectorResult::Ok { data } => Ok(data),
            ConnectorResult::Err { error } => {
                anyhow::bail!(
                    "Connector discover failed: [{}] {} ({})",
                    error.category,
                    error.message,
                    error.code
                )
            }
        }
    }

    /// Initialize the connector with open context (config, connector metadata).
    pub fn open(&mut self, ctx: &OpenContext) -> Result<OpenInfo> {
        let result: ConnectorResult<OpenInfo> =
            memory_protocol::call_with_json(&mut self.vm, "rb_open", ctx)
                .context("Failed to call rb_open")?;

        match result {
            ConnectorResult::Ok { data } => Ok(data),
            ConnectorResult::Err { error } => {
                anyhow::bail!("Connector open failed: [{}] {} ({})", error.category, error.message, error.code)
            }
        }
    }

    /// Start reading data for a single stream.
    /// The guest emits batches via host_emit_batch during execution.
    ///
    /// Returns `PipelineError::Connector` for typed errors,
    /// preserving retry metadata for the orchestrator.
    pub fn run_read(&mut self, ctx: &StreamContext) -> Result<ReadSummary, PipelineError> {
        let raw_bytes = memory_protocol::call_with_json_raw(&mut self.vm, "rb_run_read", ctx)
            .context("Failed to call rb_run_read")
            .map_err(PipelineError::Infrastructure)?;

        parse_connector_result(&raw_bytes, "rb_run_read")
    }

    /// Start writing data for a single stream.
    /// The guest pulls batches via host_next_batch during execution.
    ///
    /// Returns `PipelineError::Connector` for typed errors,
    /// preserving retry metadata for the orchestrator.
    pub fn run_write(&mut self, ctx: &StreamContext) -> Result<WriteSummary, PipelineError> {
        let raw_bytes = memory_protocol::call_with_json_raw(&mut self.vm, "rb_run_write", ctx)
            .context("Failed to call rb_run_write")
            .map_err(PipelineError::Infrastructure)?;

        parse_connector_result(&raw_bytes, "rb_run_write")
    }

    /// Close the connector and release resources.
    pub fn close(&mut self) -> Result<()> {
        let result = self
            .vm
            .run_func(None, "rb_close", params!())
            .map_err(|e| anyhow::anyhow!("Failed to call rb_close: {:?}", e))?;

        if result.is_empty() {
            return Ok(());
        }
        let rc = result[0].to_i32();
        if rc == 0 {
            Ok(())
        } else {
            anyhow::bail!("Connector close returned error code: {}", rc)
        }
    }
}

/// Parse the raw JSON response as a ConnectorResult.
/// Returns `PipelineError::Connector` for typed errors (preserving retry metadata),
/// or `PipelineError::Infrastructure` for deserialization failures.
fn parse_connector_result<T: DeserializeOwned>(
    raw_bytes: &[u8],
    func_name: &str,
) -> Result<T, PipelineError> {
    match serde_json::from_slice::<ConnectorResult<T>>(raw_bytes) {
        Ok(ConnectorResult::Ok { data }) => Ok(data),
        Ok(ConnectorResult::Err { error }) => {
            tracing::warn!(
                func = func_name,
                category = %error.category,
                retryable = error.retryable,
                code = %error.code,
                "Connector returned error"
            );
            Err(PipelineError::Connector(error))
        }
        Err(_) => {
            let snippet = String::from_utf8_lossy(
                &raw_bytes[..raw_bytes.len().min(200)]
            );
            Err(PipelineError::Infrastructure(anyhow::anyhow!(
                "Failed to deserialize response from '{}': {}",
                func_name,
                snippet
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::errors::{ConnectorError, ErrorCategory};

    #[test]
    fn test_parse_ok_response() {
        let json = serde_json::json!({
            "status": "ok",
            "data": { "records_read": 42, "bytes_read": 1024, "batches_emitted": 1, "checkpoint_count": 0 }
        });
        let bytes = serde_json::to_vec(&json).unwrap();

        let result: Result<rapidbyte_sdk::protocol::ReadSummary, PipelineError> =
            parse_connector_result(&bytes, "rb_run_read");
        let summary = result.unwrap();
        assert_eq!(summary.records_read, 42);
        assert_eq!(summary.bytes_read, 1024);
    }

    #[test]
    fn test_parse_error_response_preserves_metadata() {
        let err = ConnectorError::transient_db("DEADLOCK", "deadlock detected");
        let json = serde_json::json!({
            "status": "err",
            "error": err,
        });
        let bytes = serde_json::to_vec(&json).unwrap();

        let result: Result<rapidbyte_sdk::protocol::ReadSummary, PipelineError> =
            parse_connector_result(&bytes, "rb_run_read");
        let err = result.unwrap_err();

        assert!(err.is_retryable());
        let ce = err.as_connector_error().unwrap();
        assert_eq!(ce.category, ErrorCategory::TransientDb);
        assert_eq!(ce.code, "DEADLOCK");
        assert!(ce.retryable);
    }

    #[test]
    fn test_parse_garbage_becomes_infrastructure_error() {
        let bytes = b"this is not json at all";

        let result: Result<rapidbyte_sdk::protocol::ReadSummary, PipelineError> =
            parse_connector_result(bytes, "rb_run_read");
        let err = result.unwrap_err();

        assert!(matches!(err, PipelineError::Infrastructure(_)));
        assert!(format!("{}", err).contains("Failed to deserialize"));
    }
}
