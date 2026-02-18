use anyhow::{Context, Result};
use wasmedge_sdk::vm::SyncInst;
use wasmedge_sdk::{params, Vm};

use rapidbyte_sdk::errors::{ConnectorResult, ValidationResult};
use rapidbyte_sdk::protocol::{
    Catalog, OpenContext, OpenInfo, ReadSummary, StreamContext, WriteSummary,
};

use super::memory_protocol;

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
                    "Connector validate failed: {} ({})",
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
                    "Connector discover failed: {} ({})",
                    error.message,
                    error.code
                )
            }
        }
    }

    // === v1 lifecycle methods ===

    /// Initialize the connector with v1 open context (config, connector metadata).
    pub fn open(&mut self, ctx: &OpenContext) -> Result<OpenInfo> {
        let result: ConnectorResult<OpenInfo> =
            memory_protocol::call_with_json(&mut self.vm, "rb_open", ctx)
                .context("Failed to call rb_open")?;

        match result {
            ConnectorResult::Ok { data } => Ok(data),
            ConnectorResult::Err { error } => {
                anyhow::bail!("Connector open failed: {} ({})", error.message, error.code)
            }
        }
    }

    /// Start reading data for a single stream.
    /// The guest emits batches via host_emit_batch during execution.
    pub fn run_read(&mut self, ctx: &StreamContext) -> Result<ReadSummary> {
        let result: ConnectorResult<ReadSummary> =
            memory_protocol::call_with_json(&mut self.vm, "rb_run_read", ctx)
                .context("Failed to call rb_run_read")?;

        match result {
            ConnectorResult::Ok { data } => Ok(data),
            ConnectorResult::Err { error } => {
                anyhow::bail!(
                    "Connector run_read failed: {} ({})",
                    error.message,
                    error.code
                )
            }
        }
    }

    /// Start writing data for a single stream.
    /// The guest pulls batches via host_next_batch during execution.
    pub fn run_write(&mut self, ctx: &StreamContext) -> Result<WriteSummary> {
        let result: ConnectorResult<WriteSummary> =
            memory_protocol::call_with_json(&mut self.vm, "rb_run_write", ctx)
                .context("Failed to call rb_run_write")?;

        match result {
            ConnectorResult::Ok { data } => Ok(data),
            ConnectorResult::Err { error } => {
                anyhow::bail!(
                    "Connector run_write failed: {} ({})",
                    error.message,
                    error.code
                )
            }
        }
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
