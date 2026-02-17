use anyhow::{Context, Result};
use wasmedge_sdk::vm::SyncInst;
use wasmedge_sdk::{params, Vm, WasmVal};
use wasmedge_sys::AsInstance;

use rapidbyte_sdk::errors::{ConnectorResult, ValidationResult};
use rapidbyte_sdk::protocol::{Catalog, ReadRequest, ReadSummary, WriteSummary};

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

    /// Initialize the connector with its configuration.
    pub fn init(&mut self, config: &serde_json::Value) -> Result<()> {
        let result: ConnectorResult<()> =
            memory_protocol::call_with_json(&mut self.vm, "rb_init", config)
                .context("Failed to call rb_init")?;

        match result {
            ConnectorResult::Ok { .. } => Ok(()),
            ConnectorResult::Err { error } => {
                anyhow::bail!("Connector init failed: {} ({})", error.message, error.code)
            }
        }
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

    /// Start reading data. The guest will emit record batches via
    /// `rb_host_emit_record_batch` host function calls during execution.
    pub fn read(&mut self, request: &ReadRequest) -> Result<ReadSummary> {
        let result: ConnectorResult<ReadSummary> =
            memory_protocol::call_with_json(&mut self.vm, "rb_read", request)
                .context("Failed to call rb_read")?;

        match result {
            ConnectorResult::Ok { data } => Ok(data),
            ConnectorResult::Err { error } => {
                anyhow::bail!(
                    "Connector read failed: {} ({})",
                    error.message,
                    error.code
                )
            }
        }
    }

    /// Write a batch of Arrow IPC data to the destination.
    pub fn write_batch(&mut self, stream: &str, ipc_bytes: &[u8]) -> Result<()> {
        // Write stream name to guest memory
        let (stream_ptr, stream_len) =
            memory_protocol::write_to_guest(&mut self.vm, stream.as_bytes())
                .context("Failed to write stream name to guest")?;

        // Write IPC bytes to guest memory
        let (batch_ptr, batch_len) =
            memory_protocol::write_to_guest(&mut self.vm, ipc_bytes)
                .context("Failed to write batch data to guest")?;

        // Call the guest function
        let result = self
            .vm
            .run_func(
                None,
                "rb_write_batch",
                params!(stream_ptr, stream_len, batch_ptr, batch_len),
            )
            .map_err(|e| anyhow::anyhow!("Failed to call rb_write_batch: {:?}", e))?;

        let packed = result[0].to_i64();
        let (result_ptr, result_len) = memory_protocol::unpack_ptr_len(packed);

        // Read result from guest memory
        let result_bytes = {
            let active = self
                .vm
                .active_module_mut()
                .context("No active module instance")?;

            let memory = active
                .get_memory_ref("memory")
                .map_err(|e| anyhow::anyhow!("Guest has no exported 'memory': {:?}", e))?;

            memory
                .get_data(result_ptr as u32, result_len as u32)
                .map_err(|e| anyhow::anyhow!("Failed to read write_batch result: {:?}", e))?
        };

        // Deallocate the result buffer
        let _ = self
            .vm
            .run_func(None, "rb_deallocate", params!(result_ptr, result_len));

        let response: ConnectorResult<()> = serde_json::from_slice(&result_bytes)
            .context("Failed to deserialize write_batch response")?;

        match response {
            ConnectorResult::Ok { .. } => Ok(()),
            ConnectorResult::Err { error } => {
                anyhow::bail!(
                    "Connector write_batch failed: {} ({})",
                    error.message,
                    error.code
                )
            }
        }
    }

    /// Finalize writing — flush remaining data and return summary.
    pub fn write_finalize(&mut self) -> Result<WriteSummary> {
        let empty = serde_json::json!({});
        let result: ConnectorResult<WriteSummary> =
            memory_protocol::call_with_json(&mut self.vm, "rb_write_finalize", &empty)
                .context("Failed to call rb_write_finalize")?;

        match result {
            ConnectorResult::Ok { data } => Ok(data),
            ConnectorResult::Err { error } => {
                anyhow::bail!(
                    "Connector write_finalize failed: {} ({})",
                    error.message,
                    error.code
                )
            }
        }
    }
}
