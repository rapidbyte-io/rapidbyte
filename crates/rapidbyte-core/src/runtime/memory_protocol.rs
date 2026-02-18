use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use wasmedge_sdk::vm::SyncInst;
use wasmedge_sdk::{params, CallingFrame, Vm, WasmVal};
use wasmedge_sys::AsInstance;

/// Write data into guest linear memory by calling the guest's `rb_allocate` export.
/// Returns (ptr, len) in guest address space.
pub fn write_to_guest<T: ?Sized + SyncInst>(
    vm: &mut Vm<'_, T>,
    data: &[u8],
) -> Result<(i32, i32)> {
    let len = data.len() as i32;

    // Call guest's rb_allocate to get a pointer
    let result = vm
        .run_func(None, "rb_allocate", params!(len))
        .map_err(|e| anyhow::anyhow!("Failed to call rb_allocate: {:?}", e))?;

    let ptr = result[0].to_i32();
    if ptr == 0 {
        anyhow::bail!("rb_allocate returned null pointer");
    }

    // Write data into guest memory at the allocated pointer
    {
        let active = vm
            .active_module_mut()
            .context("No active module instance")?;

        let mut memory = active
            .get_memory_mut("memory")
            .map_err(|e| anyhow::anyhow!("Guest has no exported 'memory': {:?}", e))?;

        memory
            .set_data(data, ptr as u32)
            .map_err(|e| anyhow::anyhow!("Failed to write data to guest memory: {:?}", e))?;
    }

    Ok((ptr, len))
}

/// Read data from guest linear memory at (ptr, len) via a CallingFrame.
/// Used inside host functions where we have the CallingFrame.
pub fn read_from_guest(frame: &CallingFrame, ptr: i32, len: i32) -> Result<Vec<u8>> {
    let memory = frame
        .memory_ref(0)
        .context("No memory available in calling frame")?;

    let bytes = memory
        .get_data(ptr as u32, len as u32)
        .map_err(|e| anyhow::anyhow!("Failed to read data from guest memory: {:?}", e))?;

    Ok(bytes)
}

/// Read a string from guest linear memory at (ptr, len).
pub fn read_string_from_guest(frame: &CallingFrame, ptr: i32, len: i32) -> Result<String> {
    let bytes = read_from_guest(frame, ptr, len)?;
    String::from_utf8(bytes).context("Guest string is not valid UTF-8")
}

/// Read data from guest memory via the VM's active module instance.
/// Used outside of host function calls (e.g., reading results after run_func).
fn read_from_guest_via_vm<T: ?Sized + SyncInst>(
    vm: &mut Vm<'_, T>,
    ptr: i32,
    len: i32,
) -> Result<Vec<u8>> {
    let active = vm
        .active_module_mut()
        .context("No active module instance")?;

    let memory = active
        .get_memory_ref("memory")
        .map_err(|e| anyhow::anyhow!("Guest has no exported 'memory': {:?}", e))?;

    let bytes = memory
        .get_data(ptr as u32, len as u32)
        .map_err(|e| anyhow::anyhow!("Failed to read from guest memory: {:?}", e))?;

    Ok(bytes)
}

/// Pack two i32 values (ptr, len) into a single i64 return value.
pub fn pack_ptr_len(ptr: i32, len: i32) -> i64 {
    ((ptr as i64) << 32) | (len as u32 as i64)
}

/// Unpack an i64 into (ptr, len) pair.
pub fn unpack_ptr_len(packed: i64) -> (i32, i32) {
    let ptr = (packed >> 32) as i32;
    let len = (packed & 0xFFFFFFFF) as i32;
    (ptr, len)
}

/// High-level helper: serialize arg as JSON, write to guest, call function,
/// read JSON response from guest, deserialize.
pub fn call_with_json<S: Serialize, R: DeserializeOwned, T: ?Sized + SyncInst>(
    vm: &mut Vm<'_, T>,
    func_name: &str,
    arg: &S,
) -> Result<R> {
    let result_bytes = call_with_json_raw(vm, func_name, arg)?;

    serde_json::from_slice(&result_bytes)
        .with_context(|| format!("Failed to deserialize response from '{}'", func_name))
}

/// Like `call_with_json`, but returns the raw JSON bytes instead of deserializing.
/// Useful when the caller needs to attempt multiple deserialization strategies
/// (e.g., trying ConnectorResultV1 before falling back to ConnectorResult).
pub fn call_with_json_raw<S: Serialize, T: ?Sized + SyncInst>(
    vm: &mut Vm<'_, T>,
    func_name: &str,
    arg: &S,
) -> Result<Vec<u8>> {
    let arg_bytes = serde_json::to_vec(arg).context("Failed to serialize function argument")?;

    // Write argument to guest memory
    let (arg_ptr, arg_len) = write_to_guest(vm, &arg_bytes)?;

    // Call the guest function — it returns a packed i64 (ptr, len)
    let result = vm
        .run_func(None, func_name, params!(arg_ptr, arg_len))
        .map_err(|e| anyhow::anyhow!("Failed to call guest function '{}': {:?}", func_name, e))?;

    let packed = result[0].to_i64();
    let (result_ptr, result_len) = unpack_ptr_len(packed);

    // Read result from guest memory
    let result_bytes = read_from_guest_via_vm(vm, result_ptr, result_len)?;

    // Deallocate the result buffer in guest memory
    let _ = vm.run_func(None, "rb_deallocate", params!(result_ptr, result_len));

    Ok(result_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_roundtrip() {
        let cases = vec![
            (0, 0),
            (1, 1),
            (1024, 256),
            (65536, 4096),
            (i32::MAX, i32::MAX),
        ];

        for (ptr, len) in cases {
            let packed = pack_ptr_len(ptr, len);
            let (p, l) = unpack_ptr_len(packed);
            assert_eq!(p, ptr, "ptr mismatch for ({}, {})", ptr, len);
            assert_eq!(l, len, "len mismatch for ({}, {})", ptr, len);
        }
    }
}
