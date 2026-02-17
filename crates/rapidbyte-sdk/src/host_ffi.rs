//! Guest-side FFI stubs for calling host functions.
//! These are linked at Wasm instantiation time when the host
//! registers its import object under module name "rapidbyte".

use crate::errors::ConnectorErrorV1;
use crate::protocol::{Checkpoint, CheckpointKind, Metric, PayloadEnvelope, StateScope};

// === v0 host function declarations (kept during transition) ===

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "rapidbyte")]
extern "C" {
    fn rb_host_emit_record_batch(
        stream_ptr: i32,
        stream_len: i32,
        batch_ptr: i32,
        batch_len: i32,
    ) -> i32;

    fn rb_host_get_state(
        key_ptr: i32,
        key_len: i32,
        out_ptr: i32,
        out_len: i32,
    ) -> i32;

    fn rb_host_set_state(
        key_ptr: i32,
        key_len: i32,
        val_ptr: i32,
        val_len: i32,
    ) -> i32;

    fn rb_host_log(level: i32, msg_ptr: i32, msg_len: i32) -> i32;

    fn rb_host_report_progress(records: i64, bytes: i64) -> i32;
}

// === v1 host function declarations ===

#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "rapidbyte")]
extern "C" {
    fn rb_host_emit_batch(ptr: u32, len: u32) -> i32;
    fn rb_host_next_batch(out_ptr: u32, out_cap: u32) -> i32;
    fn rb_host_last_error(out_ptr: u32, out_cap: u32) -> i32;
    fn rb_host_state_get(
        scope: i32,
        key_ptr: u32,
        key_len: u32,
        out_ptr: u32,
        out_cap: u32,
    ) -> i32;
    fn rb_host_state_put(
        scope: i32,
        key_ptr: u32,
        key_len: u32,
        val_ptr: u32,
        val_len: u32,
    ) -> i32;
    fn rb_host_checkpoint(kind: i32, payload_ptr: u32, payload_len: u32) -> i32;
    fn rb_host_metric(payload_ptr: u32, payload_len: u32) -> i32;
}

// === v0 safe wrappers (kept during transition) ===

#[cfg(target_arch = "wasm32")]
pub fn emit_record_batch(stream: &str, ipc_bytes: &[u8]) -> i32 {
    unsafe {
        rb_host_emit_record_batch(
            stream.as_ptr() as i32,
            stream.len() as i32,
            ipc_bytes.as_ptr() as i32,
            ipc_bytes.len() as i32,
        )
    }
}

#[cfg(target_arch = "wasm32")]
pub fn get_state(key: &str) -> Option<serde_json::Value> {
    let mut out_buf: Vec<u8> = Vec::with_capacity(4096);
    let out_ptr = out_buf.as_mut_ptr() as i32;
    let out_len = out_buf.capacity() as i32;
    let result = unsafe {
        rb_host_get_state(
            key.as_ptr() as i32,
            key.len() as i32,
            out_ptr,
            out_len,
        )
    };
    if result <= 0 {
        return None;
    }
    unsafe {
        out_buf.set_len(result as usize);
    }
    serde_json::from_slice(&out_buf).ok()
}

#[cfg(target_arch = "wasm32")]
pub fn set_state(key: &str, value: &serde_json::Value) {
    let val_bytes = serde_json::to_vec(value).unwrap();
    unsafe {
        rb_host_set_state(
            key.as_ptr() as i32,
            key.len() as i32,
            val_bytes.as_ptr() as i32,
            val_bytes.len() as i32,
        );
    }
}

#[cfg(target_arch = "wasm32")]
pub fn log(level: i32, message: &str) {
    unsafe {
        rb_host_log(level, message.as_ptr() as i32, message.len() as i32);
    }
}

#[cfg(target_arch = "wasm32")]
pub fn report_progress(records: u64, bytes: u64) {
    unsafe {
        rb_host_report_progress(records as i64, bytes as i64);
    }
}

// === v1 safe wrappers ===

/// Emit an Arrow IPC batch to the host. Blocks until channel has capacity.
/// Returns Ok(()) on success, Err with structured error on failure.
#[cfg(target_arch = "wasm32")]
pub fn emit_batch(ipc_bytes: &[u8]) -> Result<(), ConnectorErrorV1> {
    let rc = unsafe { rb_host_emit_batch(ipc_bytes.as_ptr() as u32, ipc_bytes.len() as u32) };
    if rc == 0 {
        Ok(())
    } else {
        Err(fetch_last_error())
    }
}

/// Pull the next batch from the host. Returns None on EOF.
/// `buf` is reused across calls to amortize allocation.
/// `max_bytes` caps buffer growth to prevent OOM.
#[cfg(target_arch = "wasm32")]
pub fn next_batch(buf: &mut Vec<u8>, max_bytes: u64) -> Result<Option<usize>, ConnectorErrorV1> {
    if buf.capacity() == 0 {
        buf.reserve_exact(64 * 1024); // Start with 64KB
    }

    loop {
        let cap = buf.capacity();
        // SAFETY: We set_len to capacity so the host can write into the full allocation.
        // We immediately set_len back to 0 before any early return.
        unsafe { buf.set_len(cap) };

        let rc = unsafe { rb_host_next_batch(buf.as_mut_ptr() as u32, cap as u32) };

        // Reset length to 0 before processing return code
        unsafe { buf.set_len(0) };

        if rc > 0 {
            let n = rc as usize;
            // SAFETY: host wrote n bytes into our buffer, n <= cap
            unsafe { buf.set_len(n) };
            return Ok(Some(n));
        } else if rc == 0 {
            return Ok(None); // EOF
        } else if rc == -1 {
            return Err(fetch_last_error());
        } else {
            // -N means need N bytes
            let needed = (-rc) as usize;
            if needed as u64 > max_bytes {
                return Err(ConnectorErrorV1::internal(
                    "BATCH_TOO_LARGE",
                    &format!("Host needs {} bytes, exceeds max {}", needed, max_bytes),
                ));
            }
            buf.reserve_exact(needed.saturating_sub(buf.capacity()));
            // Loop will retry with larger buffer
        }
    }
}

/// Fetch and clear the last error from the host.
#[cfg(target_arch = "wasm32")]
pub fn fetch_last_error() -> ConnectorErrorV1 {
    let mut buf = vec![0u8; 4096];
    let rc = unsafe { rb_host_last_error(buf.as_mut_ptr() as u32, buf.len() as u32) };
    if rc > 0 {
        buf.truncate(rc as usize);
        serde_json::from_slice(&buf).unwrap_or_else(|_| {
            ConnectorErrorV1::internal("PARSE_ERROR", "Failed to parse host error")
        })
    } else {
        ConnectorErrorV1::internal("UNKNOWN_ERROR", "No error details available from host")
    }
}

/// Get state from host with scoped key. Returns None if not found.
#[cfg(target_arch = "wasm32")]
pub fn state_get(scope: StateScope, key: &str) -> Result<Option<String>, ConnectorErrorV1> {
    let mut buf = vec![0u8; 4096];
    let max_state_bytes: usize = 16 * 1024 * 1024; // 16MB cap

    loop {
        let rc = unsafe {
            rb_host_state_get(
                scope.to_i32(),
                key.as_ptr() as u32,
                key.len() as u32,
                buf.as_mut_ptr() as u32,
                buf.len() as u32,
            )
        };

        if rc > 0 {
            buf.truncate(rc as usize);
            return Ok(Some(
                String::from_utf8(buf)
                    .map_err(|_| ConnectorErrorV1::internal("UTF8_ERROR", "State value not UTF-8"))?,
            ));
        } else if rc == 0 {
            return Ok(None);
        } else if rc == -1 {
            return Err(fetch_last_error());
        } else {
            let needed = (-rc) as usize;
            if needed > max_state_bytes {
                return Err(ConnectorErrorV1::internal(
                    "STATE_TOO_LARGE",
                    &format!("State value {} bytes exceeds 16MB cap", needed),
                ));
            }
            buf.resize(needed, 0);
            // Loop retries with larger buffer
        }
    }
}

/// Put state to host with scoped key.
#[cfg(target_arch = "wasm32")]
pub fn state_put(scope: StateScope, key: &str, value: &str) -> Result<(), ConnectorErrorV1> {
    let rc = unsafe {
        rb_host_state_put(
            scope.to_i32(),
            key.as_ptr() as u32,
            key.len() as u32,
            value.as_ptr() as u32,
            value.len() as u32,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(fetch_last_error())
    }
}

/// Emit a checkpoint to the host.
#[cfg(target_arch = "wasm32")]
pub fn checkpoint(
    connector_id: &str,
    stream_name: &str,
    cp: &Checkpoint,
) -> Result<(), ConnectorErrorV1> {
    let kind_i32 = match cp.kind {
        CheckpointKind::Source => 0,
        CheckpointKind::Dest => 1,
    };

    let envelope = PayloadEnvelope {
        protocol_version: "1".to_string(),
        connector_id: connector_id.to_string(),
        stream_name: stream_name.to_string(),
        payload: cp.clone(),
    };
    let payload = serde_json::to_vec(&envelope)
        .map_err(|e| ConnectorErrorV1::internal("SERIALIZE", &e.to_string()))?;

    let rc = unsafe {
        rb_host_checkpoint(kind_i32, payload.as_ptr() as u32, payload.len() as u32)
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(fetch_last_error())
    }
}

/// Emit a metric to the host.
#[cfg(target_arch = "wasm32")]
pub fn metric(
    connector_id: &str,
    stream_name: &str,
    m: &Metric,
) -> Result<(), ConnectorErrorV1> {
    let envelope = PayloadEnvelope {
        protocol_version: "1".to_string(),
        connector_id: connector_id.to_string(),
        stream_name: stream_name.to_string(),
        payload: m.clone(),
    };
    let payload = serde_json::to_vec(&envelope)
        .map_err(|e| ConnectorErrorV1::internal("SERIALIZE", &e.to_string()))?;

    let rc = unsafe { rb_host_metric(payload.as_ptr() as u32, payload.len() as u32) };
    if rc == 0 {
        Ok(())
    } else {
        Err(fetch_last_error())
    }
}

// === No-op stubs for native compilation (tests) ===

#[cfg(not(target_arch = "wasm32"))]
pub fn emit_record_batch(_stream: &str, _ipc_bytes: &[u8]) -> i32 {
    0
}
#[cfg(not(target_arch = "wasm32"))]
pub fn get_state(_key: &str) -> Option<serde_json::Value> {
    None
}
#[cfg(not(target_arch = "wasm32"))]
pub fn set_state(_key: &str, _value: &serde_json::Value) {}
#[cfg(not(target_arch = "wasm32"))]
pub fn log(_level: i32, _message: &str) {}
#[cfg(not(target_arch = "wasm32"))]
pub fn report_progress(_records: u64, _bytes: u64) {}

// v1 no-op stubs
#[cfg(not(target_arch = "wasm32"))]
pub fn emit_batch(_ipc_bytes: &[u8]) -> Result<(), ConnectorErrorV1> {
    Ok(())
}
#[cfg(not(target_arch = "wasm32"))]
pub fn next_batch(
    _buf: &mut Vec<u8>,
    _max_bytes: u64,
) -> Result<Option<usize>, ConnectorErrorV1> {
    Ok(None)
}
#[cfg(not(target_arch = "wasm32"))]
pub fn fetch_last_error() -> ConnectorErrorV1 {
    ConnectorErrorV1::internal("STUB", "No-op stub")
}
#[cfg(not(target_arch = "wasm32"))]
pub fn state_get(
    _scope: StateScope,
    _key: &str,
) -> Result<Option<String>, ConnectorErrorV1> {
    Ok(None)
}
#[cfg(not(target_arch = "wasm32"))]
pub fn state_put(
    _scope: StateScope,
    _key: &str,
    _value: &str,
) -> Result<(), ConnectorErrorV1> {
    Ok(())
}
#[cfg(not(target_arch = "wasm32"))]
pub fn checkpoint(
    _connector_id: &str,
    _stream_name: &str,
    _cp: &Checkpoint,
) -> Result<(), ConnectorErrorV1> {
    Ok(())
}
#[cfg(not(target_arch = "wasm32"))]
pub fn metric(
    _connector_id: &str,
    _stream_name: &str,
    _m: &Metric,
) -> Result<(), ConnectorErrorV1> {
    Ok(())
}
