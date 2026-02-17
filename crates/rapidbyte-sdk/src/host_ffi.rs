//! Guest-side FFI stubs for calling host functions.
//! These are linked at Wasm instantiation time when the host
//! registers its import object under module name "rapidbyte".

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

/// Emit an Arrow IPC record batch to the host pipeline.
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

/// Read connector state by key. Returns None if not found.
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

/// Write connector state.
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

/// Log a message to the host.
#[cfg(target_arch = "wasm32")]
pub fn log(level: i32, message: &str) {
    unsafe {
        rb_host_log(level, message.as_ptr() as i32, message.len() as i32);
    }
}

/// Report progress to the host.
#[cfg(target_arch = "wasm32")]
pub fn report_progress(records: u64, bytes: u64) {
    unsafe {
        rb_host_report_progress(records as i64, bytes as i64);
    }
}

// No-op stubs for native compilation (tests)
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
