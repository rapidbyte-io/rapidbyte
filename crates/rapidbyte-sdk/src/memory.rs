use std::mem;

/// Allocate `size` bytes in guest linear memory.
/// Called by the host to reserve space before writing data.
#[no_mangle]
pub extern "C" fn rb_allocate(size: i32) -> i32 {
    let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
    let ptr = buffer.as_mut_ptr();
    mem::forget(buffer);
    ptr as i32
}

/// Free memory previously allocated by `rb_allocate`.
#[no_mangle]
pub extern "C" fn rb_deallocate(ptr: i32, capacity: i32) {
    unsafe {
        let _ = Vec::from_raw_parts(ptr as *mut u8, 0, capacity as usize);
    }
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

/// Read bytes from guest memory at (ptr, len) into a Vec.
/// Consumes the allocation — the caller is responsible for the memory.
///
/// # Safety
/// `ptr` must point to a valid allocation of at least `len` bytes
/// made by `rb_allocate` or `write_guest_bytes`.
pub unsafe fn read_guest_bytes(ptr: i32, len: i32) -> Vec<u8> {
    Vec::from_raw_parts(ptr as *mut u8, len as usize, len as usize)
}

/// Write bytes to guest memory, returning (ptr, len) for the host.
/// The caller must ensure the host calls rb_deallocate later.
pub fn write_guest_bytes(data: &[u8]) -> (i32, i32) {
    let len = data.len();
    let mut buffer = data.to_vec();
    let ptr = buffer.as_mut_ptr() as i32;
    mem::forget(buffer);
    (ptr, len as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_ptr_len() {
        let ptr = 1024i32;
        let len = 256i32;
        let packed = pack_ptr_len(ptr, len);
        let (p, l) = unpack_ptr_len(packed);
        assert_eq!(p, ptr);
        assert_eq!(l, len);
    }

    #[test]
    fn test_pack_unpack_large_values() {
        let ptr = 0x7FFF_FFFFi32;
        let len = 0x7FFF_FFFFi32;
        let packed = pack_ptr_len(ptr, len);
        let (p, l) = unpack_ptr_len(packed);
        assert_eq!(p, ptr);
        assert_eq!(l, len);
    }

    #[test]
    fn test_pack_unpack_zero() {
        let packed = pack_ptr_len(0, 0);
        let (p, l) = unpack_ptr_len(packed);
        assert_eq!(p, 0);
        assert_eq!(l, 0);
    }

    // Note: rb_allocate, rb_deallocate, write_guest_bytes, read_guest_bytes
    // use i32 pointers designed for wasm32 linear memory. They truncate
    // pointers on 64-bit native targets. Tested via connector integration tests
    // on the wasm32-wasip1 target instead.
}
