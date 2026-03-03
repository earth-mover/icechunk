//! Shared helpers for FFI boundary code: C string parsing and
//! malloc-based allocation for returning data to C callers.

use std::ffi::CStr;

use crate::error::{ICECHUNK_ERROR, ICECHUNK_ERROR_NULL_ARGUMENT, set_last_error};

/// Parse a C string pointer into a Rust `&str`.
///
/// Returns `ICECHUNK_ERROR_NULL_ARGUMENT` if `ptr` is null, or
/// `ICECHUNK_ERROR` if the bytes are not valid UTF-8.
/// On failure, the thread-local error message is set with `param_name`
/// for diagnostics.
///
/// # Safety
/// `ptr` must point to a valid null-terminated C string (or be null).
pub(crate) unsafe fn parse_c_str<'a>(
    ptr: *const libc::c_char,
    param_name: &str,
) -> Result<&'a str, i32> {
    if ptr.is_null() {
        set_last_error(format!("{param_name} must not be null"));
        return Err(ICECHUNK_ERROR_NULL_ARGUMENT);
    }
    unsafe { CStr::from_ptr(ptr) }.to_str().map_err(|e| {
        set_last_error(format!("invalid UTF-8 in {param_name}: {e}"));
        ICECHUNK_ERROR
    })
}

/// Allocate a C buffer via `malloc` and copy `data` into it.
///
/// Returns `ICECHUNK_ERROR` if `malloc` fails.
/// The caller (C side) is responsible for calling `free()`.
pub(crate) fn alloc_c_bytes(data: &[u8]) -> Result<*mut u8, i32> {
    let len = data.len();
    let buf = unsafe { libc::malloc(len) as *mut u8 };
    if buf.is_null() {
        set_last_error("malloc failed".to_string());
        return Err(ICECHUNK_ERROR);
    }
    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), buf, len) };
    Ok(buf)
}

/// Allocate a null-terminated C string via `malloc` and copy `s` into it.
///
/// Returns `ICECHUNK_ERROR` if `malloc` fails.
/// The caller (C side) is responsible for calling `free()`.
pub(crate) fn alloc_c_string(s: &str) -> Result<*mut libc::c_char, i32> {
    let len = s.len();
    let buf = unsafe { libc::malloc(len + 1) as *mut libc::c_char };
    if buf.is_null() {
        set_last_error("malloc failed".to_string());
        return Err(ICECHUNK_ERROR);
    }
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr() as *const libc::c_char, buf, len);
        *buf.add(len) = 0; // null terminator
    };
    Ok(buf)
}
