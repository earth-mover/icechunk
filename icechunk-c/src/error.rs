//! Thread-local error string and return codes.

use std::cell::RefCell;
use std::ffi::CString;
use std::ptr;

/// Return codes for all icechunk_* functions.
pub const ICECHUNK_SUCCESS: i32 = 0;
pub const ICECHUNK_ERROR: i32 = -1;
pub const ICECHUNK_ERROR_NOT_FOUND: i32 = -2;
pub const ICECHUNK_ERROR_READONLY: i32 = -3;
pub const ICECHUNK_ERROR_NULL_ARGUMENT: i32 = -4;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/// Store an error message in thread-local storage.
pub(crate) fn set_last_error(msg: String) {
    LAST_ERROR.with(|cell| {
        // Replace interior NULs so CString::new won't fail.
        let sanitized = msg.replace('\0', "\\0");
        *cell.borrow_mut() = CString::new(sanitized).ok();
    });
}

/// Clear the last error.
pub(crate) fn clear_last_error() {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Return a pointer to the thread-local error string, or null if none.
///
/// # Safety
/// The returned pointer is valid until the next icechunk call on this thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_last_error() -> *const libc::c_char {
    LAST_ERROR.with(|cell| {
        let borrow = cell.borrow();
        match borrow.as_ref() {
            Some(cstr) => cstr.as_ptr(),
            None => ptr::null(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_roundtrip() {
        clear_last_error();
        let ptr = unsafe { icechunk_last_error() };
        assert!(ptr.is_null());

        set_last_error("something went wrong".to_string());
        let ptr = unsafe { icechunk_last_error() };
        assert!(!ptr.is_null());
        let msg = unsafe { std::ffi::CStr::from_ptr(ptr) };
        assert_eq!(msg.to_str().unwrap(), "something went wrong");

        clear_last_error();
        let ptr = unsafe { icechunk_last_error() };
        assert!(ptr.is_null());
    }

    #[test]
    fn test_error_with_nul_byte() {
        set_last_error("bad\0byte".to_string());
        let ptr = unsafe { icechunk_last_error() };
        assert!(!ptr.is_null());
        let msg = unsafe { std::ffi::CStr::from_ptr(ptr) };
        assert_eq!(msg.to_str().unwrap(), "bad\\0byte");
    }
}
