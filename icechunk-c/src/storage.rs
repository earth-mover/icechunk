//! Opaque storage handles and constructors.

use std::sync::Arc;

use icechunk::Storage;

use crate::error::*;
use crate::ffi::parse_c_str;

/// Opaque handle to an Icechunk storage backend.
pub struct IcechunkStorage {
    pub(crate) inner: Arc<dyn Storage + Send + Sync>,
}

/// Create an in-memory storage backend.
///
/// Returns null on failure (check `icechunk_last_error()`).
#[unsafe(no_mangle)]
pub extern "C" fn icechunk_storage_new_in_memory() -> *mut IcechunkStorage {
    clear_last_error();
    match crate::runtime::block_on(icechunk::new_in_memory_storage()) {
        Ok(storage) => Box::into_raw(Box::new(IcechunkStorage { inner: storage })),
        Err(e) => {
            set_last_error(format!("{e}"));
            std::ptr::null_mut()
        }
    }
}

/// Create a local filesystem storage backend.
///
/// `path` must be a valid null-terminated UTF-8 string.
/// Returns null on failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_storage_new_local_filesystem(
    path: *const libc::c_char,
) -> *mut IcechunkStorage {
    clear_last_error();

    let path_str = match unsafe { parse_c_str(path, "path") } {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    let path = std::path::Path::new(path_str);
    match crate::runtime::block_on(icechunk::new_local_filesystem_storage(path)) {
        Ok(storage) => Box::into_raw(Box::new(IcechunkStorage { inner: storage })),
        Err(e) => {
            set_last_error(format!("{e}"));
            std::ptr::null_mut()
        }
    }
}

/// Free a storage handle.
///
/// Does nothing if `storage` is null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_storage_free(storage: *mut IcechunkStorage) {
    if !storage.is_null() {
        drop(unsafe { Box::from_raw(storage) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_storage_roundtrip() {
        let ptr = icechunk_storage_new_in_memory();
        assert!(!ptr.is_null());
        unsafe { icechunk_storage_free(ptr) };
    }

    #[test]
    fn test_local_filesystem_storage() {
        let tmp = tempfile::tempdir().expect("failed to create tempdir");
        let path = std::ffi::CString::new(
            tmp.path().to_str().expect("tempdir path is not valid UTF-8"),
        )
        .expect("CString::new failed");
        let ptr = unsafe { icechunk_storage_new_local_filesystem(path.as_ptr()) };
        assert!(!ptr.is_null());
        unsafe { icechunk_storage_free(ptr) };
    }

    #[test]
    fn test_null_path_returns_null() {
        let ptr = unsafe { icechunk_storage_new_local_filesystem(std::ptr::null()) };
        assert!(ptr.is_null());
        let err = unsafe { icechunk_last_error() };
        assert!(!err.is_null());
    }

    #[test]
    fn test_free_null_is_safe() {
        unsafe { icechunk_storage_free(std::ptr::null_mut()) };
    }
}
