//! Opaque Zarr store handle and operations.

use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::Arc;

use icechunk::format::ByteRange;
use icechunk::{Repository, Store};
use tokio::sync::RwLock;

use crate::error::*;
use crate::storage::IcechunkStorage;

/// Opaque handle to an Icechunk Zarr store.
pub struct IcechunkStore {
    pub(crate) inner: Store,
    // Keep the repo alive so storage is not dropped.
    pub(crate) _repo: Repository,
}

/// Open a new store by creating a new repository and a writable session on
/// the given branch.
///
/// `storage` is consumed (freed) by this call — do NOT call
/// `icechunk_storage_free` on it afterwards.
///
/// `branch` must be a valid null-terminated UTF-8 string (e.g. "main").
/// Returns null on failure (check `icechunk_last_error()`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_open(
    storage: *mut IcechunkStorage,
    branch: *const libc::c_char,
) -> *mut IcechunkStore {
    clear_last_error();

    if storage.is_null() {
        set_last_error("storage must not be null".to_string());
        return std::ptr::null_mut();
    }
    if branch.is_null() {
        set_last_error("branch must not be null".to_string());
        return std::ptr::null_mut();
    }

    let storage_box = unsafe { Box::from_raw(storage) };
    let branch_str = match unsafe { CStr::from_ptr(branch) }.to_str() {
        Ok(s) => s.to_owned(),
        Err(e) => {
            set_last_error(format!("invalid UTF-8 in branch: {e}"));
            return std::ptr::null_mut();
        }
    };

    crate::runtime::block_on(async {
        // Create a new repository.
        let repo = match Repository::create(
            None,
            storage_box.inner,
            HashMap::new(),
            None,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                set_last_error(format!("failed to create repository: {e}"));
                return std::ptr::null_mut();
            }
        };

        // Open a writable session on the requested branch.
        let session = match repo.writable_session(&branch_str).await {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!("failed to open session on branch '{branch_str}': {e}"));
                return std::ptr::null_mut();
            }
        };

        let session = Arc::new(RwLock::new(session));
        let store = Store::from_session(session).await;

        Box::into_raw(Box::new(IcechunkStore {
            inner: store,
            _repo: repo,
        }))
    })
}

/// Free a store handle.
///
/// Does nothing if `store` is null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_free(store: *mut IcechunkStore) {
    if !store.is_null() {
        drop(unsafe { Box::from_raw(store) });
    }
}

/// Returns 1 if the store is read-only, 0 if writable, or a negative error
/// code on failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_is_read_only(
    store: *const IcechunkStore,
) -> i32 {
    clear_last_error();
    if store.is_null() {
        set_last_error("store must not be null".to_string());
        return ICECHUNK_ERROR_NULL_ARGUMENT;
    }
    let store = unsafe { &*store };
    crate::runtime::block_on(async {
        if store.inner.read_only().await { 1 } else { 0 }
    })
}

/// Get the value for a Zarr key.
///
/// On success, returns `ICECHUNK_SUCCESS`, sets `*out_data` to a `malloc`-allocated
/// buffer and `*out_len` to its length. Caller must `free()` the buffer.
///
/// On not-found, returns `ICECHUNK_ERROR_NOT_FOUND` and sets `*out_data` to null.
/// On other failures, returns `ICECHUNK_ERROR` and sets `*out_data` to null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_get(
    store: *const IcechunkStore,
    key: *const libc::c_char,
    out_data: *mut *mut u8,
    out_len: *mut libc::size_t,
) -> i32 {
    clear_last_error();

    if store.is_null() || key.is_null() || out_data.is_null() || out_len.is_null() {
        set_last_error("null argument".to_string());
        if !out_data.is_null() {
            unsafe { *out_data = std::ptr::null_mut() };
        }
        return ICECHUNK_ERROR_NULL_ARGUMENT;
    }

    let store_ref = unsafe { &*store };
    let key_str = match unsafe { CStr::from_ptr(key) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            set_last_error(format!("invalid UTF-8 in key: {e}"));
            unsafe { *out_data = std::ptr::null_mut() };
            return ICECHUNK_ERROR;
        }
    };

    match crate::runtime::block_on(store_ref.inner.get(key_str, &ByteRange::ALL)) {
        Ok(bytes) => {
            let len = bytes.len();
            if len == 0 {
                unsafe {
                    *out_data = std::ptr::null_mut();
                    *out_len = 0;
                };
                return ICECHUNK_SUCCESS;
            }
            let buf = unsafe { libc::malloc(len) as *mut u8 };
            if buf.is_null() {
                set_last_error("malloc failed".to_string());
                return ICECHUNK_ERROR;
            }
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, len);
                *out_data = buf;
                *out_len = len;
            };
            ICECHUNK_SUCCESS
        }
        Err(e) => {
            unsafe { *out_data = std::ptr::null_mut() };
            let code =
                if matches!(e.kind, icechunk::store::StoreErrorKind::NotFound(_)) {
                    ICECHUNK_ERROR_NOT_FOUND
                } else {
                    ICECHUNK_ERROR
                };
            set_last_error(format!("{e}"));
            code
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_store() -> *mut IcechunkStore {
        let storage = crate::storage::icechunk_storage_new_in_memory();
        let branch = std::ffi::CString::new("main").unwrap();
        unsafe { icechunk_store_open(storage, branch.as_ptr()) }
    }

    #[test]
    fn test_store_open_and_free() {
        let store = make_test_store();
        assert!(!store.is_null());
        unsafe { icechunk_store_free(store) };
    }

    #[test]
    fn test_store_is_writable() {
        let store = make_test_store();
        assert!(!store.is_null());
        let ro = unsafe { icechunk_store_is_read_only(store) };
        assert_eq!(ro, 0); // writable session should not be read-only
        unsafe { icechunk_store_free(store) };
    }

    #[test]
    fn test_store_null_args() {
        let branch = std::ffi::CString::new("main").unwrap();
        let ptr = unsafe { icechunk_store_open(std::ptr::null_mut(), branch.as_ptr()) };
        assert!(ptr.is_null());

        let storage = crate::storage::icechunk_storage_new_in_memory();
        let ptr = unsafe { icechunk_store_open(storage, std::ptr::null()) };
        assert!(ptr.is_null());
    }

    #[test]
    fn test_store_free_null_is_safe() {
        unsafe { icechunk_store_free(std::ptr::null_mut()) };
    }

    #[test]
    fn test_store_is_read_only_null_returns_error() {
        let rc = unsafe { icechunk_store_is_read_only(std::ptr::null()) };
        assert_eq!(rc, ICECHUNK_ERROR_NULL_ARGUMENT);
    }

    #[test]
    fn test_store_get_not_found() {
        let store = make_test_store();
        assert!(!store.is_null());

        let key = std::ffi::CString::new("nonexistent/zarr.json").unwrap();
        let mut data: *mut u8 = std::ptr::null_mut();
        let mut len: libc::size_t = 0;

        let rc = unsafe {
            icechunk_store_get(store, key.as_ptr(), &mut data, &mut len)
        };
        assert_eq!(rc, ICECHUNK_ERROR_NOT_FOUND);
        assert!(data.is_null());

        unsafe { icechunk_store_free(store) };
    }
}
