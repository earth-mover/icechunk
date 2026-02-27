//! Opaque Zarr store handle and operations.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::stream::BoxStream;
use icechunk::format::ByteRange;
use icechunk::store::{StoreErrorKind, StoreResult};
use icechunk::{Repository, Store};
use tokio::sync::RwLock;

use crate::error::*;
use crate::ffi::{alloc_c_bytes, alloc_c_string, parse_c_str};
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
/// `storage` is consumed (freed) by this call -- do NOT call
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
    let branch_str = match unsafe { parse_c_str(branch, "branch") } {
        Ok(s) => s.to_owned(),
        Err(_) => return std::ptr::null_mut(),
    };

    let storage_box = unsafe { Box::from_raw(storage) };

    crate::runtime::block_on(async {
        let repo = match Repository::create(None, storage_box.inner, HashMap::new(), None)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                set_last_error(format!("failed to create repository: {e}"));
                return std::ptr::null_mut();
            }
        };

        let session = match repo.writable_session(&branch_str).await {
            Ok(s) => s,
            Err(e) => {
                set_last_error(format!(
                    "failed to open session on branch \
                     '{branch_str}': {e}"
                ));
                return std::ptr::null_mut();
            }
        };

        let store = Store::from_session(Arc::new(RwLock::new(session))).await;

        Box::into_raw(Box::new(IcechunkStore { inner: store, _repo: repo }))
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

/// Returns 1 if the store is read-only, 0 if writable, or a negative
/// error code on failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_is_read_only(store: *const IcechunkStore) -> i32 {
    clear_last_error();
    if store.is_null() {
        set_last_error("store must not be null".to_string());
        return ICECHUNK_ERROR_NULL_ARGUMENT;
    }
    let store = unsafe { &*store };
    crate::runtime::block_on(async { i32::from(store.inner.read_only().await) })
}

// ---------------------------------------------------------------------------
// Key-value operations
// ---------------------------------------------------------------------------

/// Get the value for a Zarr key.
///
/// On success, returns `ICECHUNK_SUCCESS`, sets `*out_data` to a
/// `malloc`-allocated buffer and `*out_len` to its length.  Caller
/// must `free()` the buffer.
///
/// On not-found, returns `ICECHUNK_ERROR_NOT_FOUND` and sets
/// `*out_data` to null.
/// On other failures, returns `ICECHUNK_ERROR` and sets `*out_data`
/// to null.
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
    let key_str = match unsafe { parse_c_str(key, "key") } {
        Ok(s) => s,
        Err(code) => {
            unsafe { *out_data = std::ptr::null_mut() };
            return code;
        }
    };

    match crate::runtime::block_on(store_ref.inner.get(key_str, &ByteRange::ALL)) {
        Ok(bytes) => {
            if bytes.is_empty() {
                unsafe {
                    *out_data = std::ptr::null_mut();
                    *out_len = 0;
                };
                return ICECHUNK_SUCCESS;
            }
            match alloc_c_bytes(&bytes) {
                Ok(buf) => unsafe {
                    *out_data = buf;
                    *out_len = bytes.len();
                    ICECHUNK_SUCCESS
                },
                Err(code) => code,
            }
        }
        Err(e) => {
            unsafe { *out_data = std::ptr::null_mut() };
            let code = if matches!(e.kind, StoreErrorKind::NotFound(_)) {
                ICECHUNK_ERROR_NOT_FOUND
            } else {
                ICECHUNK_ERROR
            };
            set_last_error(format!("{e}"));
            code
        }
    }
}

/// Set the value for a Zarr key.
///
/// `data` is a pointer to `len` bytes. The data is copied; the caller
/// retains ownership of the buffer.
///
/// Returns `ICECHUNK_SUCCESS` on success, or a negative error code on
/// failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_set(
    store: *const IcechunkStore,
    key: *const libc::c_char,
    data: *const u8,
    len: libc::size_t,
) -> i32 {
    clear_last_error();

    if store.is_null() || key.is_null() {
        set_last_error("null argument".to_string());
        return ICECHUNK_ERROR_NULL_ARGUMENT;
    }
    if data.is_null() && len > 0 {
        set_last_error("data is null but len > 0".to_string());
        return ICECHUNK_ERROR_NULL_ARGUMENT;
    }

    let store_ref = unsafe { &*store };
    let key_str = match unsafe { parse_c_str(key, "key") } {
        Ok(s) => s,
        Err(code) => return code,
    };

    let bytes = if len == 0 {
        bytes::Bytes::new()
    } else {
        let slice = unsafe { std::slice::from_raw_parts(data, len) };
        bytes::Bytes::copy_from_slice(slice)
    };

    match crate::runtime::block_on(store_ref.inner.set(key_str, bytes)) {
        Ok(()) => ICECHUNK_SUCCESS,
        Err(e) => {
            set_last_error(format!("{e}"));
            ICECHUNK_ERROR
        }
    }
}

/// Check if a key exists in the store.
///
/// Returns 1 if the key exists, 0 if not, or a negative error code on
/// failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_exists(
    store: *const IcechunkStore,
    key: *const libc::c_char,
) -> i32 {
    clear_last_error();
    if store.is_null() || key.is_null() {
        set_last_error("null argument".to_string());
        return ICECHUNK_ERROR_NULL_ARGUMENT;
    }

    let store_ref = unsafe { &*store };
    let key_str = match unsafe { parse_c_str(key, "key") } {
        Ok(s) => s,
        Err(code) => return code,
    };

    match crate::runtime::block_on(store_ref.inner.exists(key_str)) {
        Ok(found) => i32::from(found),
        Err(e) => {
            set_last_error(format!("{e}"));
            ICECHUNK_ERROR
        }
    }
}

/// Delete a key from the store.
///
/// Returns `ICECHUNK_SUCCESS` on success (including if the key didn't
/// exist).  Returns a negative error code on failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_delete(
    store: *const IcechunkStore,
    key: *const libc::c_char,
) -> i32 {
    clear_last_error();
    if store.is_null() || key.is_null() {
        set_last_error("null argument".to_string());
        return ICECHUNK_ERROR_NULL_ARGUMENT;
    }

    let store_ref = unsafe { &*store };
    let key_str = match unsafe { parse_c_str(key, "key") } {
        Ok(s) => s,
        Err(code) => return code,
    };

    match crate::runtime::block_on(store_ref.inner.delete(key_str)) {
        Ok(()) => ICECHUNK_SUCCESS,
        Err(e) => {
            set_last_error(format!("{e}"));
            ICECHUNK_ERROR
        }
    }
}

// ---------------------------------------------------------------------------
// Key listing
// ---------------------------------------------------------------------------

/// Opaque iterator over key listings.
pub struct IcechunkStoreListIter {
    stream: Mutex<BoxStream<'static, StoreResult<String>>>,
}

/// Wrap a `StoreResult<BoxStream>` into a heap-allocated list iterator.
///
/// Shared implementation for `list`, `list_prefix`, and `list_dir`.
fn wrap_list_stream(
    result: StoreResult<BoxStream<'static, StoreResult<String>>>,
) -> *mut IcechunkStoreListIter {
    match result {
        Ok(stream) => {
            Box::into_raw(Box::new(IcechunkStoreListIter { stream: Mutex::new(stream) }))
        }
        Err(e) => {
            set_last_error(format!("{e}"));
            std::ptr::null_mut()
        }
    }
}

/// Begin listing all keys in the store.
///
/// Returns null on failure (check `icechunk_last_error()`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_list(
    store: *const IcechunkStore,
) -> *mut IcechunkStoreListIter {
    clear_last_error();
    if store.is_null() {
        set_last_error("store must not be null".to_string());
        return std::ptr::null_mut();
    }
    let store_ref = unsafe { &*store };
    let result = crate::runtime::block_on(store_ref.inner.list());
    wrap_list_stream(result.map(|s| s.boxed()))
}

/// Begin listing keys under a prefix.
///
/// `prefix` must be a valid null-terminated UTF-8 string.
/// Returns null on failure (check `icechunk_last_error()`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_list_prefix(
    store: *const IcechunkStore,
    prefix: *const libc::c_char,
) -> *mut IcechunkStoreListIter {
    clear_last_error();
    if store.is_null() {
        set_last_error("store must not be null".to_string());
        return std::ptr::null_mut();
    }
    let store_ref = unsafe { &*store };
    let prefix_str = match unsafe { parse_c_str(prefix, "prefix") } {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let result = crate::runtime::block_on(store_ref.inner.list_prefix(prefix_str));
    wrap_list_stream(result.map(|s| s.boxed()))
}

/// Begin listing directory contents under a prefix.
///
/// `prefix` must be a valid null-terminated UTF-8 string.
/// Returns null on failure (check `icechunk_last_error()`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_list_dir(
    store: *const IcechunkStore,
    prefix: *const libc::c_char,
) -> *mut IcechunkStoreListIter {
    clear_last_error();
    if store.is_null() {
        set_last_error("store must not be null".to_string());
        return std::ptr::null_mut();
    }
    let store_ref = unsafe { &*store };
    let prefix_str = match unsafe { parse_c_str(prefix, "prefix") } {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let result = crate::runtime::block_on(store_ref.inner.list_dir(prefix_str));
    wrap_list_stream(result.map(|s| s.boxed()))
}

/// Get the next key from a list iterator.
///
/// On success, returns `ICECHUNK_SUCCESS` and sets `*out_key` to a
/// `malloc`-allocated null-terminated string.  Caller must `free()` it.
///
/// When iteration is exhausted, returns `ICECHUNK_SUCCESS` and sets
/// `*out_key` to null.
///
/// On error, returns a negative error code.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_list_next(
    iter: *mut IcechunkStoreListIter,
    out_key: *mut *mut libc::c_char,
) -> i32 {
    clear_last_error();
    if iter.is_null() || out_key.is_null() {
        set_last_error("null argument".to_string());
        return ICECHUNK_ERROR_NULL_ARGUMENT;
    }

    let iter_ref = unsafe { &*iter };
    let Ok(mut stream) = iter_ref.stream.lock() else {
        set_last_error("mutex poisoned".to_string());
        return ICECHUNK_ERROR;
    };

    match crate::runtime::block_on(stream.next()) {
        None => {
            unsafe { *out_key = std::ptr::null_mut() };
            ICECHUNK_SUCCESS
        }
        Some(Ok(key)) => match alloc_c_string(&key) {
            Ok(buf) => {
                unsafe { *out_key = buf };
                ICECHUNK_SUCCESS
            }
            Err(code) => code,
        },
        Some(Err(e)) => {
            unsafe { *out_key = std::ptr::null_mut() };
            set_last_error(format!("{e}"));
            ICECHUNK_ERROR
        }
    }
}

/// Free a list iterator.
///
/// Does nothing if `iter` is null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn icechunk_store_list_free(iter: *mut IcechunkStoreListIter) {
    if !iter.is_null() {
        drop(unsafe { Box::from_raw(iter) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;

    fn make_test_store() -> *mut IcechunkStore {
        let storage = crate::storage::icechunk_storage_new_in_memory();
        let branch = std::ffi::CString::new("main").unwrap();
        unsafe { icechunk_store_open(storage, branch.as_ptr()) }
    }

    /// Collect all keys from a list iterator into a `Vec<String>`.
    fn collect_iter_keys(iter: *mut IcechunkStoreListIter) -> Vec<String> {
        let mut keys = Vec::new();
        loop {
            let mut key: *mut libc::c_char = std::ptr::null_mut();
            let rc = unsafe { icechunk_store_list_next(iter, &mut key) };
            assert_eq!(rc, ICECHUNK_SUCCESS);
            if key.is_null() {
                break;
            }
            let s = unsafe { CStr::from_ptr(key) }.to_str().unwrap().to_owned();
            unsafe { libc::free(key as *mut libc::c_void) };
            keys.push(s);
        }
        keys
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
        assert_eq!(ro, 0);
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

        let rc = unsafe { icechunk_store_get(store, key.as_ptr(), &mut data, &mut len) };
        assert_eq!(rc, ICECHUNK_ERROR_NOT_FOUND);
        assert!(data.is_null());

        unsafe { icechunk_store_free(store) };
    }

    #[test]
    fn test_store_set_and_get_roundtrip() {
        let store = make_test_store();
        assert!(!store.is_null());

        let meta_key = std::ffi::CString::new("array/zarr.json").unwrap();
        let meta = br#"{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0.0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"#;
        let rc = unsafe {
            icechunk_store_set(store, meta_key.as_ptr(), meta.as_ptr(), meta.len())
        };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        let key = std::ffi::CString::new("array/c/0").unwrap();
        let payload: [u8; 16] = [
            0, 0, 128, 63, // 1.0f32
            0, 0, 0, 64, // 2.0f32
            0, 0, 64, 64, // 3.0f32
            0, 0, 128, 64, // 4.0f32
        ];
        let rc = unsafe {
            icechunk_store_set(store, key.as_ptr(), payload.as_ptr(), payload.len())
        };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        let mut data: *mut u8 = std::ptr::null_mut();
        let mut len: libc::size_t = 0;
        let rc = unsafe { icechunk_store_get(store, key.as_ptr(), &mut data, &mut len) };
        assert_eq!(rc, ICECHUNK_SUCCESS);
        assert_eq!(len, 16);
        assert!(!data.is_null());

        let result = unsafe { std::slice::from_raw_parts(data, len) };
        assert_eq!(result, &payload);

        unsafe { libc::free(data as *mut libc::c_void) };
        unsafe { icechunk_store_free(store) };
    }

    #[test]
    fn test_exists_and_delete() {
        let store = make_test_store();
        assert!(!store.is_null());

        let meta_key = std::ffi::CString::new("arr/zarr.json").unwrap();
        let meta = br#"{"zarr_format":3,"node_type":"array","shape":[1],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"#;
        let rc = unsafe {
            icechunk_store_set(store, meta_key.as_ptr(), meta.as_ptr(), meta.len())
        };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        let key = std::ffi::CString::new("arr/c/0").unwrap();
        let data = [1u8, 2, 3, 4];
        let rc =
            unsafe { icechunk_store_set(store, key.as_ptr(), data.as_ptr(), data.len()) };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        assert_eq!(unsafe { icechunk_store_exists(store, meta_key.as_ptr()) }, 1);
        assert_eq!(unsafe { icechunk_store_exists(store, key.as_ptr()) }, 1);

        let rc = unsafe { icechunk_store_delete(store, key.as_ptr()) };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        assert_eq!(unsafe { icechunk_store_exists(store, key.as_ptr()) }, 0);
        assert_eq!(unsafe { icechunk_store_exists(store, meta_key.as_ptr()) }, 1);

        let missing_meta = std::ffi::CString::new("nonexistent/zarr.json").unwrap();
        let rc = unsafe { icechunk_store_delete(store, missing_meta.as_ptr()) };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        unsafe { icechunk_store_free(store) };
    }

    #[test]
    fn test_list_empty_store() {
        let store = make_test_store();
        assert!(!store.is_null());

        let iter = unsafe { icechunk_store_list(store) };
        assert!(!iter.is_null());

        let keys = collect_iter_keys(iter);
        assert!(keys.is_empty());

        unsafe { icechunk_store_list_free(iter) };
        unsafe { icechunk_store_free(store) };
    }

    #[test]
    fn test_list_after_set() {
        let store = make_test_store();
        assert!(!store.is_null());

        let meta_key = std::ffi::CString::new("myarray/zarr.json").unwrap();
        let meta = br#"{"zarr_format":3,"node_type":"array","shape":[2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"#;
        let rc = unsafe {
            icechunk_store_set(store, meta_key.as_ptr(), meta.as_ptr(), meta.len())
        };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        let key0 = std::ffi::CString::new("myarray/c/0").unwrap();
        let key1 = std::ffi::CString::new("myarray/c/1").unwrap();
        let data = [0u8; 4];
        let rc = unsafe {
            icechunk_store_set(store, key0.as_ptr(), data.as_ptr(), data.len())
        };
        assert_eq!(rc, ICECHUNK_SUCCESS);
        let rc = unsafe {
            icechunk_store_set(store, key1.as_ptr(), data.as_ptr(), data.len())
        };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        let iter = unsafe { icechunk_store_list(store) };
        assert!(!iter.is_null());

        let keys = collect_iter_keys(iter);

        assert!(
            keys.len() >= 3,
            "expected at least 3 keys, got {}: {keys:?}",
            keys.len()
        );
        assert!(keys.contains(&"myarray/zarr.json".to_string()));
        assert!(keys.contains(&"myarray/c/0".to_string()));
        assert!(keys.contains(&"myarray/c/1".to_string()));

        unsafe { icechunk_store_list_free(iter) };
        unsafe { icechunk_store_free(store) };
    }

    #[test]
    fn test_list_prefix() {
        let store = make_test_store();
        assert!(!store.is_null());

        let meta_key = std::ffi::CString::new("arr/zarr.json").unwrap();
        let meta = br#"{"zarr_format":3,"node_type":"array","shape":[2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"#;
        let rc = unsafe {
            icechunk_store_set(store, meta_key.as_ptr(), meta.as_ptr(), meta.len())
        };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        let key0 = std::ffi::CString::new("arr/c/0").unwrap();
        let data = [0u8; 4];
        let rc = unsafe {
            icechunk_store_set(store, key0.as_ptr(), data.as_ptr(), data.len())
        };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        let prefix = std::ffi::CString::new("arr").unwrap();
        let iter = unsafe { icechunk_store_list_prefix(store, prefix.as_ptr()) };
        assert!(!iter.is_null());

        let keys = collect_iter_keys(iter);

        assert!(
            keys.len() >= 2,
            "expected at least 2 keys, got {}: {keys:?}",
            keys.len()
        );
        assert!(keys.contains(&"arr/zarr.json".to_string()));
        assert!(keys.contains(&"arr/c/0".to_string()));

        unsafe { icechunk_store_list_free(iter) };
        unsafe { icechunk_store_free(store) };
    }

    #[test]
    fn test_list_dir() {
        let store = make_test_store();
        assert!(!store.is_null());

        let meta_key = std::ffi::CString::new("myarr/zarr.json").unwrap();
        let meta = br#"{"zarr_format":3,"node_type":"array","shape":[1],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"#;
        let rc = unsafe {
            icechunk_store_set(store, meta_key.as_ptr(), meta.as_ptr(), meta.len())
        };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        let key0 = std::ffi::CString::new("myarr/c/0").unwrap();
        let data = [0u8; 4];
        let rc = unsafe {
            icechunk_store_set(store, key0.as_ptr(), data.as_ptr(), data.len())
        };
        assert_eq!(rc, ICECHUNK_SUCCESS);

        let prefix = std::ffi::CString::new("myarr").unwrap();
        let iter = unsafe { icechunk_store_list_dir(store, prefix.as_ptr()) };
        assert!(!iter.is_null());

        let keys = collect_iter_keys(iter);

        assert!(
            keys.len() >= 2,
            "expected at least 2 entries, got {}: {keys:?}",
            keys.len()
        );
        assert!(keys.contains(&"zarr.json".to_string()));
        assert!(keys.contains(&"c".to_string()));

        unsafe { icechunk_store_list_free(iter) };
        unsafe { icechunk_store_free(store) };
    }

    #[test]
    fn test_list_free_null_is_safe() {
        unsafe { icechunk_store_list_free(std::ptr::null_mut()) };
    }

    #[test]
    fn test_list_null_args() {
        let iter = unsafe { icechunk_store_list(std::ptr::null()) };
        assert!(iter.is_null());

        let mut key: *mut libc::c_char = std::ptr::null_mut();
        let rc = unsafe { icechunk_store_list_next(std::ptr::null_mut(), &mut key) };
        assert_eq!(rc, ICECHUNK_ERROR_NULL_ARGUMENT);
    }
}
