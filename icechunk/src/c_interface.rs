use libc::{c_char,c_int,c_void};
use std::boxed::Box;
use std::sync::Arc;
use std::ffi::CStr;
use std::ptr::null_mut;
use futures::executor::block_on;
use crate::repository::{Path, Repository};
use crate::Storage;
use crate::storage::{MemCachingStorage, ObjectStorage};

#[no_mangle]
pub extern "C" fn create_inmemory_repository(ptr: *mut *mut Repository)->c_int {
    if ptr.is_null() {
        return -1;
    }
    let storage: Arc<dyn Storage + Send + Sync> =
        Arc::new(ObjectStorage::new_in_memory_store(None));
    let ds = Repository::init(
        Arc::new(MemCachingStorage::new(Arc::clone(&storage), 2, 2, 0, 0)),
        false,
    );
    let ds_finished = block_on(ds).unwrap().build();
    unsafe {
        *ptr = null_mut();
        *ptr = Box::into_raw(Box::new(ds_finished));
    }
    return 0.into();
}


#[no_mangle]
pub unsafe extern "C" fn icechunk_add_root_group(ptr: *mut Repository)->c_int {
    let mut ds: Box<Repository> = Box::from_raw(ptr);
    let fut = ds.add_group(Path::root());
    block_on(fut).unwrap();
    // Put the box into a raw pointer to prevent it from being cleaned
    let _ = Box::into_raw(ds);
    return 0
}

#[no_mangle]
pub unsafe extern "C" fn icechunk_add_group(ptr: *mut Repository, group_name_ptr: *const c_char)->c_int {
    
    let group_name: &CStr = unsafe { CStr::from_ptr(group_name_ptr) };
    let group_name_slice: &str = group_name.to_str().unwrap();
    
    let mut ds: Box<Repository> = Box::from_raw(ptr);

    let fut = ds.add_group(group_name_slice.try_into().unwrap());
    //Block until finished
    block_on(fut).unwrap();
    // Put the box into a raw pointer to prevent it from being cleaned
    let _ = Box::into_raw(ds);
    return 0
}

#[no_mangle]
pub extern "C" fn icechunk_free_repository(repo: *mut Repository)->c_int {
    if !repo.is_null() {
        unsafe {
            drop(Box::from_raw(repo));
        }
    }
    return 0
}