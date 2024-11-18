use libc::{c_char,c_int};
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
    let ds_finished = match block_on(ds) {
        Ok(inst) => inst.build(),
        Err(_) => return -1
    };
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
    let res = block_on(fut);
    // Put the box into a raw pointer to prevent it from being cleaned
    let _ = Box::into_raw(ds);
    match res {
        Ok(_) => return 0,
        Err(_) => return -1, 
    }
}

#[no_mangle]
pub unsafe extern "C" fn icechunk_add_group(ptr: *mut Repository, group_name_ptr: *const c_char)->c_int {
    
    //We wrap the char ptr into a &CStr to avoid taking ownership
    let group_name: &CStr = CStr::from_ptr(group_name_ptr);
    let path: Path = match group_name.to_str() {
        Ok(x) => match x.try_into()  {
            Ok(x) => x,
            Err(_) => return -1,
        },
        Err(_) => return -1,
    };
    
    let mut ds: Box<Repository> = Box::from_raw(ptr);

    let fut = ds.add_group(path);
    //Block until finished
    let res = block_on(fut);
    // Put the box into a raw pointer to prevent it from being cleaned
    let _ = Box::into_raw(ds);
    match res {
        Ok(_) => return 0,
        Err(_) => return -1, 
    }
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

