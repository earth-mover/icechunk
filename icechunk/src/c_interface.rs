use libc::{c_int, size_t, c_char,c_uchar, c_double};
use std::boxed::Box;
use std::sync::Arc;
use crate::repository::{Path, Repository};
use crate::Storage;
use crate::storage::{MemCachingStorage, ObjectStorage};

#[no_mangle]
pub async extern "C" fn create_inmemory_repository()->Box<Repository> {
    let storage: Arc<dyn Storage + Send + Sync> =
        Arc::new(ObjectStorage::new_in_memory_store(None));
    let mut ds = Repository::init(
        Arc::new(MemCachingStorage::new(Arc::clone(&storage), 2, 2, 0, 0)),
        false,
    )
    .await.unwrap()
    .build();
    return Box::new(ds)
}


#[no_mangle]
pub unsafe extern "C" fn icechunk_add_root_group(mut dsboxed: Box<Repository>)->Box<Repository> {
    let mut ds: Repository = *dsboxed;
    ds.add_group(Path::root());
    return Box::new(ds)
}