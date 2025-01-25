//! General design:
//! - Most things are async even if they don't need to be. Async propagates unfortunately. If
//!   something can be async sometimes it needs to be async always. In our example: fetching from
//!   storage.
//! - There is a high level interface that knows about arrays, groups, user attributes, etc. This
//!   is the [`repository::Repository`] type.
//! - There is a low level interface that speaks zarr keys and values, and is used to provide the
//!   zarr store that will be used from python. This is the [`zarr::Store`] type.
//! - There is a translation language between low and high levels. When user writes to a zarr key,
//!   we need to convert that key to the language of arrays and groups. This is implemented it the
//!   [`zarr`] module
//! - There is an abstract type for loading and saving of the Arrow datastructures.
//!   This is the [`Storage`] trait. It knows how to fetch and write arrow.
//!   We have:
//!     - an in memory implementation
//!     - an s3 implementation that writes to parquet
//!     - a caching wrapper implementation
//! - The datastructures are represented by concrete types in the [`mod@format`] modules.
//!   These datastructures use Arrow RecordBatches for representation.
pub mod asset_manager;
pub mod change_set;
pub mod config;
pub mod conflicts;
pub mod format;
pub mod metadata;
pub mod ops;
pub mod refs;
pub mod repository;
pub mod session;
pub mod storage;
pub mod store;
#[cfg(test)]
pub mod strategies;
pub mod virtual_chunks;

pub use config::{ObjectStoreConfig, RepositoryConfig};
pub use repository::Repository;
pub use storage::{
    new_in_memory_storage, new_local_filesystem_storage, new_s3_storage, ObjectStorage,
    Storage, StorageError,
};
pub use store::Store;

mod private {
    /// Used to seal traits we don't want user code to implement, to maintain compatibility.
    /// See https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed
    pub trait Sealed {}
}
