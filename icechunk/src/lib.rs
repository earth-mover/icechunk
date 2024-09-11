//! General design:
//! - Most things are async even if they don't need to be. Async propagates unfortunately. If
//!   something can be async sometimes it needs to be async always. In our example: fetching from
//!   storage.
//! - There is a high level interface that knows about arrays, groups, user attributes, etc. This
//!   is the [`dataset::Dataset`] type.
//! - There is a low level interface that speaks zarr keys and values, and is used to provide the
//!   zarr store that will be used from python. This is the [`zarr::Store`] type.
//! - There is a translation language between low and high levels. When user writes to a zarr key,
//!   we need to convert that key to the language of arrays and groups. This is implmented it the
//!   [`zarr`] module
//! - There is an abstract type for loading and saving of the Arrow datastructures.
//!   This is the [`Storage`] trait. It knows how to fetch and write arrow.
//!   We have:
//!     - an in memory implementation
//!     - an s3 implementation that writes to parquet
//!     - a caching wrapper implementation
//! - The datastructures are represented by concrete types in the [`mod@format`] modules.
//!   These datastructures use Arrow RecordBatches for representation.
pub mod dataset;
pub mod format;
pub mod metadata;
pub mod refs;
pub mod rocksdb;
pub mod storage;
pub mod strategies;
pub mod zarr;

pub use dataset::{Dataset, DatasetBuilder, DatasetConfig};
pub use storage::{MemCachingStorage, ObjectStorage, Storage, StorageError};
pub use zarr::Store;
