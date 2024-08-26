/// General design:
/// - Most things are async even if they don't need to be. Async propagates unfortunately. If
///   something can be async sometimes it needs to be async always. In our example: fetching from
///   storage.
/// - There is a high level interface that knows about arrays, groups, user attributes, etc.
/// - There is a low level interface that speaks zarr keys and values, and is used to provide the
///   zarr store that will be used from python
/// - There is a translation language between low and high levels. When user writes to a zarr key,
///   we need to convert that key to the language of arrays and groups.
/// - We have a facade to forward Zarr store methods to, without having to know all the complexity
///   of the Rust system. This facade is configured with appropriate information (like caching
///   mechanisms)
/// - There is an abstract type for storage of the Arrow datastructures.
///   This is the `Storage` trait. It knows how to fetch and write arrow.
///   We have:
///     - an in memory implementations
///     - an s3 implementation that writes to parquet
///     - caching decorators
/// - The datastructures are represented by concrete types (Structure|Attribute|Manifests)Table
///   Methods on those types help generating the high level interface of the library. These
///   datastructures use Arrow  RecordBatches for representation.
/// - A `Dataset` concrete type, implements the high level interface, using an Storage
///   implementation and the data tables.
pub mod dataset;
pub mod format;
pub mod metadata;
pub mod storage;
pub mod strategies;
pub mod zarr;

pub use dataset::{Dataset, DatasetBuilder, DatasetConfig};
pub use storage::{MemCachingStorage, ObjectStorage, Storage, StorageError};
pub use zarr::Store;
