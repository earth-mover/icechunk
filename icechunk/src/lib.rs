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
pub mod cli;
pub mod config;
pub mod conflicts;
pub mod error;
pub mod format;
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
    ObjectStorage, Storage, StorageError, new_in_memory_storage,
    new_local_filesystem_storage, new_s3_storage,
};
pub use store::Store;

mod private {
    /// Used to seal traits we don't want user code to implement, to maintain compatibility.
    /// See https://rust-lang.github.io/api-guidelines/future-proofing.html#sealed-traits-protect-against-downstream-implementations-c-sealed
    pub trait Sealed {}
}

#[cfg(feature = "logs")]
#[allow(clippy::type_complexity)]
static LOG_FILTER: std::sync::LazyLock<
    std::sync::Mutex<
        Option<
            tracing_subscriber::reload::Handle<
                tracing_subscriber::EnvFilter,
                tracing_subscriber::layer::Layered<
                    tracing_error::ErrorLayer<tracing_subscriber::Registry>,
                    tracing_subscriber::Registry,
                >,
            >,
        >,
    >,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(None));

#[cfg(feature = "logs")]
pub fn initialize_tracing(log_filter_directive: Option<&str>) {
    use tracing_error::ErrorLayer;
    use tracing_subscriber::{
        EnvFilter, Layer, Registry, layer::SubscriberExt, reload, util::SubscriberInitExt,
    };

    // We have two Layers. One keeps track of the spans to feed the ICError instances.
    // The other is the one spitting logs to stdout. Filtering only applies to the second Layer.

    let filter = log_filter_directive
        .map(EnvFilter::new)
        .unwrap_or_else(|| EnvFilter::from_env("ICECHUNK_LOG"));
    match LOG_FILTER.lock() {
        Ok(mut guard) => match guard.as_ref() {
            Some(handle) => {
                if let Err(err) = handle.reload(filter) {
                    println!("Error reloading log settings: {err}")
                }
            }
            None => {
                let (filter, handle) = reload::Layer::new(filter);
                *guard = Some(handle);
                let stdout_layer =
                    tracing_subscriber::fmt::layer().pretty().with_filter(filter);

                let error_span_layer = ErrorLayer::default();

                if let Err(err) = Registry::default()
                    .with(error_span_layer)
                    .with(stdout_layer)
                    .try_init()
                {
                    println!("Error initializing logs: {err}");
                }
            }
        },
        Err(err) => {
            println!("Error setting up logs: {err}")
        }
    }
}
