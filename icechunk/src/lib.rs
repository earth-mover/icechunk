//! A transactional storage engine for Zarr.
//!
//! Icechunk provides version-controlled, transactional access to Zarr data on cloud
//! object storage. It adds a layer of indirection between Zarr keys and on-disk storage,
//! enabling:
//!
//! - **Serializable isolation** - Reads are isolated from concurrent writes and always
//!   use a committed snapshot. Writes are committed atomically and never partially visible.
//! - **Time travel** - Previous snapshots remain accessible after new ones are written.
//! - **Version control** - Repositories support branches (mutable) and tags (immutable)
//!   that reference snapshots.
//!
//! # Core concepts
//!
//! A **repository** contains a Zarr hierarchy (groups and arrays) with full version history.
//! Each update creates a new **snapshot** with a unique ID. **Branches** are mutable
//! references to snapshots (like Git branches), while **tags** are immutable references
//! (like Git tags). The default branch is `main`.
//!
//! # Key types
//!
//! - [`Repository`] - Handle to a versioned data store
//! - [`session::Session`] - Transaction context for reading/writing data
//! - [`Store`] - Zarr-compatible key-value interface backed by a Session
//!
//! # Architecture
//!
//! ```text
//! Repository ─creates─► Session
//!                           ├── ChangeSet (uncommitted modifications)
//!                           └── AssetManager (typed I/O with caching)
//!                                   └── Storage (S3, GCS, Azure, local, etc.)
//! ```
//!
pub mod asset_manager;
pub mod change_set;
pub mod cli;
pub mod config;
pub mod conflicts;
pub mod error;
pub mod format;
pub mod inspect;
pub mod migrations;
pub mod ops;
pub mod refs;
pub mod repository;
pub mod session;
pub mod storage;
pub mod store;
#[cfg(test)]
pub mod strategies;
mod stream_utils;
pub mod virtual_chunks;

pub use config::{ObjectStoreConfig, RepositoryConfig};
pub use repository::Repository;
#[cfg(feature = "object-store-fs")]
pub use storage::new_local_filesystem_storage;
#[cfg(feature = "object-store-s3")]
pub use storage::new_s3_object_store_storage;
#[cfg(feature = "s3")]
pub use storage::new_s3_storage;
pub use storage::{ObjectStorage, Storage, StorageError, new_in_memory_storage};
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
    use tracing::Level;
    use tracing_error::ErrorLayer;
    use tracing_subscriber::{
        EnvFilter, Layer, Registry, layer::SubscriberExt, reload, util::SubscriberInitExt,
    };

    // We have two Layers. One keeps track of the spans to feed the ICError instances.
    // The other is the one spitting logs to stdout. Filtering only applies to the second Layer.

    let filter = log_filter_directive.map(EnvFilter::new).unwrap_or_else(|| {
        EnvFilter::from_env("ICECHUNK_LOG").add_directive(Level::WARN.into())
    });
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
