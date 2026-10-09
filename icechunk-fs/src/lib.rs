//! Filesystem storage backend for icechunk. See design-docs/020-filesystem-storage.md.

#[cfg(unix)]
mod cas;
#[cfg(unix)]
mod leftover;
#[cfg(unix)]
mod list;
#[cfg(unix)]
mod lock;
#[cfg(unix)]
mod path;
#[cfg(unix)]
mod probe;
#[cfg(unix)]
mod read;
#[cfg(unix)]
mod storage;
#[cfg(unix)]
mod sweep;
#[cfg(unix)]
mod version;
#[cfg(unix)]
mod write;

#[cfg(unix)]
pub use storage::{FilesystemStorage, new_filesystem_storage};

/// `FilesystemStorage` is only available on Unix platforms; this always fails.
#[cfg(not(unix))]
pub async fn new_filesystem_storage(
    _path: &std::path::Path,
) -> icechunk_storage::StorageResult<
    std::sync::Arc<dyn icechunk_storage::Storage + Send + Sync>,
> {
    Err(icechunk_storage::other_error(
        "FilesystemStorage is only supported on Unix platforms",
    ))
}
