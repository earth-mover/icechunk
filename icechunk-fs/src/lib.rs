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

use std::time::Duration;

#[cfg(unix)]
pub use storage::{
    FilesystemStorage, new_filesystem_storage, new_filesystem_storage_with,
};

const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(60);

/// Settings of a filesystem storage, see [`new_filesystem_storage_with`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FilesystemSettings {
    /// fsync files and directories after writes. Turning it off risks losing
    /// writes after an OS crash or power loss; it never affects safety
    /// between concurrent writers.
    pub fsync: bool,
    /// How old a `repo` lock must be before another writer may steal it.
    /// It must exceed how long a writer holds the lock: from taking it to
    /// its commit rename, only a few metadata operations, slower on network
    /// filesystems. Shorter values make writers steal live locks and cause
    /// spurious conflicts; longer values delay recovery after a writer
    /// crashes while holding the lock. Safety never depends on it.
    pub stale_after: Duration,
}

impl Default for FilesystemSettings {
    fn default() -> Self {
        Self { fsync: true, stale_after: DEFAULT_STALE_AFTER }
    }
}

impl FilesystemSettings {
    /// Sets [`Self::fsync`].
    pub fn with_fsync(self, fsync: bool) -> Self {
        Self { fsync, ..self }
    }

    /// Sets [`Self::stale_after`].
    pub fn with_stale_after(self, stale_after: Duration) -> Self {
        Self { stale_after, ..self }
    }
}

#[cfg(not(unix))]
const UNSUPPORTED_PLATFORM: &str =
    "FilesystemStorage is only supported on Unix platforms";

/// `FilesystemStorage` is only available on Unix platforms; this always fails.
#[cfg(not(unix))]
pub async fn new_filesystem_storage(
    _path: &std::path::Path,
) -> icechunk_storage::StorageResult<
    std::sync::Arc<dyn icechunk_storage::Storage + Send + Sync>,
> {
    Err(icechunk_storage::other_error(UNSUPPORTED_PLATFORM))
}

/// `FilesystemStorage` is only available on Unix platforms; this always fails.
#[cfg(not(unix))]
pub async fn new_filesystem_storage_with(
    _path: &std::path::Path,
    _settings: FilesystemSettings,
) -> icechunk_storage::StorageResult<
    std::sync::Arc<dyn icechunk_storage::Storage + Send + Sync>,
> {
    Err(icechunk_storage::other_error(UNSUPPORTED_PLATFORM))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_builders_set_fields() {
        let settings = FilesystemSettings::default()
            .with_fsync(false)
            .with_stale_after(Duration::from_secs(7));
        assert!(!settings.fsync);
        assert_eq!(settings.stale_after, Duration::from_secs(7));

        let defaults = FilesystemSettings::default();
        assert_eq!(defaults.with_fsync(true), defaults);
        assert_eq!(defaults.with_stale_after(DEFAULT_STALE_AFTER), defaults);
    }
}
