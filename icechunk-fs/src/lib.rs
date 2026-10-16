//! Filesystem storage backend for icechunk. See design-docs/020-filesystem-storage.md.

#[cfg(unix)]
mod hooks;
#[cfg(unix)]
mod list;
#[cfg(unix)]
mod names;
#[cfg(unix)]
mod nudge;
#[cfg(unix)]
mod path;
#[cfg(unix)]
mod read;
#[cfg(unix)]
mod repo_d;
#[cfg(all(test, unix))]
#[path = "../tests/common/repo_infos.rs"]
mod repo_infos;
#[cfg(unix)]
mod storage;
#[cfg(unix)]
mod write;

#[cfg(unix)]
pub use repo_d::{convert::ConversionOutcome, mirror::PublishOutcome};
#[cfg(unix)]
pub use storage::{
    FilesystemStorage, new_filesystem_storage, new_filesystem_storage_with,
};

#[cfg(all(unix, feature = "fault-injection"))]
#[doc(hidden)]
pub mod fault_injection {
    pub use crate::hooks::{
        ConversionStep, CreationStep, FromStandardStep, Observed, Point, ProtocolHooks,
        Sabotage, ToStandardStep,
    };
}

/// Settings of a filesystem storage, see [`new_filesystem_storage_with`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FilesystemSettings {
    /// fsync data files after writes, and their directories before every
    /// change. Turning it off risks losing writes after an OS crash or power
    /// loss; it never affects safety between concurrent writers. The fsyncs of
    /// the change itself always run: those of `repo.d`, of `repo` and the root
    /// directory, and of a conversion's backup.
    pub fsync: bool,
}

impl Default for FilesystemSettings {
    fn default() -> Self {
        Self { fsync: true }
    }
}

impl FilesystemSettings {
    /// Sets [`Self::fsync`].
    pub fn with_fsync(self, fsync: bool) -> Self {
        Self { fsync }
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
        assert!(FilesystemSettings::default().fsync);
        assert!(!FilesystemSettings::default().with_fsync(false).fsync);
        let defaults = FilesystemSettings::default();
        assert_eq!(defaults.with_fsync(true), defaults);
    }
}
