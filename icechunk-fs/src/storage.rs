use std::{
    fmt,
    io::{self, Cursor},
    ops::Range,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{
    Stream, StreamExt as _,
    future::ready,
    stream::{self, BoxStream},
};
use icechunk_storage::{
    DeleteObjectsResult, GetModifiedResult, ListInfo, PutCondition, Settings, Storage,
    StorageContext, StorageError, StorageErrorKind, StorageInfo, StorageResult,
    VersionInfo, VersionedUpdateResult, WriteHint, other_error, sealed,
};
use icechunk_types::ICResultExt as _;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::{
    list::{delete_files, list_recursive},
    path::{key_path, prefix_path},
    probe::probe_hard_links,
    read::{last_modified, read_all, read_range},
    version::content_version,
    write::{CreateOutcome, create_exclusive, write_new, write_staged},
};

/// A [`Storage`] on a local or shared POSIX filesystem, safe for concurrent
/// writers, including writers on different hosts of a network filesystem.
///
/// - Conditional updates of the `repo` object use the lock-free protocol of
///   design document 020 (`design-docs/020-filesystem-storage.md`). The
///   version of an object is the sha256 of its content.
/// - Repositories in the Icechunk 1 format are refused.
/// - `unsafe_use_conditional_create` and `unsafe_use_conditional_update` are
///   ignored, with a warning.
/// - Only available on Unix platforms.
#[derive(Debug, Serialize, Deserialize)]
pub struct FilesystemStorage {
    root: PathBuf,
    fsync: bool,
    stale_after: Duration,
    #[serde(skip)]
    links_checked: OnceCell<()>,
    #[serde(skip)]
    warned_unsafe_settings: AtomicBool,
}

impl FilesystemStorage {
    /// How old a `repo` lock must be before another writer may steal it.
    pub const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(60);

    /// A storage rooted at `root`, made absolute against the current
    /// directory. Does no other I/O.
    pub fn new(root: &Path) -> StorageResult<Self> {
        Ok(Self {
            root: std::path::absolute(root).capture()?,
            fsync: true,
            stale_after: Self::DEFAULT_STALE_AFTER,
            links_checked: OnceCell::new(),
            warned_unsafe_settings: AtomicBool::new(false),
        })
    }

    /// Whether writes `fsync` files and directories. On by default; without
    /// it, a power loss can leave `repo` pointing at empty files.
    pub fn with_fsync(self, fsync: bool) -> Self {
        Self { fsync, ..self }
    }

    /// Sets the lock staleness threshold, [`Self::DEFAULT_STALE_AFTER`] by default.
    pub fn with_stale_after(self, stale_after: Duration) -> Self {
        Self { stale_after, ..self }
    }

    /// The absolute directory holding the repository.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn warn_if_unsafe_settings(&self, settings: &Settings) {
        let disabled = !settings.unsafe_use_conditional_create()
            || !settings.unsafe_use_conditional_update();
        if disabled && !self.warned_unsafe_settings.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                root = %self.root.display(),
                "FilesystemStorage ignores unsafe_use_conditional_create and \
                 unsafe_use_conditional_update: it always uses its conditional-write protocol"
            );
        }
    }

    /// Runs the hard-link probe once; only success is cached.
    async fn ensure_links_supported(&self) -> StorageResult<()> {
        self.links_checked
            .get_or_try_init(|| {
                let root = self.root.clone();
                blocking(move || probe_hard_links(&root).map_err(write_error))
            })
            .await
            .map(|_| ())
    }
}

impl fmt::Display for FilesystemStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FilesystemStorage(root={})", self.root.display())
    }
}

/// Creates a [`FilesystemStorage`] at `path`. Unlike
/// `new_local_filesystem_storage`, it is safe for concurrent writers.
pub async fn new_filesystem_storage(
    path: &Path,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    Ok(Arc::new(FilesystemStorage::new(path)?))
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> StorageResult<T> + Send + 'static,
) -> StorageResult<T> {
    tokio::task::spawn_blocking(f).await.map_err(|e| {
        other_error(format!("FilesystemStorage blocking task failed: {e}"))
    })?
}

/// Missing objects and objects under a path component that is a file.
pub(crate) fn is_missing(err: &io::Error) -> bool {
    matches!(err.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory)
}

/// For reads, listing, deletes and last-modified.
fn io_error(err: io::Error) -> StorageError {
    if is_missing(&err) {
        StorageError::capture(StorageErrorKind::ObjectNotFound)
    } else {
        write_error(err)
    }
}

/// For writes, where a missing path is a failure, not a missing object.
fn write_error(err: io::Error) -> StorageError {
    StorageError::capture(StorageErrorKind::IOError(err))
}

fn not_implemented<T>(method: &str) -> StorageResult<T> {
    Err(other_error(format!("FilesystemStorage::{method} is not implemented yet")))
}

impl sealed::Sealed for FilesystemStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for FilesystemStorage {
    fn storage_info(&self) -> StorageInfo {
        StorageInfo {
            backend_type: "filesystem",
            fields: vec![("path", self.root.display().to_string())],
        }
    }

    async fn can_write(&self) -> StorageResult<bool> {
        Ok(true)
    }

    async fn create_location_if_needed(&self) -> StorageResult<()> {
        let root = self.root.clone();
        blocking(move || std::fs::create_dir_all(&root).capture()).await?;
        self.ensure_links_supported().await
    }

    async fn get_object_range(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>>
    {
        let path = key_path(&self.root, path)?;
        let range = range.cloned();
        let bytes =
            blocking(move || read_range(&path, range.as_ref()).map_err(io_error)).await?;
        Ok(stream::once(ready(Ok(bytes))).boxed())
    }

    /// `content_type`, `metadata` and storage-class settings are ignored.
    async fn put_object(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        _content_type: Option<&str>,
        _metadata: Vec<(String, String)>,
        hint: WriteHint,
    ) -> StorageResult<()> {
        self.ensure_links_supported().await?;
        let path = key_path(&self.root, path)?;
        let fsync = self.fsync;
        blocking(move || {
            match hint {
                WriteHint::Atomic => write_staged(&path, &bytes, fsync),
                WriteHint::NewObject => write_new(&path, &bytes, fsync),
            }
            .map_err(write_error)
        })
        .await
    }

    /// `Create` stages the content and hard-links it into place, so the object
    /// appears complete or not at all. The conditions always hold: settings
    /// that disable them are ignored, with a warning. `content_type` and
    /// `metadata` are ignored.
    async fn put_object_conditional(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        _content_type: Option<&str>,
        _metadata: Vec<(String, String)>,
        condition: PutCondition<'_>,
    ) -> StorageResult<VersionedUpdateResult> {
        self.warn_if_unsafe_settings(ctx.settings);
        self.ensure_links_supported().await?;
        match condition {
            PutCondition::Create => {
                let path = key_path(&self.root, path)?;
                let fsync = self.fsync;
                let new_version = content_version(&bytes);
                let outcome = blocking(move || {
                    create_exclusive(&path, &bytes, fsync).map_err(write_error)
                })
                .await?;
                Ok(match outcome {
                    CreateOutcome::Created => {
                        VersionedUpdateResult::Updated { new_version }
                    }
                    CreateOutcome::Exists => VersionedUpdateResult::NotOnLatestVersion,
                })
            }
            PutCondition::IfVersion(_) => {
                not_implemented("put_object_conditional(IfVersion)")
            }
        }
    }

    async fn copy_object(
        &self,
        ctx: &StorageContext<'_>,
        _from: &str,
        _to: &str,
        _content_type: Option<&str>,
        _version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        self.warn_if_unsafe_settings(ctx.settings);
        self.ensure_links_supported().await?;
        not_implemented("copy_object")
    }

    /// The order is unspecified: it is the directory walk order. In-flight
    /// staging files are not listed; `created_at` is the file's mtime.
    async fn list_objects<'a>(
        &'a self,
        _ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let dir = prefix_path(&self.root, prefix)?;
        Ok(list_recursive(dir).map(|info| info.map_err(io_error)).boxed())
    }

    /// Ids live in flat directories and `readdir` has no prefix filter, so each
    /// id-prefix listing would re-read the whole directory.
    fn lists_id_prefixes_natively(&self) -> bool {
        false
    }

    fn supports_ic1_repos(&self) -> bool {
        false
    }

    /// A missing object counts as deleted. Other per-object failures are
    /// logged and left out of the result. Empty directories are kept.
    async fn delete_batch(
        &self,
        _ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        let prefix = prefix.strip_suffix('/').unwrap_or(prefix);
        let paths = batch
            .into_iter()
            .map(|(id, size)| {
                let key = if prefix.is_empty() { id } else { format!("{prefix}/{id}") };
                Ok((key_path(&self.root, &key)?, size))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        blocking(move || Ok(delete_files(paths))).await
    }

    async fn get_object_last_modified(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        let path = key_path(&self.root, path)?;
        blocking(move || last_modified(&path).map_err(io_error)).await
    }

    /// Reads `path` through a new file descriptor. The returned version's
    /// etag, the sha256 of the content, is the object's identity.
    async fn get_object_conditional(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        let path = key_path(&self.root, path)?;
        let bytes = blocking(move || read_all(&path).map_err(io_error)).await?;
        let new_version = content_version(&bytes);
        if previous_version.and_then(VersionInfo::etag) == new_version.etag() {
            Ok(GetModifiedResult::OnLatestVersion)
        } else {
            Ok(GetModifiedResult::Modified {
                data: Box::pin(Cursor::new(bytes)),
                new_version,
            })
        }
    }
}
