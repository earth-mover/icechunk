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
    time::{Duration, SystemTime},
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
    DeleteObjectsResult, ETag, GetModifiedResult, ListInfo, PutCondition, Settings,
    Storage, StorageContext, StorageError, StorageErrorKind, StorageInfo, StorageResult,
    VersionInfo, VersionedUpdateResult, WriteHint, other_error, sealed,
};
use icechunk_types::ICResultExt as _;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::{
    FilesystemSettings,
    cas::{CasError, CasOptions, CasOutcome, NoHooks, conditional_update},
    list::{delete_files, list_recursive},
    lock::is_valid_hash,
    path::{key_path, prefix_path},
    probe::probe_hard_links,
    read::{FreshRead, last_modified, read_all, read_fresh, read_range},
    sweep::sweep_root,
    version::{sha256_hex, version_of},
    write::{CreateOutcome, create_exclusive, write_new, write_staged},
};

/// A [`Storage`] on a local or shared POSIX filesystem, safe for concurrent
/// writers, including writers on different hosts of a network filesystem, under
/// the conditions below.
///
/// - Safe only if every writer of the repository uses this backend: older
///   Icechunk versions and `new_local_filesystem_storage` overwrite `repo`
///   unconditionally, so they must never run on the same repository at the
///   same time as this backend.
/// - The filesystem must meet the requirements
///   (exclusive hard links, atomic rename, fresh opens).
/// - Conditional updates use the lock-free protocol of design document 020
///   (`design-docs/020-filesystem-storage.md`). The version of an object is
///   the sha256 of its content; a conditional update of a missing object is
///   `NotOnLatestVersion`.
/// - Conditional updates require that an object never holds the same bytes
///   twice, not just in consecutive versions. Only an update to the bytes it
///   already holds is detected, as an error. The Icechunk 2 format
///   guarantees this for `repo`.
/// - A corrupt lock file makes conditional updates of its object fail until
///   it is recovered manually: once no writer is active, delete it.
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
    pub const DEFAULT_STALE_AFTER: Duration = crate::DEFAULT_STALE_AFTER;

    /// Leftovers younger than this are never swept, whatever cutoff the
    /// caller gives: a write that stalled for a long time, such as one on a
    /// suspended machine, may still be using them.
    pub const SWEEP_MIN_AGE: Duration = Duration::from_secs(24 * 60 * 60);

    /// A storage rooted at `root`, made absolute against the current
    /// directory. Does no other I/O.
    pub fn new(root: &Path) -> StorageResult<Self> {
        Self::from_settings(root, FilesystemSettings::default())
    }

    fn from_settings(root: &Path, settings: FilesystemSettings) -> StorageResult<Self> {
        Ok(Self {
            root: std::path::absolute(root).capture()?,
            fsync: settings.fsync,
            stale_after: settings.stale_after,
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

/// Creates a [`FilesystemStorage`] at `path` with the default
/// [`FilesystemSettings`].
///
/// Unlike `new_local_filesystem_storage`, it is safe for concurrent writers,
/// as long as every writer of the repository uses it: older Icechunk versions
/// and `new_local_filesystem_storage` overwrite `repo` unconditionally, so
/// they must never run on one repository at the same time as this backend.
/// Requires a filesystem that meets the requirements of design document 020
/// (exclusive hard links, atomic rename, fresh opens.
pub async fn new_filesystem_storage(
    path: &Path,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    new_filesystem_storage_with(path, FilesystemSettings::default()).await
}

/// Creates a [`FilesystemStorage`] at `path` with the given settings.
///
/// Unlike `new_local_filesystem_storage`, it is safe for concurrent writers,
/// as long as every writer of the repository uses it: older Icechunk versions
/// and `new_local_filesystem_storage` overwrite `repo` unconditionally, so
/// they must never run on one repository at the same time as this backend.
/// Requires a filesystem that meets the requirements of design document 020
/// (exclusive hard links, atomic rename, fresh opens.
pub async fn new_filesystem_storage_with(
    path: &Path,
    settings: FilesystemSettings,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    Ok(Arc::new(FilesystemStorage::from_settings(path, settings)?))
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> StorageResult<T> + Send + 'static,
) -> StorageResult<T> {
    // errors capture their SpanTrace here, and spans don't follow spawn_blocking
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || span.in_scope(f)).await.map_err(|e| {
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

/// The hash a conditional call compares against, `None` if `version` can't
/// be the version of any content. Only a valid hash is ever put into a path.
fn base_hash(version: &VersionInfo, method: &str) -> StorageResult<Option<String>> {
    let etag = version.etag().ok_or_else(|| {
        other_error(format!("FilesystemStorage: {method} needs a version with an etag"))
    })?;
    Ok(is_valid_hash(etag).then(|| etag.clone()))
}

fn cas_error(err: CasError) -> StorageError {
    match err {
        CasError::RepeatedContent => other_error(
            "FilesystemStorage: new content equals the base version's content; repo content must never repeat",
        ),
        CasError::CorruptLock { path } => other_error(format!(
            "FilesystemStorage: corrupt lock file {} needs manual recovery: once no writer is active, delete it",
            path.display()
        )),
        CasError::Io(err) => write_error(err),
    }
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
    /// appears complete or not at all. If the object already holds exactly
    /// these bytes, `Create` returns `Updated`: a retried create whose first
    /// reply was lost looks the same. `IfVersion` runs the conditional update
    /// of design document 020: the new content replaces the object only if a
    /// fresh read of it hashes to the version's etag, and is
    /// `NotOnLatestVersion` if the object is missing. A version without an
    /// etag is an error. The object must never hold the same bytes twice;
    /// only new content equal to the version's is detected, as an error. A
    /// corrupt lock file is an error that needs manual recovery. The
    /// conditions always hold: settings that disable them are ignored, with a
    /// warning. `content_type` and `metadata` are ignored.
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
                blocking(move || {
                    let etag = Some(ETag(sha256_hex(&bytes)));
                    let outcome =
                        create_exclusive(&path, &bytes, fsync).map_err(write_error)?;
                    Ok(match outcome {
                        CreateOutcome::Created { generation } => {
                            VersionedUpdateResult::Updated {
                                new_version: VersionInfo { etag, generation },
                            }
                        }
                        CreateOutcome::Exists => {
                            VersionedUpdateResult::NotOnLatestVersion
                        }
                    })
                })
                .await
            }
            PutCondition::IfVersion(version) => {
                let Some(base_hash) = base_hash(version, "IfVersion")? else {
                    return Ok(VersionedUpdateResult::NotOnLatestVersion);
                };
                let path = key_path(&self.root, path)?;
                let opts =
                    CasOptions { fsync: self.fsync, stale_after: self.stale_after };
                blocking(move || {
                    let outcome =
                        conditional_update(&path, &bytes, &base_hash, &opts, &NoHooks)
                            .map_err(cas_error)?;
                    Ok(match outcome {
                        CasOutcome::Updated { generation } => {
                            VersionedUpdateResult::Updated {
                                new_version: VersionInfo {
                                    etag: Some(ETag(sha256_hex(&bytes))),
                                    generation,
                                },
                            }
                        }
                        CasOutcome::NotOnLatestVersion => {
                            VersionedUpdateResult::NotOnLatestVersion
                        }
                    })
                })
                .await
            }
        }
    }

    /// Reads `from` through a new file descriptor and, if its content hashes
    /// to `version`'s etag, writes those same bytes to `to`, staged and
    /// renamed into place. The copy is `version`'s content whatever happens
    /// concurrently; it takes no lock. Returns `version` unchanged. A version
    /// without an etag is an error; a missing `from` is `ObjectNotFound`. The
    /// check always runs: a setting that disables it is ignored, with a
    /// warning. `content_type` is ignored.
    async fn copy_object(
        &self,
        ctx: &StorageContext<'_>,
        from: &str,
        to: &str,
        _content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        self.warn_if_unsafe_settings(ctx.settings);
        self.ensure_links_supported().await?;
        let Some(hash) = base_hash(version, "copy_object")? else {
            return Ok(VersionedUpdateResult::NotOnLatestVersion);
        };
        let from = key_path(&self.root, from)?;
        let to = key_path(&self.root, to)?;
        let fsync = self.fsync;
        let version = version.clone();
        blocking(move || {
            let bytes = read_all(&from).map_err(io_error)?;
            if sha256_hex(&bytes) != hash {
                return Ok(VersionedUpdateResult::NotOnLatestVersion);
            }
            write_staged(&to, &bytes, fsync).map_err(write_error)?;
            Ok(VersionedUpdateResult::Updated { new_version: version })
        })
        .await
    }

    /// The order is unspecified: it is the directory walk order. Staging and
    /// protocol files are not listed, and non-UTF-8 names are skipped with a
    /// warning; `created_at` is the file's mtime.
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

    /// Sweeps only the repository root: tickets, probe files and staging
    /// files, lock files whose hash isn't the current content of their
    /// object, and lockinfos not linked as a lock it keeps. The cutoff is never later than
    /// [`FilesystemStorage::SWEEP_MIN_AGE`] ago. Per-file failures are logged
    /// and left out of the result. See design document 020, "Sweeping".
    async fn sweep_leftovers(
        &self,
        _ctx: &StorageContext<'_>,
        older_than: DateTime<Utc>,
        dry_run: bool,
    ) -> StorageResult<DeleteObjectsResult> {
        let floor = SystemTime::now()
            .checked_sub(Self::SWEEP_MIN_AGE)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let cutoff = SystemTime::from(older_than).min(floor);
        let root = self.root.clone();
        blocking(move || sweep_root(&root, cutoff, dry_run).map_err(write_error)).await
    }

    async fn get_object_last_modified(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        let path = key_path(&self.root, path)?;
        blocking(move || last_modified(&path).map_err(io_error)).await
    }

    /// Reads `path` through a new file descriptor. The version's etag, the
    /// sha256 of the content, is the object's identity: `OnLatestVersion`
    /// means the content is `previous_version`'s. Its generation, from an
    /// `fstat` of the descriptor, is a fast path: when it matches
    /// `previous_version`'s, the file isn't read.
    async fn get_object_conditional(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        let path = key_path(&self.root, path)?;
        let known_generation =
            previous_version.and_then(VersionInfo::generation).cloned();
        let read = blocking(move || {
            let read =
                read_fresh(&path, known_generation.as_deref()).map_err(io_error)?;
            Ok(match read {
                FreshRead::Unchanged => None,
                FreshRead::Content { bytes, generation } => {
                    let version = version_of(&bytes, Some(generation));
                    Some((bytes, version))
                }
            })
        })
        .await?;
        let Some((bytes, new_version)) = read else {
            return Ok(GetModifiedResult::OnLatestVersion);
        };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_settings() {
        assert_eq!(
            FilesystemSettings::default(),
            FilesystemSettings { fsync: true, stale_after: Duration::from_secs(60) }
        );
        assert_eq!(
            FilesystemSettings::default().stale_after,
            FilesystemStorage::DEFAULT_STALE_AFTER
        );
    }

    #[test]
    fn from_settings_applies_settings() -> StorageResult<()> {
        let dir = tempfile::tempdir().capture()?;
        let stale_after = Duration::from_secs(7);
        let st = FilesystemStorage::from_settings(
            dir.path(),
            FilesystemSettings { fsync: false, stale_after },
        )?;
        assert!(!st.fsync);
        assert_eq!(st.stale_after, stale_after);

        let st = FilesystemStorage::new(dir.path())?;
        assert!(st.fsync);
        assert_eq!(st.stale_after, Duration::from_secs(60));
        Ok(())
    }
}
