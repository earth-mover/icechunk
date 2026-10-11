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
    split_in_multiple_requests,
};
use icechunk_types::ICResultExt as _;
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncBufRead, sync::OnceCell};

use crate::{
    FilesystemSettings,
    hooks::{NoHooks, ProtocolHooks},
    list::{delete_files, list_recursive},
    names::{REPO, Version},
    path::{key_path, prefix_path},
    read::{last_modified, read_all, read_range, slice_range},
    repo_d::{
        ProtocolError, RepoD,
        commit::UpdateOutcome,
        freeze::{FreezeResult, UnfreezeResult},
        lifecycle::{CreateOutcome as RepoCreateOutcome, MigrateOutcome},
        read::{Current, Since},
        sweep::sweep,
    },
    version::sha256_hex,
    write::{CreateOutcome, create_exclusive, write_new, write_staged},
};

/// A [`Storage`] on a local or shared POSIX filesystem, safe for concurrent
/// writers, including writers on different hosts of a network filesystem, under
/// the conditions below. See design document 020
/// (`design-docs/020-filesystem-storage.md`).
///
/// - Safe only if every writer of the repository uses this backend: older
///   Icechunk versions, `new_local_filesystem_storage` and
///   `force_write_repo_info` overwrite `repo` unconditionally, so they must
///   never write to a repository in this backend's format. Each instance
///   refuses such an overwritten repository at its first use of `repo`
///   (opening a repository creates one), so an overwrite while an instance
///   is already in use goes unnoticed until the next instance.
/// - Repositories in the single-file format of `new_local_filesystem_storage`
///   and older Icechunk versions are refused until converted, once, with
///   [`FilesystemStorage::migrate_legacy_repo`].
/// - A frozen repository ([`FilesystemStorage::freeze_repo`]) refuses commits
///   until it is unfrozen.
/// - The filesystem must provide hard links, atomic rename by name and
///   exclusive directory rename.
/// - The routing of the `repo` key and the reserved root names are
///   case-sensitive, so on a case-insensitive filesystem (APFS by default,
///   SMB) keys that differ from `repo` or `repo.d` only in case must not be
///   used through the [`Storage`] API. Icechunk never generates them.
/// - Repositories in the Icechunk 1 format are refused.
/// - `unsafe_use_conditional_create` and `unsafe_use_conditional_update` are
///   ignored, with a warning.
/// - Only available on Unix platforms.
#[derive(Debug, Serialize, Deserialize)]
pub struct FilesystemStorage {
    root: PathBuf,
    fsync: bool,
    #[serde(skip)]
    repo_checked: OnceCell<()>,
    #[serde(skip)]
    warned_unsafe_settings: AtomicBool,
    #[serde(skip, default = "no_hooks")]
    hooks: Arc<dyn ProtocolHooks>,
}

fn no_hooks() -> Arc<dyn ProtocolHooks> {
    Arc::new(NoHooks)
}

/// What [`FilesystemStorage::freeze_repo`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FreezeOutcome {
    /// The repository is now frozen at this version.
    Frozen(VersionInfo),
    /// It already was frozen.
    AlreadyFrozen,
}

/// What [`FilesystemStorage::unfreeze_repo`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnfreezeOutcome {
    /// The repository accepts commits again, on top of this new version
    /// holding the frozen version's bytes.
    Unfrozen(VersionInfo),
    /// It wasn't frozen.
    NotFrozen,
}

impl FilesystemStorage {
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
            repo_checked: OnceCell::new(),
            warned_unsafe_settings: AtomicBool::new(false),
            hooks: no_hooks(),
        })
    }

    /// Whether writes of data files `fsync` the file and its directory. On by
    /// default; without it, a power loss can leave `repo` pointing at empty
    /// files. The fsyncs of `repo` always run.
    pub fn with_fsync(self, fsync: bool) -> Self {
        Self { fsync, ..self }
    }

    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    /// Testing only: installs hooks into the `repo` protocol. Not serialized.
    pub fn with_protocol_hooks(self, hooks: Arc<dyn ProtocolHooks>) -> Self {
        Self { hooks, ..self }
    }

    /// The absolute directory holding the repository.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Stops every commit: renames the marker of the current version to a
    /// name no commit renames from. The current version stays readable, so a
    /// frozen repository can be copied or exported consistently. Writers get
    /// a [`StorageErrorKind::RepositoryFrozen`] error.
    ///
    /// Only [`FreezeOutcome::Frozen`] proves that this call froze the
    /// repository. [`FreezeOutcome::AlreadyFrozen`] comes from a directory
    /// listing that can lag, so an export must not rely on it.
    pub async fn freeze_repo(&self) -> StorageResult<FreezeOutcome> {
        self.check_repo_once().await?;
        self.on_repo(|repo| {
            Ok(match repo.freeze()? {
                FreezeResult::Frozen(version) => {
                    FreezeOutcome::Frozen(version.to_version_info())
                }
                FreezeResult::AlreadyFrozen => FreezeOutcome::AlreadyFrozen,
            })
        })
        .await
    }

    /// Accepts commits again, on top of a new version holding the frozen
    /// version's bytes: writers based on the frozen version get one conflict.
    ///
    /// Only [`UnfreezeOutcome::Unfrozen`] proves that this call unfroze the
    /// repository. [`UnfreezeOutcome::NotFrozen`] comes from a directory
    /// listing that can lag.
    pub async fn unfreeze_repo(&self) -> StorageResult<UnfreezeOutcome> {
        self.check_repo_once().await?;
        self.on_repo(|repo| {
            Ok(match repo.unfreeze()? {
                UnfreezeResult::Unfrozen(version) => {
                    UnfreezeOutcome::Unfrozen(version.to_version_info())
                }
                UnfreezeResult::NotFrozen => UnfreezeOutcome::NotFrozen,
            })
        })
        .await
    }

    /// Converts a repository in the single-file format of
    /// `new_local_filesystem_storage` and older Icechunk versions to this
    /// backend's format. Offline only: no other process may write to the
    /// repository from when this starts until every writer uses this backend.
    /// A rerun completes an interrupted migration.
    pub async fn migrate_legacy_repo(&self) -> StorageResult<MigrateOutcome> {
        self.on_repo(|repo| repo.migrate()).await
    }

    /// The `open` classification, the first time this instance touches
    /// `repo`: a `repo` overwritten by a writer that doesn't use this backend
    /// is refused even though `repo.d` still reads fine. Only success is
    /// cached; a root without a repository is checked again.
    async fn check_repo_once(&self) -> StorageResult<()> {
        if self.repo_checked.initialized() {
            return Ok(());
        }
        if self.on_repo(|repo| repo.opens()).await? {
            let _ = self.repo_checked.set(());
        }
        Ok(())
    }

    /// Runs `f` on the `repo` protocol, on a blocking thread.
    async fn on_repo<T: Send + 'static>(
        &self,
        f: impl FnOnce(RepoD<'_>) -> Result<T, ProtocolError> + Send + 'static,
    ) -> StorageResult<T> {
        let root = self.root.clone();
        let hooks = Arc::clone(&self.hooks);
        blocking(move || {
            f(RepoD { root: &root, hooks: &*hooks })
                .map_err(|err| protocol_error(&root, err))
        })
        .await
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
/// they must never write to a repository in this backend's format. Their
/// repositories must first be converted with
/// [`FilesystemStorage::migrate_legacy_repo`]. Requires a filesystem that
/// meets the requirements of design document 020 (hard links, atomic rename
/// by name and exclusive directory rename).
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
/// they must never write to a repository in this backend's format. Their
/// repositories must first be converted with
/// [`FilesystemStorage::migrate_legacy_repo`]. Requires a filesystem that
/// meets the requirements of design document 020 (hard links, atomic rename
/// by name and exclusive directory rename).
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

/// Errors that need a remedy name it in their message.
fn protocol_error(root: &Path, err: ProtocolError) -> StorageError {
    let root = root.display();
    match err {
        ProtocolError::Frozen => {
            StorageError::capture(StorageErrorKind::RepositoryFrozen)
        }
        ProtocolError::Io(err) => write_error(err),
        ProtocolError::LegacyFormat => other_error(format!(
            "FilesystemStorage: the repository in {root} uses the single-file format of \
             local_filesystem_storage or an older Icechunk: stop every writer, then run \
             migrate_legacy_repo once"
        )),
        ProtocolError::MigrationInterrupted => other_error(format!(
            "FilesystemStorage: a migration of the repository in {root} was interrupted: \
             rerun migrate_legacy_repo"
        )),
        ProtocolError::MixedFormats => other_error(format!(
            "FilesystemStorage: {root}/repo was overwritten by a writer that doesn't use \
             FilesystemStorage (an older Icechunk, local_filesystem_storage or \
             force_write_repo_info): manual recovery needed"
        )),
        ProtocolError::ManualRecovery(msg) => other_error(format!(
            "FilesystemStorage: the repository in {root} needs manual recovery: {msg}"
        )),
        ProtocolError::InvalidVersion(msg) => {
            other_error(format!("FilesystemStorage: {msg}"))
        }
        ProtocolError::NoConsistentView(cause) => other_error(format!(
            "FilesystemStorage: no consistent view of {root}/repo.d: {cause}"
        )),
        ProtocolError::Corrupt(msg) => other_error(format!("FilesystemStorage: {msg}")),
    }
}

fn parse_version(version: &VersionInfo) -> Result<Version, ProtocolError> {
    Version::from_version_info(version).map_err(ProtocolError::InvalidVersion)
}

/// A read of `repo`, where a missing `repo.d` is classified as `open` does:
/// `None` if there is no repository.
fn classified<T>(repo: RepoD<'_>, read: Option<T>) -> Result<Option<T>, ProtocolError> {
    if read.is_none() {
        repo.opens()?;
    }
    Ok(read)
}

fn repo_not_found() -> StorageError {
    StorageError::capture(StorageErrorKind::ObjectNotFound)
}

fn unconditional_write_of_repo() -> StorageError {
    other_error("FilesystemStorage: repo can only be written with put_object_conditional")
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
        blocking(move || std::fs::create_dir_all(&root).capture()).await
    }

    /// `repo` is the current version of the repository, read whole and then
    /// sliced. A single-file or half-migrated repository is an error naming
    /// the remedy.
    async fn get_object_range(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>>
    {
        let range = range.cloned();
        let bytes = if path == REPO {
            self.check_repo_once().await?;
            let repo = self.root.join(REPO);
            self.on_repo(move |repo_d| {
                let Some(Current { bytes, .. }) =
                    classified(repo_d, repo_d.read_latest()?)?
                else {
                    return Ok(None);
                };
                Ok(Some(match &range {
                    Some(range) => slice_range(&bytes, range, &repo)?,
                    None => bytes,
                }))
            })
            .await?
            .ok_or_else(repo_not_found)?
        } else {
            let path = key_path(&self.root, path)?;
            blocking(move || read_range(&path, range.as_ref()).map_err(io_error)).await?
        };
        Ok(stream::once(ready(Ok(bytes))).boxed())
    }

    /// `repo` is one read, whatever the length of `range`: reads split into
    /// several requests could each see a different version.
    async fn get_object_concurrently(
        &self,
        ctx: &StorageContext<'_>,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Pin<Box<dyn AsyncBufRead + Send>>> {
        if key == REPO {
            return self.get_object_range_read(ctx, key, Some(range)).await;
        }
        let concurrency = ctx.settings.concurrency();
        let parts = split_in_multiple_requests(
            range,
            concurrency.ideal_concurrent_request_size().get(),
            concurrency.max_concurrent_requests_for_object().get(),
        )
        .collect::<Vec<_>>();
        if parts.len() > 1 {
            self.get_object_concurrently_multiple(ctx, key, parts).await
        } else {
            self.get_object_range_read(ctx, key, Some(range)).await
        }
    }

    /// `repo` can't be written unconditionally. `content_type`, `metadata` and
    /// storage-class settings are ignored.
    async fn put_object(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        _content_type: Option<&str>,
        _metadata: Vec<(String, String)>,
        hint: WriteHint,
    ) -> StorageResult<()> {
        if path == REPO {
            return Err(unconditional_write_of_repo());
        }
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

    /// On `repo`, `Create` creates the repository with version 0 and
    /// `IfVersion` commits a new version on top of the given one, which needs
    /// both its etag and its generation; a frozen repository is a
    /// [`StorageErrorKind::RepositoryFrozen`] error. Creating it again with
    /// the same bytes returns `Updated`: a retried create whose first reply
    /// was lost looks the same.
    ///
    /// On other keys only `Create` is supported. It stages the content and
    /// hard-links it into place, so the object appears complete or not at
    /// all, and returns `Updated` if the object already holds exactly these
    /// bytes. Its version is the sha256 of the content.
    ///
    /// The conditions always hold: settings that disable them are ignored,
    /// with a warning. `content_type` and `metadata` are ignored.
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
        match condition {
            PutCondition::Create if path == REPO => {
                self.on_repo(move |repo| {
                    Ok(match repo.create(&bytes)? {
                        RepoCreateOutcome::Created(version) => {
                            VersionedUpdateResult::Updated {
                                new_version: version.to_version_info(),
                            }
                        }
                        RepoCreateOutcome::AlreadyExists => {
                            VersionedUpdateResult::NotOnLatestVersion
                        }
                    })
                })
                .await
            }
            PutCondition::IfVersion(version) if path == REPO => {
                let base = parse_version(version)
                    .map_err(|err| protocol_error(&self.root, err))?;
                self.check_repo_once().await?;
                self.on_repo(move |repo| {
                    Ok(match repo.conditional_update(&bytes, &base)? {
                        UpdateOutcome::Updated(version) => {
                            VersionedUpdateResult::Updated {
                                new_version: version.to_version_info(),
                            }
                        }
                        UpdateOutcome::NotOnLatestVersion => {
                            VersionedUpdateResult::NotOnLatestVersion
                        }
                    })
                })
                .await
            }
            PutCondition::Create => {
                let path = key_path(&self.root, path)?;
                let fsync = self.fsync;
                blocking(move || {
                    let outcome =
                        create_exclusive(&path, &bytes, fsync).map_err(write_error)?;
                    Ok(match outcome {
                        CreateOutcome::Created => VersionedUpdateResult::Updated {
                            new_version: VersionInfo {
                                etag: Some(ETag(sha256_hex(&bytes))),
                                generation: None,
                            },
                        },
                        CreateOutcome::Exists => {
                            VersionedUpdateResult::NotOnLatestVersion
                        }
                    })
                })
                .await
            }
            PutCondition::IfVersion(_) => Err(other_error(format!(
                "FilesystemStorage: conditional updates are only supported for `repo`, not {path:?}"
            ))),
        }
    }

    /// From `repo`, copies the bytes of `version`, which needs both its etag
    /// and its generation, or returns `NotOnLatestVersion` once a later commit
    /// has deleted them. From other keys, copies the content if it hashes to
    /// `version`'s etag, which is required; a missing `from` is
    /// `ObjectNotFound`. The copy is staged and renamed into place, and
    /// returns `version` unchanged. `to` can't be `repo`. The check always
    /// runs: a setting that disables it is ignored, with a warning.
    /// `content_type` is ignored.
    async fn copy_object(
        &self,
        ctx: &StorageContext<'_>,
        from: &str,
        to: &str,
        _content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        self.warn_if_unsafe_settings(ctx.settings);
        if to == REPO {
            return Err(unconditional_write_of_repo());
        }
        let to = key_path(&self.root, to)?;
        let fsync = self.fsync;
        let updated = VersionedUpdateResult::Updated { new_version: version.clone() };
        if from == REPO {
            let base =
                parse_version(version).map_err(|err| protocol_error(&self.root, err))?;
            self.check_repo_once().await?;
            return self
                .on_repo(move |repo| {
                    let Some(bytes) = repo.read_version(&base)? else {
                        return Ok(VersionedUpdateResult::NotOnLatestVersion);
                    };
                    write_staged(&to, &bytes, fsync)?;
                    Ok(updated)
                })
                .await;
        }
        let etag = version
            .etag()
            .ok_or_else(|| {
                other_error("FilesystemStorage: copy_object needs a version with an etag")
            })?
            .clone();
        let from = key_path(&self.root, from)?;
        blocking(move || {
            let bytes = read_all(&from).map_err(io_error)?;
            if sha256_hex(&bytes) != etag {
                return Ok(VersionedUpdateResult::NotOnLatestVersion);
            }
            write_staged(&to, &bytes, fsync).map_err(write_error)?;
            Ok(updated)
        })
        .await
    }

    /// The order is unspecified: it is the directory walk order. Staging and
    /// protocol files are not listed, the `repo` stub is, and non-UTF-8 names
    /// are skipped with a warning; `created_at` is the file's mtime.
    async fn list_objects<'a>(
        &'a self,
        _ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let dir = prefix_path(&self.root, prefix)?;
        let is_root = dir == self.root;
        Ok(list_recursive(dir, is_root).map(|info| info.map_err(io_error)).boxed())
    }

    /// Ids live in flat directories and `readdir` has no prefix filter, so each
    /// id-prefix listing would re-read the whole directory.
    fn lists_id_prefixes_natively(&self) -> bool {
        false
    }

    fn supports_ic1_repos(&self) -> bool {
        false
    }

    /// A missing object counts as deleted. `repo` is never deleted: like
    /// objects that fail to delete, it is logged and left out of the result.
    /// Empty directories are kept.
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
                (if prefix.is_empty() { id } else { format!("{prefix}/{id}") }, size)
            })
            .filter(|(key, _)| {
                let is_repo = key == REPO;
                if is_repo {
                    tracing::error!(
                        root = %self.root.display(),
                        "Error deleting object: FilesystemStorage never deletes repo"
                    );
                }
                !is_repo
            })
            .map(|(key, size)| Ok((key_path(&self.root, &key)?, size)))
            .collect::<StorageResult<Vec<_>>>()?;
        blocking(move || Ok(delete_files(paths))).await
    }

    /// Sweeps the version files of `repo` that its current version proves
    /// dead, whatever their age, and staging files and abandoned creation
    /// directories last modified before the cutoff, which is never later than
    /// [`FilesystemStorage::SWEEP_MIN_AGE`] ago: `repo.d/stage.*`, and in the
    /// root `repo.stub.*`, `repo.d.tmp.*` and staging files of other objects.
    /// Subdirectories other than `repo.d` aren't visited. Per-file failures
    /// are logged and left out of the result. See design document 020,
    /// "Sweep".
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
        let hooks = Arc::clone(&self.hooks);
        blocking(move || sweep(&root, cutoff, dry_run, &*hooks).map_err(write_error))
            .await
    }

    /// For `repo`, the modification time of its stub.
    async fn get_object_last_modified(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        let path = key_path(&self.root, path)?;
        blocking(move || last_modified(&path).map_err(io_error)).await
    }

    /// On `repo`, the version is the current version's hash as the etag and
    /// its number as the generation, and `OnLatestVersion` means the current
    /// version is `previous_version` or an older one than it: a listing can
    /// lag. A `previous_version` without both is never on the latest version.
    /// A single-file or half-migrated repository is an error naming the
    /// remedy.
    ///
    /// On other keys, the version's etag is the sha256 of the content, and
    /// `OnLatestVersion` means the content is `previous_version`'s.
    async fn get_object_conditional(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        if path == REPO {
            self.check_repo_once().await?;
            let previous = previous_version.and_then(|v| parse_version(v).ok());
            let read = self
                .on_repo(move |repo| {
                    let read = match &previous {
                        Some(previous) => repo.read_since(previous)?,
                        None => repo.read_latest()?.map(Since::Modified),
                    };
                    classified(repo, read)
                })
                .await?;
            return Ok(match read.ok_or_else(repo_not_found)? {
                Since::NotModified => GetModifiedResult::OnLatestVersion,
                Since::Modified(Current { bytes, version }) => {
                    GetModifiedResult::Modified {
                        data: Box::pin(Cursor::new(bytes)),
                        new_version: version.to_version_info(),
                    }
                }
            });
        }
        let path = key_path(&self.root, path)?;
        let bytes = blocking(move || read_all(&path).map_err(io_error)).await?;
        let new_version =
            VersionInfo { etag: Some(ETag(sha256_hex(&bytes))), generation: None };
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
    fn from_settings_applies_settings() -> StorageResult<()> {
        let dir = tempfile::tempdir().capture()?;
        let st = FilesystemStorage::from_settings(
            dir.path(),
            FilesystemSettings::default().with_fsync(false),
        )?;
        assert!(!st.fsync);
        assert!(FilesystemStorage::new(dir.path())?.fsync);
        Ok(())
    }
}
