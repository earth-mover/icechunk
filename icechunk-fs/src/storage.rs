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
use icechunk_format::layout_v1;
use icechunk_storage::{
    DeleteObjectsResult, ETag, GcPhase, GetModifiedResult, ListInfo, PutCondition,
    Settings, Storage, StorageContext, StorageError, StorageErrorKind, StorageInfo,
    StorageResult, StorageWriterFeatures, VersionInfo, VersionedUpdateResult, WriteHint,
    other_error, sealed, split_in_multiple_requests,
};
use icechunk_types::ICResultExt as _;
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncBufRead, sync::OnceCell};

use crate::{
    FilesystemSettings,
    hooks::{NoHooks, ProtocolHooks},
    list::{delete_files, list_recursive},
    names::{REPO, Version, sha256_hex},
    path::{key_path, prefix_path},
    read::{last_modified, read_all, read_range, slice_range},
    repo_d::{
        ProtocolError, RepoD,
        commit::{UpdateOutcome, check_change_bytes},
        convert::ConversionOutcome,
        lifecycle::{CreateOutcome as RepoCreateOutcome, FEATURES, check_in_layout},
        mirror::PublishOutcome,
        read::{Current, Since},
        sweep::sweep,
    },
    write::{CreateOutcome, create_exclusive, sync_data_dirs, write_new, write_staged},
};

/// A [`Storage`] on a local or shared distributed filesystem, safe for concurrent
/// writers, including writers on different hosts of a network filesystem, under
/// the conditions below. See design document 020
/// (`design-docs/020-filesystem-storage.md`) and the filesystem layout v1 page
/// of the specification.
///
/// - The repo info is stored in the filesystem layout v1: `repo.d/` holds its
///   versions and a marker naming the current one, and `repo` is the mirror,
///   a byte-identical copy of a version that was current. Every change
///   publishes the mirror before returning. Other backends and older Icechunk
///   versions read the repository through the mirror, read-only: the repo
///   info lists the writer feature `filesystem-layout-v1`, which they don't
///   implement.
/// - The mirror can lag behind the current version after a crash or under
///   contention. [`FilesystemStorage::publish_mirror`] repairs it on a
///   best-effort basis; a change, such as setting the status `ReadOnly`,
///   makes it current once every process that changed the repository
///   earlier has finished.
/// - Writing `repo` with anything that doesn't implement the layout (Icechunk
///   2.0 to 2.3 changing the status, `force_write_repo_info` through another
///   backend, other tools) forks the repository, which is unsupported. The
///   fork check before every publication finds forks on a best-effort basis
///   and never overwrites one it finds; recovery is manual.
/// - Repositories in the standard layout, as `new_local_filesystem_storage`
///   and older Icechunk versions write them, are refused until converted,
///   once, with [`FilesystemStorage::convert_to_filesystem_layout`].
///   [`FilesystemStorage::convert_to_standard_layout`] converts back.
/// - The filesystem must provide hard links, atomic rename by name and
///   exclusive directory rename.
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

    /// Whether data files are fsynced, and their directories before every
    /// change. On by default; without it, a power loss can leave `repo`
    /// pointing at empty or missing files. The fsyncs of the change itself
    /// always run: those of `repo.d`, of `repo` and the root directory, and of
    /// a conversion's backup.
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

    /// Converts a repository in the standard layout, as
    /// `new_local_filesystem_storage` and older Icechunk versions write it, to
    /// the filesystem layout v1. It backs `repo` up to `overwritten/`, builds
    /// `repo.d` whose first version is that repo info with the writer feature
    /// `filesystem-layout-v1` added, and publishes it as the mirror `repo`.
    /// Only the writer features, the status gate and the ops log change; the
    /// repository's own status is kept, whatever it is.
    ///
    /// Administrative: run it with nothing else using the repository, no
    /// writer in any process and no other administrative operation; anything
    /// else is undefined behaviour. If it is interrupted, run it again under
    /// the same condition: the rerun completes the conversion. It returns
    /// [`ConversionOutcome::NothingToConvert`] for a repository in the
    /// filesystem layout, and [`StorageErrorKind::ObjectNotFound`] if there is
    /// no repository.
    ///
    /// Refuses, writing nothing, a repository that lists writer features this
    /// storage doesn't implement ([`StorageErrorKind::MissingWriterFeatures`]),
    /// a malformed repo info, and an interrupted conversion to the standard
    /// layout ([`StorageErrorKind::LayoutConversionInProgress`]). A rerun that
    /// finds `repo.d` next to a `repo` it can't attribute to its earlier run
    /// stops and names the manual recovery.
    pub async fn convert_to_filesystem_layout(&self) -> StorageResult<ConversionOutcome> {
        self.sync_data_dirs().await?;
        self.on_repo(|repo| repo.convert_from_standard())
            .await?
            .ok_or_else(repo_not_found)
    }

    /// Converts a repository in the filesystem layout v1 back to the standard
    /// layout that `new_local_filesystem_storage` and older Icechunk versions
    /// write. It backs the current version up to `overwritten/`, changes the
    /// repo info to that version with the writer feature
    /// `filesystem-layout-v1` removed, publishes it as `repo`, then renames
    /// `repo.d` aside and deletes it. Only the writer features, the status
    /// gate and the ops log change; the repository's own status is kept,
    /// whatever it is.
    ///
    /// Administrative: run it with nothing else using the repository, no
    /// writer in any process, no process still publishing the mirror and no
    /// other administrative operation; anything else is undefined behaviour.
    /// If it is interrupted, run it again under the same condition: the rerun
    /// completes the conversion. Until then, every write and GC through this
    /// storage refuses with [`StorageErrorKind::LayoutConversionInProgress`].
    /// It returns [`ConversionOutcome::NothingToConvert`] for a repository in
    /// the standard layout, and [`StorageErrorKind::ObjectNotFound`] if there
    /// is no repository.
    ///
    /// Refuses, changing nothing, a repository that lists writer features this
    /// storage doesn't implement ([`StorageErrorKind::MissingWriterFeatures`]),
    /// a malformed repo info, a fork ([`StorageErrorKind::RepositoryForked`]),
    /// which includes a conversion from the standard layout interrupted after
    /// its directory rename, and a `repo` that can't be read or decoded
    /// ([`StorageErrorKind::MirrorUnreadable`]).
    pub async fn convert_to_standard_layout(&self) -> StorageResult<ConversionOutcome> {
        self.sync_data_dirs().await?;
        self.on_repo(|repo| repo.convert_to_standard()).await?.ok_or_else(repo_not_found)
    }

    /// Publishes the current version of the repository as the mirror `repo`,
    /// repairing a mirror left behind by a process that died between its
    /// change and its publication, or by a publication that gave up. It never
    /// changes the repo info and is safe while writers run.
    ///
    /// Best effort: it publishes the version its listing shows, which a change
    /// landing meanwhile, or a lagging listing, can make an older version than
    /// the current one, and it gives up after four rounds while the marker
    /// keeps moving. The way to a current mirror is a change, such as
    /// `set_status(ReadOnly)`, made once every process that changed the
    /// repository earlier has finished.
    ///
    /// Refuses a current version without `filesystem-layout-v1`
    /// ([`StorageErrorKind::LayoutConversionInProgress`]), a fork
    /// ([`StorageErrorKind::RepositoryForked`]) and a `repo` that can't be read
    /// or decoded ([`StorageErrorKind::MirrorUnreadable`]), writing nothing.
    pub async fn publish_mirror(&self) -> StorageResult<PublishOutcome> {
        self.on_repo(|repo| classified(repo, repo.publish_mirror()?))
            .await?
            .ok_or_else(repo_not_found)
    }

    /// The `open` classification, the first time this instance touches
    /// `repo`. With `repo.d` it doesn't read the mirror `repo`. Only success
    /// is cached; a root without a repository is checked again.
    async fn check_repo_once(&self) -> StorageResult<()> {
        if self.repo_checked.initialized() {
            return Ok(());
        }
        if self.on_repo(|repo| repo.opens()).await? {
            let _ = self.repo_checked.set(());
        }
        Ok(())
    }

    /// The barrier before `repo` can name a new version: data files are
    /// fsynced, but the names of data files only here. Skipped without fsync.
    async fn sync_data_dirs(&self) -> StorageResult<()> {
        if !self.fsync {
            return Ok(());
        }
        let root = self.root.clone();
        blocking(move || sync_data_dirs(&root).map_err(write_error)).await
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
/// [`FilesystemStorage::convert_to_filesystem_layout`]. Requires a filesystem that
/// meets the requirements (hard links, atomic rename
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
/// [`FilesystemStorage::convert_to_filesystem_layout`]. Requires a filesystem that
/// meets the requirements (hard links, atomic rename
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
        ProtocolError::Io(err) => write_error(err),
        ProtocolError::NotInLayout(msg) => {
            other_error(format!("FilesystemStorage: {msg}"))
        }
        ProtocolError::StandardLayout => other_error(format!(
            "FilesystemStorage: the repository in {root} is in the standard layout: \
             {root}/repo doesn't list the writer feature {feature} and {root}/repo.d \
             doesn't exist. Stop every writer, then run convert_to_filesystem_layout() \
             once",
            feature = layout_v1::FEATURE
        )),
        ProtocolError::OrphanMirror => other_error(format!(
            "FilesystemStorage: {root}/repo lists the writer feature {feature} but \
             {root}/repo.d doesn't exist: an orphan mirror, repo.d was deleted or not \
             copied. Restore repo.d if a copy exists. Otherwise create repo.d/ holding \
             <N>.<h> with the bytes of {root}/repo, where N is the current Unix time in \
             milliseconds and h their SHA-256, and an empty latest.<K>.<N>.<h>, \
             K = 999999999999999 - N in 15 digits; changes after the version the \
             mirror holds are lost",
            feature = layout_v1::FEATURE
        )),
        ProtocolError::RepoUnreadable(why) => other_error(format!(
            "FilesystemStorage: {root}/repo can't be read or decoded and {root}/repo.d \
             doesn't exist ({why}); nothing overwrites it"
        )),
        ProtocolError::ManualRecovery(msg) => other_error(format!(
            "FilesystemStorage: the repository in {root} needs manual recovery: {msg}"
        )),
        ProtocolError::MissingFeatures { missing, min_version } => {
            StorageError::capture(StorageErrorKind::MissingWriterFeatures {
                missing,
                min_version,
            })
        }
        ProtocolError::Malformed(msg) | ProtocolError::Incomplete(msg) => {
            other_error(format!("FilesystemStorage: {msg}"))
        }
        ProtocolError::InvalidVersion(msg) => {
            other_error(format!("FilesystemStorage: {msg}"))
        }
        ProtocolError::NoConsistentView(cause) => other_error(format!(
            "FilesystemStorage: no consistent view of {root}/repo.d: {cause}"
        )),
        ProtocolError::Corrupt(msg) => other_error(format!("FilesystemStorage: {msg}")),
        ProtocolError::Forked(detail) => {
            StorageError::capture(StorageErrorKind::RepositoryForked(detail))
        }
        ProtocolError::ForkedAfterUpdate { new, detail } => {
            StorageError::capture(StorageErrorKind::RepositoryForkedAfterUpdate {
                new_version: Box::new(new.to_version_info()),
                detail,
            })
        }
        ProtocolError::GcWithoutChange(msg) => {
            other_error(format!("FilesystemStorage: {msg}"))
        }
        ProtocolError::MirrorUnreadable(detail) => {
            StorageError::capture(StorageErrorKind::MirrorUnreadable(detail))
        }
        ProtocolError::ConversionInProgress(detail) => {
            StorageError::capture(StorageErrorKind::LayoutConversionInProgress(detail))
        }
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

    /// Before the deletes, publishes the version GC's change made, and
    /// refuses unless the mirror then holds it or a later version: a fork, a
    /// mirror that can't be read or decoded, or a publication that installed
    /// nothing. After the deletes, publishes once more from the listed
    /// version, never one below GC's; failures are logged. Without the
    /// version of GC's change it refuses while `repo.d` exists.
    async fn gc_hook(
        &self,
        _ctx: &StorageContext<'_>,
        phase: GcPhase<'_>,
    ) -> StorageResult<()> {
        let (changed, before_deletes) = match phase {
            GcPhase::BeforeDeletes { changed } => (changed, true),
            GcPhase::AfterDeletes { changed } => (changed, false),
        };
        let Some(changed) = changed else {
            return self.on_repo(|repo| repo.refuse_gc_without_change()).await;
        };
        let changed = match parse_version(changed) {
            Ok(changed) => changed,
            Err(err) if before_deletes => return Err(protocol_error(&self.root, err)),
            // GC has deleted already: failures after the deletes are logged.
            Err(err) => {
                tracing::error!(
                    root = %self.root.display(),
                    "Publishing the mirror after GC's deletes failed: the version of GC's \
                     change is invalid: {err:?}. FilesystemStorage::publish_mirror() \
                     repairs a mirror left behind"
                );
                return Ok(());
            }
        };
        self.on_repo(move |repo| {
            if before_deletes {
                repo.publish_before_gc_deletes(&changed)
            } else {
                repo.publish_after_gc_deletes(&changed);
                Ok(())
            }
        })
        .await
    }

    async fn writer_features(&self) -> StorageResult<StorageWriterFeatures> {
        Ok(StorageWriterFeatures::new(FEATURES, []))
    }

    async fn create_location_if_needed(&self) -> StorageResult<()> {
        let root = self.root.clone();
        blocking(move || std::fs::create_dir_all(&root).capture()).await
    }

    /// `repo` is the current version of the repository, read whole and then
    /// sliced. A repository in the standard layout is an error naming the
    /// remedy.
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

    /// On `repo`, `Create` creates the repository, whose repo info must list
    /// `filesystem-layout-v1`: its first version, numbered with the Unix time
    /// in milliseconds, and the mirror `repo`. It is `NotOnLatestVersion` if
    /// the repository exists or `repo` holds anything but an interrupted
    /// creation, and an error naming the remedy if `repo` is a repository in
    /// the standard layout. `IfVersion` changes the repo info to a new
    /// version on top of the given one, which needs both its etag and its
    /// generation, and publishes it as the mirror `repo` before returning.
    /// Its bytes must list `filesystem-layout-v1`, otherwise it is
    /// [`StorageErrorKind::LayoutConversionInProgress`] and nothing is
    /// written. A change that landed is `Updated` even when its publication
    /// failed, which is logged and repaired by the next change, except when
    /// the publication found a fork:
    /// [`StorageErrorKind::RepositoryForkedAfterUpdate`], the change is
    /// durable.
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
                check_in_layout(&bytes).map_err(|err| protocol_error(&self.root, err))?;
                self.sync_data_dirs().await?;
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
                    .and_then(|base| check_change_bytes(&bytes).map(|()| base))
                    .map_err(|err| protocol_error(&self.root, err))?;
                self.check_repo_once().await?;
                self.sync_data_dirs().await?;
                self.on_repo(move |repo| {
                    Ok(match repo.change(&bytes, &base)? {
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
    /// and its generation, or returns `NotOnLatestVersion` once a listing of
    /// `repo.d` shows a later version or a later commit has deleted them. From
    /// other keys, copies the content if it hashes to
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
                    let Some(bytes) = repo.read_backup_source(&base)? else {
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

    /// The order is unspecified: it is the directory walk order. At the root,
    /// `repo` is the mirror, with its own size and modification time, which
    /// can differ from the current version's (see
    /// [`Storage::get_object_last_modified`]); `repo.d`, `repo.d.tmp.*` and
    /// staging files are not listed. Non-UTF-8 names are skipped with a
    /// warning; `created_at` is the file's mtime.
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
    /// [`FilesystemStorage::SWEEP_MIN_AGE`] ago: `repo.d/stage.*` and
    /// `repo.d/mirror.*`, and in the root `repo.d.tmp.*` and staging files,
    /// `repo.staging.*` included.
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

    /// For `repo`, the modification time of the current version's file: it
    /// describes the object `get_object` returns, not the mirror.
    async fn get_object_last_modified(
        &self,
        _ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        if path == REPO {
            self.check_repo_once().await?;
            let modified = self
                .on_repo(|repo| classified(repo, repo.current_modified()?))
                .await?
                .ok_or_else(repo_not_found)?;
            return Ok(modified.into());
        }
        let path = key_path(&self.root, path)?;
        blocking(move || last_modified(&path).map_err(io_error)).await
    }

    /// On `repo`, the version is the current version's hash as the etag and
    /// its number as the generation, and `OnLatestVersion` means the current
    /// version is `previous_version` or an older one than it: a listing can
    /// lag. A `previous_version` without both is never on the latest version.
    /// A repository in the standard layout is an error naming the remedy.
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
