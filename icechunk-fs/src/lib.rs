//! Native local filesystem implementation of [`Storage`](icechunk_storage::Storage).
//!
//! Unlike the `object_store`-based local backend, this implementation supports
//! safe concurrent commits. The only operation that needs it — updating an
//! existing branch tip — is a compare-and-swap serialized through a lock file
//! next to the target, using content hashes as version tokens so that distinct
//! ref contents can never collide.
//!
//! # The conditional-update protocol
//!
//! Updating an existing object (the branch-tip CAS) is serialized through a lock
//! file next to the target. Each writer stamps its lock with a unique nonce.
//! Acquisition is exclusive create (`O_EXCL`); a lock older than the stale
//! timeout is presumed abandoned by a crashed writer and stolen by an atomic
//! rename-aside (exactly one stealer can rename a given inode). Immediately
//! before the commit-point rename, and again at release, the writer re-reads the
//! lock and proceeds only if it still carries its own nonce. This makes the
//! lost-commit failure require pathological timing rather than ordinary crash
//! recovery.
//!
//! ## Residual windows (all require a writer to stall past the stale timeout)
//!
//! - Commit re-verify to rename: a writer confirms its nonce, then performs the
//!   object rename. If, between those two steps, another writer both judged this
//!   lock stale and stole it, two commits could land. The gap is a single
//!   `rename` syscall, and it only opens if the first writer's lock already
//!   looks stale (i.e. it stalled longer than the timeout). This is the same
//!   bounded "paused process resuming past expiry" hazard inherent to any
//!   timeout-based lock.
//! - Stealing an already-refreshed lock: the stealer moves the lock aside and
//!   then re-checks, on the captured file, both that its nonce matches the
//!   instance judged stale and that it is *still* older than the stale timeout
//!   (rename preserves mtime). A lock that was refreshed between the staleness
//!   stat and the rename is therefore restored, not removed — a fresh lock is
//!   never stolen. The only removable instance is one that is genuinely stale,
//!   i.e. its holder stalled past the timeout (the residual above).
//!
//! `copy_object` is not serialized through the lock (it is a read-then-write of
//! content-addressed data, where races are benign), unlike `put_object`'s CAS.
//!
//! # Concurrency and portability caveats
//!
//! Correctness relies on the underlying filesystem providing atomic exclusive
//! create (`O_EXCL` / `CreateFile(CREATE_NEW)`) and atomic rename. This holds on
//! local filesystems and on properly mounted network filesystems, but:
//!
//! - `flock`/`fcntl` are never used: on Lustre they are node-local unless the
//!   filesystem is mounted `-o flock`, giving false safety on exactly the HPC
//!   systems this targets.
//! - `NFSv2` `O_EXCL` is unreliable; the `link()` idiom used for object creation
//!   is safe there, but lock acquisition relies on `O_EXCL` and should be
//!   treated with caution on very old NFS.
//! - Stale-lock detection compares the breaker's wall clock against the lock
//!   file's mtime, so it assumes the breaker's clock and the file server's clock
//!   agree to within the stale timeout. On a network filesystem with skewed
//!   clocks a fresh lock can look stale (or a stale one fresh); the default
//!   timeout is deliberately generous (5 minutes) to absorb ordinary skew, since
//!   a commit holds the lock for milliseconds.
//!
//! # Spec version and on-disk keys
//!
//! This backend writes an object key straight to disk as its filename, matching
//! how object stores treat a key as opaque bytes. Spec-v2 keys (the consolidated
//! `repo` file and hex/base32 ids) never contain bytes an object store would
//! percent-encode, so no encoding knowledge is needed here. Spec-v1 repositories,
//! which the old `object_store` local backend wrote with percent-encoded ref
//! filenames, are served by that legacy backend via Repository-level dispatch and
//! never reach this type.

use std::{
    fmt,
    io::{Cursor, SeekFrom, Write as _},
    ops::Range,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{
    Stream, StreamExt as _,
    stream::{self, BoxStream},
};
use icechunk_storage::{
    DeleteObjectsResult, GetModifiedResult, ListInfo, Settings, Storage, StorageError,
    StorageInfo, StorageResult, VersionInfo, VersionedUpdateResult, io_error,
    other_error, sealed, strip_quotes,
};
use icechunk_types::ICResultExt as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tempfile::Builder as TempFileBuilder;
use tokio::{
    fs,
    io::{AsyncReadExt as _, AsyncSeekExt as _},
    task::spawn_blocking,
    time::sleep,
};
use tracing::instrument;

/// Files ending in this suffix are lock files guarding a conditional update.
const LOCK_SUFFIX: &str = ".__icechunk_lock__";
/// Prefix for the staging files `tempfile` creates before an atomic rename, so
/// a half-written object is recognizable and never listed as a real object.
const TMP_PREFIX: &str = ".__icechunk_tmp__";
/// Infix for the transient path a stale/released lock is renamed aside to before
/// being removed. Recognizable so it is never listed and can be swept if leaked.
const GRAVEYARD_INFIX: &str = ".__icechunk_graveyard__";
/// A lock older than this is assumed abandoned by a crashed writer. Generous by
/// default: a commit holds the lock for milliseconds, so a large timeout costs
/// little and buys margin against network-filesystem clock skew.
const DEFAULT_STALE_LOCK_TIMEOUT_MS: u64 = 5 * 60 * 1000;
/// How often to re-check a contended lock while waiting for its holder.
const LOCK_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Serialize, Deserialize)]
pub struct FilesystemStorage {
    root: PathBuf,
    #[serde(default)]
    stale_lock_timeout_ms: Option<u64>,
}

impl fmt::Display for FilesystemStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FilesystemStorage(path={})", self.root.display())
    }
}

impl FilesystemStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        // Pin a relative root to an absolute path at construction, so a later
        // chdir (or a pickle carrying a relative path to another process) still
        // resolves to the same directory. `absolute` is purely lexical and, unlike
        // `canonicalize`, does not require the path to exist yet.
        let root = std::path::absolute(&root).unwrap_or(root);
        FilesystemStorage { root, stale_lock_timeout_ms: None }
    }

    /// Override the timeout after which a lock left behind by a crashed writer
    /// is broken. Lower values speed up recovery but risk breaking a lock still
    /// held by a paused (not crashed) writer.
    pub fn with_stale_lock_timeout(mut self, timeout: Duration) -> Self {
        self.stale_lock_timeout_ms = Some(timeout.as_millis() as u64);
        self
    }

    fn stale_lock_timeout(&self) -> Duration {
        Duration::from_millis(
            self.stale_lock_timeout_ms.unwrap_or(DEFAULT_STALE_LOCK_TIMEOUT_MS),
        )
    }

    /// Absolute filesystem path for a repository-relative object key.
    ///
    /// Object keys use `/` separators and may carry a leading slash. Empty, `.`,
    /// and `..` components are dropped so a key can never escape the repository
    /// root or resolve against the filesystem root.
    fn object_path(&self, relpath: &str) -> PathBuf {
        let mut path = self.root.clone();
        for component in relpath.split('/').filter(|c| is_key_component(c)) {
            path.push(component);
        }
        path
    }
}

fn hash_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

fn lock_path(target: &Path) -> PathBuf {
    let name = target.file_name().map(|n| n.to_string_lossy().into_owned());
    let name = name.unwrap_or_else(|| "object".to_string());
    target.with_file_name(format!("{name}{LOCK_SUFFIX}"))
}

fn is_internal_file(name: &str) -> bool {
    name.ends_with(LOCK_SUFFIX)
        || name.starts_with(TMP_PREFIX)
        || name.contains(GRAVEYARD_INFIX)
}

/// A path component that contributes to the on-disk path. Drops empty segments
/// (leading/interior/trailing slashes) and the `.`/`..` traversal segments.
fn is_key_component(component: &str) -> bool {
    !component.is_empty() && component != "." && component != ".."
}

/// fsync the directory entry so a rename/create becomes durable.
#[cfg(unix)]
fn sync_dir(dir: &Path) {
    if let Ok(handle) = std::fs::File::open(dir) {
        let _ = handle.sync_all();
    }
}

/// Run one of the synchronous filesystem primitives (`tempfile`,
/// `File::create_new`, ...) off the async runtime via `spawn_blocking`.
async fn run_blocking<F, T>(f: F) -> StorageResult<T>
where
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match spawn_blocking(f).await {
        Ok(result) => result.map_err(io_error),
        Err(join) => Err(other_error(format!("blocking task failed: {join}"))),
    }
}

/// Write bytes into a fsync'd staging file in the target's directory, ready to be
/// atomically renamed (or linked) into place by the caller.
fn stage(target: &Path, bytes: &[u8]) -> std::io::Result<tempfile::NamedTempFile> {
    std::fs::create_dir_all(staging_dir(target))?;
    let mut tmp =
        TempFileBuilder::new().prefix(TMP_PREFIX).tempfile_in(staging_dir(target))?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    Ok(tmp)
}

/// Stage the bytes, then atomically rename over the target so a reader sees
/// either the old or the new bytes, never a partial write.
fn write_atomic_blocking(target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    stage(target, bytes)?.persist(target).map_err(|e| e.error)?;
    #[cfg(unix)]
    sync_dir(&staging_dir(target));
    Ok(())
}

/// Stage the new bytes, then re-check that `lock` still carries `nonce` and, only
/// then, atomically rename into place. Returns `false` (dropping the staged file)
/// when ownership was lost, so a stolen lock never leads to a write. The window
/// between the nonce check and the rename is a single syscall; see the module docs.
fn commit_under_lock_blocking(
    target: &Path,
    bytes: &[u8],
    lock: &Path,
    nonce: &str,
) -> std::io::Result<bool> {
    let tmp = stage(target, bytes)?;
    let still_ours = match std::fs::read(lock) {
        Ok(contents) => {
            parse_nonce(&String::from_utf8_lossy(&contents)).as_deref() == Some(nonce)
        }
        Err(_) => false,
    };
    if !still_ours {
        return Ok(false);
    }
    tmp.persist(target).map_err(|e| e.error)?;
    #[cfg(unix)]
    sync_dir(&staging_dir(target));
    Ok(true)
}

/// Like [`write_atomic_blocking`] but fails (without replacing) if the target
/// already exists. `persist_noclobber` uses the atomic `link` idiom on POSIX,
/// which is reliable where `O_EXCL` on the target is not (e.g. `NFSv2`).
/// Returns `false` when the target already exists.
fn create_exclusive_blocking(target: &Path, bytes: &[u8]) -> std::io::Result<bool> {
    match stage(target, bytes)?.persist_noclobber(target) {
        Ok(_) => {
            #[cfg(unix)]
            sync_dir(&staging_dir(target));
            Ok(true)
        }
        Err(err) if err.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(err) => Err(err.error),
    }
}

/// Try to create the lock file exclusively, stamped with `nonce`. `File::create_new`
/// maps to `O_EXCL` on POSIX and `CREATE_NEW` on Windows. Returns `false` if it
/// already exists.
fn create_lock_blocking(lock: &Path, nonce: &str) -> std::io::Result<bool> {
    if let Some(parent) = lock.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::File::create_new(lock) {
        Ok(mut file) => {
            file.write_all(lock_contents(nonce).as_bytes())?;
            file.sync_all()?;
            Ok(true)
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(err) => Err(err),
    }
}

/// A per-acquisition identifier stamped into a lock file, unique across processes
/// (pid) and within a process (monotonic counter + timestamp).
fn new_nonce() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("{}-{nanos}-{counter}", std::process::id())
}

fn parse_nonce(contents: &str) -> Option<String> {
    contents.lines().find_map(|line| line.strip_prefix("nonce=")).map(str::to_string)
}

/// Read the nonce stamped in a lock file, or `None` if it is absent/unreadable.
async fn read_lock_nonce(lock: &Path) -> Option<String> {
    let bytes = fs::read(lock).await.ok()?;
    parse_nonce(&String::from_utf8_lossy(&bytes))
}

fn graveyard_path(lock: &Path, tag: &str) -> PathBuf {
    let name = lock.file_name().map(|n| n.to_string_lossy().into_owned());
    let name = name.unwrap_or_else(|| "lock".to_string());
    lock.with_file_name(format!("{name}{GRAVEYARD_INFIX}{tag}"))
}

fn staging_dir(target: &Path) -> PathBuf {
    target.parent().map(Path::to_path_buf).unwrap_or_else(|| PathBuf::from("."))
}

async fn write_atomic(target: &Path, bytes: Bytes) -> StorageResult<()> {
    let target = target.to_path_buf();
    run_blocking(move || write_atomic_blocking(&target, &bytes)).await
}

/// Read `range` (or the whole file when `None`) into memory.
async fn read_object(
    target: &Path,
    range: Option<&Range<u64>>,
) -> StorageResult<Vec<u8>> {
    match range {
        None => fs::read(target).await.map_err(io_error),
        Some(range) => {
            let mut file = fs::File::open(target).await.map_err(io_error)?;
            let file_len = file.metadata().await.map_err(io_error)?.len();
            file.seek(SeekFrom::Start(range.start)).await.map_err(io_error)?;
            let len = range.end.min(file_len).saturating_sub(range.start);
            let mut out = Vec::with_capacity(len as usize);
            file.take(len).read_to_end(&mut out).await.map_err(io_error)?;
            Ok(out)
        }
    }
}

fn bytes_stream(
    data: Vec<u8>,
) -> Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>> {
    Box::pin(stream::once(async move { Ok(Bytes::from(data)) }))
}

impl FilesystemStorage {
    /// Create the target only if it does not already exist, atomically. Returns
    /// `false` when the target already exists.
    async fn create_exclusive(&self, target: &Path, bytes: Bytes) -> StorageResult<bool> {
        let target = target.to_path_buf();
        run_blocking(move || create_exclusive_blocking(&target, &bytes)).await
    }

    /// Compare-and-swap: replace the object at `path` only if its current content
    /// still hashes to `previous_etag`, serialized through an exclusive lock file.
    ///
    /// The current value is read under the lock, so once one commit writes the
    /// target the next holder sees the new value and loses the CAS.
    async fn conditional_update(
        &self,
        path: &str,
        bytes: Bytes,
        new_etag: &str,
        previous_etag: &str,
    ) -> StorageResult<VersionedUpdateResult> {
        let target = self.object_path(path);
        let lock = lock_path(&target);
        let (nonce, broke_stale) = self.acquire_lock(&lock).await?;
        // Only pay a directory scan when a crash leftover is plausible, i.e. we
        // actually broke a stale lock in this directory.
        if broke_stale && let Some(parent) = target.parent() {
            sweep_stale_internal(parent, self.stale_lock_timeout()).await;
        }

        let outcome = async {
            let current = match fs::read(&target).await {
                Ok(bytes) => Some(bytes),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                Err(err) => return Err(io_error(err)),
            };
            let matches =
                current.as_deref().map(hash_hex).as_deref() == Some(previous_etag);
            if !matches {
                return Ok(VersionedUpdateResult::NotOnLatestVersion);
            }
            let target = target.clone();
            let lock = lock.clone();
            let nonce = nonce.clone();
            let committed = run_blocking(move || {
                commit_under_lock_blocking(&target, &bytes, &lock, &nonce)
            })
            .await?;
            if committed {
                Ok(VersionedUpdateResult::Updated {
                    new_version: VersionInfo::from_etag_only(new_etag.to_string()),
                })
            } else {
                Ok(VersionedUpdateResult::NotOnLatestVersion)
            }
        }
        .await;

        self.release_lock(&lock, &nonce).await;
        outcome
    }

    /// Block until the lock is held, returning the nonce that proves ownership and
    /// whether a stale lock had to be broken to get it. A lock older than the
    /// stale timeout is presumed abandoned by a crashed writer and stolen
    /// atomically.
    async fn acquire_lock(&self, lock: &Path) -> StorageResult<(String, bool)> {
        let timeout = self.stale_lock_timeout();
        let nonce = new_nonce();
        let mut broke_stale = false;
        loop {
            let lock_owned = lock.to_path_buf();
            let nonce_owned = nonce.clone();
            let created =
                run_blocking(move || create_lock_blocking(&lock_owned, &nonce_owned))
                    .await?;
            if created {
                return Ok((nonce, broke_stale));
            }
            // The lock exists. Break it only if it is stale, and only the exact
            // instance we observed: `reclaim_if` restores anything else.
            if lock_is_stale(lock, timeout).await {
                let observed = read_lock_nonce(lock).await;
                self.reclaim_if(lock, observed.as_deref(), &nonce, RequireStale::Yes)
                    .await;
                broke_stale = true;
            }
            // Always back off before retrying so a contended or repeatedly-lost
            // reclaim never becomes a busy spin.
            sleep(LOCK_POLL_INTERVAL).await;
        }
    }

    /// Release a lock we hold, removing it only if it still carries our nonce so
    /// a successor's lock is never deleted. Our own live lock is fresh, so the
    /// staleness gate does not apply here.
    async fn release_lock(&self, lock: &Path, nonce: &str) {
        self.reclaim_if(lock, Some(nonce), nonce, RequireStale::No).await;
    }

    /// Atomically move `lock` aside and remove it, but only if the moved instance
    /// carries `expected` (where `None` means a lock with no readable nonce — a
    /// crash leftover) and, when `require_stale` is set, is *still* older than the
    /// stale timeout. Otherwise restore it so its owner is not disrupted. `tag`
    /// makes the aside path unique to this caller.
    ///
    /// Renaming a given inode succeeds for exactly one caller, and the staleness
    /// is re-checked on the captured file (rename preserves mtime) rather than on
    /// the pre-rename path. That closes the window where, between an acquirer's
    /// staleness stat and its rename, another writer breaks the stale lock and
    /// installs a fresh one: the fresh lock is captured but seen to be recent, so
    /// it is restored, never removed. A fresh lock is therefore never stolen.
    async fn reclaim_if(
        &self,
        lock: &Path,
        expected: Option<&str>,
        tag: &str,
        require_stale: RequireStale,
    ) {
        let grave = graveyard_path(lock, tag);
        if fs::rename(lock, &grave).await.is_err() {
            // Already gone or taken by another reclaimer; nothing to do.
            return;
        }
        let nonce_matches = read_lock_nonce(&grave).await.as_deref() == expected;
        let stale_ok = match require_stale {
            RequireStale::No => true,
            RequireStale::Yes => lock_is_stale(&grave, self.stale_lock_timeout()).await,
        };
        if nonce_matches && stale_ok {
            let _ = fs::remove_file(&grave).await;
        } else {
            // Not the instance we meant to remove (different nonce, or refreshed
            // since we judged it stale); put it back if the slot is free
            // (no-clobber via hard_link), otherwise discard our copy.
            let _ = fs::hard_link(&grave, lock).await;
            let _ = fs::remove_file(&grave).await;
        }
    }
}

/// Whether [`FilesystemStorage::reclaim_if`] may only remove a lock that is
/// still stale (stealing a crashed writer's lock) or any matching instance
/// (releasing our own live lock).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequireStale {
    Yes,
    No,
}

/// A staging or graveyard file left behind by a crashed writer, older than the
/// stale timeout. Active locks and in-flight staging files are excluded.
fn is_sweepable_transient(entry: &std::fs::DirEntry, timeout: Duration) -> bool {
    let name = entry.file_name();
    let name = name.to_string_lossy();
    if !(name.starts_with(TMP_PREFIX) || name.contains(GRAVEYARD_INFIX)) {
        return false;
    }
    entry
        .metadata()
        .and_then(|m| m.modified())
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age > timeout)
}

/// Best-effort removal of leaked transient files in a single directory. Runs in
/// one blocking task rather than several async `fs` hops per commit.
async fn sweep_stale_internal(dir: &Path, timeout: Duration) {
    let dir = dir.to_path_buf();
    let _ = spawn_blocking(move || {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if is_sweepable_transient(&entry, timeout) {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    })
    .await;
}

/// Best-effort removal of leaked transient files across the whole tree under
/// `root`. Used by garbage collection to reclaim what per-commit sweeps (which
/// only touch ref directories) never reach.
fn sweep_tree_blocking(root: &Path, timeout: Duration) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            match entry.file_type() {
                Ok(ft) if ft.is_dir() => stack.push(entry.path()),
                Ok(_) if is_sweepable_transient(&entry, timeout) => {
                    let _ = std::fs::remove_file(entry.path());
                }
                _ => {}
            }
        }
    }
}

async fn lock_is_stale(lock: &Path, timeout: Duration) -> bool {
    match fs::metadata(lock).await.and_then(|m| m.modified()) {
        Ok(modified) => SystemTime::now()
            .duration_since(modified)
            .map(|age| age > timeout)
            .unwrap_or(false),
        // If we can't stat it, it may have just been released; let the caller
        // retry the create rather than break a lock we know nothing about.
        Err(_) => false,
    }
}

fn lock_contents(nonce: &str) -> String {
    let pid = std::process::id();
    let timestamp_ms =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    format!("nonce={nonce}\npid={pid}\nhost={host}\ntimestamp_ms={timestamp_ms}\n")
}

impl sealed::Sealed for FilesystemStorage {}

#[async_trait]
#[typetag::serde(name = "native_local_filesystem")]
impl Storage for FilesystemStorage {
    fn storage_info(&self) -> StorageInfo {
        StorageInfo {
            backend_type: "local filesystem (native)",
            fields: vec![("path", self.root.display().to_string())],
        }
    }

    fn local_filesystem_root(&self) -> Option<&Path> {
        Some(&self.root)
    }

    async fn can_write(&self) -> StorageResult<bool> {
        Ok(true)
    }

    async fn create_location_if_needed(&self) -> StorageResult<()> {
        fs::create_dir_all(&self.root).await.capture()?;
        Ok(())
    }

    async fn sweep_transient_files(&self, _settings: &Settings) -> StorageResult<()> {
        let root = self.root.clone();
        let timeout = self.stale_lock_timeout();
        let _ = spawn_blocking(move || sweep_tree_blocking(&root, timeout)).await;
        Ok(())
    }

    async fn default_settings(&self) -> StorageResult<Settings> {
        Ok(Settings {
            // The filesystem never carries object metadata for us.
            unsafe_use_metadata: Some(false),
            // Local I/O has no transient failures worth retrying; a genuine CAS
            // loss surfaces as NotOnLatestVersion, which refs.rs retries itself.
            retries: Some(icechunk_storage::RetriesSettings {
                max_tries: std::num::NonZeroU16::new(1),
                initial_backoff_ms: Some(0),
                max_backoff_ms: Some(0),
            }),
            ..Default::default()
        })
    }

    async fn get_object_range(
        &self,
        _settings: &Settings,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )> {
        let target = self.object_path(path);
        let data = read_object(&target, range).await?;
        // A full read returns the object's content-hash etag; the Storage contract
        // requires that version to round-trip through `get_object_conditional`
        // (used for the branch-tip CAS and the config compare-and-swap). A ranged
        // read returns no etag: a whole-object hash would mean reading the whole
        // object on every partial GET (quadratic across the concurrent multi-part
        // path), and ranged reads only ever serve chunk data, whose version is
        // never used conditionally. The ranged case diverges from S3, which
        // reports the full-object etag on ranged GETs.
        let version = match range {
            None => VersionInfo::from_etag_only(hash_hex(&data)),
            Some(_) => VersionInfo::for_creation(),
        };
        Ok((bytes_stream(data), version))
    }

    async fn put_object(
        &self,
        settings: &Settings,
        path: &str,
        bytes: Bytes,
        _content_type: Option<&str>,
        _metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        let target = self.object_path(path);

        match previous_version {
            None => {
                write_atomic(&target, bytes).await?;
                // Unconditional writes (chunks/manifests/snapshots) return no etag;
                // no caller reads it, so the content hash is not computed. The
                // comment guards a future consumer against a meaningless value.
                Ok(VersionedUpdateResult::Updated {
                    new_version: VersionInfo::for_creation(),
                })
            }
            Some(previous) if previous.is_create() => {
                let new_etag = hash_hex(&bytes);
                let updated = VersionedUpdateResult::Updated {
                    new_version: VersionInfo::from_etag_only(new_etag),
                };
                if settings.unsafe_use_conditional_create() {
                    if self.create_exclusive(&target, bytes).await? {
                        Ok(updated)
                    } else {
                        Ok(VersionedUpdateResult::NotOnLatestVersion)
                    }
                } else {
                    write_atomic(&target, bytes).await?;
                    Ok(updated)
                }
            }
            Some(previous) => match previous.etag() {
                Some(etag) if settings.unsafe_use_conditional_update() => {
                    let new_etag = hash_hex(&bytes);
                    self.conditional_update(path, bytes, &new_etag, strip_quotes(etag))
                        .await
                }
                _ => {
                    let new_etag = hash_hex(&bytes);
                    write_atomic(&target, bytes).await?;
                    Ok(VersionedUpdateResult::Updated {
                        new_version: VersionInfo::from_etag_only(new_etag),
                    })
                }
            },
        }
    }

    async fn copy_object(
        &self,
        settings: &Settings,
        from: &str,
        to: &str,
        _content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        let from = self.object_path(from);
        let to = self.object_path(to);
        let data = fs::read(&from).await.map_err(io_error)?;

        if settings.unsafe_use_conditional_update()
            && let Some(etag) = version.etag()
            && hash_hex(&data) != strip_quotes(etag)
        {
            return Ok(VersionedUpdateResult::NotOnLatestVersion);
        }

        write_atomic(&to, Bytes::from(data)).await?;
        Ok(VersionedUpdateResult::Updated { new_version: version.clone() })
    }

    #[instrument(skip(self, _settings))]
    async fn list_objects<'a>(
        &'a self,
        _settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        let base = self.object_path(prefix);
        let entries = collect_list(&base).await?;
        Ok(stream::iter(entries.into_iter().map(Ok)).boxed())
    }

    #[instrument(skip(self, _settings, batch))]
    async fn delete_batch(
        &self,
        _settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        let result = stream::iter(batch)
            .map(|(id, size)| async move {
                let relpath = if prefix.is_empty() {
                    id
                } else {
                    format!("{}/{}", prefix.trim_end_matches('/'), id)
                };
                let target = self.object_path(&relpath);
                let removed = match fs::remove_file(&target).await {
                    Ok(()) => true,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
                    Err(err) => {
                        tracing::error!(error = ?err, path = %target.display(), "Error deleting object");
                        false
                    }
                };
                (removed, size)
            })
            .buffer_unordered(16)
            .fold(DeleteObjectsResult::default(), |mut acc, (removed, size)| async move {
                if removed {
                    acc.deleted_objects += 1;
                    acc.deleted_bytes += size;
                }
                acc
            })
            .await;
        Ok(result)
    }

    #[instrument(skip(self, _settings))]
    async fn get_object_last_modified(
        &self,
        path: &str,
        _settings: &Settings,
    ) -> StorageResult<DateTime<Utc>> {
        let target = self.object_path(path);
        let modified =
            fs::metadata(&target).await.and_then(|m| m.modified()).map_err(io_error)?;
        Ok(DateTime::<Utc>::from(modified))
    }

    #[instrument(skip(self, _settings))]
    async fn get_object_conditional(
        &self,
        _settings: &Settings,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        let target = self.object_path(path);
        let data = fs::read(&target).await.map_err(io_error)?;
        let etag = hash_hex(&data);

        if let Some(previous) = previous_version
            && let Some(previous_etag) = previous.etag()
            && strip_quotes(previous_etag) == etag
        {
            return Ok(GetModifiedResult::OnLatestVersion);
        }

        Ok(GetModifiedResult::Modified {
            data: Box::pin(Cursor::new(data)),
            new_version: VersionInfo::from_etag_only(etag),
        })
    }
}

/// Recursively list every object file under `base`, returning ids relative to
/// `base` with `/` separators, sorted by id. Lock and temp files are skipped.
///
/// The full listing is materialized and sorted because directory read order is
/// arbitrary and callers (e.g. ref listing) rely on a stable order.
async fn collect_list(base: &Path) -> StorageResult<Vec<ListInfo<String>>> {
    let mut out: Vec<ListInfo<String>> = Vec::new();
    match fs::metadata(base).await {
        Ok(meta) if meta.is_dir() => {}
        Ok(_) => return Ok(Vec::new()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(io_error(err)),
    }

    let mut stack = vec![base.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut read_dir = match fs::read_dir(&dir).await {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(io_error(err));
            }
        };
        while let Some(entry) = read_dir.next_entry().await.capture()? {
            let file_type = entry.file_type().await.capture()?;
            let path = entry.path();
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            let name = entry.file_name();
            if is_internal_file(&name.to_string_lossy()) {
                continue;
            }
            let Ok(rel) = path.strip_prefix(base) else {
                continue;
            };
            let id = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let meta = entry.metadata().await.capture()?;
            let created_at =
                meta.modified().map(DateTime::<Utc>::from).unwrap_or_else(|_| Utc::now());
            out.push(ListInfo { id, created_at, size_bytes: meta.len() });
        }
    }

    out.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(out)
}

/// Build native local filesystem storage rooted at `path`.
pub fn new_filesystem_storage(
    path: impl Into<PathBuf>,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    Ok(Arc::new(FilesystemStorage::new(path)))
}

/// The default local filesystem repository [`Storage`].
///
/// Uses the native [`FilesystemStorage`], which serializes branch-tip updates
/// through a lock file and is therefore safe for concurrent commits, unlike the
/// `object_store` local backend it replaces.
pub async fn new_local_filesystem_storage(
    path: &Path,
) -> StorageResult<Arc<dyn Storage + Send + Sync>> {
    new_filesystem_storage(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use icechunk_macros::tokio_test;

    /// Write an object unconditionally, then read its content-hash version back —
    /// mirroring how a conditional-update parent is obtained in production (from a
    /// read, not from the unconditional write, which returns no etag).
    async fn seed(
        storage: &FilesystemStorage,
        settings: &Settings,
        path: &str,
        content: &[u8],
    ) -> VersionInfo {
        match storage
            .put_object(
                settings,
                path,
                Bytes::copy_from_slice(content),
                None,
                vec![],
                None,
            )
            .await
        {
            Ok(VersionedUpdateResult::Updated { .. }) => {}
            other => panic!("unexpected seed result: {other:?}"),
        }
        let (_, version) = storage
            .get_object_range(settings, path, None)
            .await
            .expect("read seeded object back");
        version
    }

    /// Many tasks racing a conditional update from the same parent version: the
    /// CAS must let exactly one win and report the rest as stale.
    #[tokio_test]
    async fn test_concurrent_conditional_update_single_winner() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = Arc::new(FilesystemStorage::new(dir.path()));
        let settings = storage.default_settings().await.expect("settings");
        let path = "refs/branch.main/ref.json";
        let base = seed(&storage, &settings, path, b"v0").await;

        let mut set = tokio::task::JoinSet::new();
        for i in 0..8u32 {
            let storage = Arc::clone(&storage);
            let settings = settings.clone();
            let base = base.clone();
            set.spawn(async move {
                storage
                    .put_object(
                        &settings,
                        "refs/branch.main/ref.json",
                        Bytes::from(format!("value-{i}")),
                        None,
                        vec![],
                        Some(&base),
                    )
                    .await
                    .expect("put should not error")
            });
        }

        let mut updated = 0;
        let mut stale = 0;
        for res in set.join_all().await {
            match res {
                VersionedUpdateResult::Updated { .. } => updated += 1,
                VersionedUpdateResult::NotOnLatestVersion => stale += 1,
            }
        }
        assert_eq!(updated, 1, "exactly one writer should win");
        assert_eq!(stale, 7, "every other writer should see a stale version");
    }

    /// A conditional create loses to an existing object.
    #[tokio_test]
    async fn test_conditional_create_conflict() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = FilesystemStorage::new(dir.path());
        let settings = storage.default_settings().await.expect("settings");
        let path = "refs/tag.v1/ref.json";

        let first = storage
            .put_object(
                &settings,
                path,
                Bytes::from_static(b"first"),
                None,
                vec![],
                Some(&VersionInfo::for_creation()),
            )
            .await
            .expect("first create");
        assert!(matches!(first, VersionedUpdateResult::Updated { .. }));

        let second = storage
            .put_object(
                &settings,
                path,
                Bytes::from_static(b"second"),
                None,
                vec![],
                Some(&VersionInfo::for_creation()),
            )
            .await
            .expect("second create");
        assert!(matches!(second, VersionedUpdateResult::NotOnLatestVersion));
    }

    /// A lock left behind by a crashed writer is broken once it ages past the
    /// stale timeout, so a later conditional update can still proceed.
    #[tokio_test]
    async fn test_stale_lock_recovery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = FilesystemStorage::new(dir.path())
            .with_stale_lock_timeout(Duration::from_millis(50));
        let settings = storage.default_settings().await.expect("settings");
        let path = "refs/branch.main/ref.json";
        let base = seed(&storage, &settings, path, b"v0").await;

        let target = storage.object_path(path);
        let lock = lock_path(&target);
        fs::write(&lock, b"pid=999999\nhost=ghost\n").await.expect("write stale lock");
        sleep(Duration::from_millis(70)).await;

        let result = storage
            .put_object(
                &settings,
                path,
                Bytes::from_static(b"v1"),
                None,
                vec![],
                Some(&base),
            )
            .await
            .expect("update should recover from stale lock");
        assert!(matches!(result, VersionedUpdateResult::Updated { .. }));
        assert!(!fs::try_exists(&lock).await.unwrap_or(true), "lock released");
    }

    /// Crash recovery under contention: with a stale lock already present, race
    /// several conditional updates from the same parent version. Exactly one may
    /// win, even though every racer independently observes and breaks the stale
    /// lock. Looped to shake out interleavings.
    ///
    /// The stale timeout (150ms) is comfortably longer than a commit (sub-ms) but
    /// shorter than the aging sleep, so only the seeded lock is ever judged
    /// stale; a racer's own fresh lock never is.
    #[tokio_test]
    async fn test_stale_lock_concurrent_single_winner() {
        const RACERS: u32 = 4;
        const ITERATIONS: u32 = 20;
        let path = "refs/branch.main/ref.json";

        for iteration in 0..ITERATIONS {
            let dir = tempfile::tempdir().expect("tempdir");
            let storage = Arc::new(
                FilesystemStorage::new(dir.path())
                    .with_stale_lock_timeout(Duration::from_millis(150)),
            );
            let settings = storage.default_settings().await.expect("settings");
            let base = seed(&storage, &settings, path, b"v0").await;

            // Seed a lock as if a writer crashed holding it, then age it past the
            // stale timeout so every racer will try to break it.
            let lock = lock_path(&storage.object_path(path));
            fs::write(&lock, lock_contents("crashed-writer").as_bytes())
                .await
                .expect("write stale lock");
            sleep(Duration::from_millis(200)).await;

            let mut set = tokio::task::JoinSet::new();
            for racer in 0..RACERS {
                let storage = Arc::clone(&storage);
                let settings = settings.clone();
                let base = base.clone();
                set.spawn(async move {
                    storage
                        .put_object(
                            &settings,
                            path,
                            Bytes::from(format!("winner-{racer}")),
                            None,
                            vec![],
                            Some(&base),
                        )
                        .await
                        .expect("put should not error")
                });
            }

            let mut updated = 0;
            for res in set.join_all().await {
                if matches!(res, VersionedUpdateResult::Updated { .. }) {
                    updated += 1;
                }
            }
            assert_eq!(
                updated, 1,
                "iteration {iteration}: exactly one racer must win, got {updated}"
            );
            // The seeded lock and all graveyard files must be cleaned up.
            assert!(!fs::try_exists(&lock).await.unwrap_or(true), "lock released");
        }
    }

    /// A lock that is not stale must never be removed by a stealer, even when its
    /// nonce matches what the stealer expected. This guards the window between an
    /// acquirer's staleness check and its rename.
    #[tokio_test]
    async fn test_reclaim_preserves_fresh_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Default (5 minute) timeout, so the freshly written lock is not stale.
        let storage = FilesystemStorage::new(dir.path());
        let target = storage.object_path("refs/branch.main/ref.json");
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).await.expect("mkdir");
        }
        let lock = lock_path(&target);
        fs::write(&lock, lock_contents("fresh-owner").as_bytes())
            .await
            .expect("write lock");

        storage
            .reclaim_if(&lock, Some("fresh-owner"), "breaker", RequireStale::Yes)
            .await;

        assert!(
            fs::try_exists(&lock).await.unwrap_or(false),
            "a non-stale lock must be preserved by a stealer"
        );
        assert_eq!(read_lock_nonce(&lock).await.as_deref(), Some("fresh-owner"));
    }

    /// The restore path: when the captured lock is not the instance the stealer
    /// meant to break (its nonce differs), it is put back, preserving the owner's
    /// lock — even though it is genuinely stale.
    #[tokio_test]
    async fn test_reclaim_restores_on_nonce_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = FilesystemStorage::new(dir.path())
            .with_stale_lock_timeout(Duration::from_millis(20));
        let target = storage.object_path("refs/branch.main/ref.json");
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).await.expect("mkdir");
        }
        let lock = lock_path(&target);
        fs::write(&lock, lock_contents("real-owner").as_bytes())
            .await
            .expect("write lock");
        // Age it so staleness is not what protects it — only the nonce mismatch is.
        sleep(Duration::from_millis(40)).await;

        storage
            .reclaim_if(&lock, Some("departed-instance"), "breaker", RequireStale::Yes)
            .await;

        assert!(
            fs::try_exists(&lock).await.unwrap_or(false),
            "a lock whose nonce does not match must be restored"
        );
        assert_eq!(read_lock_nonce(&lock).await.as_deref(), Some("real-owner"));
    }

    /// Lock and temp files never leak into listings.
    #[tokio_test]
    async fn test_internal_files_excluded_from_listing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = FilesystemStorage::new(dir.path());
        let settings = storage.default_settings().await.expect("settings");
        seed(&storage, &settings, "refs/branch.main/ref.json", b"v0").await;

        // A leftover lock/temp sibling must not appear as an object.
        let refs_dir = storage.object_path("refs/branch.main");
        fs::write(refs_dir.join(format!("ref.json{LOCK_SUFFIX}")), b"x")
            .await
            .expect("write lock");
        fs::write(refs_dir.join(format!("{TMP_PREFIX}abc123")), b"x")
            .await
            .expect("write temp");

        let ids = collect_list(&storage.object_path("refs"))
            .await
            .expect("list")
            .into_iter()
            .map(|li| li.id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["branch.main/ref.json".to_string()]);
    }

    #[tokio_test]
    async fn test_typetag_roundtrips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(FilesystemStorage::new(dir.path()));
        let json = serde_json::to_string(&storage).expect("serialize");
        assert!(json.contains("native_local_filesystem"), "got: {json}");
        let restored: Arc<dyn Storage + Send + Sync> =
            serde_json::from_str(&json).expect("deserialize");
        assert!(restored.to_string().contains("FilesystemStorage"));
    }

    /// A relative root is pinned to an absolute path at construction, so later
    /// working-directory changes (or a serialized storage crossing processes)
    /// cannot re-target the repository.
    #[test]
    fn test_relative_root_is_absolutized() {
        let storage = FilesystemStorage::new("relative/repo");
        assert!(storage.root.is_absolute());
        let json = serde_json::to_string(&storage).expect("serialize");
        let restored: FilesystemStorage =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored.root, storage.root);
    }

    /// Traversal components in a key are dropped, so a key can never resolve
    /// outside the repository root.
    #[test]
    fn test_object_path_drops_traversal_components() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = FilesystemStorage::new(dir.path());
        let escaped = storage.object_path("refs/../../../etc/passwd");
        assert!(escaped.starts_with(dir.path()));
        assert_eq!(escaped, storage.object_path("refs/etc/passwd"));
    }
}
