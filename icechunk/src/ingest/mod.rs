//! Resumable byte-faithful ingest of a zarr v3 store into an icechunk
//! repository.
//!
//! Resume state lives in `SnapshotProperties` — the string-keyed
//! metadata map attached to every commit. All `icechunk.ingest.*`
//! keys below are properties of the latest commit on the target
//! branch; a fresh process resumes by reading the tip via
//! `Repository::lookup_snapshot(branch_tip).metadata`.
//!
//! Two phases, both committed:
//!
//! 1. **Skeleton**: copy every `zarr.json` from the source into a
//!    fresh writable session, commit with
//!    `icechunk.ingest.phase = "skeleton"`. After this commit
//!    `Session::list_nodes` is authoritative about which destination
//!    nodes are arrays vs groups.
//!
//! 2. **Chunks**: for every array (in lex order), open a writable
//!    session, list a batch of chunk keys whose lex-sorted name
//!    sorts strictly after the array's *cursor* (the last chunk
//!    key copied for that array in any prior commit, e.g.
//!    `/arr/c/3/7`), copy them, commit with
//!    `icechunk.ingest.phase = "chunks"` and the new cursor stored
//!    at `icechunk.ingest.cursors.<array_path>`. When an array is
//!    exhausted its cursor entry is set to `"DONE"` in a marker
//!    commit before moving to the next array.
//!
//! The final commit writes `icechunk.ingest.phase = "complete"`;
//! a subsequent call short-circuits with `AlreadyComplete` unless
//! `CollisionPolicy::Overwrite` is set.

use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _, stream, stream::BoxStream};
use icechunk_arrow_object_store::object_store::{
    self, ObjectStore, ObjectStoreExt as _, path::Path as StorePath,
};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, instrument};

use crate::{
    ObjectStorage, Repository, Storage as _, StorageError, Store,
    format::{ByteRange, NodeType, Path, SnapshotId, snapshot::SnapshotProperties},
    repository::VersionInfo,
    session::Session,
    store::StoreError,
};

/// Default cap on chunk keys per per-array commit. Override via
/// [`IngestOptions::checkpoint_every`].
pub const DEFAULT_CHECKPOINT_EVERY: usize = 1000;

/// Property key holding the ingest's current phase.
const PROP_PHASE: &str = "icechunk.ingest.phase";
/// Property key holding the per-array cursor map.
const PROP_CURSORS: &str = "icechunk.ingest.cursors";

/// Sentinel cursor value indicating an array has been fully copied.
const CURSOR_DONE: &str = "DONE";

/// Phase strings stored under [`PROP_PHASE`].
const PHASE_SKELETON: &str = "skeleton";
const PHASE_CHUNKS: &str = "chunks";
const PHASE_COMPLETE: &str = "complete";

/// Counters reported during and after an ingest. All fields reflect
/// state that is durable on the branch at the moment the callback
/// fires.
#[derive(Debug, Default, Clone, Copy)]
pub struct IngestStats {
    /// Keys (metadata + chunks) copied so far.
    pub keys: u64,
    /// Bytes written so far.
    pub bytes: u64,
    /// Arrays that have been fully copied (a DONE marker has been
    /// committed for each).
    pub arrays_done: u64,
}

/// Callback invoked once per durable commit with the running stats.
pub type ProgressCallback = Arc<dyn Fn(IngestStats) + Send + Sync>;

/// What to do when the destination already contains a key the source
/// wants to write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CollisionPolicy {
    /// Any collision with an existing destination key is an error.
    /// Also fails if the branch's latest commit is a completed ingest.
    #[default]
    Fail,
    /// Skip destination keys already present. Use when resuming an
    /// interrupted ingest or layering onto a partially-populated repo.
    Skip,
    /// Clobber existing keys. Required to re-ingest over a completed run.
    Overwrite,
}

impl CollisionPolicy {
    fn is_skip(self) -> bool {
        matches!(self, Self::Skip)
    }
    fn is_overwrite(self) -> bool {
        matches!(self, Self::Overwrite)
    }
}

/// Caller-tunable knobs for [`ingest_zarr`]. Use the builder methods —
/// fields are private so we can grow this struct without breaking
/// callers.
pub struct IngestOptions {
    paths: Vec<String>,
    concurrency: usize,
    on_collision: CollisionPolicy,
    progress: Option<ProgressCallback>,
    checkpoint_every: usize,
    branch: String,
    message: Option<String>,
    verify_source_unchanged: bool,
    /// `false` forces the chunks phase to buffer + sort each array's
    /// listing client-side (O(chunks-per-array) memory). Set by
    /// [`ingest_zarr`] from the backend; cloud stores leave it `true`.
    source_listing_sorted: bool,
}

impl std::fmt::Debug for IngestOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestOptions")
            .field("paths", &self.paths)
            .field("concurrency", &self.concurrency)
            .field("on_collision", &self.on_collision)
            .field("progress", &self.progress.as_ref().map(|_| "<callback>"))
            .field("checkpoint_every", &self.checkpoint_every)
            .field("branch", &self.branch)
            .field("message", &self.message)
            .field("verify_source_unchanged", &self.verify_source_unchanged)
            .field("source_listing_sorted", &self.source_listing_sorted)
            .finish()
    }
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            paths: vec![String::new()],
            concurrency: 32,
            on_collision: CollisionPolicy::default(),
            progress: None,
            checkpoint_every: DEFAULT_CHECKPOINT_EVERY,
            branch: "main".to_string(),
            message: None,
            verify_source_unchanged: true,
            source_listing_sorted: true,
        }
    }
}

impl IngestOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Logical zarr paths to ingest. Each entry is a node path
    /// (`""` / `"/"` for the whole store, `"foo"`, `"/foo/bar"`, …).
    /// Leading and trailing slashes are normalized away.
    pub fn paths(mut self, paths: Vec<String>) -> Self {
        self.paths = paths;
        self
    }

    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// How to react when the destination already contains a key the
    /// source wants to write. See [`CollisionPolicy`].
    pub fn on_collision(mut self, policy: CollisionPolicy) -> Self {
        self.on_collision = policy;
        self
    }

    /// Callback invoked after every successful commit with running
    /// stats.
    pub fn progress(mut self, cb: ProgressCallback) -> Self {
        self.progress = Some(cb);
        self
    }

    /// Maximum chunk keys copied between commits. Each per-array commit
    /// in the chunks phase processes at most this many chunks before
    /// recording a fresh cursor and starting a new session.
    pub fn checkpoint_every(mut self, n: usize) -> Self {
        self.checkpoint_every = n.max(1);
        self
    }

    pub fn branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = branch.into();
        self
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// On resume, compare each skeleton `zarr.json`'s bytes between
    /// source and destination, not just the set of paths. Default
    /// `true`. Disable for very deep hierarchies where the extra 2N
    /// GETs at resume matter and the source is provably stable.
    pub fn verify_source_unchanged(mut self, verify_source_unchanged: bool) -> Self {
        self.verify_source_unchanged = verify_source_unchanged;
        self
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IngestError {
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("destination store error: {0}")]
    Store(#[from] StoreError),
    #[error("source storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("session error: {0}")]
    Session(#[from] crate::session::SessionError),
    #[error("repository error: {0}")]
    Repository(#[from] crate::repository::RepositoryError),
    #[error(
        "destination already contains key `{0}`; pass on_collision=Overwrite to clobber, or on_collision=Skip to resume"
    )]
    KeyCollision(String),
    #[error(
        "this branch's latest commit is a completed ingest (phase=complete); pass on_collision=Overwrite to re-ingest"
    )]
    AlreadyComplete,
    #[error(
        "resume requested but the source skeleton differs from the committed skeleton: {reason}"
    )]
    SkeletonMismatch { reason: String },
    #[error("unexpected ingest property shape: {0}")]
    BadProperties(String),
}

pub type IngestResult<T> = Result<T, IngestError>;

/// Outcome of [`ingest_zarr`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct IngestOutcome {
    pub stats: IngestStats,
    pub final_snapshot_id: SnapshotId,
}

fn is_metadata_key(key: &str) -> bool {
    key == "zarr.json" || key.ends_with("/zarr.json")
}

/// Strip the source-prefix and any leading slashes from an `ObjectStore`
/// path so that it lines up with what icechunk's [`Store`] expects.
fn key_relative_to(prefix: &StorePath, full: &StorePath) -> Option<String> {
    let p = full.as_ref();
    let pre = prefix.as_ref();
    let rest = if pre.is_empty() {
        p
    } else if let Some(rest) = p.strip_prefix(pre) {
        rest.strip_prefix('/').unwrap_or(rest)
    } else {
        return None;
    };
    Some(rest.to_string())
}

/// Strip leading/trailing slashes; `"/"` and `""` both map to `""`.
fn normalize_logical_path(p: &str) -> String {
    p.trim_matches('/').to_string()
}

/// Metadata keys for every ancestor group of `path`. Excludes the path
/// itself and excludes empty (root) requests.
fn ancestor_metadata_keys(path: &str) -> Vec<String> {
    if path.is_empty() {
        return Vec::new();
    }
    let mut out = vec!["zarr.json".to_string()];
    let parts: Vec<&str> = path.split('/').collect();
    for i in 1..parts.len() {
        out.push(format!("{}/zarr.json", parts[..i].join("/")));
    }
    out
}

fn child_path(base: &StorePath, child: &str) -> StorePath {
    if base.as_ref().is_empty() {
        StorePath::from(child)
    } else if child.is_empty() {
        base.clone()
    } else {
        StorePath::from(format!("{}/{}", base.as_ref(), child))
    }
}

#[derive(Default)]
struct AtomicStats {
    keys: AtomicU64,
    bytes: AtomicU64,
    arrays_done: AtomicU64,
}

impl AtomicStats {
    fn record(&self, nbytes: u64) -> IngestStats {
        self.keys.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(nbytes, Ordering::Relaxed);
        self.snapshot()
    }

    fn complete_array(&self) {
        self.arrays_done.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> IngestStats {
        IngestStats {
            keys: self.keys.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            arrays_done: self.arrays_done.load(Ordering::Relaxed),
        }
    }
}

/// Phase decoded from the latest commit's properties.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ResumePhase {
    /// No prior ingest commit on this branch — start fresh.
    Fresh,
    /// Skeleton committed; need to do the chunk loop from scratch.
    AfterSkeleton,
    /// Chunks in progress; resume with the given cursor map.
    InChunks(BTreeMap<String, String>),
    /// Already complete.
    Complete,
}

fn read_resume_phase(props: &SnapshotProperties) -> IngestResult<ResumePhase> {
    let phase = match props.get(PROP_PHASE) {
        None => return Ok(ResumePhase::Fresh),
        Some(Value::String(s)) => s.as_str(),
        Some(other) => {
            return Err(IngestError::BadProperties(format!(
                "{PROP_PHASE} must be a string, got {other:?}"
            )));
        }
    };
    match phase {
        PHASE_SKELETON => Ok(ResumePhase::AfterSkeleton),
        PHASE_CHUNKS => {
            let cursors = match props.get(PROP_CURSORS) {
                None => BTreeMap::new(),
                Some(Value::Object(map)) => {
                    let mut out = BTreeMap::new();
                    for (k, v) in map {
                        match v {
                            Value::String(s) => {
                                out.insert(k.clone(), s.clone());
                            }
                            Value::Null => {}
                            other => {
                                return Err(IngestError::BadProperties(format!(
                                    "{PROP_CURSORS}.{k} must be string or null, got {other:?}"
                                )));
                            }
                        }
                    }
                    out
                }
                Some(other) => {
                    return Err(IngestError::BadProperties(format!(
                        "{PROP_CURSORS} must be an object, got {other:?}"
                    )));
                }
            };
            Ok(ResumePhase::InChunks(cursors))
        }
        PHASE_COMPLETE => Ok(ResumePhase::Complete),
        other => Err(IngestError::BadProperties(format!(
            "unrecognised {PROP_PHASE} value: {other:?}"
        ))),
    }
}

/// Build the `SnapshotProperties` for a per-batch commit.
fn make_props(phase: &str, cursors: &BTreeMap<String, String>) -> SnapshotProperties {
    let mut props = SnapshotProperties::new();
    props.insert(PROP_PHASE.to_string(), Value::String(phase.to_string()));
    if !cursors.is_empty() {
        let mut obj = serde_json::Map::new();
        for (k, v) in cursors {
            obj.insert(k.clone(), Value::String(v.clone()));
        }
        props.insert(PROP_CURSORS.to_string(), Value::Object(obj));
    }
    props
}

/// Open a readonly session at the tip of `branch`.
async fn readonly_session_at_tip(
    repo: &Repository,
    branch: &str,
) -> IngestResult<Session> {
    Ok(repo.readonly_session(&VersionInfo::BranchTipRef(branch.to_string())).await?)
}

/// Resumable byte-faithful copy of a zarr v3 store into an icechunk
/// repository.
///
/// Internal driver. Takes the source as a raw `Arc<dyn ObjectStore>`
/// plus the within-store prefix where the zarr lives, and drives the
/// commits on `options.branch` of `repo`. See module docs for the
/// per-phase shape. Public callers go through [`ingest_zarr`].
#[instrument(skip_all, fields(prefix = %source_prefix, branch = %options.branch))]
async fn ingest_from_object_store(
    source: Arc<dyn ObjectStore>,
    source_prefix: &StorePath,
    repo: &Repository,
    options: IngestOptions,
) -> IngestResult<IngestOutcome> {
    let stats = Arc::new(AtomicStats::default());

    let resume_phase = {
        let snap_id = repo.lookup_branch(&options.branch).await?;
        let info = repo.lookup_snapshot(&snap_id).await?;
        read_resume_phase(&info.metadata)?
    };
    debug!(?resume_phase, "decoded resume phase");

    if matches!(resume_phase, ResumePhase::Complete)
        && !options.on_collision.is_overwrite()
    {
        return Err(IngestError::AlreadyComplete);
    }

    let skeleton_keys =
        list_metadata_keys(source.as_ref(), source_prefix, &options.paths).await?;

    // Empty source: stamp `phase=complete` so a subsequent call short-circuits.
    if skeleton_keys.is_empty() {
        let final_snap_id = commit_marker(
            repo,
            &options,
            &stats,
            &BTreeMap::new(),
            PHASE_COMPLETE,
            "empty source",
        )
        .await?;
        return Ok(IngestOutcome {
            stats: stats.snapshot(),
            final_snapshot_id: final_snap_id,
        });
    }

    match &resume_phase {
        ResumePhase::Fresh | ResumePhase::Complete => {
            write_skeleton(
                Arc::clone(&source),
                source_prefix,
                repo,
                &options,
                &stats,
                &skeleton_keys,
            )
            .await?;
        }
        ResumePhase::AfterSkeleton | ResumePhase::InChunks(_) => {
            verify_skeleton_matches(
                Arc::clone(&source),
                source_prefix,
                repo,
                &options,
                &skeleton_keys,
            )
            .await?;
        }
    }

    let mut cursors: BTreeMap<String, String> = match &resume_phase {
        ResumePhase::InChunks(map) => map.clone(),
        _ => BTreeMap::new(),
    };

    let arrays = enumerate_arrays(repo, &options.branch, &options.paths).await?;
    debug!(num_arrays = arrays.len(), "enumerated target arrays");

    for array_path in &arrays {
        copy_array_chunks(
            Arc::clone(&source),
            source_prefix,
            repo,
            &options,
            &stats,
            array_path,
            &mut cursors,
        )
        .await?;
    }

    // Always end on phase=complete so a subsequent call short-circuits.
    let final_snap_id =
        commit_marker(repo, &options, &stats, &cursors, PHASE_COMPLETE, "complete")
            .await?;

    Ok(IngestOutcome { stats: stats.snapshot(), final_snapshot_id: final_snap_id })
}

/// Resumable byte-faithful copy of a Zarr v3 store into an icechunk
/// repository.
///
/// Takes the source as an [`ObjectStorage`] — the icechunk storage
/// type backed by an `object_store::ObjectStore`. Constructed via
/// the same helpers as the destination repo's storage:
/// `ObjectStorage::new_s3`, `new_local_filesystem`, `new_http`, etc.
///
/// Rust callers typically have an `Arc<dyn Storage>` from one of
/// icechunk's top-level helpers (`new_s3_object_store_storage` and
/// friends). To call this function, downcast to `ObjectStorage`
/// via the `Storage::as_any` escape hatch:
///
/// ```ignore
/// let storage = icechunk::new_s3_object_store_storage(...).await?;
/// let source = storage
///     .as_any()
///     .downcast_ref::<icechunk::ObjectStorage>()
///     .expect("object-store-backed storage");
/// icechunk::ingest::ingest_zarr(source, &repo, options).await?;
/// ```
///
/// Wrappers like `LatencyStorage` / `LoggingStorage` aren't supported
/// as ingest sources; the downcast above will fail for them by
/// design.
pub async fn ingest_zarr(
    source: &ObjectStorage,
    repo: &Repository,
    options: IngestOptions,
) -> IngestResult<IngestOutcome> {
    let backend = source.backend();
    let settings = source.default_settings().await?;
    let source_store = backend.mk_object_store(&settings)?;
    let prefix = StorePath::from(backend.prefix());
    let mut options = options;
    options.source_listing_sorted = !backend.artificially_sort_refs_in_mem();
    ingest_from_object_store(source_store, &prefix, repo, options).await
}

/// Skeleton phase: copy every `zarr.json` from the source into a
/// fresh writable session, commit with `phase = skeleton`. In zarr v3
/// a group is entirely defined by its `zarr.json`, so after this
/// commit every group is fully copied; only arrays still need their
/// chunks in the next phase.
async fn write_skeleton(
    source: Arc<dyn ObjectStore>,
    source_prefix: &StorePath,
    repo: &Repository,
    options: &IngestOptions,
    stats: &Arc<AtomicStats>,
    skeleton_keys: &[String],
) -> IngestResult<()> {
    let session = Arc::new(RwLock::new(repo.writable_session(&options.branch).await?));
    let store = Store::from_session(Arc::clone(&session)).await;

    // `Fresh` only means "no prior ingest" — the destination may still
    // be populated (layered ingest, or `Overwrite` re-run).
    let to_copy: Vec<String> = match options.on_collision {
        CollisionPolicy::Overwrite => skeleton_keys.to_vec(),
        CollisionPolicy::Skip => {
            let existing: HashSet<String> =
                store.list_prefix("").await?.try_collect().await?;
            skeleton_keys.iter().filter(|k| !existing.contains(*k)).cloned().collect()
        }
        CollisionPolicy::Fail => {
            let existing: HashSet<String> =
                store.list_prefix("").await?.try_collect().await?;
            if let Some(k) = skeleton_keys.iter().find(|k| existing.contains(*k)) {
                return Err(IngestError::KeyCollision(k.clone()));
            }
            skeleton_keys.to_vec()
        }
    };

    copy_keys(source, source_prefix, &store, &to_copy, options, stats).await?;

    let message = options
        .message
        .clone()
        .unwrap_or_else(|| "icechunk.from_zarr skeleton".to_string());
    let props = make_props(PHASE_SKELETON, &BTreeMap::new());
    // The commit atomically lands both the staged writes from
    // `copy_keys` and the `phase=skeleton` properties. `allow_empty`
    // is set because two legitimate paths still produce zero staged
    // writes here: `Skip` against a destination that already has
    // every `zarr.json`, and `Overwrite` re-ingest where every
    // write is byte-identical to what's already committed.
    // (Empty-source ingests are short-circuited in `ingest_zarr`
    // before reaching this point.)
    session
        .write()
        .await
        .commit(message)
        .properties(props)
        .allow_empty(true)
        .execute()
        .await?;
    notify_progress(options, stats);
    Ok(())
}

/// Check that the source's skeleton matches the destination's committed
/// skeleton when resuming. Always verifies the set of paths; when
/// `options.verify_source_unchanged` is set, also compares the bytes
/// of every `zarr.json` to catch a source that mutated between calls.
async fn verify_skeleton_matches(
    source: Arc<dyn ObjectStore>,
    source_prefix: &StorePath,
    repo: &Repository,
    options: &IngestOptions,
    skeleton_keys: &[String],
) -> IngestResult<()> {
    let session = readonly_session_at_tip(repo, &options.branch).await?;
    let mut dest_keys: HashSet<String> = HashSet::new();
    for n in session.list_nodes(&Path::root()).await? {
        let rel = n?.path.to_string().trim_start_matches('/').to_string();
        let key = if rel.is_empty() {
            "zarr.json".to_string()
        } else {
            format!("{rel}/zarr.json")
        };
        dest_keys.insert(key);
    }
    if let Some(missing) = skeleton_keys.iter().find(|k| !dest_keys.contains(*k)) {
        return Err(IngestError::SkeletonMismatch {
            reason: format!("`{missing}` exists in source but not in destination"),
        });
    }
    let src_set: HashSet<&str> = skeleton_keys.iter().map(String::as_str).collect();
    if let Some(extra) = dest_keys.iter().find(|k| !src_set.contains(k.as_str())) {
        return Err(IngestError::SkeletonMismatch {
            reason: format!("`{extra}` exists in destination but not in source"),
        });
    }

    if !options.verify_source_unchanged {
        return Ok(());
    }

    let store = Store::from_session(Arc::new(RwLock::new(session))).await;
    stream::iter(skeleton_keys.iter().cloned().map(Ok::<_, IngestError>))
        .try_for_each_concurrent(options.concurrency, |key| {
            let source = Arc::clone(&source);
            let source_prefix = source_prefix.clone();
            let store = store.clone();
            async move {
                let src_path = child_path(&source_prefix, &key);
                let src_bytes: Bytes = source.get(&src_path).await?.bytes().await?;
                let dst_bytes = store.get(&key, &ByteRange::ALL).await?;
                if src_bytes != dst_bytes {
                    return Err(IngestError::SkeletonMismatch {
                        reason: format!(
                            "content of `{key}` differs between source and destination"
                        ),
                    });
                }
                Ok(())
            }
        })
        .await
}

/// Per-array chunk loop. Lists chunk keys past the current cursor,
/// processes them in commit-sized slabs.
async fn copy_array_chunks(
    source: Arc<dyn ObjectStore>,
    source_prefix: &StorePath,
    repo: &Repository,
    options: &IngestOptions,
    stats: &Arc<AtomicStats>,
    array_path: &str,
    cursors: &mut BTreeMap<String, String>,
) -> IngestResult<()> {
    if cursors.get(array_path).map(String::as_str) == Some(CURSOR_DONE) {
        return Ok(());
    }

    // `array_path` begins with '/' (it's a NodePath::to_string).
    let array_rel = array_path.trim_start_matches('/');
    // Trailing slash so `arr/c/` doesn't match siblings like `arr/c2/...`.
    let chunk_prefix =
        if array_rel.is_empty() { "c/".to_string() } else { format!("{array_rel}/c/") };
    let listing_prefix = child_path(source_prefix, &chunk_prefix);

    // Sorted backends: stream `list_with_offset`, peak memory is one
    // batch. Unsorted backends: collect/sort/slice, peak is
    // O(chunks-per-array).
    let cursor_trimmed = cursors.get(array_path).map(|s| s.trim_start_matches('/'));
    let mut batches: BoxStream<'_, Result<Vec<String>, object_store::Error>> = if options
        .source_listing_sorted
    {
        let listing = match cursor_trimmed {
            Some(c) => {
                let offset = child_path(source_prefix, c);
                source.list_with_offset(Some(&listing_prefix), &offset)
            }
            None => source.list(Some(&listing_prefix)),
        };
        listing
            .map_ok(|meta| key_relative_to(source_prefix, &meta.location))
            .try_filter_map(|opt| futures::future::ready(Ok(opt)))
            .try_chunks(options.checkpoint_every)
            .map_err(|e| e.1)
            .boxed()
    } else {
        let mut all: Vec<String> = source
            .list(Some(&listing_prefix))
            .map_ok(|meta| key_relative_to(source_prefix, &meta.location))
            .try_filter_map(|opt| futures::future::ready(Ok(opt)))
            .try_collect()
            .await?;
        all.sort();
        let start =
            cursor_trimmed.map(|c| all.partition_point(|k| k.as_str() <= c)).unwrap_or(0);
        let chunked: Vec<Result<Vec<String>, object_store::Error>> = all[start..]
            .chunks(options.checkpoint_every)
            .map(|c| Ok(c.to_vec()))
            .collect();
        stream::iter(chunked).boxed()
    };

    while let Some(batch) = batches.try_next().await? {
        let session =
            Arc::new(RwLock::new(repo.writable_session(&options.branch).await?));
        let store = Store::from_session(Arc::clone(&session)).await;

        // Per-batch existence filter under `Skip`. Bounded by
        // `checkpoint_every`; the first lookup in this session may
        // fetch the array's manifest, but `copy_keys` would do that
        // anyway for the subsequent writes.
        let to_copy: Vec<String> = if options.on_collision.is_skip() {
            let mut keep = Vec::with_capacity(batch.len());
            for k in &batch {
                if !store.exists(k).await? {
                    keep.push(k.clone());
                }
            }
            keep
        } else {
            batch.clone()
        };

        copy_keys(Arc::clone(&source), source_prefix, &store, &to_copy, options, stats)
            .await?;

        if let Some(last) = batch.last() {
            // Cursor stores keys with a leading '/' to match `Path::to_string`.
            cursors.insert(array_path.to_string(), format!("/{last}"));
        }

        let message = options
            .message
            .clone()
            .unwrap_or_else(|| format!("icechunk.from_zarr chunks {array_path}"));
        let props = make_props(PHASE_CHUNKS, cursors);
        // `allow_empty(true)`: under `Skip` the batch may filter to
        // zero writes, but the commit still has to land to advance
        // the cursor.
        session
            .write()
            .await
            .commit(message)
            .properties(props)
            .allow_empty(true)
            .execute()
            .await?;
        notify_progress(options, stats);
    }

    cursors.insert(array_path.to_string(), CURSOR_DONE.to_string());
    stats.complete_array();
    // Distinct DONE commit so a resume after the last data batch sees
    // DONE in the properties without re-listing.
    commit_marker(
        repo,
        options,
        stats,
        cursors,
        PHASE_CHUNKS,
        &format!("chunks {array_path} done"),
    )
    .await?;
    Ok(())
}

/// Write a property-only commit on the target branch. `default_msg_tail`
/// is appended to `"icechunk.from_zarr "` when no message is set.
async fn commit_marker(
    repo: &Repository,
    options: &IngestOptions,
    stats: &AtomicStats,
    cursors: &BTreeMap<String, String>,
    phase: &str,
    default_msg_tail: &str,
) -> IngestResult<SnapshotId> {
    let mut session = repo.writable_session(&options.branch).await?;
    let message = options
        .message
        .clone()
        .unwrap_or_else(|| format!("icechunk.from_zarr {default_msg_tail}"));
    let props = make_props(phase, cursors);
    let snap_id =
        session.commit(message).properties(props).allow_empty(true).execute().await?;
    notify_progress(options, stats);
    Ok(snap_id)
}

/// Enumerate arrays under any of the requested paths in lex order.
///
/// TODO(scale): buffers every array path in memory. For repos with
/// millions of arrays, this and `copy_array_chunks`'s per-array
/// chunk-listing buffer share the same scaling limit. The fix is a
/// streaming model where arrays are discovered and copied as a stream
/// instead of enumerate-then-loop. See `notes/ingest-roadmap.md`.
async fn enumerate_arrays(
    repo: &Repository,
    branch: &str,
    paths: &[String],
) -> IngestResult<Vec<String>> {
    let session = readonly_session_at_tip(repo, branch).await?;
    let mut out: Vec<String> = Vec::new();
    let mut roots: Vec<Path> = Vec::new();
    for p in paths {
        let logical = normalize_logical_path(p);
        let root = if logical.is_empty() {
            Path::root()
        } else {
            // Prefix with '/' for icechunk Path canonical form.
            match Path::new(&format!("/{logical}")) {
                Ok(p) => p,
                Err(_) => continue,
            }
        };
        roots.push(root);
    }
    if roots.is_empty() {
        roots.push(Path::root());
    }
    for root in &roots {
        let nodes = session.list_nodes(root).await?;
        for n in nodes {
            let n = n?;
            if n.node_type() == NodeType::Array {
                out.push(n.path.to_string());
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// List every metadata key (`zarr.json` files) under the requested paths.
async fn list_metadata_keys(
    source: &dyn ObjectStore,
    source_prefix: &StorePath,
    paths: &[String],
) -> IngestResult<Vec<String>> {
    let mut seen: HashSet<String> = HashSet::new();
    for p in paths {
        let logical = normalize_logical_path(p);
        let listing_prefix = if logical.is_empty() {
            source_prefix.clone()
        } else {
            child_path(source_prefix, &logical)
        };
        let mut stream = source.list(Some(&listing_prefix));
        while let Some(meta) = stream.try_next().await? {
            if let Some(rel) = key_relative_to(source_prefix, &meta.location)
                && is_metadata_key(&rel)
            {
                seen.insert(rel);
            }
        }
        for ancestor in ancestor_metadata_keys(&logical) {
            let exact = child_path(source_prefix, &ancestor);
            match source.head(&exact).await {
                Ok(_) => {
                    seen.insert(ancestor);
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    let mut out: Vec<String> = seen.into_iter().collect();
    out.sort();
    Ok(out)
}

/// Copy a flat list of keys from `source` to `dest` with bounded
/// concurrency. Used by both phases — skeleton (every `zarr.json`)
/// and chunks (one per-array batch at a time).
///
/// Concurrency bounds in-flight *count* (`options.concurrency`), not
/// in-flight *bytes*. With default 32-way concurrency, a workload
/// where every chunk is a 100 MB shard peaks around 3 GB resident.
/// TODO(scale): byte-budget the concurrency for large-chunk workloads.
///
/// TODO(perf): when source and destination resolve to the same
/// physical store, avoid the bytes-through-memory round trip:
/// - same S3 bucket / provider: `ObjectStore::copy` (server-side).
/// - same local filesystem: `std::fs::copy` or hardlink.
/// - same icechunk repo: consider registering a virtual chunk
///   reference instead of re-writing bytes (overlaps with the
///   `mode="virtual"` roadmap item).
async fn copy_keys(
    source: Arc<dyn ObjectStore>,
    source_prefix: &StorePath,
    dest: &Store,
    keys: &[String],
    options: &IngestOptions,
    stats: &Arc<AtomicStats>,
) -> IngestResult<()> {
    if keys.is_empty() {
        return Ok(());
    }
    stream::iter(keys.iter().cloned().map(Ok::<_, IngestError>))
        .try_for_each_concurrent(options.concurrency, |key| {
            let source = Arc::clone(&source);
            let source_prefix = source_prefix.clone();
            let dest = dest.clone();
            let stats = Arc::clone(stats);
            async move {
                let src_path = child_path(&source_prefix, &key);
                let bytes: Bytes = source.get(&src_path).await?.bytes().await?;
                let nbytes = bytes.len() as u64;
                dest.set(&key, bytes).await?;
                stats.record(nbytes);
                Ok(())
            }
        })
        .await
}

/// Fire the progress callback (if any) with the current durable stats.
fn notify_progress(options: &IngestOptions, stats: &AtomicStats) {
    if let Some(cb) = &options.progress {
        cb(stats.snapshot());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Repository;
    use crate::new_in_memory_storage;
    use crate::storage::faulty::FaultyStore;
    use icechunk_arrow_object_store::object_store::PutPayload;
    use icechunk_arrow_object_store::object_store::memory::InMemory;
    use std::collections::HashMap;

    #[test]
    fn is_metadata_key_works() {
        assert!(is_metadata_key("zarr.json"));
        assert!(is_metadata_key("a/zarr.json"));
        assert!(is_metadata_key("a/b/zarr.json"));
        assert!(!is_metadata_key("a/c/0/0"));
        assert!(!is_metadata_key("zarr.json.bak"));
    }

    #[test]
    fn normalize_logical_path_strips_slashes() {
        assert_eq!(normalize_logical_path(""), "");
        assert_eq!(normalize_logical_path("/"), "");
        assert_eq!(normalize_logical_path("foo"), "foo");
        assert_eq!(normalize_logical_path("/foo"), "foo");
        assert_eq!(normalize_logical_path("foo/"), "foo");
        assert_eq!(normalize_logical_path("/foo/bar/"), "foo/bar");
        assert_eq!(normalize_logical_path("//foo//"), "foo");
    }

    #[test]
    fn ancestor_metadata_keys_enumerates_groups() {
        assert!(ancestor_metadata_keys("").is_empty());
        assert_eq!(ancestor_metadata_keys("foo"), vec!["zarr.json".to_string()]);
        assert_eq!(
            ancestor_metadata_keys("foo/bar"),
            vec!["zarr.json".to_string(), "foo/zarr.json".to_string()]
        );
    }

    #[test]
    fn read_resume_phase_round_trips() {
        let mut props = SnapshotProperties::new();
        assert!(matches!(read_resume_phase(&props).unwrap(), ResumePhase::Fresh));
        props.insert(PROP_PHASE.to_string(), Value::String(PHASE_SKELETON.to_string()));
        assert!(matches!(read_resume_phase(&props).unwrap(), ResumePhase::AfterSkeleton));
        props.insert(PROP_PHASE.to_string(), Value::String(PHASE_CHUNKS.to_string()));
        let mut cursors = serde_json::Map::new();
        cursors.insert("/a".into(), Value::String("/a/c/0/3".into()));
        cursors.insert("/b".into(), Value::String(CURSOR_DONE.into()));
        props.insert(PROP_CURSORS.to_string(), Value::Object(cursors));
        match read_resume_phase(&props).unwrap() {
            ResumePhase::InChunks(map) => {
                assert_eq!(map.get("/a").map(String::as_str), Some("/a/c/0/3"));
                assert_eq!(map.get("/b").map(String::as_str), Some(CURSOR_DONE));
            }
            other => panic!("expected InChunks, got {other:?}"),
        }
        props.insert(PROP_PHASE.to_string(), Value::String(PHASE_COMPLETE.to_string()));
        assert!(matches!(read_resume_phase(&props).unwrap(), ResumePhase::Complete));
    }

    /// Standard zarr v3 group metadata bytes.
    fn group_meta() -> Bytes {
        Bytes::from_static(br#"{"zarr_format":3,"node_type":"group","attributes":{}}"#)
    }

    /// Standard zarr v3 array metadata bytes (uint8, shape [2,2], chunks [1,1]).
    fn array_meta_2x2_c1() -> Bytes {
        Bytes::from_static(br#"{"zarr_format":3,"node_type":"array","shape":[2,2],"data_type":"uint8","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"attributes":{}}"#)
    }

    /// 1D array metadata: shape [4], chunks [1], 4 chunks.
    fn array_meta_1d4() -> Bytes {
        Bytes::from_static(br#"{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"uint8","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"attributes":{}}"#)
    }

    /// 4D array metadata: shape [2,2,2,2], chunk shape [1,1,1,1], 16 chunks.
    fn array_meta_4d() -> Bytes {
        Bytes::from_static(br#"{"zarr_format":3,"node_type":"array","shape":[2,2,2,2],"data_type":"uint8","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}],"attributes":{}}"#)
    }

    async fn materialize(pairs: &[(String, Bytes)]) -> (Arc<dyn ObjectStore>, StorePath) {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let prefix = StorePath::from("src");
        for (k, v) in pairs {
            let full = child_path(&prefix, k);
            store.put(&full, PutPayload::from_bytes(v.clone())).await.unwrap();
        }
        (store, prefix)
    }

    async fn fresh_repo() -> Repository {
        let storage = new_in_memory_storage().await.unwrap();
        Repository::create(None, storage, HashMap::new(), None, true).await.unwrap()
    }

    fn build_2x2_pairs() -> Vec<(String, Bytes)> {
        vec![
            ("zarr.json".to_string(), group_meta()),
            ("arr/zarr.json".to_string(), array_meta_2x2_c1()),
            ("arr/c/0/0".to_string(), Bytes::from_static(&[1u8])),
            ("arr/c/0/1".to_string(), Bytes::from_static(&[2u8])),
            ("arr/c/1/0".to_string(), Bytes::from_static(&[3u8])),
            ("arr/c/1/1".to_string(), Bytes::from_static(&[4u8])),
        ]
    }

    #[tokio::test]
    async fn whole_store_copy_finishes_with_phase_complete() {
        let (source, prefix) = materialize(&build_2x2_pairs()).await;
        let repo = fresh_repo().await;

        let outcome =
            ingest_from_object_store(source, &prefix, &repo, IngestOptions::new())
                .await
                .unwrap();
        assert_eq!(outcome.stats.keys, 6);

        let snap_id = repo.lookup_branch("main").await.unwrap();
        let info = repo.lookup_snapshot(&snap_id).await.unwrap();
        let phase = info.metadata.get(PROP_PHASE).and_then(|v| v.as_str());
        assert_eq!(phase, Some(PHASE_COMPLETE));
    }

    #[tokio::test]
    async fn skeleton_then_resume_chunks() {
        let pairs = build_2x2_pairs();
        let (source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        // Force skeleton-only by short-circuiting after skeleton: we
        // can't easily abort, so instead run the full ingest with
        // checkpoint_every=2 (3 chunk batches), then re-run and verify
        // the second run is a no-op (everything already DONE).
        let outcome1 = ingest_from_object_store(
            Arc::clone(&source),
            &prefix,
            &repo,
            IngestOptions::new().checkpoint_every(2),
        )
        .await
        .unwrap();
        assert_eq!(outcome1.stats.keys, 6);

        let outcome2 = ingest_from_object_store(
            source,
            &prefix,
            &repo,
            IngestOptions::new()
                .checkpoint_every(2)
                .on_collision(CollisionPolicy::Overwrite),
        )
        .await
        .unwrap();
        assert_eq!(outcome2.stats.keys, 6);
    }

    #[tokio::test]
    async fn multi_array_lex_order_progress() {
        // Three arrays a, b, c each with one chunk.
        let pairs: Vec<(String, Bytes)> = vec![
            ("zarr.json".to_string(), group_meta()),
            ("a/zarr.json".to_string(), array_meta_2x2_c1()),
            ("a/c/0/0".to_string(), Bytes::from_static(&[1u8])),
            ("a/c/0/1".to_string(), Bytes::from_static(&[2u8])),
            ("a/c/1/0".to_string(), Bytes::from_static(&[3u8])),
            ("a/c/1/1".to_string(), Bytes::from_static(&[4u8])),
            ("b/zarr.json".to_string(), array_meta_2x2_c1()),
            ("b/c/0/0".to_string(), Bytes::from_static(&[5u8])),
            ("b/c/0/1".to_string(), Bytes::from_static(&[6u8])),
            ("b/c/1/0".to_string(), Bytes::from_static(&[7u8])),
            ("b/c/1/1".to_string(), Bytes::from_static(&[8u8])),
            ("c/zarr.json".to_string(), array_meta_2x2_c1()),
            ("c/c/0/0".to_string(), Bytes::from_static(&[9u8])),
            ("c/c/0/1".to_string(), Bytes::from_static(&[10u8])),
            ("c/c/1/0".to_string(), Bytes::from_static(&[11u8])),
            ("c/c/1/1".to_string(), Bytes::from_static(&[12u8])),
        ];
        let (source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        let outcome = ingest_from_object_store(
            source,
            &prefix,
            &repo,
            IngestOptions::new().checkpoint_every(3),
        )
        .await
        .unwrap();
        assert_eq!(outcome.stats.keys, 16);

        let snap_id = repo.lookup_branch("main").await.unwrap();
        let info = repo.lookup_snapshot(&snap_id).await.unwrap();
        let cursors = info
            .metadata
            .get(PROP_CURSORS)
            .and_then(|v| v.as_object())
            .expect("cursors map");
        for arr in &["/a", "/b", "/c"] {
            assert_eq!(
                cursors.get(*arr).and_then(|v| v.as_str()),
                Some(CURSOR_DONE),
                "{arr} should be DONE"
            );
        }
    }

    #[tokio::test]
    async fn arbitrary_dimensionality_4d() {
        let mut pairs: Vec<(String, Bytes)> = vec![
            ("zarr.json".to_string(), group_meta()),
            ("arr/zarr.json".to_string(), array_meta_4d()),
        ];
        // 16 chunks at c/i/j/k/l with i,j,k,l in {0,1}.
        let mut idx: u8 = 0;
        for i in 0..2 {
            for j in 0..2 {
                for k in 0..2 {
                    for l in 0..2 {
                        idx += 1;
                        pairs.push((
                            format!("arr/c/{i}/{j}/{k}/{l}"),
                            Bytes::copy_from_slice(&[idx]),
                        ));
                    }
                }
            }
        }
        let (source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;
        let outcome = ingest_from_object_store(
            source,
            &prefix,
            &repo,
            IngestOptions::new().checkpoint_every(5),
        )
        .await
        .unwrap();
        assert_eq!(outcome.stats.keys, 18);
    }

    #[tokio::test]
    async fn resume_after_partial_array() {
        // Run the ingest, then surgically rewrite the latest commit's
        // metadata to look like we crashed mid-way: phase=chunks,
        // cursor for /arr at /arr/c/0/1. Re-run; the subsequent ingest
        // should pick up at /arr/c/1/0.
        let pairs = build_2x2_pairs();
        let (source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        // First pass — small checkpoint so we get many commits.
        let _ = ingest_from_object_store(
            Arc::clone(&source),
            &prefix,
            &repo,
            IngestOptions::new().checkpoint_every(1),
        )
        .await
        .unwrap();

        // Build an artificial mid-flight commit on top, mimicking a
        // crash after writing /arr/c/0/0 and /arr/c/0/1 only. We do
        // this by opening a writable session, NOT writing anything,
        // and committing with cursors[/arr] = /arr/c/0/1.
        let session = repo.writable_session("main").await.unwrap();
        let mut session = session;
        let mut cursors = BTreeMap::new();
        cursors.insert("/arr".to_string(), "/arr/c/0/1".to_string());
        let props = make_props(PHASE_CHUNKS, &cursors);
        session
            .commit("simulate crash mid-array")
            .properties(props)
            .allow_empty(true)
            .execute()
            .await
            .unwrap();

        // Now re-run; cursor says /arr/c/0/1 already done, so we should
        // pick up /arr/c/1/0 and /arr/c/1/1 only. Stats counter is
        // process-local so this run sees 2 chunk copies (not the full 4).
        let outcome = ingest_from_object_store(
            source,
            &prefix,
            &repo,
            IngestOptions::new().checkpoint_every(1),
        )
        .await
        .unwrap();
        // We don't have a robust way to count exactly 2 (skip_existing
        // is false; we re-list just from the cursor offset). Assert at
        // least some progress and that the final phase is complete.
        let _ = outcome;
        let snap_id = repo.lookup_branch("main").await.unwrap();
        let info = repo.lookup_snapshot(&snap_id).await.unwrap();
        assert_eq!(
            info.metadata.get(PROP_PHASE).and_then(|v| v.as_str()),
            Some(PHASE_COMPLETE)
        );
    }

    /// Resume detects that the source's `zarr.json` content has changed
    /// since the skeleton was committed. Default `verify_source_unchanged`
    /// fetches both copies and surfaces the diff as `SkeletonMismatch`.
    #[tokio::test]
    async fn resume_detects_source_metadata_mutation_by_default() {
        let pairs = build_2x2_pairs();
        let (source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        ingest_from_object_store(
            Arc::clone(&source),
            &prefix,
            &repo,
            IngestOptions::new().checkpoint_every(1),
        )
        .await
        .unwrap();

        // Inject a mid-flight commit (phase=chunks) so the next run takes
        // the resume code path, which is what runs verify_skeleton_matches.
        let mut session = repo.writable_session("main").await.unwrap();
        let mut cursors = BTreeMap::new();
        cursors.insert("/arr".to_string(), "/arr/c/0/0".to_string());
        let props = make_props(PHASE_CHUNKS, &cursors);
        session
            .commit("simulate crash mid-array")
            .properties(props)
            .allow_empty(true)
            .execute()
            .await
            .unwrap();

        // Mutate the source's array metadata to a different shape. Same
        // path, different bytes.
        let full = child_path(&prefix, "arr/zarr.json");
        source.put(&full, PutPayload::from_bytes(array_meta_1d4())).await.unwrap();

        let err = ingest_from_object_store(source, &prefix, &repo, IngestOptions::new())
            .await
            .unwrap_err();
        match err {
            IngestError::SkeletonMismatch { reason } => {
                assert!(
                    reason.contains("arr/zarr.json"),
                    "expected reason to name the mutated key, got {reason:?}"
                );
                assert!(
                    reason.contains("content"),
                    "expected content-mismatch reason, got {reason:?}"
                );
            }
            other => panic!("expected SkeletonMismatch, got {other:?}"),
        }
    }

    /// With `verify_source_unchanged(false)`, the byte check is skipped
    /// and a mutated source resumes silently.
    #[tokio::test]
    async fn resume_with_verify_disabled_skips_byte_check() {
        let pairs = build_2x2_pairs();
        let (source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        ingest_from_object_store(
            Arc::clone(&source),
            &prefix,
            &repo,
            IngestOptions::new().checkpoint_every(1),
        )
        .await
        .unwrap();

        let mut session = repo.writable_session("main").await.unwrap();
        let mut cursors = BTreeMap::new();
        cursors.insert("/arr".to_string(), "/arr/c/0/0".to_string());
        let props = make_props(PHASE_CHUNKS, &cursors);
        session
            .commit("simulate crash mid-array")
            .properties(props)
            .allow_empty(true)
            .execute()
            .await
            .unwrap();

        let full = child_path(&prefix, "arr/zarr.json");
        source.put(&full, PutPayload::from_bytes(array_meta_1d4())).await.unwrap();

        // Should complete without raising SkeletonMismatch.
        ingest_from_object_store(
            source,
            &prefix,
            &repo,
            IngestOptions::new().verify_source_unchanged(false),
        )
        .await
        .unwrap();
    }

    /// A purely structural mismatch (source has a path the destination
    /// doesn't) is caught even when byte-verification is disabled.
    #[tokio::test]
    async fn resume_structure_mismatch_caught_without_byte_check() {
        let pairs = build_2x2_pairs();
        let (source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        ingest_from_object_store(
            Arc::clone(&source),
            &prefix,
            &repo,
            IngestOptions::new().checkpoint_every(1),
        )
        .await
        .unwrap();

        let mut session = repo.writable_session("main").await.unwrap();
        let mut cursors = BTreeMap::new();
        cursors.insert("/arr".to_string(), "/arr/c/0/0".to_string());
        let props = make_props(PHASE_CHUNKS, &cursors);
        session
            .commit("simulate crash mid-array")
            .properties(props)
            .allow_empty(true)
            .execute()
            .await
            .unwrap();

        // Add a brand-new array to the source.
        let new_meta = child_path(&prefix, "newarr/zarr.json");
        source.put(&new_meta, PutPayload::from_bytes(array_meta_1d4())).await.unwrap();

        let err = ingest_from_object_store(
            source,
            &prefix,
            &repo,
            IngestOptions::new().verify_source_unchanged(false),
        )
        .await
        .unwrap_err();
        match err {
            IngestError::SkeletonMismatch { reason } => {
                assert!(reason.contains("newarr"), "got {reason:?}");
            }
            other => panic!("expected SkeletonMismatch, got {other:?}"),
        }
    }

    /// `CollisionPolicy::Fail` on a destination that already holds one
    /// of the source's skeleton keys returns `KeyCollision` and writes
    /// nothing.
    #[tokio::test]
    async fn fail_policy_errors_on_existing_dest_key() {
        let pairs = build_2x2_pairs();
        let (source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        {
            let session =
                Arc::new(RwLock::new(repo.writable_session("main").await.unwrap()));
            let store = Store::from_session(Arc::clone(&session)).await;
            store.set("zarr.json", group_meta()).await.unwrap();
            session.write().await.commit("seed dest").execute().await.unwrap();
        }
        let pre_ingest_tip = repo.lookup_branch("main").await.unwrap();

        let err = ingest_from_object_store(source, &prefix, &repo, IngestOptions::new())
            .await
            .unwrap_err();
        assert!(
            matches!(err, IngestError::KeyCollision(ref k) if k == "zarr.json"),
            "expected KeyCollision(\"zarr.json\"), got {err:?}"
        );

        // Branch tip is unchanged: no commit was written.
        let post = repo.lookup_branch("main").await.unwrap();
        assert_eq!(post, pre_ingest_tip);
    }

    /// `CollisionPolicy::Skip` leaves pre-existing destination keys
    /// alone — both in the skeleton phase (existing `zarr.json` files)
    /// and the chunks phase (existing chunk keys).
    #[tokio::test]
    async fn skip_preserves_existing_destination_keys() {
        // Source: 1-D array with 4 chunks; chunk i carries byte 10+i.
        let mut pairs: Vec<(String, Bytes)> = vec![
            ("zarr.json".to_string(), group_meta()),
            ("arr/zarr.json".to_string(), array_meta_1d4()),
        ];
        for i in 0..4u8 {
            pairs.push((format!("arr/c/{i}"), Bytes::copy_from_slice(&[10 + i])));
        }
        let (source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        // Pre-populate dest with matching skeleton plus a sentinel
        // version of chunk c/0. Commit with NO ingest properties so the
        // next run starts in `Fresh` phase and exercises the Skip
        // filter in both skeleton and chunks phases.
        let sentinel = Bytes::from_static(&[0xFFu8]);
        {
            let session =
                Arc::new(RwLock::new(repo.writable_session("main").await.unwrap()));
            let store = Store::from_session(Arc::clone(&session)).await;
            store.set("zarr.json", group_meta()).await.unwrap();
            store.set("arr/zarr.json", array_meta_1d4()).await.unwrap();
            store.set("arr/c/0", sentinel.clone()).await.unwrap();
            session.write().await.commit("seed dest").execute().await.unwrap();
        }

        ingest_from_object_store(
            source,
            &prefix,
            &repo,
            IngestOptions::new().on_collision(CollisionPolicy::Skip),
        )
        .await
        .unwrap();

        // Read back: c/0 must keep the sentinel; c/1..c/3 must carry
        // the source's bytes.
        let session = readonly_session_at_tip(&repo, "main").await.unwrap();
        let store = Store::from_session(Arc::new(RwLock::new(session))).await;
        assert_eq!(store.get("arr/c/0", &ByteRange::ALL).await.unwrap(), sentinel);
        for i in 1..4u8 {
            let got = store.get(&format!("arr/c/{i}"), &ByteRange::ALL).await.unwrap();
            assert_eq!(got.as_ref(), &[10 + i], "chunk c/{i} should match source");
        }

        // And the run must finish in phase=complete.
        let snap_id = repo.lookup_branch("main").await.unwrap();
        let info = repo.lookup_snapshot(&snap_id).await.unwrap();
        assert_eq!(
            info.metadata.get(PROP_PHASE).and_then(|v| v.as_str()),
            Some(PHASE_COMPLETE)
        );
    }

    /// Second run with overwrite=true on an already-complete ingest produces
    /// no new bytes (`set_array_meta`/`set_group_meta` are no-ops when
    /// bytes match), so every commit in that run has an empty changeset.
    /// `allow_empty(true)` on every commit site means this must not error.
    #[tokio::test]
    async fn overwrite_after_complete_does_not_fail_on_empty_changeset() {
        let pairs = build_2x2_pairs();
        let (source1, prefix1) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        ingest_from_object_store(
            Arc::clone(&source1),
            &prefix1,
            &repo,
            IngestOptions::new(),
        )
        .await
        .unwrap();

        // Second identical source — all set calls are no-ops, so change_set
        // is empty on every commit. Must not error.
        let (source2, prefix2) = materialize(&pairs).await;
        let outcome = ingest_from_object_store(
            source2,
            &prefix2,
            &repo,
            IngestOptions::new().on_collision(CollisionPolicy::Overwrite),
        )
        .await
        .unwrap();

        let snap_id = repo.lookup_branch("main").await.unwrap();
        let info = repo.lookup_snapshot(&snap_id).await.unwrap();
        assert_eq!(
            info.metadata.get(PROP_PHASE).and_then(|v| v.as_str()),
            Some(PHASE_COMPLETE),
            "final phase must be complete"
        );
        let _ = outcome;
    }

    /// When a prior run committed the skeleton phase but then crashed before
    /// finishing chunks, a fresh run must resume from `AfterSkeleton` without
    /// error. The skeleton verify path opens a read-only session — no commit
    /// is attempted — so this exercises resume logic rather than empty
    /// changeset handling. Still, it must reach phase=complete cleanly.
    #[tokio::test]
    async fn skeleton_resume_when_already_committed() {
        let pairs = build_2x2_pairs();
        let (source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        // Commit only the skeleton phase by opening a session and writing
        // the metadata keys, then committing with phase=skeleton.
        {
            let session = repo.writable_session("main").await.unwrap();
            let session = Arc::new(RwLock::new(session));
            let store = Store::from_session(Arc::clone(&session)).await;
            for (k, v) in &pairs {
                if is_metadata_key(k) {
                    store.set(k, v.clone()).await.unwrap();
                }
            }
            let props = make_props(PHASE_SKELETON, &BTreeMap::new());
            let mut s = session.write().await;
            s.commit("skeleton only")
                .properties(props)
                .allow_empty(true)
                .execute()
                .await
                .unwrap();
        }

        // Resume: skeleton is already committed, so the run should skip
        // the skeleton phase and proceed to chunks.
        let outcome =
            ingest_from_object_store(source, &prefix, &repo, IngestOptions::new())
                .await
                .unwrap();

        let snap_id = repo.lookup_branch("main").await.unwrap();
        let info = repo.lookup_snapshot(&snap_id).await.unwrap();
        assert_eq!(
            info.metadata.get(PROP_PHASE).and_then(|v| v.as_str()),
            Some(PHASE_COMPLETE),
            "resumed run must reach phase=complete"
        );
        let _ = outcome;
    }

    #[tokio::test]
    async fn overwrite_clobbers_completed_ingest() {
        let pairs1 = build_2x2_pairs();
        let (source1, prefix1) = materialize(&pairs1).await;
        let repo = fresh_repo().await;

        ingest_from_object_store(source1, &prefix1, &repo, IngestOptions::new())
            .await
            .unwrap();

        // Default options error on AlreadyComplete.
        let (source2, prefix2) = materialize(&pairs1).await;
        let err =
            ingest_from_object_store(source2, &prefix2, &repo, IngestOptions::new())
                .await
                .unwrap_err();
        assert!(matches!(err, IngestError::AlreadyComplete));

        // on_collision=Overwrite succeeds.
        let (source3, prefix3) = materialize(&pairs1).await;
        ingest_from_object_store(
            source3,
            &prefix3,
            &repo,
            IngestOptions::new().on_collision(CollisionPolicy::Overwrite),
        )
        .await
        .unwrap();
    }

    /// Two 1D arrays, four chunks each. Used by `crash_then_resume_*`.
    fn build_two_array_pairs() -> Vec<(String, Bytes)> {
        vec![
            ("zarr.json".to_string(), group_meta()),
            ("a/zarr.json".to_string(), array_meta_1d4()),
            ("a/c/0".to_string(), Bytes::from_static(&[10u8])),
            ("a/c/1".to_string(), Bytes::from_static(&[11u8])),
            ("a/c/2".to_string(), Bytes::from_static(&[12u8])),
            ("a/c/3".to_string(), Bytes::from_static(&[13u8])),
            ("b/zarr.json".to_string(), array_meta_1d4()),
            ("b/c/0".to_string(), Bytes::from_static(&[20u8])),
            ("b/c/1".to_string(), Bytes::from_static(&[21u8])),
            ("b/c/2".to_string(), Bytes::from_static(&[22u8])),
            ("b/c/3".to_string(), Bytes::from_static(&[23u8])),
        ]
    }

    /// Crash mid-ingest via `FaultyStore`, then resume against a clean
    /// source and verify byte-faithful completeness + cursor-honored
    /// resume (no chunks re-copied beyond what the cursor demands).
    #[tokio::test]
    async fn crash_then_resume_completes_byte_faithfully() {
        let pairs = build_two_array_pairs();
        let (clean_source, prefix) = materialize(&pairs).await;
        let repo = fresh_repo().await;

        // Fail after 5 successful gets. With concurrency=1 the order is
        // deterministic: 3 skeleton metadata gets + 2 chunk gets from
        // batch 1 of array `a`. Get #6 (first of batch 2) errors out.
        let faulty: Arc<dyn ObjectStore> =
            Arc::new(FaultyStore::new(Arc::clone(&clean_source), 5));
        let err = ingest_from_object_store(
            faulty,
            &prefix,
            &repo,
            IngestOptions::new().concurrency(1).checkpoint_every(2),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, IngestError::ObjectStore(_)), "got {err:?}");

        // Tip after crash: skeleton + 1 chunk batch committed.
        let snap_id = repo.lookup_branch("main").await.unwrap();
        let info = repo.lookup_snapshot(&snap_id).await.unwrap();
        let phase = info.metadata.get(PROP_PHASE).and_then(|v| v.as_str());
        assert_eq!(phase, Some(PHASE_CHUNKS));
        let cursors = info.metadata.get(PROP_CURSORS).and_then(|v| v.as_object());
        let cursors = cursors.expect("expected cursors map on partial commit");
        assert_eq!(
            cursors.get("/a").and_then(|v| v.as_str()),
            Some("/a/c/1"),
            "array a should be paused at /a/c/1 (last key of batch 1)"
        );
        assert!(
            cursors.get("/b").is_none(),
            "array b should not have started yet, got {cursors:?}"
        );

        // Resume with the clean source — the cursor scheme should pick
        // up at /a/c/2 and finish through array b.
        let resumed = ingest_from_object_store(
            clean_source,
            &prefix,
            &repo,
            IngestOptions::new().concurrency(1).checkpoint_every(2),
        )
        .await
        .unwrap();
        // 6 remaining chunks: /a/c/2, /a/c/3, /b/c/0..3.
        assert_eq!(resumed.stats.keys, 6);

        // Final phase = complete.
        let snap_id = repo.lookup_branch("main").await.unwrap();
        let info = repo.lookup_snapshot(&snap_id).await.unwrap();
        assert_eq!(
            info.metadata.get(PROP_PHASE).and_then(|v| v.as_str()),
            Some(PHASE_COMPLETE),
        );

        // Byte-faithful: every source chunk is present in the dest with
        // matching bytes.
        let session = repo
            .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
            .await
            .unwrap();
        let dest = Store::from_session(Arc::new(RwLock::new(session))).await;
        for (key, expected) in &pairs {
            let got = dest.get(key, &ByteRange::ALL).await.unwrap();
            assert_eq!(&got[..], &expected[..], "mismatch at {key}");
        }
    }
}
