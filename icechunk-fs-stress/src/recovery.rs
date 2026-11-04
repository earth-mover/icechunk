//! What `verify` does to the repository after checking it: a timed recovery
//! write through whatever the run left behind, then aging and sweeping the
//! protocol's leftovers, after which the storage root must hold only `repo`
//! and `repo.d/` only the marker and its version.

use std::{
    fs::{self, FileType},
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::Context as _;
use chrono::Utc;
use icechunk::{
    format::{
        CHUNKS_FILE_PATH, ChunkId, MANIFESTS_FILE_PATH, ManifestId,
        OVERWRITTEN_FILES_PATH, SNAPSHOTS_FILE_PATH, SnapshotId,
        TRANSACTION_LOGS_FILE_PATH, layout_v1,
    },
    storage::{GcPhase, GetModifiedResult, Settings, StorageContext},
};
use uuid::Uuid;

use crate::{
    config::{Backend, Mode, RunConfig, WorkerId},
    layout::{RunRoot, VERIFY_TAG},
    log::{LogWriter, OpResult, Record, read_log, start_worker_log},
    oracle::{Verdict, Violation, Warning},
    repo_mode::{self, RepoWorker},
    storage_mode::{self, Attempt, REPO_KEY, Updater},
};

/// The slot of `verify`'s recovery writes, logged as
/// `logs/verify.recovery.0.log`.
pub const RECOVERY_SLOT: &str = "recovery";

/// How far back [`age_storage_root`] sets mtimes: past
/// `FilesystemStorage::SWEEP_MIN_AGE`.
pub const AGE: Duration = Duration::from_secs(25 * 3600);

const RETRY_BACKOFF: Duration = Duration::from_millis(50);

pub fn recovery_worker() -> WorkerId {
    WorkerId {
        tag: VERIFY_TAG.to_owned(),
        slot: RECOVERY_SLOT.to_owned(),
        incarnation: 0,
    }
}

/// No crash leaves anything a writer has to wait out, so this only covers a
/// slow filesystem.
pub const RECOVERY_DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    Succeeded { elapsed: Duration },
    Failed { elapsed: Duration, last_error: String },
}

/// One write on top of whatever the run left behind, through a fresh storage
/// without fault hooks, retried until it succeeds or `deadline` passes:
/// a conditional update of `repo` in storage mode, a commit with rebase to
/// `main` in repository mode. `Err` only if the recovery log can't be
/// written.
pub async fn recovery_write(
    root: &RunRoot,
    config: &RunConfig,
    deadline: Duration,
) -> anyhow::Result<RecoveryOutcome> {
    let who = recovery_worker();
    let log = Arc::new(start_worker_log(root, &who)?);
    let started = Instant::now();
    let until = started + deadline;
    let last_error = match config.mode {
        Mode::Storage => recover_storage(root, config, &who, &log, until).await?,
        Mode::Repository => recover_repo(root, config, &who, log, until).await?,
    };
    let elapsed = started.elapsed();
    Ok(match last_error {
        None => RecoveryOutcome::Succeeded { elapsed },
        Some(last_error) => RecoveryOutcome::Failed { elapsed, last_error },
    })
}

/// The last error, or `None` once a write succeeded.
async fn recover_storage(
    root: &RunRoot,
    config: &RunConfig,
    who: &WorkerId,
    log: &LogWriter,
    until: Instant,
) -> anyhow::Result<Option<String>> {
    let storage = storage_mode::storage(root, config, None).await?;
    let mut updater = Updater::new(storage, who, config).await?;
    let mut last_error = "no attempt finished in time".to_owned();
    while Instant::now() < until {
        let attempt = tokio::time::timeout_at(until.into(), updater.attempt(log)).await;
        last_error = match attempt {
            // An attempt cut short says nothing new.
            Err(_) => break,
            Ok(attempt) => match attempt? {
                Attempt::Logged(OpResult::Ack { .. }) => return Ok(None),
                Attempt::Logged(result) => describe(&result),
                Attempt::Raced => "repo changed while it was backed up".to_owned(),
                Attempt::Failed(err) => err,
            },
        };
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
    Ok(Some(last_error))
}

/// The last error, or `None` once a commit succeeded.
async fn recover_repo(
    root: &RunRoot,
    config: &RunConfig,
    who: &WorkerId,
    log: Arc<LogWriter>,
    until: Instant,
) -> anyhow::Result<Option<String>> {
    let mut last_error = "no attempt finished in time".to_owned();
    let mut worker = None;
    while Instant::now() < until {
        let worker = match &mut worker {
            Some(worker) => worker,
            None => match tokio::time::timeout_at(
                until.into(),
                repo_mode::open_repo(root, config, None),
            )
            .await
            {
                Err(_) => {
                    last_error =
                        "opening the repository didn't finish in time".to_owned();
                    break;
                }
                Ok(Ok(repo)) => worker.insert(RepoWorker::new(
                    repo,
                    Arc::clone(&log),
                    who,
                    config.chunk_bytes,
                )?),
                Ok(Err(err)) => {
                    last_error = format!("{err:#}");
                    tokio::time::sleep(RETRY_BACKOFF).await;
                    continue;
                }
            },
        };
        let commit = tokio::time::timeout_at(until.into(), worker.main_commit()).await;
        last_error = match commit {
            Err(_) => break,
            Ok(commit) => match commit? {
                Some(OpResult::Ack { .. }) => return Ok(None),
                Some(result) => describe(&result),
                None => worker
                    .prepare_error()
                    .unwrap_or("preparing the commit failed")
                    .to_owned(),
            },
        };
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
    Ok(Some(last_error))
}

/// One write on top of a fork, through a fresh storage without fault hooks:
/// the attempt's intent id and outcome, or why there was none. `Err` only if
/// the recovery log can't be written or read.
pub async fn recovery_write_once(
    root: &RunRoot,
    config: &RunConfig,
) -> anyhow::Result<Result<(Uuid, OpResult), String>> {
    let who = recovery_worker();
    let log = Arc::new(start_worker_log(root, &who)?);
    let result = match config.mode {
        Mode::Storage => {
            let storage = storage_mode::storage(root, config, None).await?;
            let mut updater = Updater::new(storage, &who, config).await?;
            match updater.attempt(&log).await? {
                Attempt::Logged(result) => Ok(result),
                Attempt::Raced => Err("repo changed while it was backed up".to_owned()),
                Attempt::Failed(err) => Err(err),
            }
        }
        Mode::Repository => match repo_mode::open_repo(root, config, None).await {
            Ok(repo) => {
                let mut worker =
                    RepoWorker::new(repo, Arc::clone(&log), &who, config.chunk_bytes)?;
                worker.main_commit().await?.ok_or_else(|| {
                    worker
                        .prepare_error()
                        .unwrap_or("preparing the commit failed")
                        .to_owned()
                })
            }
            Err(err) => Err(format!("{err:#}")),
        },
    };
    let result = match result {
        Ok(result) => result,
        Err(why) => return Ok(Err(why)),
    };
    let path = root.worker_log(&who.tag, &who.slot, who.incarnation);
    let id = read_log(&path)?
        .into_iter()
        .rev()
        .find_map(|record| match record {
            Record::Intent { id, .. } => Some(id),
            _ => None,
        })
        .context("the recovery write logged an outcome without an intent")?;
    Ok(Ok((id, result)))
}

pub fn describe(result: &OpResult) -> String {
    match result {
        OpResult::Ack { .. } => "acknowledged".to_owned(),
        OpResult::Conflict => "conflict".to_owned(),
        OpResult::Error { message }
        | OpResult::ReadOnly { message }
        | OpResult::Forked { message }
        | OpResult::DurableButForked { message, .. }
        | OpResult::ConversionInProgress { message } => message.clone(),
    }
}

/// The version directory, and the prefix of the directories that become it.
const REPO_D: &str = "repo.d";
const REPO_D_TMP_PREFIX: &str = "repo.d.tmp.";

/// Sets the mtime of the protocol's leftovers [`AGE`] into the past, so the
/// sweep treats them as old: every regular file in the storage root other
/// than `repo` and in `repo.d` (`stage.<id>`, `mirror.<id>`, versions), and
/// every `repo.d.tmp.*` directory.
pub fn age_storage_root(root: &RunRoot) -> anyhow::Result<()> {
    let aged = SystemTime::now() - AGE;
    let storage = root.storage();
    let in_root = entries(&storage)?.into_iter().filter(|(name, file_type, _)| {
        (file_type.is_file() && name != REPO_KEY)
            || (file_type.is_dir() && name.starts_with(REPO_D_TMP_PREFIX))
    });
    let in_repo_d = entries_if_exists(&storage.join(REPO_D))?
        .into_iter()
        .filter(|(_, file_type, _)| file_type.is_file());
    age_paths(in_root.chain(in_repo_d).map(|(_, _, path)| path), aged)
}

/// Sets the mtime of every path to `aged`. A path that a fresh check shows
/// gone is skipped: a stale NFS client cache can report a vanished name as
/// `EACCES`. Every other failure is reported, after trying the rest.
fn age_paths(
    paths: impl IntoIterator<Item = PathBuf>,
    aged: SystemTime,
) -> anyhow::Result<()> {
    let errors: Vec<String> = paths
        .into_iter()
        .filter_map(|path| {
            let err = set_mtime(&path, aged).err()?;
            let gone = fs::symlink_metadata(&path)
                .is_err_and(|err| err.kind() == io::ErrorKind::NotFound);
            (!gone).then(|| format!("aging {}: {err}", path.display()))
        })
        .collect();
    if errors.is_empty() { Ok(()) } else { anyhow::bail!("{}", errors.join("; ")) }
}

/// Sets the mtime of `path` to the second, by path and without following a
/// symlink. Explicit times need only ownership, not write or read permission,
/// so this also ages the mode-000 file an `NFSv3` exclusive create leaves when
/// it is killed before its `SETATTR`.
fn set_mtime(path: &Path, mtime: SystemTime) -> io::Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt as _};

    let since_epoch =
        mtime.duration_since(SystemTime::UNIX_EPOCH).map_err(io::Error::other)?;
    let path = CString::new(path.as_os_str().as_bytes())?;
    let times = [
        libc::timespec { tv_sec: 0, tv_nsec: libc::UTIME_OMIT },
        libc::timespec {
            tv_sec: since_epoch.as_secs().try_into().map_err(io::Error::other)?,
            tv_nsec: 0,
        },
    ];
    // SAFETY: `path` is NUL-terminated and `times` holds two timespecs, which
    // `utimensat` only reads.
    #[expect(unsafe_code)]
    let rc = unsafe {
        libc::utimensat(
            libc::AT_FDCWD,
            path.as_ptr(),
            times.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

/// Deletes the protocol's leftovers in the storage root: in storage mode with
/// the storage's sweep, which on the filesystem backend GC's hook precedes,
/// as in a GC, with the current version as the one GC's change made; in
/// repository mode with a GC that also deletes everything unreachable.
pub async fn sweep(root: &RunRoot, config: &RunConfig) -> anyhow::Result<()> {
    let now = Utc::now();
    match config.mode {
        Mode::Storage => {
            let storage = storage_mode::storage(root, config, None).await?;
            let settings = Settings::default();
            let ctx = StorageContext::unattributed(&settings);
            if config.backend == Backend::Filesystem {
                let current = match storage
                    .get_object_conditional(&ctx, REPO_KEY, None)
                    .await
                    .context("reading the current version")?
                {
                    GetModifiedResult::Modified { new_version, .. } => new_version,
                    GetModifiedResult::OnLatestVersion => {
                        anyhow::bail!(
                            "reading {REPO_KEY} without a version returned no version"
                        )
                    }
                };
                let phase = GcPhase::BeforeDeletes { changed: Some(&current) };
                storage
                    .gc_hook(&ctx, phase)
                    .await
                    .context("GC's hook before the sweep")?;
            }
            storage.sweep_leftovers(&ctx, now, false).await.context("sweeping")?;
        }
        Mode::Repository => {
            let repo = repo_mode::open_repo(root, config, None).await?;
            repo_mode::collect_garbage(&repo, config, now)
                .await
                .context("garbage collecting")?;
        }
    }
    Ok(())
}

/// An entry `verify` plants before aging and sweeping, which the sweep and
/// GC must keep: an unrecognized name, or an object id that isn't canonical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Canary {
    storage: PathBuf,
    name: String,
}

impl Canary {
    /// Relative to the storage root.
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn path(&self) -> PathBuf {
        self.storage.join(&self.name)
    }
}

/// The name of the plain canaries.
const CANARY: &str = "stress-canary";

/// Plants the canaries, aged [`AGE`] into the past: unknown names in the
/// storage root (the removed concepts' among them, and a directory),
/// `overwritten/` and, on the filesystem backend, `repo.d`; in repository
/// mode object ids GC must not parse, lowercase or one character short.
pub fn plant_canaries(root: &RunRoot, config: &RunConfig) -> anyhow::Result<Vec<Canary>> {
    let storage = root.storage();
    let id = Uuid::new_v4();
    let mut files = vec![
        CANARY.to_owned(),
        format!("repo.legacy.{id}"),
        format!("repo.stub.{id}"),
        format!("{OVERWRITTEN_FILES_PATH}/{CANARY}"),
    ];
    let dirs = [format!("repo.d.converting.{id}")];
    if config.backend == Backend::Filesystem {
        let hash = "0".repeat(64);
        files.extend([
            format!("{REPO_D}/{CANARY}"),
            format!("{REPO_D}/frozen.{MAX_N:015}.0.{hash}"),
            format!("{REPO_D}/converted.{id}"),
        ]);
    }
    if config.mode == Mode::Repository {
        let chunk = ChunkId::random().to_string();
        files.extend([
            format!(
                "{SNAPSHOTS_FILE_PATH}/{}",
                lowercase(&SnapshotId::random().to_string())
            ),
            format!(
                "{MANIFESTS_FILE_PATH}/{}",
                lowercase(&ManifestId::random().to_string())
            ),
            format!(
                "{TRANSACTION_LOGS_FILE_PATH}/{}",
                lowercase(&SnapshotId::random().to_string())
            ),
            format!("{CHUNKS_FILE_PATH}/{}", &chunk[..chunk.len() - 1]),
        ]);
    }
    let aged = SystemTime::now() - AGE;
    let mut canaries = Vec::new();
    for (name, dir) in
        files.into_iter().map(|name| (name, false)).chain(dirs.map(|n| (n, true)))
    {
        let canary = Canary { storage: storage.clone(), name };
        let path = canary.path();
        #[expect(
            clippy::create_dir,
            reason = "a canary must be new: an existing directory would be aged and judged"
        )]
        let planted = if dir {
            fs::create_dir(&path)
        } else {
            path.parent()
                .map_or(Ok(()), fs::create_dir_all)
                .and_then(|()| fs::write(&path, CANARY))
        };
        planted
            .and_then(|()| set_mtime(&path, aged))
            .with_context(|| format!("planting {}", path.display()))?;
        canaries.push(canary);
    }
    Ok(canaries)
}

/// A Crockford id in lowercase, which no canonical id is: it has letters.
fn lowercase(id: &str) -> String {
    let lower = id.to_lowercase();
    if lower == id { format!("{lower}z") } else { lower }
}

/// [`Violation::DeletedUnrecognized`] for every canary that is gone.
pub fn check_canaries(canaries: &[Canary], verdict: &mut Verdict) {
    for canary in canaries {
        match fs::symlink_metadata(canary.path()) {
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                verdict
                    .violations
                    .push(Violation::DeletedUnrecognized { name: canary.name() });
            }
            Err(err) => verdict.violations.push(Violation::Harness {
                detail: format!("checking the canary {}: {err}", canary.name()),
            }),
        }
    }
}

/// Removes the canaries that exist.
pub fn remove_canaries(canaries: &[Canary]) -> anyhow::Result<()> {
    let errors: Vec<String> = canaries
        .iter()
        .filter_map(|canary| {
            let path = canary.path();
            let removed = match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&path),
                Ok(_) => fs::remove_file(&path),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            };
            removed.err().map(|err| format!("removing {}: {err}", path.display()))
        })
        .collect();
    if errors.is_empty() { Ok(()) } else { anyhow::bail!("{}", errors.join("; ")) }
}

/// The storage root after the sweep: `repo` must exist (or it is broken
/// history); `*.staging.<id>` files, `object_store`'s `repo#<n>` and
/// `repo.d.tmp.<id>/` directories are leftovers the sweep should have
/// deleted; the data directories are allowed, and staging files inside them
/// are a warning, as no sweep covers them; any other name is unknown, a
/// warning: the layout never creates it and GC never deletes it. On the
/// filesystem backend `repo.d` is checked too. `.nfs*` names, which an NFS
/// client may keep for a while after a deletion, are ignored.
pub fn check_clean_root(
    root: &RunRoot,
    config: &RunConfig,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    let layout = config.backend == Backend::Filesystem;
    let (mut leftovers, mut unknown) = (Vec::new(), Vec::new());
    let mut count = 0;
    let mut mirror = false;
    for (name, file_type, path) in entries(&root.storage())? {
        if is_nfs(&name) {
            continue;
        }
        if file_type.is_file() {
            if name == REPO_KEY {
                mirror = true;
            } else if is_staging(&name) || name.starts_with(OBJECT_STORE_TMP_PREFIX) {
                leftovers.push(name);
            } else {
                unknown.push(name);
            }
        } else if file_type.is_dir() {
            if is_tmp_dir(&name) {
                leftovers.push(name);
            } else if DATA_DIRS.contains(&name.as_str()) {
                count += count_staging(&path)?;
            } else if !(layout && name == REPO_D) {
                unknown.push(name);
            }
        } else {
            unknown.push(name);
        }
    }
    if !mirror {
        verdict.violations.push(Violation::BrokenHistory {
            detail: format!("{REPO_KEY} is missing from the storage root"),
        });
    }
    if layout {
        check_repo_d(
            &root.storage().join(REPO_D),
            &mut leftovers,
            &mut unknown,
            verdict,
        )?;
    }
    if !leftovers.is_empty() {
        leftovers.sort();
        verdict.violations.push(Violation::Leftovers { names: leftovers });
    }
    if count > 0 {
        verdict.warnings.push(Warning::SubdirStaging { count });
    }
    if !unknown.is_empty() {
        unknown.sort();
        verdict.warnings.push(Warning::UnknownEntries { names: unknown });
    }
    Ok(())
}

/// The directories icechunk writes objects to.
const DATA_DIRS: [&str; 5] = [
    OVERWRITTEN_FILES_PATH,
    SNAPSHOTS_FILE_PATH,
    MANIFESTS_FILE_PATH,
    TRANSACTION_LOGS_FILE_PATH,
    CHUNKS_FILE_PATH,
];

/// What `object_store`'s local storage writes before renaming into place.
const OBJECT_STORE_TMP_PREFIX: &str = "repo#";

/// The largest version number, whose nines' complement a marker's `K` is.
const MAX_N: u64 = 999_999_999_999_999;

/// `repo.d` after the sweep: exactly one marker `latest.<K>.<N>.<h>` with `K`
/// the 15-digit complement of `N`, its version `<N>.<h>`, which lists the
/// layout's writer feature (else a conversion to the object store layout was
/// interrupted); other versions, `stage.<id>`, `mirror.<id>` and staging
/// files are leftovers; anything else is unknown. A missing `repo.d`, a
/// number of markers other than one, or a marker without its version is a
/// broken history.
fn check_repo_d(
    dir: &Path,
    leftovers: &mut Vec<String>,
    unknown: &mut Vec<String>,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    let mut broken =
        |detail| verdict.violations.push(Violation::BrokenHistory { detail });
    if !dir.exists() {
        broken(format!("{} is missing", dir.display()));
        return Ok(());
    }
    let names: Vec<String> = entries(dir)?
        .into_iter()
        .map(|(name, ..)| name)
        .filter(|name| !is_nfs(name))
        .collect();
    let markers: Vec<&String> =
        names.iter().filter(|name| name.starts_with("latest.")).collect();
    let [marker] = markers[..] else {
        broken(format!(
            "{REPO_D} holds {} markers among: {}",
            markers.len(),
            names.join(", ")
        ));
        return Ok(());
    };
    let Some(version) = marker_version(marker) else {
        broken(format!("{REPO_D} holds the malformed marker {marker}"));
        return Ok(());
    };
    if names.iter().any(|name| name == version) {
        let path = dir.join(version);
        let bytes =
            fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        match layout_v1::lists_feature(&bytes) {
            Ok(true) => {}
            Ok(false) => verdict.violations.push(Violation::FeatureMissing {
                what: "the current version".to_owned(),
                detail: format!(
                    "{REPO_D}/{version} doesn't list {}: a conversion to the object \
                     store layout was interrupted",
                    layout_v1::FEATURE
                ),
            }),
            Err(err) => {
                broken(format!("{REPO_D}/{version} doesn't decode: {}", err.kind()));
            }
        }
    } else {
        broken(format!(
            "{REPO_D} holds the marker {marker} but not its version {version}"
        ));
    }
    for name in names.iter().filter(|name| *name != marker && *name != version) {
        let swept = parse_version(name).is_some()
            || ["stage.", "mirror."]
                .iter()
                .any(|prefix| name.strip_prefix(prefix).is_some_and(is_hyphenated_uuid))
            || is_staging(name);
        let entry = format!("{REPO_D}/{name}");
        if swept { leftovers.push(entry) } else { unknown.push(entry) }
    }
    Ok(())
}

/// The `<N>.<h>` a marker `latest.<K>.<N>.<h>` names, if `K` is the 15-digit
/// nines' complement of `N`.
fn marker_version(name: &str) -> Option<&str> {
    let rest = name.strip_prefix("latest.")?;
    let (k, version) = rest.split_once('.')?;
    let (n, _) = parse_version(version)?;
    (k == format!("{:015}", MAX_N - n)).then_some(version)
}

/// `<N>.<h>`: `N` at most [`MAX_N`] without leading zeros, `h` 64 lowercase
/// hex characters.
fn parse_version(name: &str) -> Option<(u64, &str)> {
    let (n, h) = name.split_once('.')?;
    let canonical_n = !n.is_empty()
        && n.bytes().all(|b| b.is_ascii_digit())
        && (n == "0" || !n.starts_with('0'));
    let hash = h.len() == 64 && h.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    let n: u64 = n.parse().ok().filter(|&n| canonical_n && n <= MAX_N)?;
    hash.then_some((n, h))
}

/// `<name>.staging.<id>`.
fn is_staging(name: &str) -> bool {
    name.rsplit_once(".staging.").is_some_and(|(_, id)| is_hyphenated_uuid(id))
}

/// `repo.d.tmp.<id>`.
fn is_tmp_dir(name: &str) -> bool {
    name.strip_prefix(REPO_D_TMP_PREFIX).is_some_and(is_hyphenated_uuid)
}

fn is_hyphenated_uuid(s: &str) -> bool {
    Uuid::try_parse(s).is_ok_and(|id| id.hyphenated().to_string() == s)
}

fn is_nfs(name: &str) -> bool {
    name.starts_with(".nfs")
}

fn read_dir(dir: &Path) -> anyhow::Result<fs::ReadDir> {
    fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))
}

/// The name, type and path of every entry of `dir`.
fn entries(dir: &Path) -> anyhow::Result<Vec<(String, FileType, PathBuf)>> {
    read_dir(dir)?
        .map(|entry| {
            let entry = entry.with_context(|| format!("listing {}", dir.display()))?;
            let file_type = entry
                .file_type()
                .with_context(|| format!("checking {}", entry.path().display()))?;
            Ok((
                entry.file_name().to_string_lossy().into_owned(),
                file_type,
                entry.path(),
            ))
        })
        .collect()
}

fn entries_if_exists(dir: &Path) -> anyhow::Result<Vec<(String, FileType, PathBuf)>> {
    if dir.exists() { entries(dir) } else { Ok(Vec::new()) }
}

/// Staging files anywhere below `dir`.
fn count_staging(dir: &Path) -> anyhow::Result<u64> {
    let mut count = 0;
    for entry in read_dir(dir)? {
        let entry = entry.with_context(|| format!("listing {}", dir.display()))?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            count += count_staging(&entry.path())?;
        } else if file_type.is_file()
            && entry.file_name().to_string_lossy().contains(".staging.")
        {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::PathBuf};

    use futures::TryStreamExt as _;
    use icechunk::repository::VersionInfo;
    use uuid::Uuid;

    use super::*;
    use crate::{
        config::Mode,
        log::{Op, OpResult, Record, read_log},
        oracle::{Verdict, Violation, Warning},
        repo_mode::{self, MAIN_BRANCH, parse_commit_message},
        storage_mode::{decode, payload_etag, read_repo},
        supervisor,
    };

    async fn initialized(mode: Mode) -> (tempfile::TempDir, RunRoot, RunConfig) {
        initialized_with(RunConfig::defaults(mode, 1)).await
    }

    async fn initialized_with(
        config: RunConfig,
    ) -> (tempfile::TempDir, RunRoot, RunConfig) {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().join("run"));
        supervisor::init(&root, &config).await.unwrap();
        (dir, root, config)
    }

    fn repo_d(root: &RunRoot) -> PathBuf {
        root.storage().join("repo.d")
    }

    fn names(dir: &Path) -> BTreeSet<String> {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    /// The marker's `<N>.<h>`.
    fn current(root: &RunRoot) -> (u64, String) {
        let marker = names(&repo_d(root))
            .into_iter()
            .find(|name| name.starts_with("latest."))
            .unwrap();
        let mut parts = marker.splitn(4, '.').skip(2);
        let n = parts.next().unwrap().parse().unwrap();
        (n, parts.next().unwrap().to_owned())
    }

    /// What a writer killed between its link and its rename leaves behind:
    /// its next version and a staging file.
    fn plant_crashed_attempt(root: &RunRoot) -> Vec<String> {
        let (n, _) = current(root);
        let bytes = b"a crashed attempt";
        let version = format!("{}.{}", n + 1, payload_etag(bytes));
        let stage = format!("stage.{}", Uuid::new_v4());
        for name in [&version, &stage] {
            fs::write(repo_d(root).join(name), bytes).unwrap();
        }
        vec![version, stage]
    }

    fn root_files(root: &RunRoot) -> BTreeSet<String> {
        fs::read_dir(root.storage())
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| entry.file_type().unwrap().is_file())
            .map(|entry| entry.file_name().into_string().unwrap())
            .collect()
    }

    /// `repo.d` holds exactly the marker and the version it names.
    fn assert_repo_d_is_minimal(root: &RunRoot) {
        let (n, h) = current(root);
        let k = 999_999_999_999_999 - n;
        assert_eq!(
            names(&repo_d(root)),
            BTreeSet::from([format!("{n}.{h}"), format!("latest.{k:015}.{n}.{h}")])
        );
    }

    fn assert_recovery_log_acked(root: &RunRoot) {
        let who = recovery_worker();
        let records = read_log(&root.worker_log(&who.tag, &who.slot, 0)).unwrap();
        assert!(matches!(records.first(), Some(Record::Start { .. })), "{records:?}");
        assert!(
            matches!(
                records.last(),
                Some(Record::Outcome { result: OpResult::Ack { .. }, .. })
            ),
            "{records:?}"
        );
    }

    async fn age_sweep_and_check(root: &RunRoot, config: &RunConfig) -> Verdict {
        age_storage_root(root).unwrap();
        sweep(root, config).await.unwrap();
        clean_root_verdict(root, config)
    }

    fn clean_root_verdict(root: &RunRoot, config: &RunConfig) -> Verdict {
        let mut verdict = Verdict::default();
        check_clean_root(root, config, &mut verdict).unwrap();
        verdict
    }

    async fn repo_payload(root: &RunRoot, config: &RunConfig) -> Vec<u8> {
        let storage = storage_mode::storage(root, config, None).await.unwrap();
        read_repo(&*storage).await.unwrap().to_vec()
    }

    #[tokio::test]
    async fn storage_recovery_commits_over_a_crashed_attempt_then_the_root_is_clean() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let initial = payload_etag(&repo_payload(&root, &config).await);
        let crashed = plant_crashed_attempt(&root);

        let outcome = recovery_write(&root, &config, RECOVERY_DEADLINE).await;
        let RecoveryOutcome::Succeeded { elapsed } = outcome.unwrap() else {
            panic!("recovery failed");
        };
        assert!(elapsed < RECOVERY_DEADLINE, "{elapsed:?}");
        let payload =
            decode(&repo_payload(&root, &config).await).unwrap().payload.unwrap();
        assert_eq!(payload.host, VERIFY_TAG);
        assert_eq!(payload.slot, RECOVERY_SLOT);
        assert_eq!(payload.parent, initial);
        assert_recovery_log_acked(&root);
        // The commit's tidy-up deleted the crashed attempt's version.
        let left = names(&repo_d(&root));
        assert!(!left.contains(&crashed[0]) && left.contains(&crashed[1]), "{left:?}");

        assert_eq!(age_sweep_and_check(&root, &config).await, Verdict::default());
        assert_eq!(root_files(&root), BTreeSet::from([REPO_KEY.to_owned()]));
        assert_repo_d_is_minimal(&root);
    }

    #[tokio::test]
    async fn repository_recovery_commits_to_main_then_gc_cleans_the_root() {
        let (_dir, root, config) = initialized(Mode::Repository).await;
        plant_crashed_attempt(&root);

        let outcome = recovery_write(&root, &config, RECOVERY_DEADLINE).await;
        assert!(matches!(outcome.unwrap(), RecoveryOutcome::Succeeded { .. }));
        assert_recovery_log_acked(&root);
        let repo = repo_mode::open_repo(&root, &config, None).await.unwrap();
        let main = VersionInfo::BranchTipRef(MAIN_BRANCH.to_owned());
        let ancestry: Vec<_> =
            repo.ancestry(&main).await.unwrap().try_collect().await.unwrap();
        assert!(parse_commit_message(&ancestry[0].message).is_some(), "{ancestry:?}");

        assert_eq!(age_sweep_and_check(&root, &config).await, Verdict::default());
        assert_eq!(root_files(&root), BTreeSet::from([REPO_KEY.to_owned()]));
        assert_repo_d_is_minimal(&root);
    }

    #[tokio::test]
    async fn aging_covers_repo_d_and_root_leftovers_but_not_repo() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let storage = root.storage();
        let stage = repo_d(&root).join(format!("stage.{}", Uuid::new_v4()));
        let staging = storage.join(format!("repo.staging.{}", Uuid::new_v4()));
        let tmp = storage.join(format!("repo.d.tmp.{}", Uuid::new_v4()));
        fs::write(&stage, b"s").unwrap();
        fs::write(&staging, b"s").unwrap();
        fs::create_dir_all(&tmp).unwrap();
        fs::write(tmp.join("0.x"), b"t").unwrap();

        age_storage_root(&root).unwrap();
        let age = |path: &Path| {
            fs::metadata(path).unwrap().modified().unwrap().elapsed().unwrap()
        };
        let (n, h) = current(&root);
        for path in [&stage, &staging, &tmp, &repo_d(&root).join(format!("{n}.{h}"))] {
            assert!(age(path) >= AGE, "{}", path.display());
        }
        assert!(age(&storage.join(REPO_KEY)) < AGE);

        sweep(&root, &config).await.unwrap();
        assert_eq!(clean_root_verdict(&root, &config), Verdict::default());
        assert!(!stage.exists() && !staging.exists() && !tmp.exists());
    }

    /// What an `NFSv3` exclusive create killed before its `SETATTR` leaves.
    #[tokio::test]
    async fn aging_covers_a_mode_000_file() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_dir, root, _config) = initialized(Mode::Storage).await;
        let stage = repo_d(&root).join(format!("stage.{}", Uuid::new_v4()));
        fs::write(&stage, b"").unwrap();
        fs::set_permissions(&stage, fs::Permissions::from_mode(0o000)).unwrap();

        age_storage_root(&root).unwrap();
        let age = fs::metadata(&stage).unwrap().modified().unwrap().elapsed().unwrap();
        assert!(age >= AGE, "{age:?}");
    }

    #[test]
    fn aging_skips_names_that_are_gone_and_reports_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file");
        let older = dir.path().join("older");
        fs::write(&file, b"f").unwrap();
        fs::write(&older, b"o").unwrap();
        let aged = SystemTime::now() - AGE;

        let err = age_paths(
            [dir.path().join("gone"), file.join("not-a-dir"), older.clone()],
            aged,
        )
        .unwrap_err();
        let message = format!("{err:#}");
        assert!(message.contains("not-a-dir") && !message.contains("/gone"), "{message}");
        let age = fs::metadata(&older).unwrap().modified().unwrap().elapsed().unwrap();
        assert!(age >= AGE, "{age:?}");
    }

    #[tokio::test]
    async fn a_leftover_younger_than_the_aging_fails_the_clean_root_check() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let outcome = recovery_write(&root, &config, RECOVERY_DEADLINE).await;
        assert!(matches!(outcome.unwrap(), RecoveryOutcome::Succeeded { .. }));
        age_storage_root(&root).unwrap();
        let stage = format!("stage.{}", Uuid::new_v4());
        fs::write(repo_d(&root).join(&stage), b"b").unwrap();
        sweep(&root, &config).await.unwrap();
        assert_eq!(
            clean_root_verdict(&root, &config).violations,
            [Violation::Leftovers { names: vec![format!("repo.d/{stage}")] }]
        );
    }

    #[tokio::test]
    async fn the_clean_root_check_finds_every_protocol_leftover() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let storage = root.storage();
        let (n, _) = current(&root);
        let staging = format!("repo.staging.{}", Uuid::new_v4());
        let tmp = format!("repo.d.tmp.{}", Uuid::new_v4());
        let stage = format!("stage.{}", Uuid::new_v4());
        let mirror = format!("mirror.{}", Uuid::new_v4());
        let version_staging = format!("{n}.x.staging.{}", Uuid::new_v4());
        let above = format!("{}.{}", n + 1, payload_etag(b"above"));
        let below = format!("{}.{}", n - 1, payload_etag(b"below"));
        fs::write(storage.join(&staging), b"s").unwrap();
        fs::create_dir_all(storage.join(&tmp)).unwrap();
        for name in [&stage, &mirror, &version_staging, &above, &below] {
            fs::write(repo_d(&root).join(name), name).unwrap();
        }
        fs::write(storage.join(".nfs0000000000001234"), b"").unwrap();
        fs::write(repo_d(&root).join(".nfs0000000000005678"), b"").unwrap();

        let mut expected = vec![staging, tmp];
        expected.extend(
            [above, below, mirror, stage, version_staging]
                .iter()
                .map(|name| format!("repo.d/{name}")),
        );
        expected.sort();
        let verdict = clean_root_verdict(&root, &config);
        assert_eq!(verdict.violations, [Violation::Leftovers { names: expected }]);
        assert_eq!(verdict.warnings, []);
    }

    /// Names the layout never creates and GC never deletes, the removed
    /// concepts' among them, are reported, not called leftovers.
    #[tokio::test]
    async fn unknown_names_are_a_warning() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let storage = root.storage();
        let (n, h) = current(&root);
        let id = Uuid::new_v4();
        let in_root =
            [format!("repo.stub.{id}"), format!("repo.legacy.{id}"), "notes".to_owned()];
        for name in &in_root {
            fs::write(storage.join(name), b"u").unwrap();
        }
        fs::create_dir_all(storage.join(format!("repo.d.converting.{id}"))).unwrap();
        let in_repo_d = [
            format!("frozen.{:015}.{n}.{h}", 999_999_999_999_999 - n),
            format!("converted.{id}"),
            format!("repo.legacy.{id}"),
            format!("repo.d.converting.{id}"),
            format!("stage.{}", id.simple()),
            format!("0{n}.{h}"),
        ];
        for name in &in_repo_d {
            fs::write(repo_d(&root).join(name), b"u").unwrap();
        }
        let verdict = clean_root_verdict(&root, &config);
        assert_eq!(verdict.violations, []);
        let mut expected: Vec<String> = in_root.to_vec();
        expected.push(format!("repo.d.converting.{id}"));
        expected.extend(in_repo_d.iter().map(|name| format!("repo.d/{name}")));
        expected.sort();
        assert_eq!(verdict.warnings, [Warning::UnknownEntries { names: expected }]);
    }

    #[tokio::test]
    async fn a_missing_mirror_and_inconsistent_markers_are_broken_history() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        fs::remove_file(root.storage().join(REPO_KEY)).unwrap();
        let verdict = clean_root_verdict(&root, &config);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains("repo is missing")),
            "{verdict:?}"
        );

        let (_dir, root, config) = initialized(Mode::Storage).await;
        let (n, h) = current(&root);
        let marker = |k: u64| repo_d(&root).join(format!("latest.{k:015}.{n}.{h}"));
        fs::rename(marker(999_999_999_999_999 - n), marker(999_999_999_999_999 - n - 1))
            .unwrap();
        let verdict = clean_root_verdict(&root, &config);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains("latest.")),
            "{verdict:?}"
        );
    }

    /// A current version without the layout's feature: a conversion to the
    /// object store layout was interrupted after its change.
    #[tokio::test]
    async fn a_featureless_current_version_is_an_interrupted_conversion() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let (n, h) = current(&root);
        let bytes = storage_mode::initial_repo_info(&[]).unwrap();
        let next = format!("{}.{}", n + 1, payload_etag(&bytes));
        fs::write(repo_d(&root).join(&next), &bytes).unwrap();
        let k = |n: u64| 999_999_999_999_999 - n;
        fs::rename(
            repo_d(&root).join(format!("latest.{:015}.{n}.{h}", k(n))),
            repo_d(&root).join(format!("latest.{:015}.{next}", k(n + 1))),
        )
        .unwrap();
        let verdict = clean_root_verdict(&root, &config);
        assert!(
            verdict.violations.iter().any(|v| matches!(v, Violation::FeatureMissing {
                what, detail
            } if what == "the current version" && detail.contains("interrupted"))),
            "{verdict:?}"
        );
    }

    /// The storage-mode sweep calls GC's hook first, which publishes the
    /// current version.
    #[tokio::test]
    async fn the_storage_sweep_publishes_the_current_version_first() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let mirror = root.storage().join(REPO_KEY);
        let initial = fs::read(&mirror).unwrap();
        let outcome = recovery_write(&root, &config, RECOVERY_DEADLINE).await.unwrap();
        assert!(matches!(outcome, RecoveryOutcome::Succeeded { .. }));
        fs::write(&mirror, &initial).unwrap();
        sweep(&root, &config).await.unwrap();
        let (_, h) = current(&root);
        assert_eq!(payload_etag(&fs::read(&mirror).unwrap()), h);
    }

    /// Unrecognized names and non-canonical object ids, aged past every
    /// cutoff, survive the sweep and GC, and are removed afterwards.
    #[tokio::test]
    async fn canaries_survive_the_sweep_and_gc() {
        for mode in [Mode::Storage, Mode::Repository] {
            let (_dir, root, config) = initialized(mode).await;
            if mode == Mode::Repository {
                let outcome =
                    recovery_write(&root, &config, RECOVERY_DEADLINE).await.unwrap();
                assert!(matches!(outcome, RecoveryOutcome::Succeeded { .. }));
            }
            let canaries = plant_canaries(&root, &config).unwrap();
            let names: Vec<String> = canaries.iter().map(|c| c.name()).collect();
            assert!(names.contains(&"repo.d/stress-canary".to_owned()), "{names:?}");
            if mode == Mode::Repository {
                assert!(
                    names.iter().any(|name| name.starts_with("chunks/")),
                    "{names:?}"
                );
            }
            age_storage_root(&root).unwrap();
            sweep(&root, &config).await.unwrap();
            let mut verdict = Verdict::default();
            check_canaries(&canaries, &mut verdict);
            assert_eq!(verdict.violations, [], "{mode:?}");
            remove_canaries(&canaries).unwrap();
            assert_eq!(
                clean_root_verdict(&root, &config),
                Verdict::default(),
                "{mode:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_deleted_canary_is_reported() {
        let (_dir, root, config) = initialized(Mode::Repository).await;
        let canaries = plant_canaries(&root, &config).unwrap();
        let gone = &canaries[0];
        fs::remove_file(gone.path()).unwrap();
        let mut verdict = Verdict::default();
        check_canaries(&canaries, &mut verdict);
        assert_eq!(
            verdict.violations,
            [Violation::DeletedUnrecognized { name: gone.name() }]
        );
    }

    #[tokio::test]
    async fn a_marker_without_its_version_is_broken_history() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let (n, h) = current(&root);
        fs::remove_file(repo_d(&root).join(format!("{n}.{h}"))).unwrap();
        let verdict = clean_root_verdict(&root, &config);
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains("repo.d") && detail.contains(&h)),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn the_legacy_root_holds_only_repo() {
        let config = RunConfig::defaults(Mode::Storage, 1).with_legacy_local_backend();
        let (_dir, root, config) = initialized_with(config).await;
        assert!(!repo_d(&root).exists());
        assert_eq!(clean_root_verdict(&root, &config), Verdict::default());
        assert_eq!(age_sweep_and_check(&root, &config).await, Verdict::default());
    }

    #[tokio::test]
    async fn a_storage_recovery_that_keeps_failing_gives_up_at_the_deadline() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_dir, root, config) = initialized(Mode::Storage).await;
        // Every commit fails to stage its version in a read-only `repo.d`.
        let set_mode = |mode| {
            fs::set_permissions(repo_d(&root), fs::Permissions::from_mode(mode)).unwrap();
        };
        set_mode(0o555);
        if fs::write(repo_d(&root).join("probe"), b"").is_ok() {
            // Running as root: nothing to test.
            return;
        }

        let deadline = Duration::from_millis(500);
        let outcome = recovery_write(&root, &config, deadline).await.unwrap();
        set_mode(0o755);
        match outcome {
            RecoveryOutcome::Failed { elapsed, last_error } => {
                assert!(elapsed >= deadline, "{elapsed:?}");
                assert!(last_error.contains("Permission denied"), "{last_error}");
            }
            RecoveryOutcome::Succeeded { .. } => panic!("recovery succeeded"),
        }
        let who = recovery_worker();
        let records = read_log(&root.worker_log(&who.tag, &who.slot, 0)).unwrap();
        assert!(
            records.iter().any(|record| matches!(
                record,
                Record::Intent { op: Op::Cas { .. }, .. }
            )),
            "{records:?}"
        );
    }

    #[tokio::test]
    async fn a_repository_open_that_hangs_fails_recovery_at_the_deadline() {
        let (_dir, root, config) = initialized(Mode::Repository).await;
        let (n, h) = current(&root);
        let version = repo_d(&root).join(format!("{n}.{h}"));
        fs::remove_file(&version).unwrap();
        // Reading a FIFO blocks until a writer opens it.
        let status = std::process::Command::new("mkfifo").arg(&version).status().unwrap();
        assert!(status.success());

        let deadline = Duration::from_millis(500);
        let outcome = recovery_write(&root, &config, deadline).await.unwrap();
        // Unblocks the stuck read, so the runtime can shut down.
        drop(fs::OpenOptions::new().write(true).open(&version).unwrap());
        match outcome {
            RecoveryOutcome::Failed { elapsed, last_error } => {
                // Generous: without the bound the open never returns.
                assert!(elapsed < deadline + Duration::from_secs(10), "{elapsed:?}");
                assert!(last_error.contains("opening the repository"), "{last_error}");
            }
            RecoveryOutcome::Succeeded { .. } => panic!("recovery succeeded"),
        }
    }

    #[tokio::test]
    async fn a_fresh_repository_root_holds_only_repo() {
        let (_dir, root, config) = initialized(Mode::Repository).await;
        assert_eq!(root_files(&root), BTreeSet::from([REPO_KEY.to_owned()]));
        assert_repo_d_is_minimal(&root);
        assert_eq!(clean_root_verdict(&root, &config), Verdict::default());
    }

    #[tokio::test]
    async fn staging_files_below_the_root_are_counted_as_a_warning() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let storage = root.storage();
        fs::create_dir_all(storage.join("chunks/b")).unwrap();
        fs::create_dir_all(storage.join("a")).unwrap();
        let staging = format!("overwritten.staging.{}", Uuid::new_v4());
        fs::write(storage.join(&staging), b"").unwrap();
        fs::create_dir_all(storage.join("overwritten")).unwrap();
        fs::write(storage.join("overwritten/k.staging.1"), b"").unwrap();
        fs::write(storage.join("chunks/b/k.staging.2"), b"").unwrap();
        fs::write(storage.join("chunks/b/kept"), b"").unwrap();
        // Not a directory icechunk writes to: unknown, whatever it holds.
        fs::write(storage.join("a/k.staging.3"), b"").unwrap();
        let verdict = clean_root_verdict(&root, &config);
        assert_eq!(verdict.violations, [Violation::Leftovers { names: vec![staging] }]);
        assert_eq!(
            verdict.warnings,
            [
                Warning::SubdirStaging { count: 2 },
                Warning::UnknownEntries { names: vec!["a".to_owned()] }
            ]
        );
    }
}
