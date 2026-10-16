//! The mirror `$ROOT/repo` and the writer feature, on the filesystem backend:
//! every mirror a publication installs is a version that was current (M1),
//! a publication with no other writer leaves the current version (M2), the
//! mirror lists the feature (M5) and references only objects that exist
//! (M7), every version that was current lists the feature with its gate
//! (L1), and the first version's `N` is the creation time (I1).

use std::{collections::HashSet, fs, io, path::Path};

use anyhow::Context as _;
use icechunk::{
    Repository,
    format::{
        MANIFESTS_FILE_PATH, ManifestId, SNAPSHOTS_FILE_PATH, SnapshotId,
        TRANSACTION_LOGS_FILE_PATH, layout_v1,
        repo_info::{RepoAvailability, RepoHeader, RepoInfo, RepoStatus},
        repo_info_file,
        writer_features::WriterFeature,
    },
    storage::{GetModifiedResult, StorageContext, StorageErrorKind},
};
use serde::{Deserialize, Serialize};

use crate::{
    config::RunConfig,
    layout::SUPERVISOR_SLOT,
    log::{LogName, Record, Sample},
    oracle::{
        History, Verdict, Violation, Warning, clock_allowance_ms,
        fork::unused_fork_backups, gc_cutoff::missing_after_gc_cutoff,
        storage::read_listed_backup,
    },
    storage_mode::{BACKUP_DIR, REPO_KEY, payload_etag},
    toggler::TOGGLER_REASON,
};

/// Where an M1 hash comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MirrorSource {
    /// A [`Record::MirrorRename`].
    Rename,
    /// A [`Record::MirrorSample`].
    Sample,
}

/// When `verify` checks M2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MirrorCheckpoint {
    /// After its own publication, once every worker ended.
    FinalPublish,
    /// After its recovery write.
    RecoveryWrite,
}

/// What M2 found in `$ROOT/repo`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MirrorFound {
    /// The sha256 of its bytes.
    Hash(String),
    Missing,
    Unreadable(String),
    /// The publication before the check failed with this error.
    PublishFailed(String),
}

/// The repository's own statuses a run writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatuses {
    Online,
    /// `Online`, and `ReadOnly` with [`TOGGLER_REASON`] while the status
    /// toggler holds it.
    Toggled,
}

impl RunStatuses {
    pub fn of(config: &RunConfig) -> Self {
        if config.readonly.is_some() { Self::Toggled } else { Self::Online }
    }

    fn allow(self, status: &RepoStatus) -> bool {
        match status.availability {
            RepoAvailability::Online => true,
            RepoAvailability::ReadOnly => {
                self == Self::Toggled
                    && status.limited_availability_reason.as_deref()
                        == Some(TOGGLER_REASON)
            }
        }
    }
}

/// L1 and M5: what keeps `info` from being a repo info of the filesystem
/// layout written by this run: it lists exactly `filesystem-layout-v1`, its
/// status is the gate the implementation writes over the repository's own
/// status, the gate's reason names the feature and Icechunk 2.4, and the
/// repository's own status is one the run writes.
pub fn layout_defect(info: &RepoInfo, statuses: RunStatuses) -> Option<String> {
    let checked = || -> icechunk::format::IcechunkResult<Option<String>> {
        let features = info.raw_writer_features()?;
        let names: Vec<&str> = features.iter().map(|name| name.as_str()).collect();
        if names != [layout_v1::FEATURE] {
            return Ok(Some(format!(
                "it lists the writer features {names:?}, not [{:?}]",
                layout_v1::FEATURE
            )));
        }
        let status = info.status()?;
        let header = info.header()?;
        let gate = RepoHeader::new(&[WriterFeature::FilesystemLayoutV1], &status);
        if header != gate {
            return Ok(Some(format!(
                "its status {:?} isn't the gate {:?} over its own status",
                header.status(),
                gate.status()
            )));
        }
        let stored = serde_json::to_value(header.status()).unwrap_or_default();
        let reason = stored["limited_availability_reason"].as_str().unwrap_or_default();
        if !(reason.contains(layout_v1::FEATURE) && reason.contains("2.4")) {
            return Ok(Some(format!(
                "the gate's reason {reason:?} doesn't name {} and Icechunk 2.4",
                layout_v1::FEATURE
            )));
        }
        if !statuses.allow(&status) {
            return Ok(Some(format!(
                "its own status is {:?} ({:?}), not one the run sets",
                status.availability, status.limited_availability_reason
            )));
        }
        Ok(None)
    };
    checked().unwrap_or_else(|err| Some(format!("reading it: {}", err.kind())))
}

/// [`layout_defect`] of a repo info file.
pub fn file_layout_defect(bytes: &[u8], statuses: RunStatuses) -> Option<String> {
    match repo_info_file::decode(bytes) {
        Ok(info) => layout_defect(&info, statuses),
        Err(err) => Some(format!("it doesn't decode: {}", err.kind())),
    }
}

/// Repository mode: the hashes of the versions that were current, the
/// backups of the repo info in `overwritten/` and `current`, checking each
/// distinct one for L1, except the backups named in `earlier_eras`: those
/// the conversion scenario's earlier eras wrote, in the standard layout among
/// them. Every backup is a copy of a version that was current when it was
/// read: icechunk backs up the version it updates, conditionally on that
/// version, and so do the conversions and the fork actor. An undecodable
/// backup that a fork write in `history` announced and no fork it wrote
/// names, an interrupted copy, is none: a warning.
pub fn versions_that_were_current(
    storage_root: &Path,
    current: &[u8],
    statuses: RunStatuses,
    earlier_eras: &HashSet<String>,
    history: &History,
    verdict: &mut Verdict,
) -> anyhow::Result<HashSet<String>> {
    let mut was_current = HashSet::new();
    let mut add = |what: String, checked: bool, bytes: &[u8], verdict: &mut Verdict| {
        if was_current.insert(payload_etag(bytes))
            && checked
            && let Some(detail) = file_layout_defect(bytes, statuses)
        {
            verdict.violations.push(Violation::FeatureMissing { what, detail });
        }
    };
    add("the current version".to_owned(), true, current, verdict);
    let partial = unused_fork_backups(history);
    let dir = storage_root.join(BACKUP_DIR);
    let context = || format!("listing {}", dir.display());
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(was_current),
        Err(err) => return Err(err).with_context(context),
    };
    for entry in entries {
        let path = entry.with_context(context)?.path();
        let name = path.file_name().map(|name| name.to_string_lossy().into_owned());
        let Some(name) = name.filter(|name| is_repo_info_backup(name)) else {
            continue;
        };
        let Some(bytes) = read_listed_backup(&path)? else {
            continue;
        };
        if partial.contains(name.as_str())
            && let Err(err) = repo_info_file::decode(&bytes)
        {
            let detail = err.kind().to_string();
            verdict
                .warnings
                .push(Warning::ForkActorPartialBackup { backup: name, detail });
            continue;
        }
        let checked = !earlier_eras.contains(&name);
        add(format!("the backup {BACKUP_DIR}/{name}"), checked, &bytes, verdict);
    }
    Ok(was_current)
}

/// A backup of the repo info, not of the configuration, and not the staging
/// file of an interrupted copy.
fn is_repo_info_backup(name: &str) -> bool {
    name.starts_with(&format!("{REPO_KEY}.")) && !name.contains(".staging.")
}

/// The rules of a run on the filesystem backend that need no repository:
/// M1 against `was_current`, M4, M5 on the mirror the run left, M6 and I1.
pub fn check_layout(
    storage_root: &Path,
    history: &History,
    was_current: &HashSet<String>,
    statuses: RunStatuses,
    network_fs: bool,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    check_mirror_hashes(history, was_current, verdict);
    check_readonly_holds(history, network_fs, verdict);
    check_final_mirror(storage_root, &fork_hashes(history), statuses, verdict)?;
    check_mirror_samples(history, verdict);
    check_init_version(history, verdict);
    Ok(())
}

/// The hashes of the forks the run wrote.
fn fork_hashes(history: &History) -> HashSet<&str> {
    history
        .logs
        .values()
        .flatten()
        .filter_map(|record| match record {
            Record::ForkWritten { fork_hash, .. } => Some(fork_hash.as_str()),
            _ => None,
        })
        .collect()
}

/// M1: every mirror a publication renamed into place, or may have without
/// recording it done, is a version that was current; so is every sample,
/// unless it is a fork the run wrote.
pub fn check_mirror_hashes(
    history: &History,
    was_current: &HashSet<String>,
    verdict: &mut Verdict,
) {
    let forks = fork_hashes(history);
    let renames = logged_renames(history).into_iter().map(|rename| {
        let t_ms = match rename.time {
            RenameTime::Done { done_ms, .. } => {
                verdict.stats.mirror_renames += 1;
                done_ms
            }
            RenameTime::Unresolved { announced_ms, .. } => announced_ms,
        };
        (rename.hash, MirrorSource::Rename, t_ms)
    });
    let renames: Vec<_> = renames.collect();
    let samples = history.logs.values().flatten().filter_map(|record| match record {
        Record::MirrorSample { t_ms, sample: Sample::Hash(hash) }
            if !forks.contains(hash.as_str()) =>
        {
            Some((hash.as_str(), MirrorSource::Sample, *t_ms))
        }
        _ => None,
    });
    for (hash, source, t_ms) in renames.into_iter().chain(samples) {
        if !was_current.contains(hash) {
            verdict.violations.push(Violation::MirrorNotAVersion {
                hash: hash.to_owned(),
                source,
                t_ms,
            });
        }
    }
}

/// M6: the samplers found the mirror present and readable. Each sample is
/// logged when it differs from the previous one, so the counts are of
/// changes to a missing or unreadable mirror.
pub fn check_mirror_samples(history: &History, verdict: &mut Verdict) {
    let (mut missing, mut errors) = (0, Vec::new());
    for record in history.logs.values().flatten() {
        let Record::MirrorSample { sample, .. } = record else { continue };
        verdict.stats.mirror_samples += 1;
        match sample {
            Sample::Hash(_) => {}
            Sample::Missing => missing += 1,
            Sample::Error(err) => errors.push(err),
        }
    }
    if missing > 0 {
        verdict.warnings.push(Warning::MirrorMissing { count: missing });
    }
    if !errors.is_empty() {
        verdict.warnings.push(Warning::MirrorUnreadable {
            count: errors.len() as u64,
            samples: errors.into_iter().take(UNREADABLE_SAMPLES).cloned().collect(),
        });
    }
}

/// How many errors a [`Warning::MirrorUnreadable`] keeps.
const UNREADABLE_SAMPLES: usize = 3;

/// M5: the mirror the run left lists the feature and its gate. A missing
/// mirror is left to M2, one of the run's `forks` to F5.
pub fn check_final_mirror(
    storage_root: &Path,
    forks: &HashSet<&str>,
    statuses: RunStatuses,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    let path = storage_root.join(REPO_KEY);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| format!("reading {}", path.display()));
        }
    };
    if forks.contains(payload_etag(&bytes).as_str()) {
        return Ok(());
    }
    if let Some(detail) = file_layout_defect(&bytes, statuses) {
        verdict
            .violations
            .push(Violation::FeatureMissing { what: "the mirror".to_owned(), detail });
    }
    Ok(())
}

/// I1: `init` logged the first version's `N`, which lies within its
/// creation's times.
pub fn check_init_version(history: &History, verdict: &mut Verdict) {
    let inits: Vec<_> = history
        .logs
        .values()
        .flatten()
        .filter_map(|record| match record {
            Record::InitVersion { n, t_before_ms, t_after_ms } => {
                Some((*n, *t_before_ms, *t_after_ms))
            }
            _ => None,
        })
        .collect();
    let [(n, t_before_ms, t_after_ms)] = inits[..] else {
        verdict.violations.push(Violation::Harness {
            detail: format!("{} InitVersion records, expected one", inits.len()),
        });
        return;
    };
    let in_window =
        i64::try_from(n).is_ok_and(|n| (t_before_ms..=t_after_ms).contains(&n));
    if !in_window {
        verdict.violations.push(Violation::InitialVersion { n, t_before_ms, t_after_ms });
    }
}

/// A mirror rename a publication logged: done, or announced and never
/// recorded done, which may have renamed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggedRename<'a> {
    /// The host tag of the log, whose clock its times are on.
    pub tag: &'a str,
    pub hash: &'a str,
    pub t_check_ms: Option<i64>,
    /// When its fork check's last read of `$ROOT/repo` began.
    pub t_read_ms: Option<i64>,
    pub time: RenameTime,
}

/// When a [`LoggedRename`] replaced `$ROOT/repo`, or may have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameTime {
    /// Announced at `announced_ms` and recorded done at `done_ms`, once the
    /// rename returned: it renamed in between. `announced_ms` is `done_ms`
    /// if no announcement of it precedes it in its log, as when appending
    /// the announcement failed, which the hooks only warn about.
    Done { announced_ms: i64, done_ms: i64 },
    /// Announced at `announced_ms` and never recorded done: it may have
    /// renamed any time from then until `end`.
    Unresolved { announced_ms: i64, end: UnresolvedEnd },
}

/// Until when a [`RenameTime::Unresolved`] rename may have renamed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnresolvedEnd {
    /// Its process went on: the rename call had returned, failing, by its
    /// log's next record, logged at this time. A failure may hide a rename
    /// that landed, as `ENOENT` after a lost reply on a network filesystem.
    Returned(i64),
    /// Its process died first: by this time, or, with no record of its
    /// death, any time after.
    Died(Option<i64>),
}

impl RenameTime {
    pub fn earliest(self) -> i64 {
        match self {
            Self::Done { announced_ms, .. } | Self::Unresolved { announced_ms, .. } => {
                announced_ms
            }
        }
    }

    /// `None` if unbounded.
    pub fn latest(self) -> Option<i64> {
        match self {
            Self::Done { done_ms, .. } => Some(done_ms),
            Self::Unresolved { end: UnresolvedEnd::Returned(t_ms), .. } => Some(t_ms),
            Self::Unresolved { end: UnresolvedEnd::Died(t_ms), .. } => t_ms,
        }
    }

    /// Whether it may have happened within `[from, until]`.
    pub fn may_be_within(self, from: i64, until: i64) -> bool {
        self.earliest() <= until && self.latest().is_none_or(|latest| from <= latest)
    }
}

/// Every mirror rename of `history`: its [`Record::MirrorRename`]s, each
/// with the latest unpaired [`Record::MirrorRenaming`] of the same hash and
/// fork check before it in its log, and the [`Record::MirrorRenaming`]s left
/// unpaired.
pub fn logged_renames(history: &History) -> Vec<LoggedRename<'_>> {
    let mut renames = Vec::new();
    for (name, records) in &history.logs {
        // Index, hash, fork check, read, time.
        let mut announced: Vec<(usize, &str, Option<i64>, Option<i64>, i64)> = Vec::new();
        for (i, record) in records.iter().enumerate() {
            match record {
                Record::MirrorRename { hash, t_check_ms, t_read_ms, t_rename_ms } => {
                    let paired =
                        announced.iter().rposition(|&(_, other, t_check, t_read, _)| {
                            other == hash
                                && t_check == *t_check_ms
                                && t_read == *t_read_ms
                        });
                    let announced_ms =
                        paired.map_or(*t_rename_ms, |i| announced.remove(i).4);
                    renames.push(LoggedRename {
                        tag: &name.tag,
                        hash,
                        t_check_ms: *t_check_ms,
                        t_read_ms: *t_read_ms,
                        time: RenameTime::Done { announced_ms, done_ms: *t_rename_ms },
                    });
                }
                Record::MirrorRenaming { hash, t_check_ms, t_read_ms, t_ms } => {
                    announced.push((i, hash, *t_check_ms, *t_read_ms, *t_ms));
                }
                _ => {}
            }
        }
        for (i, hash, t_check_ms, t_read_ms, announced_ms) in announced {
            let end = match records.get(i + 1) {
                Some(next) => UnresolvedEnd::Returned(logged_at_ms(next)),
                None => UnresolvedEnd::Died(died_by_ms(history, name)),
            };
            renames.push(LoggedRename {
                tag: &name.tag,
                hash,
                t_check_ms,
                t_read_ms,
                time: RenameTime::Unresolved { announced_ms, end },
            });
        }
    }
    renames
}

/// The latest time `record` holds, which was taken before it was logged.
fn logged_at_ms(record: &Record) -> i64 {
    match record {
        Record::Start { t_ms, .. }
        | Record::Intent { t_ms, .. }
        | Record::Outcome { t_ms, .. }
        | Record::Failure { t_ms, .. }
        | Record::Observed { t_ms, .. }
        | Record::Fault { t_ms, .. }
        | Record::Spawn { t_ms, .. }
        | Record::ExternalKill { t_ms, .. }
        | Record::MirrorRenaming { t_ms, .. }
        | Record::MirrorSample { t_ms, .. }
        | Record::ForkGone { t_ms, .. }
        | Record::ForkBackup { t_ms, .. }
        | Record::NothingToConvert { t_ms } => *t_ms,
        Record::MirrorRename { t_rename_ms, .. } => *t_rename_ms,
        Record::InitVersion { t_after_ms, .. }
        | Record::ForkWritten { t_after_ms, .. } => *t_after_ms,
        Record::ReadOnlyHeld { t_end_ms, .. } => *t_end_ms,
    }
}

/// A time by which the process of the worker log `name` had died, on its
/// host's clock: its supervisor's external kill of it, logged once it was
/// reaped, or the first start of a later incarnation of its slot, spawned
/// once it was. `None` if neither was logged, as for a straggler killed at
/// the end.
pub fn died_by_ms(history: &History, name: &LogName) -> Option<i64> {
    let incarnation = name.incarnation?;
    let supervisor = LogName {
        tag: name.tag.clone(),
        slot: SUPERVISOR_SLOT.to_owned(),
        incarnation: None,
    };
    let killed =
        history.logs.get(&supervisor).into_iter().flatten().find_map(
            |record| match record {
                Record::ExternalKill { t_ms, slot, incarnation: killed }
                    if *slot == name.slot && *killed == incarnation =>
                {
                    Some(*t_ms)
                }
                _ => None,
            },
        );
    killed.or_else(|| {
        let next =
            LogName { incarnation: Some(incarnation.checked_add(1)?), ..name.clone() };
        history
            .logs
            .range(next..)
            .take_while(|(later, _)| later.tag == name.tag && later.slot == name.slot)
            .filter_map(|(_, records)| match records.first()? {
                Record::Start { t_ms, .. } => Some(*t_ms),
                _ => None,
            })
            .min()
    })
}

/// M4: at the end of every hold of the status `ReadOnly`, the current version
/// is still the `ReadOnly` change's, and `$ROOT/repo` holds it, unless a
/// publication that started before that version was published renamed an
/// older version over it, which is mirror lag. A mirror mismatch is a
/// violation on a local filesystem and a warning on a network one, whose
/// reads may be stale; it isn't judged if the change's storage logged an
/// error, as a failed publication does. A change that landed is a violation
/// everywhere: a newer current version is no stale read. A rename counts at
/// any time from its announcement until it returned, recorded done or, if
/// not, by its log's next record, or until its process died. A residual, in theory: the announcement of an
/// older hash by a publication killed before it could record its rename also
/// excuses another, later-started publication that wrongly renames the same
/// hash.
pub fn check_readonly_holds(history: &History, network_fs: bool, verdict: &mut Verdict) {
    let renames = logged_renames(history);
    let epsilon = clock_allowance_ms(history);
    for record in history.logs.values().flatten() {
        let Record::ReadOnlyHeld {
            version,
            publish_error,
            t_end_ms,
            current,
            mirror,
            ..
        } = record
        else {
            continue;
        };
        verdict.stats.readonly_holds += 1;
        let mut mismatch = |detail: String, anywhere: bool| {
            let version = version.clone();
            if network_fs && !anywhere {
                verdict
                    .warnings
                    .push(Warning::ReadOnlyMirrorMismatch { version, detail });
            } else {
                verdict
                    .violations
                    .push(Violation::ReadOnlyMirrorMismatch { version, detail });
            }
        };
        let Some(version) = version.as_deref() else {
            mismatch(
                "a change landed right after the ReadOnly change, before the toggler read its \
                 version"
                    .to_owned(),
                true,
            );
            continue;
        };
        if current != version {
            mismatch(format!("a change landed: the current version was {current}"), true);
            continue;
        }
        if *publish_error {
            verdict.stats.readonly_publish_errors += 1;
            continue;
        }
        let until = t_end_ms + epsilon;
        let published = last_published(
            renames.iter().filter(|rename| rename.hash == version).map(|r| r.time),
            until,
        );
        let detail = match (mirror, published) {
            (Sample::Hash(found), _) if found == version => None,
            (Sample::Hash(found), Some((first, last))) => {
                // Whether the version may have last been published at some
                // time in `[first, last]` that this rename may have
                // followed, by ε, with its fork check before it, by ε.
                let lag = renames.iter().any(|rename| {
                    rename.hash == found
                        && rename.time.earliest() <= until
                        && rename.t_check_ms.is_some_and(|t_check| {
                            let latest = rename.time.latest();
                            first.max(t_check - epsilon)
                                <= latest.map_or(last, |t| last.min(t + epsilon))
                        })
                });
                if lag {
                    verdict.stats.readonly_mirror_lags += 1;
                    None
                } else {
                    Some(format!(
                        "repo was {found}, which no publication that started before it was \
                         published renamed over it"
                    ))
                }
            }
            (Sample::Hash(found), None) => Some(format!(
                "repo was {found}, and no publication renamed the ReadOnly version into place"
            )),
            (Sample::Missing, _) => Some("repo was missing".to_owned()),
            (Sample::Error(err), _) => Some(format!("reading repo failed: {err}")),
        };
        if let Some(detail) = detail {
            mismatch(detail, false);
        }
    }
}

/// When the last of `renames` of a version by `until` may have been: from
/// the earliest the last done one may have been, or the earliest announced
/// one if none was done, to the latest any may have been. `None` if none may
/// have been by then.
fn last_published(
    renames: impl Iterator<Item = RenameTime>,
    until: i64,
) -> Option<(i64, i64)> {
    let renames: Vec<RenameTime> =
        renames.filter(|time| time.earliest() <= until).collect();
    let last =
        renames.iter().map(|time| time.latest().map_or(until, |t| t.min(until))).max()?;
    let done = renames.iter().filter_map(|time| match time {
        RenameTime::Done { announced_ms, .. } => Some(*announced_ms),
        RenameTime::Unresolved { .. } => None,
    });
    let first = done.max().or_else(|| renames.iter().map(|t| t.earliest()).min())?;
    Some((first, last))
}

/// M2: `$ROOT/repo` holds the current version. `published` is the error of
/// the publication just before, if it failed. A violation on a local
/// filesystem, a warning on a network one, where `publish_mirror()` is best
/// effort.
pub async fn check_mirror_current(
    storage: &(dyn icechunk::Storage + Send + Sync),
    storage_root: &Path,
    after: MirrorCheckpoint,
    published: Option<String>,
    network_fs: bool,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    let settings = storage.default_settings().await?;
    let ctx = StorageContext::unattributed(&settings);
    let expected = match storage.get_object_conditional(&ctx, REPO_KEY, None).await? {
        GetModifiedResult::Modified { new_version, .. } => {
            new_version.etag().cloned().context("the current version has no etag")?
        }
        GetModifiedResult::OnLatestVersion => {
            anyhow::bail!("reading {REPO_KEY} without a version returned no version")
        }
    };
    let found = match published {
        Some(err) => MirrorFound::PublishFailed(err),
        None => match fs::read(storage_root.join(REPO_KEY)) {
            Ok(bytes) => MirrorFound::Hash(payload_etag(&bytes)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => MirrorFound::Missing,
            Err(err) => MirrorFound::Unreadable(err.to_string()),
        },
    };
    if found != MirrorFound::Hash(expected.clone()) {
        if network_fs {
            verdict.warnings.push(Warning::MirrorStale { expected, found, after });
        } else {
            verdict.violations.push(Violation::MirrorStale { expected, found, after });
        }
    }
    Ok(())
}

/// M7: every snapshot file, transaction log and manifest the mirror's
/// snapshots reference exists. A mirror that is missing or doesn't decode is
/// left to M2 and M5. The missing snapshot file or transaction log of a
/// commit that a GC's cutoff came after is no violation: it goes to the
/// commit's [`Warning::GcCutoffAfterSessionStart`] in `verdict`.
pub async fn check_mirror_objects(
    repo: &Repository,
    storage_root: &Path,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    let path = storage_root.join(REPO_KEY);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| format!("reading {}", path.display()));
        }
    };
    let Ok(info) = repo_info_file::decode(&bytes) else { return Ok(()) };
    let mut dangling =
        |detail| verdict.violations.push(Violation::MirrorDangling { detail });
    let snapshots = info
        .all_snapshots()
        .and_then(|snapshots| snapshots.collect::<Result<Vec<_>, _>>())
        .map_err(|err| {
            anyhow::anyhow!("reading the mirror's snapshots: {}", err.kind())
        })?;
    let mut tx_logs: HashSet<SnapshotId> = HashSet::new();
    for snapshot in &snapshots {
        tx_logs.insert(snapshot.id.clone());
        tx_logs.extend(snapshot.pruned_ancestor_tx_logs.iter().cloned());
        let key = format!("{SNAPSHOTS_FILE_PATH}/{}", snapshot.id);
        if !object_exists(repo, &key).await? {
            let id = snapshot.id.to_string();
            if !missing_after_gc_cutoff(&mut verdict.warnings, &id, &key) {
                dangling(format!("the mirror's snapshot {id} has no file {key}"));
            }
            continue;
        }
        let manifests = match manifest_ids(repo, &snapshot.id).await {
            Ok(manifests) => manifests,
            Err(err) => {
                dangling(format!(
                    "reading the mirror's snapshot {}: {err:#}",
                    snapshot.id
                ));
                continue;
            }
        };
        for manifest in manifests {
            let key = format!("{MANIFESTS_FILE_PATH}/{manifest}");
            if !object_exists(repo, &key).await? {
                dangling(format!(
                    "the mirror's snapshot {} references the missing {key}",
                    snapshot.id
                ));
            }
        }
    }
    for id in tx_logs {
        let key = format!("{TRANSACTION_LOGS_FILE_PATH}/{id}");
        if !object_exists(repo, &key).await?
            && !missing_after_gc_cutoff(&mut verdict.warnings, &id.to_string(), &key)
        {
            dangling(format!("the mirror references the missing {key}"));
        }
    }
    Ok(())
}

async fn manifest_ids(
    repo: &Repository,
    snapshot: &SnapshotId,
) -> anyhow::Result<Vec<ManifestId>> {
    let snapshot = repo
        .asset_manager()
        .fetch_snapshot(snapshot)
        .await
        .map_err(|err| anyhow::anyhow!("{}", err.kind()))?;
    snapshot
        .manifest_files()
        .map(|info| {
            info.map(|info| info.id).map_err(|err| anyhow::anyhow!("{}", err.kind()))
        })
        .collect()
}

/// Whether the object exists, by a fresh open of its name: a directory
/// listing may be stale on a network filesystem.
pub async fn object_exists(repo: &Repository, key: &str) -> anyhow::Result<bool> {
    let ctx = StorageContext::unattributed(repo.storage_settings());
    // `object_store` rejects empty ranges; these files are never empty.
    match repo.storage().get_object_range(&ctx, key, Some(&(0..1))).await {
        Ok(_) => Ok(true),
        Err(err) if matches!(err.kind(), StorageErrorKind::ObjectNotFound) => Ok(false),
        Err(err) => Err(anyhow::anyhow!("opening {key}: {}", err.kind())),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use chrono::Utc;

    use super::*;
    use crate::{
        config::{Mode, RunConfig, WorkerId},
        faults::FaultHooks,
        layout::RunRoot,
        log::{LogWriter, OpResult, read_all_logs, start_worker_log},
        oracle::storage::{check_versions, read_backups},
        repo_mode::tests::{repo_root, repo_worker, who},
        storage_mode::{self, Attempt, Updater, initial_repo_info, read_repo},
    };

    fn layout_repo_info() -> Vec<u8> {
        initial_repo_info(&[WriterFeature::FilesystemLayoutV1]).unwrap().to_vec()
    }

    fn history(records: Vec<Record>) -> History {
        let name =
            LogName { tag: "h".to_owned(), slot: "0".to_owned(), incarnation: Some(0) };
        History { logs: BTreeMap::from([(name, records)]), ..History::default() }
    }

    #[test]
    fn layout_defect_wants_the_feature_its_gate_and_the_runs_status() {
        use RunStatuses::{Online, Toggled};
        let layout = repo_info_file::decode(&layout_repo_info()).unwrap();
        assert_eq!(layout_defect(&layout, Online), None);
        assert_eq!(layout_defect(&layout, Toggled), None);

        let standard = initial_repo_info(&[]).unwrap();
        let detail = file_layout_defect(&standard, Online).unwrap();
        assert!(detail.contains("lists the writer features []"), "{detail}");

        let spec_version = layout.spec_version().unwrap();
        let read_only = |reason: &str| {
            let status = RepoStatus {
                availability: RepoAvailability::ReadOnly,
                set_at: Utc::now(),
                limited_availability_reason: Some(reason.to_owned()),
            };
            layout.set_status(spec_version, &status, Some(("repo.1.X", 50))).unwrap()
        };
        let held = read_only("held");
        let toggled = read_only(TOGGLER_REASON);
        for (info, statuses) in [(&held, Online), (&held, Toggled), (&toggled, Online)] {
            let detail = layout_defect(info, statuses).unwrap();
            assert!(detail.contains("own status is ReadOnly"), "{statuses:?}: {detail}");
        }
        // The status toggler's `ReadOnly` versions are the run's own.
        assert_eq!(layout_defect(&toggled, Toggled), None);

        let detail = file_layout_defect(b"garbage", Online).unwrap();
        assert!(detail.contains("doesn't decode"), "{detail}");
    }

    /// L1 and M5 compare the stored status with the gate the implementation
    /// writes, which catches every malformed shape.
    #[test]
    fn a_malformed_gate_is_a_layout_defect() {
        use icechunk::format::{
            flatbuffers::generated::RepoAvailability as Raw, repo_info::StoredRepoStatus,
            writer_features::FeatureName,
        };
        let layout = repo_info_file::decode(&layout_repo_info()).unwrap();
        let header = layout.header().unwrap();
        let set_at = layout.status().unwrap().set_at;
        let stored = serde_json::to_value(header.status()).unwrap();
        let reason = stored["limited_availability_reason"].as_str().map(str::to_owned);
        let own = || StoredRepoStatus::for_tests(Raw::Online, set_at, None, None);
        let gate = |availability, set_at, reason, underlying| {
            StoredRepoStatus::for_tests(availability, set_at, reason, underlying)
        };
        let forged = |status| {
            let feature = vec![FeatureName::new(layout_v1::FEATURE)];
            layout.with_raw_header_for_tests(feature, status).unwrap()
        };

        // The well-formed gate, forged the same way, is no defect.
        let well_formed = gate(Raw::ReadOnly, set_at, reason.clone(), Some(own()));
        assert_eq!(&well_formed, header.status());
        assert_eq!(layout_defect(&forged(well_formed), RunStatuses::Online), None);

        let later = set_at + chrono::TimeDelta::seconds(1);
        let nested = StoredRepoStatus::for_tests(Raw::Online, set_at, None, Some(own()));
        for (case, status) in [
            ("an Online gate", gate(Raw::Online, set_at, reason.clone(), Some(own()))),
            ("an Offline gate", gate(Raw::Offline, set_at, reason.clone(), Some(own()))),
            (
                "another reason",
                gate(Raw::ReadOnly, set_at, Some("held".into()), Some(own())),
            ),
            ("another set_at", gate(Raw::ReadOnly, later, reason.clone(), Some(own()))),
            ("no underlying", gate(Raw::ReadOnly, set_at, reason.clone(), None)),
            ("double nesting", gate(Raw::ReadOnly, set_at, reason.clone(), Some(nested))),
        ] {
            let defect = layout_defect(&forged(status), RunStatuses::Online);
            assert!(
                defect.as_deref().is_some_and(|defect| defect.contains("isn't the gate")),
                "{case}: {defect:?}"
            );
        }
    }

    #[test]
    fn mirror_renames_and_samples_must_be_versions_that_were_current() {
        let current = HashSet::from(["v1".to_owned(), "v2".to_owned()]);
        let rename = |t_ms, hash: &str| Record::MirrorRename {
            t_check_ms: Some(t_ms - 1),
            t_read_ms: None,
            t_rename_ms: t_ms,
            hash: hash.to_owned(),
        };
        let sample = |t_ms, sample| Record::MirrorSample { t_ms, sample };
        let records = vec![
            rename(10, "v1"),
            rename(20, "loser"),
            sample(30, Sample::Hash("v2".to_owned())),
            sample(40, Sample::Hash("torn".to_owned())),
            sample(50, Sample::Error("ESTALE".to_owned())),
        ];
        let mut verdict = Verdict::default();
        check_mirror_hashes(&history(records), &current, &mut verdict);
        assert_eq!(
            verdict.violations,
            [
                Violation::MirrorNotAVersion {
                    hash: "loser".to_owned(),
                    source: MirrorSource::Rename,
                    t_ms: 20,
                },
                Violation::MirrorNotAVersion {
                    hash: "torn".to_owned(),
                    source: MirrorSource::Sample,
                    t_ms: 40,
                },
            ]
        );
        assert_eq!(verdict.stats.mirror_renames, 2);

        // A sample of a fork the run wrote is no violation; a rename of one is.
        let fork = Record::ForkWritten {
            t_before_ms: 5,
            t_after_ms: 6,
            base_hash: "v1".to_owned(),
            fork_hash: "fork".to_owned(),
            backup: "repo.1.X".to_owned(),
            gc: None,
        };
        let records =
            vec![fork, sample(30, Sample::Hash("fork".to_owned())), rename(40, "fork")];
        let mut verdict = Verdict::default();
        check_mirror_hashes(&history(records), &current, &mut verdict);
        assert_eq!(
            verdict.violations,
            [Violation::MirrorNotAVersion {
                hash: "fork".to_owned(),
                source: MirrorSource::Rename,
                t_ms: 40,
            }]
        );

        // An announced rename counts once with its outcome, and on its own
        // too: a failed rename call may hide one that landed, and a process
        // that died after announcing it may have renamed.
        let announce = |t_ms, hash: &str| Record::MirrorRenaming {
            t_check_ms: Some(t_ms - 1),
            t_read_ms: None,
            t_ms,
            hash: hash.to_owned(),
        };
        let records = vec![
            announce(10, "v1"),
            rename(10, "v1"),
            announce(20, "failed"),
            sample(30, Sample::Hash("v2".to_owned())),
            announce(40, "ghost"),
        ];
        let mut verdict = Verdict::default();
        check_mirror_hashes(&history(records), &current, &mut verdict);
        let not_a_version = |hash: &str, t_ms| Violation::MirrorNotAVersion {
            hash: hash.to_owned(),
            source: MirrorSource::Rename,
            t_ms,
        };
        assert_eq!(
            verdict.violations,
            [not_a_version("failed", 20), not_a_version("ghost", 40)]
        );
        assert_eq!(verdict.stats.mirror_renames, 1);
    }

    /// A hold's forged records for M4: the toggler's hold of version `h`
    /// from 1000 to 4000 ms, and publications' records in other logs.
    struct Hold<'a> {
        version: Option<&'a str>,
        publish_error: bool,
        current: &'a str,
        mirror: Sample,
        /// Hash, rename time, fork-check time.
        renames: &'a [(&'a str, i64, Option<i64>)],
        /// More logs, by slot.
        logs: Vec<(&'a str, Vec<Record>)>,
    }

    impl Hold<'_> {
        fn fine() -> Self {
            Hold {
                version: Some("h"),
                publish_error: false,
                current: "h",
                mirror: Sample::Hash("h".to_owned()),
                renames: &[("h", 900, Some(899))],
                logs: vec![],
            }
        }

        fn check(&self, network_fs: bool) -> Verdict {
            let held = Record::ReadOnlyHeld {
                t_ack_ms: 1000,
                version: self.version.map(str::to_owned),
                publish_error: self.publish_error,
                t_end_ms: 4000,
                current: self.current.to_owned(),
                mirror: self.mirror.clone(),
            };
            let log = |slot: &str| LogName {
                tag: "h".to_owned(),
                slot: slot.to_owned(),
                incarnation: (slot != SUPERVISOR_SLOT).then_some(0),
            };
            let renames = self
                .renames
                .iter()
                .map(|&(hash, t_rename_ms, t_check_ms)| Record::MirrorRename {
                    t_check_ms,
                    t_read_ms: None,
                    t_rename_ms,
                    hash: hash.to_owned(),
                })
                .collect();
            let mut logs =
                BTreeMap::from([(log("toggler"), vec![held]), (log("0"), renames)]);
            logs.extend(
                self.logs.iter().map(|(slot, records)| (log(slot), records.clone())),
            );
            let history = History { logs, ..History::default() };
            let mut verdict = Verdict::default();
            check_readonly_holds(&history, network_fs, &mut verdict);
            verdict
        }
    }

    fn mismatch_details(verdict: &Verdict) -> Vec<String> {
        verdict
            .violations
            .iter()
            .map(|v| match v {
                Violation::ReadOnlyMirrorMismatch { detail, .. } => detail.clone(),
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    /// M4 on forged records.
    #[test]
    fn a_read_only_hold_must_leave_its_version_in_the_mirror() {
        let hash = |h: &str| Sample::Hash(h.to_owned());
        let fine = Hold::fine().check(false);
        assert_eq!(fine.violations, []);
        assert_eq!((fine.stats.readonly_holds, fine.stats.readonly_mirror_lags), (1, 0));

        // A publication that started before the ReadOnly version was in place
        // renamed an older version over it: lag.
        for renames in [
            &[("h", 900, Some(899)), ("old", 950, Some(880))][..],
            &[("h", 900, Some(899)), ("old", 950, Some(940))],
        ] {
            let lag = Hold { mirror: hash("old"), renames, ..Hold::fine() }.check(false);
            assert_eq!(lag.violations, [], "{renames:?}");
            assert_eq!(
                (lag.stats.readonly_holds, lag.stats.readonly_mirror_lags),
                (1, 1)
            );
        }

        let mismatches: [(&str, Hold<'_>, &str); 9] = [
            (
                "never published",
                Hold { mirror: hash("old"), renames: &[], ..Hold::fine() },
                "old",
            ),
            (
                "lag before it",
                Hold {
                    mirror: hash("old"),
                    renames: &[("old", 500, Some(499)), ("h", 900, Some(899))],
                    ..Hold::fine()
                },
                "old",
            ),
            (
                "republished after the lag",
                Hold {
                    mirror: hash("old"),
                    renames: &[
                        ("h", 900, Some(899)),
                        ("old", 950, Some(880)),
                        ("h", 1200, Some(1199)),
                    ],
                    ..Hold::fine()
                },
                "old",
            ),
            (
                "lag after the hold",
                Hold {
                    mirror: hash("old"),
                    renames: &[("h", 900, Some(899)), ("old", 5000, Some(880))],
                    ..Hold::fine()
                },
                "old",
            ),
            (
                "a publication that started after it was in place",
                Hold {
                    mirror: hash("old"),
                    renames: &[("h", 900, Some(899)), ("old", 1000, Some(990))],
                    ..Hold::fine()
                },
                "old",
            ),
            (
                "a rename without its fork check's time",
                Hold {
                    mirror: hash("old"),
                    renames: &[("h", 900, Some(899)), ("old", 950, None)],
                    ..Hold::fine()
                },
                "old",
            ),
            (
                "bytes no rename installed",
                Hold {
                    mirror: hash("other"),
                    renames: &[("h", 900, Some(899)), ("old", 950, Some(880))],
                    ..Hold::fine()
                },
                "other",
            ),
            ("missing", Hold { mirror: Sample::Missing, ..Hold::fine() }, "missing"),
            (
                "a rename announced, whose call returned before the version was published",
                Hold {
                    mirror: hash("old"),
                    logs: vec![(
                        "1",
                        vec![
                            Record::MirrorRenaming {
                                t_check_ms: Some(820),
                                t_read_ms: None,
                                t_ms: 830,
                                hash: "old".to_owned(),
                            },
                            Record::Fault {
                                t_ms: 840,
                                action: crate::log::FaultAction::Pause,
                                point: crate::config::PointName::BeforeRename,
                            },
                        ],
                    )],
                    ..Hold::fine()
                },
                "old",
            ),
        ];
        for (case, hold, expected) in mismatches {
            let found = mismatch_details(&hold.check(false));
            assert!(
                matches!(&found[..], [detail] if detail.contains(expected)),
                "{case}: {found:?}"
            );
            let network = hold.check(true);
            assert_eq!(network.violations, [], "{case}");
            assert!(
                matches!(&network.warnings[..], [Warning::ReadOnlyMirrorMismatch { .. }]),
                "{case}: {network:?}"
            );
        }

        // A change landed during the hold, or right after the ReadOnly change:
        // a newer current version is no stale read, so a violation anywhere.
        for (case, hold) in [
            ("at the end", Hold { current: "later", ..Hold::fine() }),
            ("right after", Hold { version: None, ..Hold::fine() }),
            (
                "despite a publish error",
                Hold { current: "later", publish_error: true, ..Hold::fine() },
            ),
        ] {
            for network_fs in [false, true] {
                let found = mismatch_details(&hold.check(network_fs));
                assert!(
                    matches!(&found[..], [detail] if detail.contains("a change landed")),
                    "{case}, network {network_fs}: {found:?}"
                );
            }
        }

        // A process killed between its announcement and its rename may have
        // renamed: its announcement, its log's last record, counts.
        let killed = Hold {
            mirror: hash("old"),
            logs: vec![(
                "1",
                vec![Record::MirrorRenaming {
                    t_check_ms: Some(880),
                    t_read_ms: None,
                    t_ms: 950,
                    hash: "old".to_owned(),
                }],
            )],
            ..Hold::fine()
        }
        .check(false);
        assert_eq!(killed.violations, []);
        assert_eq!(killed.stats.readonly_mirror_lags, 1);

        // It may have renamed any time until its process died, logged by an
        // external kill: an older version's announced before the ReadOnly
        // version was published, or the ReadOnly version's announced before
        // an older version's fork check.
        let announced = |t_check_ms, t_ms, hash: &str| Record::MirrorRenaming {
            t_check_ms: Some(t_check_ms),
            t_read_ms: None,
            t_ms,
            hash: hash.to_owned(),
        };
        let killed =
            |t_ms| Record::ExternalKill { t_ms, slot: "1".to_owned(), incarnation: 0 };
        for (case, renames, announcement, t_killed_ms, lag) in [
            (
                "killed after it",
                &[("h", 900, Some(899))][..],
                ("old", 600, 700),
                950,
                true,
            ),
            ("killed before it", &[("h", 900, Some(899))], ("old", 600, 700), 800, false),
            ("published late", &[("old", 1080, Some(1000))], ("h", 899, 900), 1100, true),
            (
                "published early",
                &[("old", 1080, Some(1000))],
                ("h", 899, 900),
                930,
                false,
            ),
        ] {
            let (hash, t_check_ms, t_ms) = announcement;
            let hold = Hold {
                mirror: Sample::Hash("old".to_owned()),
                renames,
                logs: vec![
                    ("1", vec![announced(t_check_ms, t_ms, hash)]),
                    (SUPERVISOR_SLOT, vec![killed(t_killed_ms)]),
                ],
                ..Hold::fine()
            };
            let verdict = hold.check(false);
            if lag {
                assert_eq!(verdict.violations, [], "{case}");
                assert_eq!(verdict.stats.readonly_mirror_lags, 1, "{case}");
            } else {
                let found = mismatch_details(&verdict);
                assert!(
                    matches!(&found[..], [detail] if detail.contains("old")),
                    "{case}"
                );
            }
        }

        // A rename recorded done may have landed any time after its
        // announcement: the ReadOnly version's slow rename may have landed
        // before an older version's.
        let renamed = |t_check_ms, t_rename_ms, hash: &str| Record::MirrorRename {
            t_check_ms: Some(t_check_ms),
            t_read_ms: None,
            t_rename_ms,
            hash: hash.to_owned(),
        };
        for (t_announced_ms, lag) in [(850, true), (960, false)] {
            let hold = Hold {
                mirror: Sample::Hash("old".to_owned()),
                renames: &[],
                logs: vec![
                    (
                        "2",
                        vec![
                            announced(849, t_announced_ms, "h"),
                            renamed(849, 1000, "h"),
                        ],
                    ),
                    ("3", vec![announced(870, 880, "old"), renamed(870, 890, "old")]),
                ],
                ..Hold::fine()
            };
            let verdict = hold.check(false);
            assert_eq!(
                verdict.violations.is_empty(),
                lag,
                "{t_announced_ms}: {verdict:?}"
            );
            assert_eq!(verdict.stats.readonly_mirror_lags, u64::from(lag));
        }

        // The storage logged an error during the ReadOnly change, as a failed
        // publication does: the mirror isn't judged.
        let unjudged = Hold {
            mirror: hash("old"),
            renames: &[],
            publish_error: true,
            ..Hold::fine()
        }
        .check(false);
        assert_eq!((unjudged.violations.len(), unjudged.warnings.len()), (0, 0));
        assert_eq!(unjudged.stats.readonly_publish_errors, 1);
    }

    #[test]
    fn a_rename_is_timed_from_its_announcement_to_its_outcome_or_death() {
        let log = |tag: &str, slot: &str, incarnation| LogName {
            tag: tag.to_owned(),
            slot: slot.to_owned(),
            incarnation,
        };
        let announced = |t_check_ms, t_ms| Record::MirrorRenaming {
            t_check_ms: Some(t_check_ms),
            t_read_ms: None,
            t_ms,
            hash: "v".to_owned(),
        };
        let renamed = |t_check_ms, t_rename_ms| Record::MirrorRename {
            t_check_ms: Some(t_check_ms),
            t_read_ms: None,
            t_rename_ms,
            hash: "v".to_owned(),
        };
        let killed = |t_ms, slot: &str, incarnation| Record::ExternalKill {
            t_ms,
            slot: slot.to_owned(),
            incarnation,
        };
        let start = |t_ms| Record::Start {
            t_ms,
            host: "h".to_owned(),
            slot: "1".to_owned(),
            incarnation: 0,
            pid: 1,
        };
        let times = |logs: Vec<(LogName, Vec<Record>)>| {
            let history =
                History { logs: logs.into_iter().collect(), ..History::default() };
            logged_renames(&history)
                .into_iter()
                .map(|rename| rename.time)
                .collect::<Vec<_>>()
        };

        // A done rename pairs with the latest announcement of its hash and
        // fork check; without one it is a point.
        let done = |announced_ms, done_ms| RenameTime::Done { announced_ms, done_ms };
        assert_eq!(
            times(vec![(
                log("h", "1", Some(0)),
                vec![
                    announced(5, 10),
                    announced(6, 11),
                    renamed(5, 20),
                    renamed(6, 21),
                    renamed(7, 30),
                ],
            )]),
            [done(10, 20), done(11, 21), done(30, 30)]
        );

        // An unresolved one's process died by its external kill, ignoring
        // other slots', incarnations' and hosts', or else by the first start
        // of a later incarnation of its slot.
        let decoys = vec![killed(100, "2", 3), killed(110, "1", 2)];
        let run = |kills: Vec<Record>, later_starts: bool| {
            let mut logs = vec![
                (log("h", "1", Some(3)), vec![announced(5, 10)]),
                (log("h", SUPERVISOR_SLOT, None), kills),
                (log("g", SUPERVISOR_SLOT, None), vec![killed(120, "1", 3)]),
                (log("h", "2", Some(4)), vec![start(200)]),
            ];
            if later_starts {
                logs.extend([
                    (log("h", "1", Some(4)), vec![]),
                    (log("h", "1", Some(5)), vec![start(400)]),
                    (log("h", "1", Some(6)), vec![start(300)]),
                ]);
            }
            times(logs)
        };
        let unresolved = |died_by_ms| RenameTime::Unresolved {
            announced_ms: 10,
            end: UnresolvedEnd::Died(died_by_ms),
        };
        assert_eq!(run(decoys.clone(), true), [unresolved(Some(300))]);
        let kill = [decoys.clone(), vec![killed(250, "1", 3)]].concat();
        assert_eq!(run(kill, true), [unresolved(Some(250))]);
        assert_eq!(run(decoys, false), [unresolved(None)]);
    }

    #[test]
    fn missing_and_unreadable_samples_are_warnings() {
        let sample = |sample| Record::MirrorSample { t_ms: 0, sample };
        let check = |records| {
            let mut verdict = Verdict::default();
            check_mirror_samples(&history(records), &mut verdict);
            verdict
        };
        let hash = || Sample::Hash("v1".to_owned());
        let fine = check(vec![sample(hash())]);
        assert_eq!((fine.violations.len(), fine.warnings.len()), (0, 0));
        assert_eq!(fine.stats.mirror_samples, 1);

        let error = |e: &str| Sample::Error(e.to_owned());
        let verdict = check(vec![
            sample(hash()),
            sample(Sample::Missing),
            sample(hash()),
            sample(Sample::Missing),
            sample(error("Stale file handle")),
            sample(error("e2")),
            sample(error("e3")),
            sample(error("e4")),
        ]);
        assert_eq!(verdict.violations, []);
        assert_eq!(
            verdict.warnings,
            [
                Warning::MirrorMissing { count: 2 },
                Warning::MirrorUnreadable {
                    count: 4,
                    samples: vec![
                        "Stale file handle".to_owned(),
                        "e2".to_owned(),
                        "e3".to_owned()
                    ],
                },
            ]
        );
        assert_eq!(verdict.stats.mirror_samples, 8);
    }

    #[test]
    fn the_final_mirror_must_list_the_feature_and_may_be_missing() {
        let dir = tempfile::tempdir().unwrap();
        let mirror = dir.path().join(REPO_KEY);
        let check = || {
            let mut verdict = Verdict::default();
            check_final_mirror(
                dir.path(),
                &HashSet::new(),
                RunStatuses::Online,
                &mut verdict,
            )
            .unwrap();
            verdict.violations
        };
        assert_eq!(check(), []);
        fs::write(&mirror, layout_repo_info()).unwrap();
        assert_eq!(check(), []);
        for (bytes, expected) in [
            (initial_repo_info(&[]).unwrap().to_vec(), "lists the writer features []"),
            (b"garbage".to_vec(), "doesn't decode"),
        ] {
            fs::write(&mirror, bytes).unwrap();
            assert!(
                matches!(&check()[..], [Violation::FeatureMissing { what, detail }]
                    if what == "the mirror" && detail.contains(expected)),
                "{expected}"
            );
        }
        // A fork the run wrote is left to F5.
        let fork = initial_repo_info(&[]).unwrap();
        fs::write(&mirror, &fork).unwrap();
        let mut verdict = Verdict::default();
        let forks = HashSet::from([payload_etag(&fork)]);
        let forks: HashSet<&str> = forks.iter().map(String::as_str).collect();
        check_final_mirror(dir.path(), &forks, RunStatuses::Online, &mut verdict)
            .unwrap();
        assert_eq!(verdict.violations, []);
    }

    #[test]
    fn the_first_version_is_numbered_by_its_creation_time() {
        let init = |n, t_before_ms, t_after_ms| Record::InitVersion {
            n,
            t_before_ms,
            t_after_ms,
        };
        let check = |records| {
            let mut verdict = Verdict::default();
            check_init_version(&history(records), &mut verdict);
            verdict.violations
        };
        assert_eq!(check(vec![init(15, 10, 20)]), []);
        assert_eq!(check(vec![init(10, 10, 10)]), []);
        assert_eq!(
            check(vec![init(9, 10, 20)]),
            [Violation::InitialVersion { n: 9, t_before_ms: 10, t_after_ms: 20 }]
        );
        assert_eq!(
            check(vec![init(21, 10, 20)]),
            [Violation::InitialVersion { n: 21, t_before_ms: 10, t_after_ms: 20 }]
        );
        for records in [vec![], vec![init(15, 10, 20), init(15, 10, 20)]] {
            assert!(matches!(&check(records)[..], [Violation::Harness { detail }]
                    if detail.contains("InitVersion")),);
        }
    }

    #[test]
    fn versions_that_were_current_are_the_repo_info_backups_and_the_current_one() {
        let dir = tempfile::tempdir().unwrap();
        let overwritten = dir.path().join(BACKUP_DIR);
        fs::create_dir_all(&overwritten).unwrap();
        let layout = layout_repo_info();
        let standard = initial_repo_info(&[]).unwrap().to_vec();
        fs::write(overwritten.join("repo.1.A"), &layout).unwrap();
        fs::write(overwritten.join("repo.2.B"), &layout).unwrap();
        fs::write(overwritten.join("repo.3.C"), &standard).unwrap();
        fs::write(overwritten.join("repo.4.D.staging.x"), b"torn").unwrap();
        fs::write(overwritten.join("config.yaml.5.E"), b"config").unwrap();
        let current = b"current".to_vec();

        let mut verdict = Verdict::default();
        let was_current = versions_that_were_current(
            dir.path(),
            &current,
            RunStatuses::Online,
            &HashSet::new(),
            &History::default(),
            &mut verdict,
        )
        .unwrap();
        let expected: HashSet<String> =
            [&layout, &standard, &current].into_iter().map(|b| payload_etag(b)).collect();
        assert_eq!(was_current, expected);
        let what: Vec<&str> = verdict
            .violations
            .iter()
            .map(|v| match v {
                Violation::FeatureMissing { what, .. } => what.as_str(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(what, ["the current version", "the backup overwritten/repo.3.C"]);

        // A backup of an earlier era is a version that was current, unchecked.
        let earlier = HashSet::from(["repo.3.C".to_owned()]);
        let mut verdict = Verdict::default();
        let with_earlier = versions_that_were_current(
            dir.path(),
            &layout,
            RunStatuses::Online,
            &earlier,
            &History::default(),
            &mut verdict,
        )
        .unwrap();
        assert_eq!(
            with_earlier,
            [&layout, &standard].into_iter().map(|b| payload_etag(b)).collect()
        );
        assert_eq!(verdict.violations, []);

        let empty = tempfile::tempdir().unwrap();
        let mut verdict = Verdict::default();
        let layout_only = versions_that_were_current(
            empty.path(),
            &layout,
            RunStatuses::Online,
            &HashSet::new(),
            &History::default(),
            &mut verdict,
        )
        .unwrap();
        assert_eq!(layout_only, HashSet::from([payload_etag(&layout)]));
        assert_eq!(verdict, Verdict::default());
    }

    /// A fork actor's interrupted copy of `repo` is no version that was
    /// current, and isn't checked; an unannounced undecodable backup is.
    #[test]
    fn an_announced_partial_fork_backup_is_no_version_that_was_current() {
        let dir = tempfile::tempdir().unwrap();
        let overwritten = dir.path().join(BACKUP_DIR);
        fs::create_dir_all(&overwritten).unwrap();
        let layout = layout_repo_info();
        fs::write(overwritten.join("repo.1.A"), &layout).unwrap();
        fs::write(overwritten.join("repo.9.B"), b"").unwrap();
        let fork_actor = |announced: &str| {
            let name = LogName {
                tag: "h".to_owned(),
                slot: crate::fork::FORK_SLOT.to_owned(),
                incarnation: Some(0),
            };
            let records =
                vec![Record::ForkBackup { t_ms: 0, backup: announced.to_owned() }];
            History { logs: BTreeMap::from([(name, records)]), ..History::default() }
        };
        let check = |history: &History| {
            let mut verdict = Verdict::default();
            let was_current = versions_that_were_current(
                dir.path(),
                &layout,
                RunStatuses::Online,
                &HashSet::new(),
                history,
                &mut verdict,
            )
            .unwrap();
            (was_current, verdict)
        };

        let (was_current, verdict) = check(&fork_actor("repo.9.B"));
        assert_eq!(was_current, HashSet::from([payload_etag(&layout)]));
        assert_eq!(verdict.violations, []);
        assert!(
            matches!(&verdict.warnings[..], [Warning::ForkActorPartialBackup { backup, .. }]
                if backup == "repo.9.B"),
            "{verdict:?}"
        );

        let (was_current, verdict) = check(&fork_actor("repo.8.A"));
        assert!(was_current.contains(&payload_etag(b"")));
        assert!(
            matches!(&verdict.violations[..], [Violation::FeatureMissing { what, .. }]
                if what.contains("repo.9.B")),
            "{verdict:?}"
        );
        assert_eq!(verdict.warnings, []);
    }

    /// A storage-mode run's own records pass M1, L1 and M5.
    #[tokio::test]
    async fn a_storage_mode_run_passes_the_mirror_rules() {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().to_path_buf());
        root.create_dirs().unwrap();
        let mut config = RunConfig::defaults(Mode::Storage, 1);
        config.faults.points = vec![];
        storage_mode::init(&root, &config).await.unwrap();
        let who = WorkerId { tag: "h".to_owned(), slot: "0".to_owned(), incarnation: 0 };
        let log = Arc::new(start_worker_log(&root, &who).unwrap());
        let hooks = FaultHooks::new(&config.faults, None, Arc::clone(&log));
        let storage =
            storage_mode::storage(&root, &config, Some(Arc::new(hooks))).await.unwrap();
        let mut updater = Updater::new(storage, &who, &config).await.unwrap();
        for _ in 0..3 {
            let attempt = updater.attempt(&log).await.unwrap();
            assert!(
                matches!(attempt, Attempt::Logged(OpResult::Ack { .. })),
                "{attempt:?}"
            );
        }

        let history =
            History { logs: read_all_logs(&root).unwrap(), ..History::default() };
        let final_repo =
            read_repo(&*storage_mode::storage(&root, &config, None).await.unwrap())
                .await
                .unwrap();
        let backups = read_backups(&root.storage()).unwrap();
        let mut verdict = Verdict::default();
        let was_current = check_versions(Some(&final_repo), &backups, &mut verdict);
        assert_eq!(was_current.len(), 4);
        check_mirror_hashes(&history, &was_current, &mut verdict);
        check_final_mirror(
            &root.storage(),
            &HashSet::new(),
            RunStatuses::Online,
            &mut verdict,
        )
        .unwrap();
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.stats.mirror_renames, 3);

        // A version the run never made current.
        log.append(&Record::MirrorRename {
            t_check_ms: None,
            t_read_ms: None,
            t_rename_ms: 1,
            hash: payload_etag(b"never current"),
        })
        .unwrap();
        let history =
            History { logs: read_all_logs(&root).unwrap(), ..History::default() };
        let mut verdict = Verdict::default();
        check_mirror_hashes(&history, &was_current, &mut verdict);
        assert!(
            matches!(&verdict.violations[..], [Violation::MirrorNotAVersion { .. }]),
            "{verdict:?}"
        );
    }

    /// A repository-mode run's own records pass M1, L1, M5 and M7.
    #[tokio::test]
    async fn a_repository_mode_run_passes_the_mirror_rules() {
        let (_dir, root, config) = repo_root().await;
        let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert!(worker.ensure_branch().await.unwrap());
        for _ in 0..2 {
            assert!(matches!(
                worker.branch_commit().await.unwrap(),
                Some(OpResult::Ack { .. })
            ));
        }
        assert!(matches!(
            worker.main_commit().await.unwrap(),
            Some(OpResult::Ack { .. })
        ));

        let repo = crate::repo_mode::open_repo(&root, &config, None).await.unwrap();
        let current = read_repo(repo.storage().as_ref()).await.unwrap();
        let history =
            History { logs: read_all_logs(&root).unwrap(), ..History::default() };
        let mut verdict = Verdict::default();
        let was_current = versions_that_were_current(
            &root.storage(),
            &current,
            RunStatuses::Online,
            &HashSet::new(),
            &History::default(),
            &mut verdict,
        )
        .unwrap();
        check_mirror_hashes(&history, &was_current, &mut verdict);
        check_final_mirror(
            &root.storage(),
            &HashSet::new(),
            RunStatuses::Online,
            &mut verdict,
        )
        .unwrap();
        check_mirror_objects(&repo, &root.storage(), &mut verdict).await.unwrap();
        assert_eq!(verdict.violations, []);
        assert!(verdict.stats.mirror_renames >= 4, "{verdict:?}");
    }

    /// M7: a snapshot file, a transaction log or a manifest the mirror's
    /// snapshots reference that doesn't exist.
    #[tokio::test]
    async fn objects_the_mirror_references_must_exist() {
        let (_dir, root, config) = repo_root().await;
        let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert!(worker.ensure_branch().await.unwrap());
        for _ in 0..3 {
            assert!(matches!(
                worker.branch_commit().await.unwrap(),
                Some(OpResult::Ack { .. })
            ));
        }
        let repo = crate::repo_mode::open_repo(&root, &config, None).await.unwrap();
        let storage = root.storage();
        let first_file = |dir: &str| {
            let mut names: Vec<_> = fs::read_dir(storage.join(dir))
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect();
            names.sort();
            names.remove(0)
        };
        let check = || async {
            let mut verdict = Verdict::default();
            check_mirror_objects(&repo, &storage, &mut verdict).await.unwrap();
            verdict.violations
        };
        assert_eq!(check().await, []);
        for dir in [MANIFESTS_FILE_PATH, TRANSACTION_LOGS_FILE_PATH, SNAPSHOTS_FILE_PATH]
        {
            let name = first_file(dir);
            let path = storage.join(dir).join(&name);
            let bytes = fs::read(&path).unwrap();
            fs::remove_file(&path).unwrap();
            let violations = check().await;
            assert!(
                matches!(&violations[..], [Violation::MirrorDangling { detail }]
                    if detail.contains(&format!("{dir}/{name}"))),
                "{dir}: {violations:?}"
            );
            fs::write(&path, bytes).unwrap();
        }
        // A missing mirror is left to the other rules.
        fs::remove_file(storage.join(REPO_KEY)).unwrap();
        assert_eq!(check().await, []);
    }

    /// M7 after a GC whose cutoff came after a commit's start, as in a run on
    /// Lustre: that commit's missing snapshot file and transaction log are
    /// part of its warning; another snapshot's missing transaction log, or
    /// those of a commit that started after the cutoff, are violations.
    #[tokio::test]
    async fn objects_of_a_commit_a_gc_cutoff_came_after_are_warned() {
        use crate::oracle::gc_cutoff::{
            check_gc_cutoffs,
            tests::{CUTOFF_MS, incident},
        };
        let (_dir, root, config) = repo_root().await;
        let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert!(worker.ensure_branch().await.unwrap());
        let mut snapshots = Vec::new();
        for _ in 0..2 {
            match worker.branch_commit().await.unwrap() {
                Some(OpResult::Ack { snapshot: Some(snapshot) }) => {
                    snapshots.push(snapshot)
                }
                other => panic!("expected an ack, got {other:?}"),
            }
        }
        let [first, second] = &snapshots[..] else { unreachable!() };
        let repo = crate::repo_mode::open_repo(&root, &config, None).await.unwrap();
        let storage = root.storage();
        let key = |dir: &str, id: &str| format!("{dir}/{id}");
        for missing in [
            key(SNAPSHOTS_FILE_PATH, first),
            key(TRANSACTION_LOGS_FILE_PATH, first),
            key(TRANSACTION_LOGS_FILE_PATH, second),
        ] {
            fs::remove_file(storage.join(missing)).unwrap();
        }
        let check = |intent_ms| {
            let (commit, gc) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
            let history = incident(commit, gc, first, intent_ms);
            let (repo, storage) = (&repo, &storage);
            async move {
                let mut verdict = Verdict::default();
                check_gc_cutoffs(&history, &mut verdict);
                check_mirror_objects(repo, storage, &mut verdict).await.unwrap();
                verdict
            }
        };

        let verdict = check(973_998).await;
        let second_tx_log = key(TRANSACTION_LOGS_FILE_PATH, second);
        assert!(
            matches!(&verdict.violations[..], [Violation::MirrorDangling { detail }]
                if detail.contains(&second_tx_log)),
            "{verdict:?}"
        );
        let excused =
            [key(SNAPSHOTS_FILE_PATH, first), key(TRANSACTION_LOGS_FILE_PATH, first)];
        assert!(
            matches!(&verdict.warnings[..], [Warning::GcCutoffAfterSessionStart { missing, .. }]
                if *missing == excused),
            "{verdict:?}"
        );

        let verdict = check(CUTOFF_MS + 1).await;
        assert_eq!(verdict.warnings, []);
        assert_eq!(verdict.violations.len(), 3, "{verdict:?}");
    }

    /// M7 through the workload: a mirror left at a version from before a
    /// branch deletion or an expiration references snapshots that GC then
    /// deleted; the mirror GC published before deleting references none.
    #[tokio::test]
    async fn gc_after_a_deletion_or_an_expiration_leaves_an_older_mirror_dangling() {
        for expiration in [false, true] {
            let (_dir, root, config) = repo_root().await;
            let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
            assert!(worker.ensure_branch().await.unwrap());
            for _ in 0..2 {
                assert!(matches!(
                    worker.branch_commit().await.unwrap(),
                    Some(OpResult::Ack { .. })
                ));
            }
            let mirror = root.storage().join(REPO_KEY);
            let before = fs::read(&mirror).unwrap();
            let repo = crate::repo_mode::open_repo(&root, &config, None).await.unwrap();
            if expiration {
                // Releases the first branch commit, which no ref points at.
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                let older_than_ms = crate::log::now_ms();
                crate::repo_mode::expire_snapshots(&repo, &config, older_than_ms)
                    .await
                    .unwrap();
            } else {
                assert!(matches!(
                    worker.delete_branch().await.unwrap(),
                    OpResult::Ack { .. }
                ));
            }
            let repo = crate::repo_mode::open_repo(&root, &config, None).await.unwrap();
            crate::repo_mode::collect_garbage(&repo, &config, Utc::now()).await.unwrap();
            let check = || async {
                let mut verdict = Verdict::default();
                check_mirror_objects(&repo, &root.storage(), &mut verdict).await.unwrap();
                verdict.violations
            };
            assert_eq!(check().await, [], "expiration: {expiration}");
            fs::write(&mirror, &before).unwrap();
            let violations = check().await;
            assert!(
                !violations.is_empty()
                    && violations
                        .iter()
                        .all(|v| matches!(v, Violation::MirrorDangling { .. })),
                "expiration: {expiration}: {violations:?}"
            );
        }
    }

    /// M2 on a real storage: the mirror must hold the current version.
    #[tokio::test]
    async fn the_mirror_must_be_the_current_version() {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().to_path_buf());
        root.create_dirs().unwrap();
        let mut config = RunConfig::defaults(Mode::Storage, 1);
        config.faults.points = vec![];
        storage_mode::init(&root, &config).await.unwrap();
        let storage = storage_mode::storage(&root, &config, None).await.unwrap();
        let mirror = root.storage().join(REPO_KEY);
        let older = fs::read(&mirror).unwrap();
        let who = WorkerId { tag: "h".to_owned(), slot: "0".to_owned(), incarnation: 0 };
        let log = LogWriter::create(&root.worker_log("h", "0", 0)).unwrap();
        let mut updater =
            Updater::new(Arc::clone(&storage), &who, &config).await.unwrap();
        let attempt = updater.attempt(&log).await.unwrap();
        assert!(matches!(attempt, Attempt::Logged(OpResult::Ack { .. })), "{attempt:?}");
        let current = payload_etag(&read_repo(&*storage).await.unwrap());

        let check = |published: Option<&str>, network_fs| {
            let storage = Arc::clone(&storage);
            let storage_root = root.storage();
            let published = published.map(str::to_owned);
            async move {
                let mut verdict = Verdict::default();
                let after = MirrorCheckpoint::FinalPublish;
                check_mirror_current(
                    &*storage,
                    &storage_root,
                    after,
                    published,
                    network_fs,
                    &mut verdict,
                )
                .await
                .unwrap();
                verdict
            }
        };
        assert_eq!(check(None, false).await, Verdict::default());

        let stale = |found| Violation::MirrorStale {
            expected: current.clone(),
            found,
            after: MirrorCheckpoint::FinalPublish,
        };
        fs::write(&mirror, &older).unwrap();
        assert_eq!(
            check(None, false).await.violations,
            [stale(MirrorFound::Hash(payload_etag(&older)))]
        );
        let network = check(None, true).await;
        assert_eq!(network.violations, []);
        assert_eq!(
            network.warnings,
            [Warning::MirrorStale {
                expected: current.clone(),
                found: MirrorFound::Hash(payload_etag(&older)),
                after: MirrorCheckpoint::FinalPublish,
            }]
        );
        fs::remove_file(&mirror).unwrap();
        assert_eq!(check(None, false).await.violations, [stale(MirrorFound::Missing)]);
        let failed = check(Some("forked"), false).await;
        assert_eq!(
            failed.violations,
            [stale(MirrorFound::PublishFailed("forked".to_owned()))]
        );
    }
}
