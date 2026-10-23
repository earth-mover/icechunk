//! `verify`: checks a finished run, sets a status the run left `ReadOnly`
//! `Online` again, publishes the mirror and makes a recovery write, checking
//! the mirror after each, plants canaries, ages and sweeps the storage root,
//! checks the canaries and the root, and writes `report.json`. Also `run`,
//! which inits, runs this host's workers and verifies, in one go.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt, fs, io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, ensure};
use chrono::Utc;
use icechunk::format::repo_info::{RepoAvailability, RepoStatus};
use icechunk_fs::FilesystemStorage;
use serde::de::DeserializeOwned;

use crate::{
    config::{Backend, Mode, RunConfig, WorkerId},
    control::{Control, read_control, read_go},
    env_info::{HostDone, HostInfo, default_tag},
    faults::FaultHooks,
    fork::{FORK_SLOT, erased},
    layout::{RunRoot, VERIFY_TAG},
    log::{
        FaultAction, LogName, OpResult, Record, now_ms, read_all_logs, read_log,
        start_worker_log,
    },
    oracle::{
        History, Verdict, Violation, check_failures, check_mode, count_observed,
        fork::{LoggedFork, check_fork_sides, check_forks, logged_forks, persisted_fork},
        liveness::{check_landed, check_liveness},
        mirror::{
            MirrorCheckpoint, check_mirror_current, check_mirror_hashes,
            check_mirror_objects,
        },
    },
    recovery::{
        RECOVERY_DEADLINE, RecoveryOutcome, age_storage_root, check_canaries,
        check_clean_root, describe, plant_canaries, recovery_worker, recovery_write,
        recovery_write_once, remove_canaries, sweep,
    },
    repo_mode,
    report::{Participants, Report},
    storage_mode::{self, classify_storage},
    supervisor,
};

pub const DEFAULT_DONE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

const DONE_POLL: Duration = Duration::from_secs(1);
const PHASE_REPORT_EVERY: Duration = Duration::from_secs(5);

/// Waits up to `done_timeout` for every participant to finish, checks the run
/// with its mode's oracle and the liveness checks, makes a recovery write, then
/// requires a clean storage root after aging and sweeping it. Writes and
/// returns the report. `Err` for setup and I/O errors; a run that can't be
/// checked is a failed report. The recovery write and the sweep change the
/// repository, so they run once per run, and only once every participant
/// finished: until then a host may still be writing. A verify interrupted
/// before its recovery write can run again: each publishes under its own
/// log.
pub async fn verify(root: &RunRoot, done_timeout: Duration) -> anyhow::Result<Report> {
    verify_phase(root, done_timeout, &EarlierPhases::default()).await
}

/// `verify` refuses a run it already verified fully: its recovery write
/// and sweep changed the repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlreadyVerified {
    /// The recovery write's log, which shows it.
    pub log: PathBuf,
}

impl fmt::Display for AlreadyVerified {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} exists: this run was already verified, and verify changes the repository",
            self.log.display()
        )
    }
}

impl std::error::Error for AlreadyVerified {}

/// What a phase of the conversion scenario adds to its verify: the
/// repository's history began before the phase.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EarlierPhases {
    /// The logs of the earlier phases, oldest first: the repository oracle
    /// checks their operations too.
    pub logs: Vec<BTreeMap<LogName, Vec<Record>>>,
    /// The backups of `overwritten/` from before this phase's era, the
    /// standard layout's among them: versions that were current, which L1
    /// doesn't check.
    pub backups: HashSet<String>,
}

/// [`verify`] of a phase of the conversion scenario after `earlier` ones.
pub async fn verify_phase(
    root: &RunRoot,
    done_timeout: Duration,
    earlier: &EarlierPhases,
) -> anyhow::Result<Report> {
    let (root, config) = &RunConfig::load_resolved(root)?;
    let config = config.clone();
    let who = recovery_worker();
    let log = root.worker_log(&who.tag, &who.slot, who.incarnation);
    if exists(&log)? {
        return Err(AlreadyVerified { log }.into());
    }
    let mut verdict = Verdict::default();
    let mut notes = Vec::new();
    let Some(participants) = participants(root, &mut verdict, &mut notes)? else {
        let report = Report::new(
            verdict,
            config,
            Participants::default(),
            vec![],
            vec![],
            None,
            notes,
        );
        return finish(root, report);
    };
    let hosts = host_infos(root, &participants, &mut verdict);
    let (done, missing) =
        wait_for_done(root, &participants, done_timeout, &mut verdict).await?;
    if let Some(detail) = no_repository(root)? {
        // A run that worked on another storage root, or none: nothing here
        // can be checked, and that must not pass.
        verdict.violations.push(Violation::Harness { detail });
        let missing = missing.into_iter().map(str::to_owned).collect();
        let participants = Participants { tags: participants, missing };
        return finish(
            root,
            Report::new(verdict, config, participants, hosts, done, None, notes),
        );
    }
    let network_fs = hosts.iter().any(|host| host.network_fs);
    if network_fs {
        notes.push(
            "network filesystem: landed conflicts and stalls are warnings".to_owned(),
        );
    }

    eprintln!("reading logs");
    // The versions that were current, on the filesystem backend, and the
    // fork still in place.
    let (was_current, fork) = match read_all_logs(root) {
        Ok(mut logs) => {
            if config.fork.is_some() && missing.is_empty() {
                observe_fork(root, &mut logs)?;
            }
            let records: usize = logs.values().map(Vec::len).sum();
            eprintln!(
                "read {records} records from {} logs; running the oracle",
                logs.len()
            );
            let own = History { logs, hosts: hosts.clone(), done: done.clone() };
            let history = own.clone().after_earlier_phases(&earlier.logs);
            let was_current = check_mode(
                root,
                &config,
                &history,
                network_fs,
                &earlier.backups,
                &mut verdict,
            )
            .await?;
            check_liveness(&own, &config, network_fs, &mut verdict);
            check_failures(&own, &mut verdict);
            count_observed(&own, &mut verdict);
            check_forks(&own, network_fs, &mut verdict);
            if missing.is_empty() {
                check_landed(&own, &mut verdict);
            }
            (was_current, persisted_fork(&logged_forks(&own)))
        }
        Err(err) => {
            verdict.violations.push(Violation::Harness {
                detail: format!("reading the logs: {err:#}"),
            });
            (None, None)
        }
    };

    let recovery_ms = if !missing.is_empty() {
        let note = format!(
            "{RECOVERY_SKIPPED}: {}; verify again once they finished",
            missing.join(", ")
        );
        eprintln!("{note}");
        notes.push(note);
        None
    } else if let Some(fork) = fork {
        recover_forked(root, &config, &fork, &mut verdict, &mut notes).await?;
        None
    } else {
        let checked = Checked { was_current: was_current.as_ref(), network_fs };
        recover_and_clean(root, &config, checked, &mut verdict, &mut notes).await?
    };
    let missing = missing.into_iter().map(str::to_owned).collect();
    let participants = Participants { tags: participants, missing };
    finish(
        root,
        Report::new(verdict, config, participants, hosts, done, recovery_ms, notes),
    )
}

/// The slot of `verify`'s record of a fork erased after the fork actor
/// stopped watching, `logs/verify.fork.<k>.log`.
pub const OBSERVED_FORK_SLOT: &str = FORK_SLOT;

/// If the last fork wasn't seen erased but `$ROOT/repo` no longer holds it,
/// as after a publication of a writer winding down, logs its erasure now,
/// for F4, in a log of `verify`'s added to `logs`.
fn observe_fork(
    root: &RunRoot,
    logs: &mut BTreeMap<LogName, Vec<Record>>,
) -> anyhow::Result<()> {
    let history = History { logs: logs.clone(), ..History::default() };
    let Some(fork) = persisted_fork(&logged_forks(&history)) else { return Ok(()) };
    let Some(found) = erased(&root.storage(), &fork.fork_hash) else { return Ok(()) };
    let who = first_free(root, OBSERVED_FORK_SLOT)?;
    let log = start_worker_log(root, &who)?;
    log.append(&Record::ForkGone { t_ms: now_ms(), fork_hash: fork.fork_hash, found })?;
    let path = root.worker_log(&who.tag, &who.slot, who.incarnation);
    let name =
        LogName { tag: who.tag, slot: who.slot, incarnation: Some(who.incarnation) };
    logs.insert(name, read_log(&path)?);
    Ok(())
}

/// After a run whose last fork is still in `$ROOT/repo`: F5, then the
/// publication, which must refuse the fork (F4), and one recovery write,
/// which must land and report the fork (F2). No canaries, aging, sweep or
/// clean-root check: the storage refuses GC while the fork is there.
async fn recover_forked(
    root: &RunRoot,
    config: &RunConfig,
    fork: &LoggedFork,
    verdict: &mut Verdict,
    notes: &mut Vec<String>,
) -> anyhow::Result<()> {
    notes.push(format!(
        "the fork {} persisted: verify checks that both sides survive, that its \
         publication and its recovery write report the fork, and doesn't sweep",
        fork.fork_hash
    ));
    eprintln!("checking both sides of the fork");
    let checked = async {
        let repo = match config.mode {
            Mode::Repository => Some(repo_mode::open_repo(root, config, None).await?),
            Mode::Storage => None,
        };
        check_fork_sides(&root.storage(), fork, repo.as_ref(), verdict).await
    };
    if let Err(err) = checked.await {
        verdict.violations.push(Violation::Harness {
            detail: format!("checking both sides of the fork: {err:#}"),
        });
    }

    eprintln!("publishing the mirror over the fork");
    let who = publisher(root)?;
    match publish_mirror(root, config, &who).await? {
        Err(OpResult::Forked { .. }) => {}
        Ok(()) => verdict.violations.push(Violation::SilentForkLoss {
            fork_hash: fork.fork_hash.clone(),
            erased_by: "verify's publish_mirror(), which didn't report it".to_owned(),
        }),
        Err(other) => verdict.violations.push(Violation::Recovery {
            detail: format!("publish_mirror() over the fork didn't report it: {other:?}"),
        }),
    }

    eprintln!("recovery write over the fork");
    match recovery_write_once(root, config).await? {
        Ok((_, OpResult::DurableButForked { .. })) => {}
        Ok((id, OpResult::Ack { .. })) => {
            verdict
                .violations
                .push(Violation::UnreportedFork { id, op: "recovery write".to_owned() });
        }
        Ok((_, other)) => verdict.violations.push(Violation::Recovery {
            detail: format!("the recovery write over the fork didn't land: {other:?}"),
        }),
        Err(err) => verdict.violations.push(Violation::Recovery {
            detail: format!("the recovery write over the fork wasn't attempted: {err}"),
        }),
    }
    Ok(())
}

/// Why the storage root holds no repository: it is missing, or has neither
/// `repo` nor `repo.d`.
fn no_repository(root: &RunRoot) -> anyhow::Result<Option<String>> {
    let storage = root.storage();
    if !exists(&storage)? {
        return Ok(Some(format!("the storage root {} doesn't exist", storage.display())));
    }
    let found = exists(&storage.join(storage_mode::REPO_KEY))?
        || exists(&storage.join("repo.d"))?;
    Ok((!found)
        .then(|| format!("the storage root {} holds no repository", storage.display())))
}

/// The note of a verify that left the repository alone.
pub const RECOVERY_SKIPPED: &str =
    "publication, recovery write, aging and sweep skipped: hosts still missing";

/// The slot of `verify`'s publications of the mirror, logged as
/// `logs/verify.publish.<k>.log`, `k` counting the verifies that published.
pub const PUBLISH_SLOT: &str = "publish";

/// The publisher of this verify: the first incarnation without a log.
fn publisher(root: &RunRoot) -> anyhow::Result<WorkerId> {
    first_free(root, PUBLISH_SLOT)
}

/// `verify`'s worker in `slot`, with the first incarnation without a log.
fn first_free(root: &RunRoot, slot: &str) -> anyhow::Result<WorkerId> {
    let mut who =
        WorkerId { tag: VERIFY_TAG.to_owned(), slot: slot.to_owned(), incarnation: 0 };
    while exists(&root.worker_log(&who.tag, &who.slot, who.incarnation))? {
        who.incarnation += 1;
    }
    Ok(who)
}

/// What the oracle found that the checks after it need.
#[derive(Debug, Clone, Copy)]
struct Checked<'a> {
    /// The versions that were current; `None` if the logs couldn't be read.
    was_current: Option<&'a HashSet<String>>,
    network_fs: bool,
}

/// In repository mode, the status set `Online` if the run left it
/// `ReadOnly`; on the filesystem backend, the mirror's objects (repository
/// mode) and a publication of the mirror; then the timed recovery write,
/// after each a check that the mirror is the current version, and finally
/// canaries, aging, sweeping and the canary and clean-root checks. Returns
/// the recovery write's milliseconds if it succeeded.
async fn recover_and_clean(
    root: &RunRoot,
    config: &RunConfig,
    checked: Checked<'_>,
    verdict: &mut Verdict,
    notes: &mut Vec<String>,
) -> anyhow::Result<Option<u64>> {
    let Checked { was_current, network_fs } = checked;
    if config.mode == Mode::Repository {
        set_online(root, config, verdict, notes).await;
    }
    let layout = config.backend == Backend::Filesystem;
    if layout {
        if config.mode == Mode::Repository {
            eprintln!("checking the mirror's objects");
            let checked = async {
                let repo = repo_mode::open_repo(root, config, None).await?;
                check_mirror_objects(&repo, &root.storage(), verdict).await
            };
            if let Err(err) = checked.await {
                verdict.violations.push(Violation::Harness {
                    detail: format!("checking the mirror's objects: {err:#}"),
                });
            }
        }
        eprintln!("publishing the mirror");
        publish_and_check(root, config, was_current, network_fs, verdict).await?;
    }

    eprintln!("recovery write");
    let recovery_ms = match recovery_write(root, config, RECOVERY_DEADLINE).await? {
        RecoveryOutcome::Succeeded { elapsed } => {
            let ms = millis(elapsed);
            eprintln!("recovery write took {ms} ms");
            if layout {
                let after = MirrorCheckpoint::RecoveryWrite;
                check_mirror(root, config, after, Ok(()), network_fs, verdict).await;
            }
            Some(ms)
        }
        RecoveryOutcome::Failed { elapsed, last_error } => {
            let ms = millis(elapsed);
            eprintln!("recovery write failed after {ms} ms");
            verdict.violations.push(Violation::Recovery {
                detail: format!(
                    "no write succeeded within {ms} ms; last error: {last_error}"
                ),
            });
            None
        }
    };

    // From here on a failure is recorded, so the report is still written.
    eprintln!("planting canaries, aging and sweeping the storage root");
    let canaries = plant_canaries(root, config).unwrap_or_else(|err| {
        verdict.violations.push(Violation::Harness { detail: format!("{err:#}") });
        Vec::new()
    });
    if let Err(err) = age_storage_root(root) {
        verdict.violations.push(Violation::Recovery {
            detail: format!("aging the storage root: {err:#}"),
        });
    }
    if let Err(err) = sweep(root, config).await {
        verdict.violations.push(Violation::Recovery {
            detail: format!("sweeping after the run: {err:#}"),
        });
    }
    check_canaries(&canaries, verdict);
    if let Err(err) = remove_canaries(&canaries) {
        verdict.violations.push(Violation::Harness { detail: format!("{err:#}") });
    }
    if let Err(err) = check_clean_root(root, config, verdict) {
        verdict.violations.push(Violation::Harness {
            detail: format!("checking the storage root: {err:#}"),
        });
    }
    Ok(recovery_ms)
}

/// Rule M2's publication, then M1 on the mirror renames it logged against
/// `was_current` (skipped without it: the oracle couldn't read the logs), and
/// M2. `Err` only if its log can't be written or read.
async fn publish_and_check(
    root: &RunRoot,
    config: &RunConfig,
    was_current: Option<&HashSet<String>>,
    network_fs: bool,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    let who = publisher(root)?;
    let published =
        publish_mirror(root, config, &who).await?.map_err(|failed| describe(&failed));
    let log =
        LogName { tag: who.tag, slot: who.slot, incarnation: Some(who.incarnation) };
    let path = root.worker_log(&log.tag, &log.slot, who.incarnation);
    let records = read_log(&path)?;
    // It carries the run's sabotage, which may fire here alone.
    let fired = records
        .iter()
        .filter(|record| {
            matches!(record, Record::Fault { action: FaultAction::Sabotage, .. })
        })
        .count();
    if fired > 0 {
        *verdict.stats.faults.entry("sabotage".to_owned()).or_default() += fired as u64;
    }
    if let Some(was_current) = was_current {
        let history = History { logs: [(log, records)].into(), ..History::default() };
        check_mirror_hashes(&history, was_current, verdict);
    }
    let after = MirrorCheckpoint::FinalPublish;
    check_mirror(root, config, after, published, network_fs, verdict).await;
    Ok(())
}

/// Rule M2's publication: `publish_mirror()` through a storage whose hooks
/// carry only the run's sabotage. `Err` only if its log can't be written;
/// the inner error is the publication's, classified.
async fn publish_mirror(
    root: &RunRoot,
    config: &RunConfig,
    who: &WorkerId,
) -> anyhow::Result<Result<(), OpResult>> {
    let log = Arc::new(start_worker_log(root, who)?);
    let hooks = FaultHooks::sabotage_only(config.sabotage, log);
    let storage = FilesystemStorage::new(&root.storage())?
        .with_fsync(config.fsync)
        .with_protocol_hooks(Arc::new(hooks));
    Ok(storage
        .publish_mirror()
        .await
        .map(|_| ())
        .map_err(|err| classify_storage(err.kind())))
}

/// Sets the status `Online` if it is `ReadOnly`, as a status toggler killed
/// during a hold leaves it, and notes it: the recovery write and GC need it.
/// A failure is a recovery violation.
async fn set_online(
    root: &RunRoot,
    config: &RunConfig,
    verdict: &mut Verdict,
    notes: &mut Vec<String>,
) {
    let set = async {
        let repo = repo_mode::open_repo(root, config, None).await?;
        let status =
            repo.get_status().await.map_err(|err| anyhow::anyhow!("{}", err.kind()))?;
        if status.availability == RepoAvailability::Online {
            return Ok(None);
        }
        let online = RepoStatus {
            availability: RepoAvailability::Online,
            set_at: Utc::now(),
            limited_availability_reason: None,
        };
        repo.set_status(&online)
            .await
            .map_err(|err| anyhow::anyhow!("{}", err.kind()))?;
        anyhow::Ok(Some(status))
    };
    match set.await {
        Ok(None) => {}
        Ok(Some(status)) => notes.push(format!(
            "the run left the status {:?} ({}); verify set it Online",
            status.availability,
            status.limited_availability_reason.as_deref().unwrap_or("no reason")
        )),
        Err(err) => verdict.violations.push(Violation::Recovery {
            detail: format!("setting the status Online: {err:#}"),
        }),
    }
}

/// Rule M2; a failure to check is a harness violation.
async fn check_mirror(
    root: &RunRoot,
    config: &RunConfig,
    after: MirrorCheckpoint,
    published: Result<(), String>,
    network_fs: bool,
    verdict: &mut Verdict,
) {
    let checked = async {
        let storage = storage_mode::storage(root, config, None).await?;
        let published = published.err();
        check_mirror_current(
            &*storage,
            &root.storage(),
            after,
            published,
            network_fs,
            verdict,
        )
        .await
    };
    if let Err(err) = checked.await {
        verdict.violations.push(Violation::Harness {
            detail: format!("checking the mirror after {after:?}: {err:#}"),
        });
    }
}

/// `init` for one host, this host's workers, then `verify`.
pub async fn run(
    root: &RunRoot,
    config: &RunConfig,
    workers: u32,
    progress_every: Duration,
) -> anyhow::Result<Report> {
    ensure!(config.hosts == 1, "run is for one host, got hosts = {}", config.hosts);
    supervisor::init(root, config).await?;
    supervisor::workers(root, default_tag(), workers, progress_every).await?;
    verify(root, DEFAULT_DONE_TIMEOUT).await
}

fn finish(root: &RunRoot, report: Report) -> anyhow::Result<Report> {
    report.write(root)?;
    Ok(report)
}

/// The participants decided at the go; `None` if the run never started.
fn participants(
    root: &RunRoot,
    verdict: &mut Verdict,
    notes: &mut Vec<String>,
) -> anyhow::Result<Option<BTreeSet<String>>> {
    let mut never_started = |why: &str| {
        verdict
            .violations
            .push(Violation::Harness { detail: format!("the run never started: {why}") });
        Ok(None)
    };
    match read_control(root)? {
        Control::Wait => return never_started("control is still wait"),
        Control::Stop => {
            notes.push("the run was stopped with `fs-stress stop`".to_owned());
        }
        Control::Go { .. } => {}
    }
    if !exists(&root.go())? {
        return never_started("it was stopped before the go");
    }
    match read_go(root) {
        Ok(participants) => Ok(Some(participants)),
        Err(err) => {
            verdict.violations.push(Violation::Harness { detail: format!("{err:#}") });
            Ok(None)
        }
    }
}

fn host_infos(
    root: &RunRoot,
    participants: &BTreeSet<String>,
    verdict: &mut Verdict,
) -> Vec<HostInfo> {
    participants
        .iter()
        .filter_map(|tag| {
            read_json(&root.host_info(tag))
                .map_err(|err| {
                    verdict
                        .violations
                        .push(Violation::Harness { detail: format!("{err:#}") });
                })
                .ok()
        })
        .collect()
}

/// The `done/` entries of `participants` and the tags still missing one, once
/// all exist or `timeout` passed.
async fn wait_for_done<'a>(
    root: &RunRoot,
    participants: &'a BTreeSet<String>,
    timeout: Duration,
    verdict: &mut Verdict,
) -> anyhow::Result<(Vec<HostDone>, Vec<&'a str>)> {
    let deadline = Instant::now() + timeout;
    let mut next_report = Instant::now();
    let missing = loop {
        let mut missing = Vec::new();
        for tag in participants {
            if !exists(&root.host_done(tag))? {
                missing.push(tag.as_str());
            }
        }
        let now = Instant::now();
        if now >= next_report || missing.is_empty() {
            let finished = participants.len() - missing.len();
            eprintln!("waiting for done/ ({finished}/{})", participants.len());
            next_report = now + PHASE_REPORT_EVERY;
        }
        if missing.is_empty() || now >= deadline {
            break missing;
        }
        tokio::time::sleep(DONE_POLL.min(deadline - now)).await;
    };
    if !missing.is_empty() {
        verdict.violations.push(Violation::Harness {
            detail: format!(
                "no done/ entry within {} s from: {}",
                timeout.as_secs(),
                missing.join(", ")
            ),
        });
    }
    let done = participants
        .iter()
        .filter(|tag| !missing.contains(&tag.as_str()))
        .filter_map(|tag| {
            read_json(&root.host_done(tag))
                .map_err(|err| {
                    verdict
                        .violations
                        .push(Violation::Harness { detail: format!("{err:#}") });
                })
                .ok()
        })
        .collect();
    Ok((done, missing))
}

fn read_json<T: DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))
}

fn exists(path: &Path) -> anyhow::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err).with_context(|| format!("checking {}", path.display())),
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Mode,
        log::LogWriter,
        oracle::mirror::MirrorSource,
        storage_mode::{Attempt, Updater, payload_etag, read_repo},
    };

    /// An initialized storage-mode run after one change, its mirror left at
    /// the initial version, and the etag of the current version.
    async fn lagging_run() -> (tempfile::TempDir, RunRoot, RunConfig, String) {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().join("run"));
        let config = RunConfig::defaults(Mode::Storage, 1);
        supervisor::init(&root, &config).await.unwrap();
        let mirror = root.storage().join(storage_mode::REPO_KEY);
        let initial = fs::read(&mirror).unwrap();
        let who = WorkerId { tag: "h".to_owned(), slot: "0".to_owned(), incarnation: 0 };
        let log = LogWriter::create(&root.worker_log("h", "0", 0)).unwrap();
        let storage = storage_mode::storage(&root, &config, None).await.unwrap();
        let mut updater =
            Updater::new(Arc::clone(&storage), &who, &config).await.unwrap();
        let attempt = updater.attempt(&log).await.unwrap();
        assert!(matches!(attempt, Attempt::Logged(OpResult::Ack { .. })), "{attempt:?}");
        fs::write(&mirror, initial).unwrap();
        let current = payload_etag(&read_repo(&*storage).await.unwrap());
        (dir, root, config, current)
    }

    /// M1 covers verify's own publication, which happens after the oracle
    /// read the logs.
    #[tokio::test]
    async fn verifys_publication_must_install_a_version_that_was_current() {
        let (_dir, root, config, current) = lagging_run().await;
        let mut verdict = Verdict::default();
        let was_current = HashSet::from([current]);
        publish_and_check(&root, &config, Some(&was_current), false, &mut verdict)
            .await
            .unwrap();
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.stats.mirror_renames, 1);

        let (_dir, root, config, current) = lagging_run().await;
        let mut verdict = Verdict::default();
        let was_current = HashSet::from(["another version".to_owned()]);
        publish_and_check(&root, &config, Some(&was_current), false, &mut verdict)
            .await
            .unwrap();
        assert!(
            matches!(&verdict.violations[..], [Violation::MirrorNotAVersion {
                hash,
                source: MirrorSource::Rename,
                ..
            }] if *hash == current),
            "{verdict:?}"
        );
    }

    #[test]
    fn run_is_single_host() {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().join("run"));
        let config = RunConfig::defaults(Mode::Storage, 2);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert!(runtime.block_on(run(&root, &config, 1, Duration::ZERO)).is_err());
        assert!(!root.root().exists());
    }
}
