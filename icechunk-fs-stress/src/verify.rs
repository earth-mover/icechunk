//! `verify`: checks a finished run, sets a status the run left `ReadOnly`
//! `Online` again, checks the objects the current version references, makes
//! a recovery write, plants canaries, ages and sweeps the storage root,
//! checks the canaries and the root, and writes `report.json`. Also `run`,
//! which inits, runs this host's workers and verifies, in one go.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt, fs, io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context as _, ensure};
use chrono::Utc;
use icechunk::format::repo_info::{RepoAvailability, RepoStatus};
use serde::de::DeserializeOwned;

use crate::{
    config::{Backend, Mode, RunConfig},
    control::{Control, read_control, read_go},
    env_info::{HostDone, HostInfo, default_tag},
    layout::RunRoot,
    log::{LogName, Record, read_all_logs},
    oracle::{
        History, Verdict, Violation, check_failures, check_mode, count_observed,
        layout::check_current_objects,
        liveness::{check_landed, check_liveness},
    },
    progress::{BRIEF_EVERY, Progress},
    recovery::{
        RECOVERY_DEADLINE, RecoveryOutcome, age_storage_root, check_canaries,
        check_clean_root, plant_canaries, recovery_worker, recovery_write,
        remove_canaries, sweep,
    },
    repo_mode,
    report::{Participants, Report},
    storage_mode, supervisor,
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
/// before its recovery write can run again.
pub async fn verify(root: &RunRoot, done_timeout: Duration) -> anyhow::Result<Report> {
    verify_with_progress(root, done_timeout, &Progress::Stderr).await
}

/// [`verify`], with its progress going to `progress`.
pub async fn verify_with_progress(
    root: &RunRoot,
    done_timeout: Duration,
    progress: &Progress,
) -> anyhow::Result<Report> {
    verify_phase(root, done_timeout, &EarlierPhases::default(), progress).await
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
    /// The backups of `overwritten/` from before this phase's era, those
    /// made in the object store layout among them: versions that were current,
    /// which L1 doesn't check.
    pub backups: HashSet<String>,
}

/// [`verify`] of a phase of the conversion scenario after `earlier` ones.
pub async fn verify_phase(
    root: &RunRoot,
    done_timeout: Duration,
    earlier: &EarlierPhases,
    progress: &Progress,
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
        wait_for_done(root, &participants, done_timeout, &mut verdict, progress).await?;
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

    progress.detail("reading logs");
    match read_all_logs(root) {
        Ok(logs) => {
            let records: usize = logs.values().map(Vec::len).sum();
            progress.detail(&format!(
                "read {records} records from {} logs; running the oracle",
                logs.len()
            ));
            let own = History { logs, hosts: hosts.clone(), done: done.clone() };
            let history = own.clone().after_earlier_phases(&earlier.logs);
            check_mode(
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
            if missing.is_empty() {
                check_landed(&own, &mut verdict);
            }
        }
        Err(err) => {
            verdict.violations.push(Violation::Harness {
                detail: format!("reading the logs: {err:#}"),
            });
        }
    }

    let recovery_ms = if missing.is_empty() {
        recover_and_clean(root, &config, &mut verdict, &mut notes, progress).await?
    } else {
        let note = format!(
            "{RECOVERY_SKIPPED}: {}; verify again once they finished",
            missing.join(", ")
        );
        progress.detail(&note);
        notes.push(note);
        None
    };
    let missing = missing.into_iter().map(str::to_owned).collect();
    let participants = Participants { tags: participants, missing };
    finish(
        root,
        Report::new(verdict, config, participants, hosts, done, recovery_ms, notes),
    )
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
    "recovery write, aging and sweep skipped: hosts still missing";

/// In repository mode, the status set `Online` if the run left it
/// `ReadOnly`, and on the filesystem backend M7 on the current version; then
/// the timed recovery write, and finally canaries, aging, sweeping and the
/// canary and clean-root checks. Returns the recovery write's milliseconds
/// if it succeeded.
async fn recover_and_clean(
    root: &RunRoot,
    config: &RunConfig,
    verdict: &mut Verdict,
    notes: &mut Vec<String>,
    progress: &Progress,
) -> anyhow::Result<Option<u64>> {
    if config.mode == Mode::Repository {
        set_online(root, config, verdict, notes).await;
        if config.backend == Backend::Filesystem {
            progress.detail("checking the current version's objects");
            let checked = async {
                let repo = repo_mode::open_repo(root, config, None).await?;
                check_current_objects(&repo, verdict).await
            };
            if let Err(err) = checked.await {
                verdict.violations.push(Violation::Harness {
                    detail: format!("checking the current version's objects: {err:#}"),
                });
            }
        }
    }

    progress.detail("recovery write");
    let recovery_ms = match recovery_write(root, config, RECOVERY_DEADLINE).await? {
        RecoveryOutcome::Succeeded { elapsed } => {
            let ms = millis(elapsed);
            progress.detail(&format!("recovery write took {ms} ms"));
            Some(ms)
        }
        RecoveryOutcome::Failed { elapsed, last_error } => {
            let ms = millis(elapsed);
            progress.detail(&format!("recovery write failed after {ms} ms"));
            verdict.violations.push(Violation::Recovery {
                detail: format!(
                    "no write succeeded within {ms} ms; last error: {last_error}"
                ),
            });
            None
        }
    };

    // From here on a failure is recorded, so the report is still written.
    progress.detail("planting canaries, aging and sweeping the storage root");
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
    progress: &Progress,
) -> anyhow::Result<(Vec<HostDone>, Vec<&'a str>)> {
    let deadline = Instant::now() + timeout;
    let mut next_report = Instant::now();
    let mut next_brief = Instant::now();
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
            progress.detail(&format!(
                "waiting for done/ ({finished}/{})",
                participants.len()
            ));
            next_report = now + PHASE_REPORT_EVERY;
        }
        if !missing.is_empty() && now >= next_brief {
            let finished = participants.len() - missing.len();
            progress.brief_line(|run| {
                format!(
                    "{run}: waiting for the hosts to finish ({finished} of {} done)",
                    participants.len()
                )
            });
            next_brief = now + BRIEF_EVERY;
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
    use crate::config::Mode;

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
