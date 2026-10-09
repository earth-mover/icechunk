//! `verify`: checks a finished run, makes a recovery write, ages and sweeps
//! the storage root, and writes `report.json`. Also `run`, which inits, runs
//! this host's workers and verifies, in one go.

use std::{
    collections::BTreeSet,
    fs, io,
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{Context as _, ensure};
use serde::de::DeserializeOwned;

use crate::{
    config::RunConfig,
    control::{Control, read_control, read_go},
    env_info::{HostDone, HostInfo, default_tag},
    layout::RunRoot,
    log::read_all_logs,
    oracle::{History, Verdict, Violation, check_mode, liveness::check_liveness},
    recovery::{
        RecoveryOutcome, age_storage_root, check_clean_root, recovery_deadline,
        recovery_worker, recovery_write, sweep,
    },
    report::{Participants, Report},
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
/// finished: until then a host may still be writing.
pub async fn verify(root: &RunRoot, done_timeout: Duration) -> anyhow::Result<Report> {
    let config = RunConfig::load(root)?;
    let who = recovery_worker();
    let recovery_log = root.worker_log(&who.tag, &who.slot, who.incarnation);
    ensure!(
        !exists(&recovery_log)?,
        "{} exists: this run was already verified, and verify changes the repository",
        recovery_log.display()
    );
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
    let network_fs = hosts.iter().any(|host| host.network_fs);
    if network_fs {
        notes.push(
            "network filesystem: landed conflicts and stalls are warnings".to_owned(),
        );
    }

    eprintln!("reading logs");
    match read_all_logs(root) {
        Ok(logs) => {
            let records: usize = logs.values().map(Vec::len).sum();
            eprintln!(
                "read {records} records from {} logs; running the oracle",
                logs.len()
            );
            let history = History { logs, hosts: hosts.clone(), done: done.clone() };
            check_mode(root, &config, &history, network_fs, &mut verdict).await?;
            check_liveness(&history, &config, network_fs, &mut verdict);
        }
        Err(err) => verdict
            .violations
            .push(Violation::Harness { detail: format!("reading the logs: {err:#}") }),
    }

    let recovery_ms = if missing.is_empty() {
        recover_and_clean(root, &config, &mut verdict).await?
    } else {
        let note = format!(
            "{RECOVERY_SKIPPED}: {}; verify again once they finished",
            missing.join(", ")
        );
        eprintln!("{note}");
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

/// The note of a verify that left the repository alone.
pub const RECOVERY_SKIPPED: &str =
    "recovery write, aging and sweep skipped: hosts still missing";

/// The timed recovery write, then aging, sweeping and the clean-root check.
/// Returns the recovery write's milliseconds if it succeeded.
async fn recover_and_clean(
    root: &RunRoot,
    config: &RunConfig,
    verdict: &mut Verdict,
) -> anyhow::Result<Option<u64>> {
    eprintln!("recovery write");
    let recovery_ms =
        match recovery_write(root, config, recovery_deadline(config)).await? {
            RecoveryOutcome::Succeeded { elapsed } => {
                let ms = millis(elapsed);
                eprintln!("recovery write took {ms} ms");
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
    eprintln!("aging and sweeping the storage root");
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
    if let Err(err) = check_clean_root(root, verdict) {
        verdict.violations.push(Violation::Harness {
            detail: format!("checking the storage root: {err:#}"),
        });
    }
    Ok(recovery_ms)
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
