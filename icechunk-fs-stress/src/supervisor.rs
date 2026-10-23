//! A host's part of a run. `init` creates the run root; `workers` registers
//! the host, waits for the go, then runs its worker processes until the
//! deadline, killing and respawning them.

use std::{
    collections::BTreeMap,
    fmt,
    os::unix::process::ExitStatusExt as _,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context as _, bail, ensure};
use chrono::{DateTime, Utc};
use icechunk::storage::{GetModifiedResult, StorageContext};
use rand::{RngExt as _, rngs::StdRng};

use crate::{
    config::{Backend, Mode, RunConfig, WorkerId, write_synced},
    control::{
        BARRIER_POLL, Control, WaitOutcome, read_control, wait_for_go_reporting,
        write_control,
    },
    env_info::HostDone,
    fork::FORK_SLOT,
    layout::{INIT_TAG, RunRoot, SCENARIO_TAG, VERIFY_TAG, is_valid_tag},
    log::{
        FaultAction, LogWriter, OpResult, Record, now_ms, read_appended, start_worker_log,
    },
    progress::{BRIEF_EVERY, Progress, RunLabel},
    repo_mode,
    sampler::Sampler,
    storage_mode,
    toggler::TOGGLER_SLOT,
};

/// Creates the run root: the layout, `run.json`, the mode's initial state,
/// and finally `control`, so no host can join a half-initialized run. On the
/// filesystem backend it logs the first version's `N` to `init`'s log.
pub async fn init(root: &RunRoot, config: &RunConfig) -> anyhow::Result<()> {
    config.validate()?;
    root.create_dirs()?;
    config.write_new(root)?;
    let t_before_ms = now_ms();
    match config.mode {
        Mode::Storage => storage_mode::init(root, config).await?,
        Mode::Repository => repo_mode::init(root, config).await?,
    }
    let t_after_ms = now_ms();
    if config.backend == Backend::Filesystem {
        log_init_version(root, config, t_before_ms, t_after_ms).await?;
    }
    write_control(root, &Control::Wait).context("writing control")
}

/// The slot of `init`'s log.
const INIT_SLOT: &str = "init";

/// Logs [`Record::InitVersion`] with the `N` of the current version, which
/// no worker has changed yet, to `logs/init.init.0.log`.
pub async fn log_init_version(
    root: &RunRoot,
    config: &RunConfig,
    t_before_ms: i64,
    t_after_ms: i64,
) -> anyhow::Result<()> {
    let storage = storage_mode::storage(root, config, None).await?;
    let settings = storage.default_settings().await?;
    let ctx = StorageContext::unattributed(&settings);
    let GetModifiedResult::Modified { new_version, .. } =
        storage.get_object_conditional(&ctx, storage_mode::REPO_KEY, None).await?
    else {
        bail!("reading the first version returned no version");
    };
    let n = new_version
        .generation()
        .and_then(|generation| generation.parse().ok())
        .with_context(|| format!("the first version {new_version:?} has no number"))?;
    let who =
        WorkerId { tag: INIT_TAG.to_owned(), slot: INIT_SLOT.to_owned(), incarnation: 0 };
    start_worker_log(root, &who)?
        .append(&Record::InitVersion { n, t_before_ms, t_after_ms })
        .context("logging the first version")
}

pub const ASSUMED_OPS_PER_SECOND_PER_WORKER: u64 = 1000;
/// A repo info file around a payload's padding, its ops log full.
const PAYLOAD_OVERHEAD_BYTES: u64 = 4 * 1024;
/// Snapshot, manifest, transaction log and `repo` rewrite.
const COMMIT_OVERHEAD_BYTES: u64 = 8 * 1024;

/// An upper bound on the bytes a run leaves under `storage/`, assuming
/// [`ASSUMED_OPS_PER_SECOND_PER_WORKER`] and the default worker count.
pub fn data_volume_estimate(config: &RunConfig) -> u64 {
    let workers = u64::from(config.hosts) * u64::from(config.workers_per_host_default);
    let duration_ms = u64::try_from(config.duration.as_millis()).unwrap_or(u64::MAX);
    let ops = ASSUMED_OPS_PER_SECOND_PER_WORKER
        .saturating_mul(workers)
        .saturating_mul(duration_ms)
        / 1000;
    let bytes = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    let per_op = match config.mode {
        // The backup of every update's base, plus `repo` itself.
        Mode::Storage => bytes(config.payload_bytes) + PAYLOAD_OVERHEAD_BYTES,
        Mode::Repository => bytes(config.chunk_bytes) + COMMIT_OVERHEAD_BYTES,
    };
    per_op.saturating_mul(ops.saturating_add(1))
}

/// A host's tag must be valid and not one of verify's, init's or the
/// scenario's.
pub fn check_tag(tag: &str) -> anyhow::Result<()> {
    ensure!(is_valid_tag(tag), "invalid tag {tag:?}");
    for reserved in [VERIFY_TAG, INIT_TAG, SCENARIO_TAG] {
        ensure!(tag != reserved, "the tag {reserved:?} is reserved");
    }
    Ok(())
}

/// `Run 1 of 2 started.`, with how many of the `hosts` took part if there are
/// several.
fn started_line(run: &RunLabel, participants: usize, hosts: u32) -> String {
    match usize::try_from(hosts) {
        Ok(1) => format!("{run} started."),
        Ok(hosts) if participants >= hosts => {
            format!("{run} started with {participants} hosts.")
        }
        _ => format!(
            "{run} started with {participants} of {hosts} hosts; the others came too late."
        ),
    }
}

/// `workers` registered after the go (or a stop): the run went on without it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LateHost {
    pub hosts: u32,
}

impl fmt::Display for LateHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "this host registered after the run started (control is go/stop); \
             start every host before {} have registered",
            self.hosts
        )
    }
}

impl std::error::Error for LateHost {}

pub const GC_SLOT: &str = "gc";

const TICK: Duration = Duration::from_millis(100);
const CONTROL_EVERY: Duration = Duration::from_secs(1);
const WIND_DOWN_SLACK: Duration = Duration::from_secs(30);

/// Registers this host, waits for the go, and runs `workers` worker processes
/// (plus the GC worker, the status toggler and the fork actor on the first
/// participant, if configured, and the mirror sampler thread) until the run's deadline or a
/// stop. Writes and returns `done/<tag>.json`. A host that
/// missed the go fails with [`LateHost`]. Every `progress_every` (never if
/// zero) it prints a status line to stderr.
pub async fn workers(
    root: &RunRoot,
    tag: String,
    workers: u32,
    progress_every: Duration,
) -> anyhow::Result<HostDone> {
    workers_with_go_after(root, tag, workers, progress_every, &Progress::Stderr, None)
        .await
}

/// [`workers`], with its status lines going to `progress`, whose barrier
/// starts the run with the hosts registered by then once `go_after` passed
/// since this host registered (see [`wait_for_go_reporting`]).
pub async fn workers_with_go_after(
    root: &RunRoot,
    tag: String,
    workers: u32,
    progress_every: Duration,
    progress: &Progress,
    go_after: Option<Duration>,
) -> anyhow::Result<HostDone> {
    check_tag(&tag)?;
    ensure!(workers >= 1, "a host needs at least 1 worker");
    let (root, config) = &RunConfig::load_resolved(root)?;
    let config = config.clone();
    let exe = std::env::current_exe().context("locating the fs-stress binary")?;

    let hosts = config.hosts;
    let outcome = tokio::task::spawn_blocking({
        let (root, tag, progress) = (root.clone(), tag.clone(), progress.clone());
        move || {
            wait_for_go_reporting(
                &root,
                &tag,
                hosts,
                BARRIER_POLL,
                progress_every,
                &progress,
                go_after,
            )
        }
    })
    .await??;
    let WaitOutcome::Participant { go_seen_at, participants } = outcome else {
        return Err(LateHost { hosts }.into());
    };
    progress.brief_line(|run| started_line(run, participants.len(), hosts));

    let until = go_seen_at + config.duration;
    let mut slots: Vec<String> = (0..workers).map(|i| i.to_string()).collect();
    if participants.first() == Some(&tag) {
        if config.mode == Mode::Repository && config.gc.is_some() {
            slots.push(GC_SLOT.to_owned());
        }
        if config.readonly.is_some() {
            slots.push(TOGGLER_SLOT.to_owned());
        }
        if config.fork.is_some_and(|fork| !fork.during_gc) {
            slots.push(FORK_SLOT.to_owned());
        }
    }
    let sampler = config
        .mirror_sample_every
        .map(|every| Sampler::start(root, &tag, every))
        .transpose()?;

    let log_path = root.supervisor_log(&tag);
    let log = LogWriter::create(&log_path)
        .with_context(|| format!("creating {}", log_path.display()))?;
    let mut pool = Pool {
        root,
        tag: &tag,
        exe,
        until_ms: u64::try_from(unix_ms(until))?,
        log,
        children: Vec::new(),
        incarnations_spawned: 0,
        external_kills: 0,
        progress: (!progress_every.is_zero()).then(LogProgress::default),
    };
    for slot in slots {
        pool.spawn(slot, 0)?;
    }

    let mut rng: StdRng = rand::make_rng();
    let mut next_kill = Instant::now() + config.external_kill_every.unwrap_or_default();
    let mut next_control = Instant::now() + CONTROL_EVERY;
    let mut next_progress = Instant::now() + progress_every;
    let mut next_brief = Instant::now() + BRIEF_EVERY;
    let mut stopped = false;
    while SystemTime::now() < until {
        pool.respawn_exited()?;
        let now = Instant::now();
        if now >= next_progress
            && let Some(line) = pool.progress_line(go_seen_at, config.duration)
        {
            progress.detail(&line);
            next_progress = now + progress_every;
        }
        if now >= next_brief {
            let elapsed = go_seen_at.elapsed().unwrap_or_default();
            progress.time_left(elapsed, config.duration);
            next_brief = now + BRIEF_EVERY;
        }
        if let Some(every) = config.external_kill_every
            && now >= next_kill
        {
            pool.kill_one(&mut rng)?;
            next_kill = now + every;
        }
        if now >= next_control {
            next_control = now + CONTROL_EVERY;
            match read_control(root) {
                Ok(Control::Stop) => {
                    stopped = true;
                    break;
                }
                Ok(_) => {}
                Err(err) => tracing::warn!("{err:#}"),
            }
        }
        tokio::time::sleep(TICK).await;
    }

    let stopped_at = SystemTime::now();
    if pool.progress.is_some() {
        let why = if stopped { "stop received" } else { "deadline reached" };
        progress.detail(&format!(
            "{why}, waiting for {} workers to exit…",
            pool.children.len()
        ));
    }
    // A worker finishes its attempt first, which may include a pause.
    let deadline = stopped_at + config.faults.pause + WIND_DOWN_SLACK;
    let stragglers = loop {
        pool.reap_exited()?;
        if pool.children.is_empty() {
            break 0;
        }
        if SystemTime::now() >= deadline {
            break pool.kill_stragglers();
        }
        tokio::time::sleep(TICK).await;
    };
    // After every worker, so it sees their last publications. A sampler
    // that couldn't log is reported, and the host still finishes.
    let sampler_error = sampler.and_then(|sampler| sampler.stop().err()).map(|err| {
        tracing::warn!("the mirror sampler stopped logging: {err:#}");
        format!("{err:#}")
    });

    let done = HostDone {
        tag: tag.clone(),
        go_seen_at_ms: unix_ms(go_seen_at),
        stopped_at_ms: unix_ms(stopped_at),
        workers,
        incarnations_spawned: pool.incarnations_spawned,
        external_kills: pool.external_kills,
        stragglers,
        sampler_error,
    };
    let path = root.host_done(&tag);
    write_synced(&path, &serde_json::to_vec_pretty(&done)?)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(done)
}

fn unix_ms(t: SystemTime) -> i64 {
    DateTime::<Utc>::from(t).timestamp_millis()
}

#[derive(Debug)]
struct Worker {
    slot: String,
    incarnation: u32,
    child: Child,
}

/// This host's worker processes. Dropping it kills any still running, so an
/// error leaves no orphans.
#[derive(Debug)]
struct Pool<'a> {
    root: &'a RunRoot,
    tag: &'a str,
    exe: PathBuf,
    until_ms: u64,
    log: LogWriter,
    children: Vec<Worker>,
    incarnations_spawned: u64,
    external_kills: u64,
    /// `None` without progress output.
    progress: Option<LogProgress>,
}

impl Pool<'_> {
    fn spawn(&mut self, slot: String, incarnation: u32) -> anyhow::Result<()> {
        let child = Command::new(&self.exe)
            .arg("worker")
            .arg("--root")
            .arg(self.root.root())
            .args(["--tag", self.tag, "--slot", &slot])
            .args(["--incarnation", &incarnation.to_string()])
            .args(["--until-ms", &self.until_ms.to_string()])
            .stdin(Stdio::null())
            .spawn()
            .with_context(|| format!("spawning worker {slot}.{incarnation}"))?;
        let pid = child.id();
        if let Some(progress) = &mut self.progress {
            progress.track(self.root.worker_log(self.tag, &slot, incarnation));
        }
        self.children.push(Worker { slot: slot.clone(), incarnation, child });
        self.incarnations_spawned += 1;
        self.log.append(&Record::Spawn { t_ms: now_ms(), slot, incarnation, pid })?;
        Ok(())
    }

    /// Removes and returns the workers that have exited.
    fn take_exited(&mut self) -> anyhow::Result<Vec<(Worker, ExitStatus)>> {
        let mut exited = Vec::new();
        // Descending, so `swap_remove` only moves workers already checked.
        for i in (0..self.children.len()).rev() {
            if let Some(status) = self.children[i].child.try_wait()? {
                let worker = self.children.swap_remove(i);
                self.exited(&worker);
                exited.push((worker, status));
            }
        }
        Ok(exited)
    }

    fn exited(&mut self, worker: &Worker) {
        if let Some(progress) = &mut self.progress {
            progress.exited(&self.root.worker_log(
                self.tag,
                &worker.slot,
                worker.incarnation,
            ));
        }
    }

    /// `None` without progress output.
    fn progress_line(
        &mut self,
        go_seen_at: SystemTime,
        duration: Duration,
    ) -> Option<String> {
        let counts = self.progress.as_mut()?.poll();
        let elapsed = go_seen_at.elapsed().unwrap_or_default();
        Some(format!(
            "[{}] {}s/{}s  workers {} live, {} incarnations, {} external kills | \
             {} intents, {} acks, {} conflicts, {} errors, {} failed before intent | \
             faults: {} kills, {} pauses",
            self.tag,
            elapsed.as_secs(),
            duration.as_secs(),
            self.children.len(),
            self.incarnations_spawned,
            self.external_kills,
            counts.intents,
            counts.acks,
            counts.conflicts,
            counts.errors,
            counts.failures,
            counts.kills,
            counts.pauses,
        ))
    }

    fn respawn_exited(&mut self) -> anyhow::Result<()> {
        for (worker, status) in self.take_exited()? {
            warn_on_failure(&worker, status);
            self.spawn(worker.slot, worker.incarnation + 1)?;
        }
        Ok(())
    }

    fn reap_exited(&mut self) -> anyhow::Result<()> {
        for (worker, status) in self.take_exited()? {
            warn_on_failure(&worker, status);
        }
        Ok(())
    }

    /// SIGKILLs a random running worker, never the status toggler, and
    /// respawns it.
    fn kill_one(&mut self, rng: &mut StdRng) -> anyhow::Result<()> {
        let candidates =
            external_kill_candidates(self.children.iter().map(|w| w.slot.as_str()));
        if candidates.is_empty() {
            return Ok(());
        }
        let i = candidates[rng.random_range(0..candidates.len())];
        let child = &mut self.children[i].child;
        // One that exited on its own since the last check isn't ours to count.
        let running = child.try_wait()?.is_none();
        if running {
            child.kill()?;
            child.wait()?;
        }
        let worker = self.children.swap_remove(i);
        self.exited(&worker);
        let Worker { slot, incarnation, .. } = worker;
        if running {
            self.external_kills += 1;
            self.log.append(&Record::ExternalKill {
                t_ms: now_ms(),
                slot: slot.clone(),
                incarnation,
            })?;
        }
        self.spawn(slot, incarnation + 1)
    }

    /// Kills every worker still running and returns how many there were.
    fn kill_stragglers(&mut self) -> u32 {
        let stragglers = self.children.len();
        for worker in &self.children {
            tracing::warn!(
                "worker {}.{} still running at the deadline, killing it",
                worker.slot,
                worker.incarnation
            );
        }
        self.kill_all();
        u32::try_from(stragglers).unwrap_or(u32::MAX)
    }

    fn kill_all(&mut self) {
        for mut worker in self.children.drain(..) {
            // Best effort: nothing more can be done about a worker that won't
            // die or be reaped.
            let _ = worker.child.kill();
            let _ = worker.child.wait();
        }
    }
}

impl Drop for Pool<'_> {
    fn drop(&mut self) {
        self.kill_all();
    }
}

/// The indices of the slots an external kill may pick: all but the status
/// toggler's, which must finish its holds to judge their publications.
fn external_kill_candidates<'a>(slots: impl IntoIterator<Item = &'a str>) -> Vec<usize> {
    slots
        .into_iter()
        .enumerate()
        .filter_map(|(i, slot)| (slot != TOGGLER_SLOT).then_some(i))
        .collect()
}

/// What this host's worker logs record so far, for the progress lines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProgressCounts {
    pub intents: u64,
    pub acks: u64,
    pub conflicts: u64,
    pub errors: u64,
    pub failures: u64,
    pub kills: u64,
    pub pauses: u64,
}

impl ProgressCounts {
    fn add(&mut self, record: &Record) {
        match record {
            Record::Intent { .. } => self.intents += 1,
            Record::Outcome { result, .. } if result.landed().is_some() => self.acks += 1,
            Record::Outcome { result: OpResult::Conflict, .. } => self.conflicts += 1,
            Record::Outcome { .. } => self.errors += 1,
            Record::Failure { .. } => self.failures += 1,
            Record::Fault { action: FaultAction::Kill, .. } => self.kills += 1,
            Record::Fault { action: FaultAction::Pause, .. } => self.pauses += 1,
            _ => {}
        }
    }
}

/// Reads worker logs incrementally: each poll reads only what was appended
/// since the previous one. A log is dropped after the first poll that
/// follows its worker's exit, which reads the rest of it.
#[derive(Debug, Default)]
pub struct LogProgress {
    tails: BTreeMap<PathBuf, Tail>,
    counts: ProgressCounts,
}

#[derive(Debug)]
struct Tail {
    offset: u64,
    exited: bool,
}

impl LogProgress {
    pub fn track(&mut self, log: PathBuf) {
        self.tails.insert(log, Tail { offset: 0, exited: false });
    }

    pub fn exited(&mut self, log: &Path) {
        if let Some(tail) = self.tails.get_mut(log) {
            tail.exited = true;
        }
    }

    pub fn poll(&mut self) -> &ProgressCounts {
        let counts = &mut self.counts;
        self.tails.retain(|path, tail| {
            match read_appended(path, tail.offset) {
                Ok(appended) => {
                    if appended.malformed_lines > 0 {
                        tracing::warn!(
                            "{}: {} malformed lines",
                            path.display(),
                            appended.malformed_lines
                        );
                    }
                    appended.records.iter().for_each(|record| counts.add(record));
                    tail.offset = appended.offset;
                }
                Err(err) => tracing::warn!("reading {}: {err}", path.display()),
            }
            !tail.exited
        });
        &self.counts
    }
}

/// Fault kills are SIGKILLs, so only other signals and error exits are news.
fn warn_on_failure(worker: &Worker, status: ExitStatus) {
    if !status.success() && status.signal() != Some(libc::SIGKILL) {
        tracing::warn!(
            "worker {}.{} exited with {status}",
            worker.slot,
            worker.incarnation
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write as _};

    use super::*;

    #[test]
    fn the_started_line_counts_the_hosts_only_if_there_are_several() {
        let run = RunLabel { number: 2, of: 2 };
        assert_eq!(started_line(&run, 1, 1), "Run 2 of 2 started.");
        assert_eq!(started_line(&run, 5, 5), "Run 2 of 2 started with 5 hosts.");
        assert_eq!(started_line(&run, 6, 5), "Run 2 of 2 started with 6 hosts.");
        assert_eq!(
            started_line(&run, 3, 5),
            "Run 2 of 2 started with 3 of 5 hosts; the others came too late."
        );
    }

    #[test]
    fn data_volume_estimate_is_per_op_times_ops() {
        let mut config = RunConfig::defaults(Mode::Storage, 2);
        config.workers_per_host_default = 3;
        config.duration = Duration::from_secs(10);
        config.payload_bytes = 1000;
        let ops = 1000 * 2 * 3 * 10;
        assert_eq!(data_volume_estimate(&config), (1000 + 4096) * (ops + 1));

        config.mode = Mode::Repository;
        config.chunk_bytes = 1024;
        assert_eq!(data_volume_estimate(&config), (1024 + 8192) * (ops + 1));

        config.duration = Duration::MAX;
        assert_eq!(data_volume_estimate(&config), u64::MAX);
    }

    #[test]
    fn log_progress_counts_appended_records_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.0.0.log");
        let mut progress = LogProgress::default();
        progress.track(path.clone());
        assert_eq!(*progress.poll(), ProgressCounts::default());

        let log = LogWriter::create(&path).unwrap();
        let id = uuid::Uuid::new_v4();
        let op = crate::log::Op::Gc { older_than_ms: None };
        let intent = Record::Intent { t_ms: 1, id, op };
        let outcome = |result| Record::Outcome { t_ms: 2, id, result };
        log.append(&intent).unwrap();
        log.append(&outcome(OpResult::Ack { snapshot: None })).unwrap();
        let fault = |action| Record::Fault {
            t_ms: 3,
            action,
            point: crate::config::PointName::BeforeRename,
        };
        log.append(&fault(FaultAction::Pause)).unwrap();
        let expected = ProgressCounts {
            intents: 1,
            acks: 1,
            pauses: 1,
            ..ProgressCounts::default()
        };
        assert_eq!(*progress.poll(), expected);
        assert_eq!(*progress.poll(), expected);

        let line = serde_json::to_vec(&outcome(OpResult::Conflict)).unwrap();
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&line).unwrap();
        assert_eq!(*progress.poll(), expected);
        file.write_all(b"\n").unwrap();
        log.append(&fault(FaultAction::Kill)).unwrap();
        log.append(&outcome(OpResult::Error { message: "e".to_owned() })).unwrap();
        log.append(&Record::Failure { t_ms: 4, message: "f".to_owned() }).unwrap();
        let expected =
            ProgressCounts { conflicts: 1, kills: 1, errors: 1, failures: 1, ..expected };
        assert_eq!(*progress.poll(), expected);

        progress.exited(&path);
        log.append(&intent).unwrap();
        let expected = ProgressCounts { intents: 2, ..expected };
        assert_eq!(*progress.poll(), expected);
        assert!(progress.tails.is_empty());
        log.append(&intent).unwrap();
        assert_eq!(*progress.poll(), expected);
    }

    #[test]
    fn external_kills_spare_the_status_toggler() {
        let slots = ["0", "1", GC_SLOT, TOGGLER_SLOT, FORK_SLOT];
        assert_eq!(external_kill_candidates(slots), [0, 1, 2, 4]);
        assert!(external_kill_candidates([TOGGLER_SLOT]).is_empty());
        assert!(external_kill_candidates([]).is_empty());
    }

    #[test]
    fn late_host_names_the_expected_count() {
        assert_eq!(
            LateHost { hosts: 3 }.to_string(),
            "this host registered after the run started (control is go/stop); start \
             every host before 3 have registered"
        );
    }
}
