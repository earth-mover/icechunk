//! The conversion scenario (`fs-stress convert-scenario`): on one storage
//! root, in rounds, a repository in the standard layout is converted to the
//! filesystem layout, stressed, converted back and written in the standard
//! layout again, while the converter is killed at random steps of both
//! conversions and rerun. Each round is a phase with its own run root,
//! `phase-<k>/`; the phases share `storage/`, and every check reads the logs
//! of the earlier phases too, so that every acknowledged change must still
//! be there (C1). The conversion rules are in
//! [`oracle::conversion`](crate::oracle::conversion).
//!
//! The converter runs in a child process (`fs-stress conversion`), so that a
//! kill is a `SIGKILL` at a conversion step, as a crash there would be. Only
//! the conversions run during a conversion, as the conversions require.

use std::{
    collections::{BTreeMap, HashSet},
    fs, io,
    os::unix::process::ExitStatusExt as _,
    path::PathBuf,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, bail, ensure};
use icechunk::{
    Repository, format::OVERWRITTEN_FILES_PATH, storage::new_local_filesystem_storage,
};
use icechunk_fs::{
    ConversionOutcome, FilesystemStorage,
    fault_injection::{
        ConversionStep, Point, ProtocolHooks, Sabotage as ProtocolSabotage,
        ToStandardStep,
    },
};
use rand::{RngExt as _, rngs::StdRng};
use serde::{Deserialize, Serialize};

use crate::{
    config::{FaultConfig, PointName, RunConfig, Sabotage, WorkerId, write_synced},
    control::{Control, write_control},
    env_info::{default_tag, host_info},
    faults::{FaultHooks, Trigger},
    fork::{announce_backup, erased, log_fork, write_fork},
    layout::{RunRoot, SCENARIO_TAG, ScenarioRoot},
    log::{
        Direction, FaultAction, LogName, LogWriter, Op, OpResult, Record, now_ms,
        read_all_logs, read_log, start_worker_log,
    },
    oracle::{
        History, Verdict, Violation, Warning,
        conversion::{
            conversion_defect, deleted_history, end_state_defect,
            left_by_stale_root_listing, standard_overwritten, writer_defect,
        },
        fork::check_forks,
        mirror::check_init_version,
        repo::{check_repo, count_operations},
    },
    repo_mode::RepoWorker,
    report::{Participants, Report},
    storage_mode::{REPO_KEY, classify_storage, payload_etag},
    supervisor::{self, log_init_version},
    verify::{DEFAULT_DONE_TIMEOUT, EarlierPhases, verify_phase},
};

/// `scenario.json`: the scenario's settings, which its converters read too.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioConfig {
    pub rounds: u32,
    /// The stress phase of every round; `None` for none.
    #[serde(deserialize_with = "Option::deserialize")]
    pub stress: Option<RunConfig>,
    pub workers: u32,
    /// For each run of a conversion, the probability that its converter is
    /// killed at one of its steps, chosen at random.
    pub kill_probability: f64,
    /// Commits in the standard layout before each conversion to the
    /// filesystem layout.
    pub standard_commits: u32,
    pub fsync: bool,
    /// Set only by `selftest`, for the converters.
    #[serde(deserialize_with = "Option::deserialize")]
    pub sabotage: Option<Sabotage>,
    /// Kills the first conversion from the standard layout after its
    /// directory rename, then replaces `$ROOT/repo` as Icechunk 2.3 writes a
    /// status change, before the rerun: a standard write during the
    /// interrupted conversion, which must stop the rerun.
    pub plant_standard_write: bool,
    /// Right after a conversion to the standard layout read the current
    /// version, a commit lands that publishes nothing: what a stale listing
    /// that hides it from the conversion shows. The conversion must convert
    /// it too.
    pub late_commit: bool,
    /// The first run of each conversion is killed: from the standard layout
    /// at a random step, to it right after its change or its publication,
    /// with a writer tried during the interruption (C7). The scenario fails
    /// if no conversion from the standard layout was killed or C7 never ran.
    pub guaranteed_kills: bool,
}

impl ScenarioConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.rounds >= 1, "rounds must be at least 1");
        ensure!(
            (0.0..=1.0).contains(&self.kill_probability),
            "the kill probability must be in [0, 1]"
        );
        if let Some(stress) = &self.stress {
            stress.validate()?;
            ensure!(stress.hosts == 1, "the stress phases run on one host");
            ensure!(
                stress.mode == crate::config::Mode::Repository,
                "the stress phases run in repository mode"
            );
            ensure!(self.workers >= 1, "a stress phase needs at least 1 worker");
        }
        Ok(())
    }

    /// The configuration of the stress phases, or the one the scenario's
    /// own writes and checks use without them.
    pub fn run_config(&self) -> RunConfig {
        self.stress.clone().unwrap_or_else(|| {
            let mut config = RunConfig::defaults(crate::config::Mode::Repository, 1);
            config.fsync = self.fsync;
            config
        })
    }

    fn load(root: &ScenarioRoot) -> anyhow::Result<Self> {
        let path = root.scenario_json();
        let bytes =
            fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        let config: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }
}

/// The converter's slot in a phase's logs.
fn converter_slot(direction: Direction) -> &'static str {
    match direction {
        Direction::FromStandard => "from-standard",
        Direction::ToStandard => "to-standard",
    }
}

/// The slots of the scenario's other logs in a phase.
const STANDARD_SLOT: &str = "standard";
const WRITER_SLOT: &str = "writer";
const PLANT_SLOT: &str = "plant";

/// At most this many runs of one conversion, and at most this many of them
/// killed.
const MAX_RUNS: u32 = 12;
const MAX_KILLS: u32 = 4;
/// On a network filesystem, the reruns of a conversion to the standard
/// layout after runs that missed `repo.d` ([`Warning::StaleRootListing`]),
/// on top of [`MAX_RUNS`]: the first after [`FIRST_STALE_ROOT_DELAY`], each
/// later one after twice the delay before, at most [`MAX_STALE_ROOT_DELAY`].
/// Together they wait 79.5 s, longer than NFS caches directory attributes
/// (`acdirmax`, 60 s by default).
const STALE_ROOT_RERUNS: u32 = 9;
const FIRST_STALE_ROOT_DELAY: Duration =
    Duration::from_millis(if cfg!(test) { 1 } else { 500 });
const MAX_STALE_ROOT_DELAY: Duration =
    Duration::from_millis(if cfg!(test) { 32 } else { 16_000 });

/// The delay before the stale-root rerun numbered `k`, from 0.
fn stale_root_delay(k: u32) -> Duration {
    FIRST_STALE_ROOT_DELAY.saturating_mul(1 << k.min(16)).min(MAX_STALE_ROOT_DELAY)
}

fn steps(direction: Direction) -> &'static [PointName] {
    match direction {
        Direction::FromStandard => &[
            PointName::FromStandardBackup,
            PointName::FromStandardBuild,
            PointName::FromStandardRenameDir,
            PointName::FromStandardPublish,
        ],
        Direction::ToStandard => &[
            PointName::ToStandardForkCheck,
            PointName::ToStandardChange,
            PointName::ToStandardPublish,
            PointName::ToStandardRenameDir,
            PointName::ToStandardDeleteDir,
        ],
    }
}

/// Where the runs of one conversion are killed: at the guaranteed step,
/// until a run is killed there (a run can end before reaching it), then at
/// random steps with the scenario's probability, [`MAX_KILLS`] times at most.
#[derive(Debug)]
struct Kills {
    direction: Direction,
    guaranteed: Option<PointName>,
    kills: u32,
}

impl Kills {
    /// With `guaranteed`: from the standard layout a random step; to it
    /// right after its change or its publication, where C7 runs.
    fn new(direction: Direction, guaranteed: bool, rng: &mut StdRng) -> Self {
        let guaranteed = guaranteed.then(|| {
            let steps = match direction {
                Direction::FromStandard => steps(direction),
                Direction::ToStandard => {
                    &[PointName::ToStandardChange, PointName::ToStandardPublish][..]
                }
            };
            steps[rng.random_range(0..steps.len())]
        });
        Self { direction, guaranteed, kills: 0 }
    }

    fn choose(&mut self, rng: &mut StdRng, probability: f64) -> Option<PointName> {
        if self.guaranteed.is_some() {
            return self.guaranteed;
        }
        (self.kills < MAX_KILLS && rng.random_bool(probability)).then(|| {
            let steps = steps(self.direction);
            steps[rng.random_range(0..steps.len())]
        })
    }

    /// A run was killed at `step`.
    fn killed(&mut self, step: PointName) {
        self.kills += 1;
        if self.guaranteed == Some(step) {
            self.guaranteed = None;
        }
    }
}

/// Hooks for the late commit: its change returns without publishing, as
/// the change of a writer whose publication a stale listing hides does.
#[derive(Debug)]
struct NoPublication;

impl ProtocolHooks for NoPublication {
    fn sabotaged(&self, sabotage: ProtocolSabotage) -> bool {
        sabotage == ProtocolSabotage::CommitWithoutPublish
    }
}

/// One run of a conversion in `direction`, the `attempt`th of the `round`,
/// with `wrap` turning its fault hooks into the storage's: logs an intent
/// and the outcome to `logs/scenario.<direction>.<attempt>.log`. With
/// `late_commit`, the trigger at the conversion to the standard layout's
/// fork-check step commits once.
async fn convert_with(
    root: &ScenarioRoot,
    config: &ScenarioConfig,
    round: u32,
    direction: Direction,
    attempt: u32,
    faults: &FaultConfig,
    wrap: impl FnOnce(FaultHooks) -> Arc<dyn ProtocolHooks>,
) -> anyhow::Result<Run> {
    let phase = root.phase(round);
    let who = WorkerId {
        tag: SCENARIO_TAG.to_owned(),
        slot: converter_slot(direction).to_owned(),
        incarnation: attempt,
    };
    let log = Arc::new(start_worker_log(&phase, &who)?);
    let mut hooks = FaultHooks::new(faults, config.sabotage, Arc::clone(&log));
    if config.late_commit && direction == Direction::ToStandard {
        let point =
            Point::Conversion(ConversionStep::ToStandard(ToStandardStep::ForkCheck));
        let trigger =
            Arc::new(Trigger::new(point, late_commit(&phase, config, &log, &who)));
        trigger.arm();
        hooks = hooks.with_trigger(trigger);
    }
    let storage = FilesystemStorage::new(&root.storage())?
        .with_fsync(config.fsync)
        .with_protocol_hooks(wrap(hooks));
    let id = uuid::Uuid::new_v4();
    log.append(&Record::Intent { t_ms: now_ms(), id, op: Op::Convert { direction } })?;
    let converted = match direction {
        Direction::FromStandard => storage.convert_to_filesystem_layout().await,
        Direction::ToStandard => storage.convert_to_standard_layout().await,
    };
    let (result, run) = match converted {
        Ok(ConversionOutcome::Converted) => (
            OpResult::Ack { snapshot: None },
            Run::Finished(OpResult::Ack { snapshot: None }),
        ),
        Ok(ConversionOutcome::NothingToConvert) => {
            log.append(&Record::NothingToConvert { t_ms: now_ms() })?;
            (OpResult::Ack { snapshot: None }, Run::NothingToConvert)
        }
        Err(err) => {
            let result = classify_storage(err.kind());
            (result.clone(), Run::Finished(result))
        }
    };
    log.append(&Record::Outcome { t_ms: now_ms(), id, result })?;
    Ok(run)
}

/// The late commit, run by a trigger inside the conversion: on a thread of
/// its own, with a runtime of its own, as the conversion's thread blocks.
/// It commits to `main` through a storage whose changes don't publish, and
/// logs as the converter does.
fn late_commit(
    phase: &RunRoot,
    config: &ScenarioConfig,
    log: &Arc<LogWriter>,
    who: &WorkerId,
) -> impl Fn() + Send + Sync + 'static {
    let (storage, fsync, log, who) =
        (phase.storage(), config.fsync, Arc::clone(log), who.clone());
    let chunk_bytes = config.run_config().chunk_bytes;
    move || {
        let (storage, log, who) = (storage.clone(), Arc::clone(&log), who.clone());
        let committed = std::thread::spawn(move || -> anyhow::Result<OpResult> {
            let runtime =
                tokio::runtime::Builder::new_current_thread().enable_all().build()?;
            runtime.block_on(async {
                let storage = FilesystemStorage::new(&storage)?
                    .with_fsync(fsync)
                    .with_protocol_hooks(Arc::new(NoPublication));
                let repo = Repository::open(Arc::new(storage)).execute().await.map_err(
                    |err| anyhow::anyhow!("opening the repository: {}", err.kind()),
                )?;
                let mut worker = RepoWorker::new(repo, log, &who, chunk_bytes)?;
                worker.logged_main_commit().await
            })
        })
        .join();
        match committed {
            Ok(Ok(OpResult::Ack { .. })) => {}
            Ok(Ok(other)) => {
                tracing::warn!("the late commit wasn't acknowledged: {other:?}")
            }
            Ok(Err(err)) => tracing::warn!("the late commit failed: {err:#}"),
            Err(_) => tracing::warn!("the late commit panicked"),
        }
    }
}

/// What the hidden `fs-stress conversion` subcommand runs: one run of a
/// conversion, killed by `SIGKILL` at `kill_at` if it gets there.
pub async fn converter(
    root: &ScenarioRoot,
    round: u32,
    direction: Direction,
    attempt: u32,
    kill_at: Option<PointName>,
) -> anyhow::Result<()> {
    let config = ScenarioConfig::load(root)?;
    let faults = FaultConfig {
        points: kill_at.into_iter().collect(),
        kill_probability: 1.0,
        pause_probability: 0.0,
        pause: Duration::ZERO,
    };
    convert_with(root, &config, round, direction, attempt, &faults, |hooks| {
        Arc::new(hooks)
    })
    .await?;
    Ok(())
}

/// How the scenario runs its converters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Converter {
    /// `fs-stress conversion` child processes of this binary.
    Process(PathBuf),
    /// In this process, a kill emulated by unwinding the conversion's thread
    /// at the step, which leaves the files as a crash there does.
    #[cfg(test)]
    InProcess,
    /// [`Converter::InProcess`], whose runs of the conversion to the standard
    /// layout numbered in `attempts` miss `repo.d`, as under a stale listing
    /// of the storage root: it is moved out of the storage root meanwhile.
    #[cfg(test)]
    MissingRepoD { attempts: std::ops::RangeInclusive<u32> },
}

/// What a run of a conversion did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Run {
    Killed,
    /// `NothingToConvert` is [`Run::NothingToConvert`].
    Finished(OpResult),
    /// It returned `NothingToConvert`, logged as an `Ack`.
    NothingToConvert,
}

impl Converter {
    async fn run(
        &self,
        root: &ScenarioRoot,
        round: u32,
        direction: Direction,
        attempt: u32,
        kill_at: Option<PointName>,
    ) -> anyhow::Result<Run> {
        match self {
            Self::Process(exe) => {
                let mut command = Command::new(exe);
                command
                    .arg("conversion")
                    .arg("--root")
                    .arg(root.root())
                    .args([
                        "--round",
                        &round.to_string(),
                        "--attempt",
                        &attempt.to_string(),
                    ])
                    .args(["--direction", &serde_name(direction)?])
                    .stdin(Stdio::null());
                if let Some(step) = kill_at {
                    command.args(["--kill-at", &serde_name(step)?]);
                }
                let mut child = command.spawn().context("spawning a converter")?;
                let status = tokio::task::spawn_blocking(move || child.wait()).await??;
                if status.signal() == Some(libc::SIGKILL) {
                    return Ok(Run::Killed);
                }
                ensure!(status.success(), "the converter exited with {status}");
                let slot = converter_slot(direction);
                let log = root.phase(round).worker_log(SCENARIO_TAG, slot, attempt);
                let records = read_log(&log)?;
                let outcome = conversion_outcome(&records)
                    .context("the converter logged no outcome")?;
                let nothing_to_convert = records
                    .iter()
                    .any(|record| matches!(record, Record::NothingToConvert { .. }));
                Ok(match outcome {
                    OpResult::Ack { .. } if nothing_to_convert => Run::NothingToConvert,
                    outcome => Run::Finished(outcome),
                })
            }
            #[cfg(test)]
            Self::InProcess => {
                tests::run_in_process(root, round, direction, attempt, kill_at).await
            }
            #[cfg(test)]
            Self::MissingRepoD { attempts } => {
                let missing =
                    direction == Direction::ToStandard && attempts.contains(&attempt);
                let repo_d = root.storage().join("repo.d");
                let moved = root.root().join("repo.d.missed-by-a-stale-listing");
                if missing {
                    fs::rename(&repo_d, &moved)?;
                }
                let run =
                    tests::run_in_process(root, round, direction, attempt, kill_at).await;
                if missing {
                    fs::rename(&moved, &repo_d)?;
                }
                run
            }
        }
    }
}

impl Converter {
    /// Runs a stress phase's workers on `phase`, whose `run.json` is written:
    /// `fs-stress worker` processes of the supervisor, or, in tests, tasks
    /// that resolve the run root from its path as those processes do.
    async fn stress_phase(
        &self,
        phase: &RunRoot,
        tag: String,
        workers: u32,
        progress_every: Duration,
    ) -> anyhow::Result<()> {
        match self {
            Self::Process(_) => {
                supervisor::workers(phase, tag, workers, progress_every).await?;
            }
            #[cfg(test)]
            Self::InProcess | Self::MissingRepoD { .. } => {
                tests::in_process_workers(phase, &tag, workers).await?
            }
        }
        Ok(())
    }
}

/// What is wrong with the late commit the converters to the standard layout
/// logged in `logs`: it must have been acknowledged, or the case shows
/// nothing.
fn late_commit_defect(logs: &BTreeMap<LogName, Vec<Record>>) -> Option<String> {
    let records: Vec<&Record> = logs
        .iter()
        .filter(|(name, _)| {
            name.tag == SCENARIO_TAG && name.slot == converter_slot(Direction::ToStandard)
        })
        .flat_map(|(_, records)| records)
        .collect();
    let ids: Vec<uuid::Uuid> = records
        .iter()
        .filter_map(|record| match record {
            Record::Intent { id, op: Op::MainCommit, .. } => Some(*id),
            _ => None,
        })
        .collect();
    let outcomes: Vec<&OpResult> = records
        .iter()
        .filter_map(|record| match record {
            Record::Outcome { id, result, .. } if ids.contains(id) => Some(result),
            _ => None,
        })
        .collect();
    if outcomes.iter().any(|result| matches!(result, OpResult::Ack { .. })) {
        None
    } else if ids.is_empty() {
        Some("the late commit wasn't attempted".to_owned())
    } else {
        Some(format!("the late commit wasn't acknowledged: {outcomes:?}"))
    }
}

/// The outcome of the conversion a converter's log records: that of its
/// `Op::Convert` intent, not of the late commit logged in between.
fn conversion_outcome(records: &[Record]) -> Option<OpResult> {
    let id = records.iter().find_map(|record| match record {
        Record::Intent { id, op: Op::Convert { .. }, .. } => Some(*id),
        _ => None,
    })?;
    records.iter().find_map(|record| match record {
        Record::Outcome { id: of, result, .. } if *of == id => Some(result.clone()),
        _ => None,
    })
}

/// How `serde` names `value`, as the CLI takes it.
fn serde_name(value: impl Serialize) -> anyhow::Result<String> {
    match serde_json::to_value(value)? {
        serde_json::Value::String(name) => Ok(name),
        other => bail!("{other} is no name"),
    }
}

/// The scenario as it runs: what it checked and the logs of the phases done.
struct Scenario<'a> {
    root: &'a ScenarioRoot,
    config: &'a ScenarioConfig,
    converter: &'a Converter,
    network_fs: bool,
    verdict: Verdict,
    notes: Vec<String>,
    /// The logs of the finished phases, oldest first.
    earlier: Vec<BTreeMap<LogName, Vec<Record>>>,
    rng: StdRng,
    /// The steps runs were killed at, by round and direction.
    killed: Vec<(u32, Direction, PointName)>,
    /// The writers tried during an interrupted conversion (C7).
    c7_writers: u32,
    /// The kill steps of the next runs, before random ones.
    kill_plan: std::collections::VecDeque<Option<PointName>>,
}

/// Runs the conversion scenario in `root`, which must be missing or empty,
/// and writes and returns its report, which also holds what each phase's
/// `verify` found. `Err` for setup and I/O errors.
pub async fn convert_scenario(
    root: &ScenarioRoot,
    config: &ScenarioConfig,
    converter: &Converter,
    progress_every: Duration,
) -> anyhow::Result<Report> {
    config.validate()?;
    create_root(root, config)?;
    let mut scenario = Scenario {
        root,
        config,
        converter,
        network_fs: host_info(SCENARIO_TAG, &root.storage()).network_fs,
        verdict: Verdict::default(),
        notes: Vec::new(),
        earlier: Vec::new(),
        rng: rand::make_rng(),
        killed: Vec::new(),
        c7_writers: 0,
        kill_plan: std::collections::VecDeque::new(),
    };
    if scenario.network_fs {
        scenario.notes.push(
            "network filesystem: reruns that fail as under stale listings are warnings"
                .to_owned(),
        );
    }
    let first = root.phase(1);
    first.create_dirs()?;
    eprintln!("creating a repository in the standard layout");
    create_standard(&first).await?;
    scenario.standard_commits(&first).await?;
    let mut finished = true;
    for round in 1..=config.rounds {
        if round > 1 {
            root.phase(round).create_dirs()?;
        }
        if !scenario.round(round, progress_every).await? {
            finished = false;
            break;
        }
    }
    if finished {
        eprintln!("checking every acknowledged change in the standard layout");
        scenario.check_standard_side(None).await?;
    }
    scenario.finish(finished)
}

fn create_root(root: &ScenarioRoot, config: &ScenarioConfig) -> anyhow::Result<()> {
    let path = root.root();
    match fs::read_dir(path) {
        Ok(mut entries) => {
            ensure!(entries.next().is_none(), "{} is not empty", path.display())
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path)
                .with_context(|| format!("creating {}", path.display()))?;
        }
        Err(err) => {
            return Err(err).with_context(|| format!("reading {}", path.display()));
        }
    }
    fs::create_dir_all(root.storage())?;
    write_synced(&root.scenario_json(), &serde_json::to_vec_pretty(config)?)
        .with_context(|| format!("writing {}", root.scenario_json().display()))
}

async fn create_standard(phase: &RunRoot) -> anyhow::Result<()> {
    let storage = new_local_filesystem_storage(&phase.storage()).await?;
    Repository::create(storage)
        .execute()
        .await
        .map_err(|err| anyhow::anyhow!("creating the repository: {}", err.kind()))?;
    Ok(())
}

/// The first incarnation of the scenario's `slot` without a log in `phase`.
fn next_incarnation(phase: &RunRoot, slot: &str) -> u32 {
    (0..).find(|&n| !phase.worker_log(SCENARIO_TAG, slot, n).exists()).unwrap_or(0)
}

impl Scenario<'_> {
    /// One round; whether the scenario can go on.
    async fn round(
        &mut self,
        round: u32,
        progress_every: Duration,
    ) -> anyhow::Result<bool> {
        let phase = self.root.phase(round);
        eprintln!("round {round}: converting to the filesystem layout");
        let t_before_ms = now_ms();
        if !self.convert(round, Direction::FromStandard).await? {
            return Ok(false);
        }
        let t_after_ms = now_ms();
        let stress = self.config.stress.clone();
        log_init_version(&phase, &self.config.run_config(), t_before_ms, t_after_ms)
            .await?;
        let backups = backup_names(&self.root.storage())?;

        if let Some(stress) = &stress {
            eprintln!("round {round}: stress phase");
            // Its workers and verify know the phase's run root only: the
            // shared storage root must be in its `run.json`.
            let stress =
                RunConfig { storage: Some(self.root.storage()), ..stress.clone() };
            stress.write_new(&phase)?;
            write_control(&phase, &Control::Wait)?;
            let tag = format!("{}-p{round}", default_tag());
            self.converter
                .stress_phase(&phase, tag, self.config.workers, progress_every)
                .await?;
            let earlier = EarlierPhases { logs: self.earlier.clone(), backups };
            let report = verify_phase(&phase, DEFAULT_DONE_TIMEOUT, &earlier).await?;
            if !report.passed {
                self.notes.push(format!(
                    "round {round}: the stress phase failed, see {}",
                    phase.report_json().display()
                ));
            }
            self.verdict.violations.extend(report.verdict.violations);
            self.verdict.warnings.extend(report.verdict.warnings);
        } else {
            let history = History { logs: read_all_logs(&phase)?, ..History::default() };
            check_init_version(&history, &mut self.verdict);
        }

        eprintln!("round {round}: converting to the standard layout");
        if !self.convert(round, Direction::ToStandard).await? {
            return Ok(false);
        }
        // The last round's is the scenario's final check, after its commits.
        if round < self.config.rounds {
            self.check_standard_side(Some(round)).await?;
        }
        eprintln!("round {round}: commits in the standard layout");
        self.standard_commits(&phase).await?;
        if let Some(detail) = standard_overwritten(&self.root.storage())? {
            self.verdict.violations.push(Violation::OverwroteStandard { detail });
        }
        self.earlier.push(read_all_logs(&phase)?);
        Ok(true)
    }

    /// Runs a conversion until it finishes, killing some runs, and checks
    /// it: C6 after every run of a conversion to the standard layout, C7
    /// after one killed after its change, then C2 and C3. Whether it
    /// finished; a conversion that couldn't is a [`Violation::ConversionEndState`].
    async fn convert(
        &mut self,
        round: u32,
        direction: Direction,
    ) -> anyhow::Result<bool> {
        let storage = self.root.storage();
        let phase = self.root.phase(round);
        let mut planted = None;
        let mut chooser =
            Kills::new(direction, self.config.guaranteed_kills, &mut self.rng);
        let mut stale_root_reruns = 0;
        for run in 0.. {
            if run == MAX_RUNS + stale_root_reruns {
                break;
            }
            let attempt = next_incarnation(&phase, converter_slot(direction));
            let plant = self.config.plant_standard_write
                && direction == Direction::FromStandard
                && round == 1
                && run == 0;
            let kill_at = if plant {
                Some(PointName::FromStandardRenameDir)
            } else if let Some(planned) = self.kill_plan.pop_front() {
                planned
            } else {
                chooser.choose(&mut self.rng, self.config.kill_probability)
            };
            let run =
                self.converter.run(self.root, round, direction, attempt, kill_at).await?;
            if direction == Direction::ToStandard
                && let Some(detail) = deleted_history(&storage)?
            {
                self.verdict.violations.push(Violation::DeletedHistory { detail });
            }
            let (result, nothing_to_convert) = match run {
                Run::Killed => {
                    if let Some(step) = kill_at {
                        chooser.killed(step);
                        self.killed.push((round, direction, step));
                    }
                    eprintln!("  run {attempt} killed at {kill_at:?}; running it again");
                    if matches!(
                        kill_at,
                        Some(PointName::ToStandardChange | PointName::ToStandardPublish)
                    ) {
                        self.writer_during_conversion(round).await?;
                    }
                    if plant {
                        planted = Some(self.plant(round)?);
                    }
                    continue;
                }
                Run::Finished(result) => (result, false),
                Run::NothingToConvert => (OpResult::Ack { snapshot: None }, true),
            };
            if let Some(fork_hash) = &planted {
                return self.planted_rerun(round, fork_hash, &result);
            }
            match result {
                OpResult::Ack { .. }
                    if nothing_to_convert
                        && direction == Direction::ToStandard
                        && self.network_fs
                        && left_by_stale_root_listing(&storage)? =>
                {
                    if stale_root_reruns == STALE_ROOT_RERUNS {
                        self.verdict.violations.push(Violation::ConversionEndState {
                            direction,
                            detail: format!(
                                "repo.d exists: run {attempt} and the {STALE_ROOT_RERUNS} \
                                 reruns before it returned NothingToConvert, their listing \
                                 of the storage root missing repo.d; it never refreshed"
                            ),
                        });
                        return Ok(false);
                    }
                    let delay = stale_root_delay(stale_root_reruns);
                    stale_root_reruns += 1;
                    self.verdict.warnings.push(Warning::StaleRootListing {
                        attempt,
                        rerun_after_ms: u64::try_from(delay.as_millis())?,
                    });
                    eprintln!(
                        "  run {attempt} missed repo.d; running it again in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                }
                OpResult::Ack { .. } => {
                    self.check_converted(direction)?;
                    if self.config.late_commit && direction == Direction::ToStandard {
                        let logs = read_all_logs(&phase)?;
                        if let Some(detail) = late_commit_defect(&logs) {
                            self.verdict.violations.push(Violation::Harness { detail });
                        }
                    }
                    return Ok(true);
                }
                result if self.as_under_stale_listings(&result)? => {
                    self.retried(direction, format!("run {attempt}: {result:?}"));
                }
                result => {
                    self.verdict.violations.push(Violation::ConversionEndState {
                        direction,
                        detail: format!("run {attempt} failed: {result:?}"),
                    });
                    return Ok(false);
                }
            }
        }
        self.verdict.violations.push(Violation::ConversionEndState {
            direction,
            detail: format!("it didn't finish in {} runs", MAX_RUNS + stale_root_reruns),
        });
        Ok(false)
    }

    /// A rerun that failed as under stale listings, retried: a warning on a
    /// network filesystem, a violation on a local one.
    fn retried(&mut self, direction: Direction, detail: String) {
        if self.network_fs {
            self.verdict.warnings.push(Warning::ConversionRetried { direction, detail });
        } else {
            self.verdict
                .violations
                .push(Violation::ConversionRetried { direction, detail });
        }
    }

    /// Whether a rerun failed as only stale listings make it fail: relisting
    /// ran out, or a fork reported while `$ROOT/repo` holds the bytes of a
    /// version of `repo.d`. icechunk-fs has no error kind for running out of
    /// relistings (it is `Other`): its messages say "are stale", which a test
    /// pins against the real error.
    fn as_under_stale_listings(&self, result: &OpResult) -> anyhow::Result<bool> {
        Ok(match result {
            OpResult::Error { message } => message.contains("are stale"),
            OpResult::Forked { .. } => {
                let storage = self.root.storage();
                let mirror = match fs::read(storage.join(REPO_KEY)) {
                    Ok(bytes) => payload_etag(&bytes),
                    Err(_) => return Ok(false),
                };
                let repo_d = storage.join("repo.d");
                match fs::read_dir(&repo_d) {
                    Ok(entries) => entries.filter_map(Result::ok).any(|entry| {
                        entry
                            .file_name()
                            .to_string_lossy()
                            .ends_with(&format!(".{mirror}"))
                            && !entry.file_name().to_string_lossy().starts_with("latest.")
                    }),
                    Err(_) => false,
                }
            }
            _ => false,
        })
    }

    /// C2 and C3 of a finished conversion.
    fn check_converted(&mut self, direction: Direction) -> anyhow::Result<()> {
        let storage = self.root.storage();
        let mirror = fs::read(storage.join(REPO_KEY)).context("reading repo")?;
        if let Some(detail) = conversion_defect(direction, &mirror, &storage)? {
            self.verdict
                .violations
                .push(Violation::ConversionMismatch { direction, detail });
        }
        if let Some(detail) = end_state_defect(direction, &storage)? {
            self.verdict
                .violations
                .push(Violation::ConversionEndState { direction, detail });
        }
        Ok(())
    }

    /// C7: a writer through the filesystem storage while the conversion to
    /// the standard layout is interrupted after its change.
    async fn writer_during_conversion(&mut self, round: u32) -> anyhow::Result<()> {
        let phase = self.root.phase(round);
        let who = WorkerId {
            tag: SCENARIO_TAG.to_owned(),
            slot: WRITER_SLOT.to_owned(),
            incarnation: next_incarnation(&phase, WRITER_SLOT),
        };
        let log = Arc::new(start_worker_log(&phase, &who)?);
        let storage =
            FilesystemStorage::new(&phase.storage())?.with_fsync(self.config.fsync);
        let repo = match Repository::open(Arc::new(storage)).execute().await {
            Ok(repo) => repo,
            Err(err) => {
                self.verdict.violations.push(Violation::Harness {
                    detail: format!(
                        "opening the repository during an interrupted conversion: {}",
                        err.kind()
                    ),
                });
                return Ok(());
            }
        };
        let chunk_bytes = self.config.run_config().chunk_bytes;
        let mut worker = RepoWorker::new(repo, log, &who, chunk_bytes)?;
        let result = worker.logged_main_commit().await?;
        self.c7_writers += 1;
        if let Some(detail) = writer_defect(Some(&result)) {
            let path = phase.worker_log(&who.tag, &who.slot, who.incarnation);
            let id = read_log(&path)?
                .into_iter()
                .find_map(|record| match record {
                    Record::Intent { id, .. } => Some(id),
                    _ => None,
                })
                .context("the writer logged no intent")?;
            self.verdict
                .violations
                .push(Violation::WriterDuringConversion { id, detail });
        }
        Ok(())
    }

    /// The planted standard write: `$ROOT/repo` replaced as Icechunk 2.3
    /// writes a status change, logged as a fork. Returns its hash.
    fn plant(&self, round: u32) -> anyhow::Result<String> {
        let phase = self.root.phase(round);
        let who = WorkerId {
            tag: SCENARIO_TAG.to_owned(),
            slot: PLANT_SLOT.to_owned(),
            incarnation: next_incarnation(&phase, PLANT_SLOT),
        };
        let log = start_worker_log(&phase, &who)?;
        let fork = write_fork(&phase.storage(), announce_backup(&log)?)?;
        log_fork(&log, &fork, None);
        eprintln!("  planted a standard write in repo");
        Ok(fork.fork_hash)
    }

    /// The rerun after the planted standard write must have stopped,
    /// leaving it in place; then the scenario can't go on, as recovery is
    /// manual. An erased planted write is logged as a fork's erasure, for
    /// F4.
    fn planted_rerun(
        &mut self,
        round: u32,
        fork_hash: &str,
        result: &OpResult,
    ) -> anyhow::Result<bool> {
        let phase = self.root.phase(round);
        let direction = Direction::FromStandard;
        if let Some(found) = erased(&phase.storage(), fork_hash) {
            let who = WorkerId {
                tag: SCENARIO_TAG.to_owned(),
                slot: PLANT_SLOT.to_owned(),
                incarnation: next_incarnation(&phase, PLANT_SLOT),
            };
            start_worker_log(&phase, &who)?.append(&Record::ForkGone {
                t_ms: now_ms(),
                fork_hash: fork_hash.to_owned(),
                found,
            })?;
            self.verdict.violations.push(Violation::ConversionEndState {
                direction,
                detail: format!(
                    "the rerun after a standard write to repo during the interrupted \
                     conversion ({result:?}) erased that write"
                ),
            });
        } else if result.landed().is_some() {
            self.verdict.violations.push(Violation::ConversionEndState {
                direction,
                detail:
                    "the rerun after a standard write to repo during the interrupted \
                         conversion reported success"
                        .to_owned(),
            });
        } else {
            self.notes.push(format!(
                "round {round}: the standard write planted during the interrupted conversion \
                 stopped its rerun, as it must ({result:?}); recovery is manual, so the \
                 scenario ends there"
            ));
        }
        let history = History { logs: read_all_logs(&phase)?, ..History::default() };
        check_forks(&history, self.network_fs, &mut self.verdict);
        Ok(false)
    }

    /// C1 in the standard layout: the repository oracle, through the
    /// standard local storage, over the logs of every phase, `round`'s
    /// included if given.
    async fn check_standard_side(&mut self, round: Option<u32>) -> anyhow::Result<()> {
        let own = match round {
            Some(round) => read_all_logs(&self.root.phase(round))?,
            None => BTreeMap::new(),
        };
        let history = History { logs: own, ..History::default() }
            .after_earlier_phases(&self.earlier);
        let storage = new_local_filesystem_storage(&self.root.storage()).await?;
        let repo = Repository::open(storage).execute().await.map_err(|err| {
            anyhow::anyhow!("opening the standard repository: {}", err.kind())
        })?;
        let mut checked = Verdict::default();
        check_repo(
            &repo,
            &history,
            &self.config.run_config(),
            self.network_fs,
            &mut checked,
        )
        .await?;
        self.verdict.violations.extend(checked.violations);
        self.verdict.warnings.extend(checked.warnings);
        self.verdict.stats.chain_len = checked.stats.chain_len;
        Ok(())
    }

    /// `standard_commits` commits to `main` in the standard layout, through
    /// the standard local storage, logged in `phase`.
    async fn standard_commits(&mut self, phase: &RunRoot) -> anyhow::Result<()> {
        let who = WorkerId {
            tag: SCENARIO_TAG.to_owned(),
            slot: STANDARD_SLOT.to_owned(),
            incarnation: next_incarnation(phase, STANDARD_SLOT),
        };
        let log = Arc::new(start_worker_log(phase, &who)?);
        let storage = new_local_filesystem_storage(&phase.storage()).await?;
        let repo = Repository::open(storage).execute().await.map_err(|err| {
            anyhow::anyhow!("opening the standard repository: {}", err.kind())
        })?;
        let mut worker =
            RepoWorker::new(repo, log, &who, self.config.run_config().chunk_bytes)?;
        for _ in 0..self.config.standard_commits {
            let result = worker.logged_main_commit().await?;
            if result.landed().is_none() {
                self.verdict.violations.push(Violation::Recovery {
                    detail: format!("a commit in the standard layout failed: {result:?}"),
                });
            }
        }
        Ok(())
    }

    /// The report, with the sabotages and kills every phase's logs record.
    /// `finished`: every round ran.
    fn finish(mut self, finished: bool) -> anyhow::Result<Report> {
        let killed: Vec<String> = self
            .killed
            .iter()
            .map(|(round, direction, step)| {
                format!(
                    "round {round} {}: {}",
                    converter_slot(*direction),
                    serde_name(step).unwrap_or_default()
                )
            })
            .collect();
        self.notes.push(format!(
            "converter runs killed at: {}; writers tried during an interrupted conversion \
             (C7): {}",
            if killed.is_empty() { "none".to_owned() } else { killed.join(", ") },
            self.c7_writers
        ));
        if self.config.guaranteed_kills {
            let mut missing = Vec::new();
            if !self
                .killed
                .iter()
                .any(|(_, direction, _)| *direction == Direction::FromStandard)
            {
                missing.push(
                    "no run of a conversion from the standard layout was killed"
                        .to_owned(),
                );
            }
            if self.c7_writers == 0 {
                missing.push(
                    "C7 never ran: no writer was tried during an interrupted conversion to the \
                     standard layout"
                        .to_owned(),
                );
            }
            // A scenario that stopped on a real violation never got there:
            // that violation is the finding, not the tool.
            if !finished && !self.verdict.violations.is_empty() {
                for detail in missing {
                    self.notes.push(format!(
                        "{detail}, as the scenario ended early on the violations above"
                    ));
                }
            } else {
                for detail in missing {
                    self.verdict.violations.push(Violation::Harness { detail });
                }
            }
        }
        let unfinished = (1..=self.config.rounds)
            .map(|round| self.root.phase(round))
            .filter(|phase| phase.root().exists())
            .skip(self.earlier.len());
        let mut phases = self.earlier.clone();
        for phase in unfinished {
            phases.push(read_all_logs(&phase)?);
        }
        for logs in &phases {
            count_faults(logs, &mut self.verdict);
        }
        // Over every phase's logs, wherever the scenario stopped.
        let all = History::default().after_earlier_phases(&phases);
        let counted = count_operations(&all);
        let stats = &mut self.verdict.stats;
        stats.intents = counted.intents;
        stats.acks = counted.acks;
        stats.conflicts = counted.conflicts;
        stats.errors = counted.errors;
        stats.refused = counted.refused;
        stats.unresolved = counted.unresolved;
        let mut config = self.config.run_config();
        config.sabotage = self.config.sabotage;
        let report = Report::new(
            self.verdict,
            config,
            Participants::default(),
            vec![],
            vec![],
            None,
            self.notes,
        );
        report.write(&self.root.top())?;
        Ok(report)
    }
}

/// Adds the sabotages and kills `logs` record to the verdict's fault counts.
fn count_faults(logs: &BTreeMap<LogName, Vec<Record>>, verdict: &mut Verdict) {
    for record in logs.values().flatten() {
        let kind = match record {
            Record::Fault { action: FaultAction::Sabotage, .. } => "sabotage",
            Record::Fault { action: FaultAction::Kill, .. } => "in_window_kill",
            _ => continue,
        };
        *verdict.stats.faults.entry(kind.to_owned()).or_default() += 1;
    }
}

/// The names in `overwritten/`.
fn backup_names(storage_root: &std::path::Path) -> anyhow::Result<HashSet<String>> {
    let dir = storage_root.join(OVERWRITTEN_FILES_PATH);
    match fs::read_dir(&dir) {
        Ok(entries) => entries
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect(),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(HashSet::new()),
        Err(err) => Err(err).with_context(|| format!("listing {}", dir.display())),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        path::Path,
        sync::atomic::{AtomicBool, Ordering},
    };

    use super::*;

    /// Unwinds the conversion's thread at `at`, without the panic hook's
    /// message, as a kill there leaves the files.
    #[derive(Debug)]
    pub(crate) struct UnwindAt {
        pub(crate) hooks: FaultHooks,
        pub(crate) at: Option<Point>,
        pub(crate) killed: Arc<AtomicBool>,
    }

    impl ProtocolHooks for UnwindAt {
        fn at(&self, point: Point) {
            self.hooks.at(point);
            if Some(point) == self.at && !self.killed.swap(true, Ordering::SeqCst) {
                std::panic::resume_unwind(Box::new("killed"));
            }
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.hooks.rename(from, to)
        }

        fn sabotaged(&self, sabotage: ProtocolSabotage) -> bool {
            self.hooks.sabotaged(sabotage)
        }
    }

    /// [`Converter::InProcess`]'s run.
    pub(crate) async fn run_in_process(
        root: &ScenarioRoot,
        round: u32,
        direction: Direction,
        attempt: u32,
        kill_at: Option<PointName>,
    ) -> anyhow::Result<Run> {
        let killed = Arc::new(AtomicBool::new(false));
        let faults = FaultConfig {
            points: vec![],
            kill_probability: 0.0,
            pause_probability: 0.0,
            pause: Duration::ZERO,
        };
        let kill_at = kill_at
            .map(Point::try_from)
            .transpose()
            .map_err(|name| anyhow::anyhow!("{name:?} is no conversion step"))?;
        let wrap = {
            let killed = Arc::clone(&killed);
            move |hooks| -> Arc<dyn ProtocolHooks> {
                Arc::new(UnwindAt { hooks, at: kill_at, killed })
            }
        };
        // As the converter process does.
        let config = ScenarioConfig::load(root)?;
        let run =
            convert_with(root, &config, round, direction, attempt, &faults, wrap).await?;
        Ok(if killed.load(Ordering::SeqCst) { Run::Killed } else { run })
    }

    /// One host's stress phase in this process: registers, starts the run,
    /// runs `workers` repository-mode workers (and the GC worker) on the run
    /// root resolved from the phase's path alone, as `fs-stress worker`
    /// does, and writes its `done/` entry.
    pub(crate) async fn in_process_workers(
        phase: &RunRoot,
        tag: &str,
        workers: u32,
    ) -> anyhow::Result<()> {
        use std::{collections::BTreeSet, time::SystemTime};

        use crate::{control, env_info::HostDone, repo_mode, supervisor::GC_SLOT};

        control::register(phase, &host_info(tag, &phase.storage()))?;
        fs::write(phase.go(), format!("{tag}\n"))?;
        let participants = BTreeSet::from([tag.to_owned()]);
        write_control(phase, &Control::Go { participants })?;
        let (root, config) =
            RunConfig::load_resolved(&RunRoot::new(phase.root().to_path_buf()))?;
        let go = SystemTime::now();
        let until = go + config.duration;
        let slots = (0..workers)
            .map(|i| i.to_string())
            .chain(config.gc.map(|_| GC_SLOT.to_owned()));
        let tasks: Vec<_> = slots
            .map(|slot| {
                let (root, config) = (root.clone(), config.clone());
                let who = WorkerId { tag: tag.to_owned(), slot, incarnation: 0 };
                tokio::spawn(async move {
                    if who.slot == GC_SLOT {
                        repo_mode::gc_worker(&root, &config, &who, until).await
                    } else {
                        repo_mode::worker(&root, &config, &who, until).await
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await??;
        }
        let ms =
            |t: SystemTime| chrono::DateTime::<chrono::Utc>::from(t).timestamp_millis();
        let done = HostDone {
            tag: tag.to_owned(),
            go_seen_at_ms: ms(go),
            stopped_at_ms: ms(SystemTime::now()),
            workers,
            incarnations_spawned: u64::from(workers),
            external_kills: 0,
            stragglers: 0,
            sampler_error: None,
        };
        write_synced(&phase.host_done(tag), &serde_json::to_vec(&done)?)?;
        Ok(())
    }

    /// A stress phase's configuration: short, without in-window faults,
    /// external kills or a fault floor, with GC.
    fn short_stress() -> RunConfig {
        let mut stress = RunConfig::defaults(crate::config::Mode::Repository, 1);
        stress.duration = Duration::from_secs(2);
        stress.faults.kill_probability = 0.0;
        stress.faults.pause_probability = 0.0;
        stress.external_kill_every = None;
        stress.fault_floor = crate::config::FaultFloor::ZERO;
        stress.fsync = false;
        stress.gc = Some(crate::config::GcConfig {
            every: Duration::from_millis(500),
            ..crate::config::GcConfig::defaults(stress.faults.pause)
        });
        stress
    }

    /// The stress phases' workers find the shared storage root from the
    /// phase's run root alone, and their commits survive both conversions.
    #[tokio::test]
    async fn stress_phases_run_on_the_shared_storage() {
        let config =
            ScenarioConfig { stress: Some(short_stress()), workers: 2, ..config() };
        let (_dir, root, report) = run(&config).await;
        assert!(report.passed, "{}", report.summary());
        for round in 1..=config.rounds {
            let phase = Report::load(&root.phase(round)).unwrap();
            assert!(phase.passed, "round {round}: {}", phase.summary());
            assert!(!root.phase(round).root().join("storage").exists(), "round {round}");
            let tag = format!("{}-p{round}", default_tag());
            let commits = read_all_logs(&root.phase(round))
                .unwrap()
                .into_iter()
                .filter(|(name, _)| name.tag == tag)
                .flat_map(|(_, records)| records)
                .filter(|record| {
                    matches!(
                        record,
                        Record::Outcome {
                            result: OpResult::Ack { snapshot: Some(_) },
                            ..
                        }
                    )
                })
                .count();
            assert!(commits > 0, "round {round}: no acknowledged stress commit");
        }
    }

    /// Each round kills one conversion run from the standard layout and one
    /// to it after its change or publication, with a C7 writer, and says so.
    #[tokio::test]
    async fn every_round_kills_both_conversions_and_runs_c7() {
        let config = ScenarioConfig { guaranteed_kills: true, ..config() };
        let (_dir, _root, report) = run(&config).await;
        assert!(report.passed, "{}", report.summary());
        let note = report
            .notes
            .iter()
            .find(|note| note.contains("converter runs killed at"))
            .unwrap();
        for round in 1..=config.rounds {
            assert!(note.contains(&format!("round {round} from-standard: ")), "{note}");
            assert!(
                note.contains(&format!("round {round} to-standard: to_standard_change"))
                    || note.contains(&format!(
                        "round {round} to-standard: to_standard_publish"
                    )),
                "{note}"
            );
        }
        assert!(note.ends_with(&format!("(C7): {}", config.rounds)), "{note}");
    }

    /// Without the guaranteed kills, a scenario fails, saying so.
    #[test]
    fn a_scenario_without_kills_or_c7_fails() {
        let dir = tempfile::tempdir().unwrap();
        let root = ScenarioRoot::new(dir.path().join("scenario"));
        let config = ScenarioConfig { guaranteed_kills: true, ..config() };
        create_root(&root, &config).unwrap();
        let scenario = Scenario {
            root: &root,
            config: &config,
            converter: &Converter::InProcess,
            network_fs: false,
            verdict: Verdict::default(),
            notes: vec![],
            earlier: vec![],
            rng: rand::make_rng(),
            killed: vec![(1, Direction::ToStandard, PointName::ToStandardRenameDir)],
            c7_writers: 0,
            kill_plan: std::collections::VecDeque::new(),
        };
        let report = scenario.finish(true).unwrap();
        let details: Vec<_> = report
            .verdict
            .violations
            .iter()
            .map(|v| match v {
                Violation::Harness { detail } => detail.as_str(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(details.len(), 2, "{details:?}");
        assert!(
            details[0].contains("from the standard layout was killed"),
            "{details:?}"
        );
        assert!(details[1].starts_with("C7 never ran"), "{details:?}");
        assert!(
            report
                .notes
                .iter()
                .any(|note| note.contains("round 1 to-standard: to_standard_rename_dir"))
        );
    }

    /// A storage root converted to the filesystem layout, and a scenario on it.
    async fn converted(dir: &Path, config: &ScenarioConfig) -> ScenarioRoot {
        let root = ScenarioRoot::new(dir.join("scenario"));
        create_root(&root, config).unwrap();
        let phase = root.phase(1);
        phase.create_dirs().unwrap();
        create_standard(&phase).await.unwrap();
        let run = Converter::InProcess
            .run(&root, 1, Direction::FromStandard, 0, None)
            .await
            .unwrap();
        assert_eq!(run, Run::Finished(OpResult::Ack { snapshot: None }));
        root
    }

    fn scenario<'a>(
        root: &'a ScenarioRoot,
        config: &'a ScenarioConfig,
        network_fs: bool,
    ) -> Scenario<'a> {
        Scenario {
            root,
            config,
            converter: &Converter::InProcess,
            network_fs,
            verdict: Verdict::default(),
            notes: vec![],
            earlier: vec![],
            rng: rand::make_rng(),
            killed: vec![],
            c7_writers: 0,
            kill_plan: std::collections::VecDeque::new(),
        }
    }

    /// Relisting runs out against the real conversion, whose message is the
    /// signal, and is retried as a warning on a network filesystem and a
    /// violation on a local one; a fork report is retried only while
    /// `$ROOT/repo` holds a version file's bytes.
    #[tokio::test]
    async fn reruns_that_fail_as_under_stale_listings_are_retried() {
        let dir = tempfile::tempdir().unwrap();
        let config = ScenarioConfig { rounds: 1, ..config() };
        let root = converted(dir.path(), &config).await;
        let storage = root.storage();
        let repo_d = storage.join("repo.d");
        // `$ROOT/repo` holds a version far above the marker, which no
        // listing shows as current: as when the listings are stale.
        let marker = fs::read_dir(&repo_d)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .find(|name| name.starts_with("latest."))
            .unwrap();
        let n: u64 = marker.split('.').nth(2).unwrap().parse().unwrap();
        let ahead = b"a version a stale listing hides";
        fs::write(repo_d.join(format!("{}.{}", n + 5, payload_etag(ahead))), ahead)
            .unwrap();
        fs::write(storage.join(REPO_KEY), ahead).unwrap();
        let Run::Finished(result) = Converter::InProcess
            .run(&root, 1, Direction::ToStandard, 0, None)
            .await
            .unwrap()
        else {
            panic!("killed");
        };
        assert!(
            matches!(&result, OpResult::Error { message } if message.contains("are stale")),
            "{result:?}"
        );
        for network_fs in [false, true] {
            let mut scenario = scenario(&root, &config, network_fs);
            assert!(scenario.as_under_stale_listings(&result).unwrap());
            scenario.retried(Direction::ToStandard, "run 0".to_owned());
            let (violations, warnings) =
                (scenario.verdict.violations.len(), scenario.verdict.warnings.len());
            assert_eq!((violations, warnings), if network_fs { (0, 1) } else { (1, 0) });
        }

        let scenario = scenario(&root, &config, false);
        let forked = OpResult::Forked { message: "fork".to_owned() };
        assert!(
            scenario.as_under_stale_listings(&forked).unwrap(),
            "repo holds a version file"
        );
        fs::write(storage.join(REPO_KEY), b"a fork").unwrap();
        assert!(
            !scenario.as_under_stale_listings(&forked).unwrap(),
            "repo is no version file"
        );
        let other = OpResult::Error { message: "No space left on device".to_owned() };
        assert!(!scenario.as_under_stale_listings(&other).unwrap());
    }

    /// The killed run 0 published its version without the feature; the
    /// runs numbered in `missing` miss `repo.d`. What `convert` returned.
    async fn convert_missing_repo_d(
        root: &ScenarioRoot,
        config: &ScenarioConfig,
        network_fs: bool,
        missing: std::ops::RangeInclusive<u32>,
    ) -> (bool, Verdict) {
        let converter = Converter::MissingRepoD { attempts: missing };
        let mut scenario =
            Scenario { converter: &converter, ..scenario(root, config, network_fs) };
        scenario.kill_plan = [Some(PointName::ToStandardPublish)].into();
        let finished = scenario.convert(1, Direction::ToStandard).await.unwrap();
        (finished, scenario.verdict)
    }

    /// A rerun of the conversion to the standard layout whose listing of the
    /// storage root misses `repo.d` returns `NothingToConvert` and leaves it:
    /// on a network filesystem a warning, and the conversion runs again; on a
    /// local one a violation.
    #[tokio::test]
    async fn a_rerun_that_misses_repo_d_runs_again_on_a_network_filesystem() {
        let config = ScenarioConfig { rounds: 1, ..config() };
        let dir = tempfile::tempdir().unwrap();
        let root = converted(dir.path(), &config).await;
        let (finished, verdict) =
            convert_missing_repo_d(&root, &config, true, 1..=1).await;
        assert!(finished);
        assert_eq!(verdict.violations, []);
        let first = u64::try_from(FIRST_STALE_ROOT_DELAY.as_millis()).unwrap();
        assert_eq!(
            verdict.warnings,
            [Warning::StaleRootListing { attempt: 1, rerun_after_ms: first }]
        );
        assert_eq!(
            end_state_defect(Direction::ToStandard, &root.storage()).unwrap(),
            None
        );

        let dir = tempfile::tempdir().unwrap();
        let root = converted(dir.path(), &config).await;
        let (_, verdict) = convert_missing_repo_d(&root, &config, false, 1..=1).await;
        assert!(
            matches!(
                &verdict.violations[..],
                [Violation::ConversionEndState { detail, .. }] if detail == "repo.d exists"
            ),
            "{:?}",
            verdict.violations
        );
        assert_eq!(verdict.warnings, []);
    }

    /// Reruns that keep missing `repo.d` end, after a bounded number with
    /// growing delays, in a violation saying so.
    #[tokio::test]
    async fn reruns_that_keep_missing_repo_d_fail_the_conversion() {
        let config = ScenarioConfig { rounds: 1, ..config() };
        let dir = tempfile::tempdir().unwrap();
        let root = converted(dir.path(), &config).await;
        let (finished, verdict) =
            convert_missing_repo_d(&root, &config, true, 1..=u32::MAX).await;
        assert!(!finished);
        // Outside tests the first delay is 500 ms: 79.5 s in all, longer than
        // NFS's default `acdirmax` of 60 s.
        let total: Duration = (0..STALE_ROOT_RERUNS).map(stale_root_delay).sum();
        assert_eq!(total, FIRST_STALE_ROOT_DELAY * 159);
        let expected: Vec<_> = (0..STALE_ROOT_RERUNS)
            .map(|k| Warning::StaleRootListing {
                attempt: k + 1,
                rerun_after_ms: u64::try_from(stale_root_delay(k).as_millis()).unwrap(),
            })
            .collect();
        assert_eq!(verdict.warnings, expected);
        assert!(
            matches!(
                &verdict.violations[..],
                [Violation::ConversionEndState { direction: Direction::ToStandard, detail }]
                    if detail.contains("repo.d exists") && detail.contains("never")
            ),
            "{:?}",
            verdict.violations
        );
    }

    #[test]
    fn the_late_commit_must_be_acknowledged() {
        let name = LogName {
            tag: SCENARIO_TAG.to_owned(),
            slot: "to-standard".to_owned(),
            incarnation: Some(0),
        };
        let id = uuid::Uuid::new_v4();
        let logs = |result| {
            BTreeMap::from([(
                name.clone(),
                vec![
                    Record::Intent { t_ms: 1, id, op: Op::MainCommit },
                    Record::Outcome { t_ms: 2, id, result },
                ],
            )])
        };
        assert_eq!(
            late_commit_defect(&logs(OpResult::Ack { snapshot: Some("S".to_owned()) })),
            None
        );
        let failed =
            late_commit_defect(&logs(OpResult::Error { message: "boom".to_owned() }));
        assert!(failed.is_some_and(|detail| detail.contains("boom")));
        assert!(
            late_commit_defect(&BTreeMap::new()).is_some_and(|d| d.contains("attempted"))
        );
    }

    /// A scenario that ended early on a real violation says why C7 didn't
    /// run, without blaming the tool.
    #[test]
    fn an_early_end_on_a_violation_explains_the_missing_kills() {
        let dir = tempfile::tempdir().unwrap();
        let config = ScenarioConfig { guaranteed_kills: true, ..config() };
        let root = ScenarioRoot::new(dir.path().join("scenario"));
        create_root(&root, &config).unwrap();
        let mut early = scenario(&root, &config, false);
        let found = Violation::ConversionEndState {
            direction: Direction::FromStandard,
            detail: "boom".to_owned(),
        };
        early.verdict.violations.push(found.clone());
        let report = early.finish(false).unwrap();
        assert_eq!(report.verdict.violations, [found]);
        assert_eq!(
            report.notes.iter().filter(|note| note.contains("ended early")).count(),
            2,
            "{:?}",
            report.notes
        );
        // Ended early without a violation: the tool's failure.
        let report = scenario(&root, &config, false).finish(false).unwrap();
        assert_eq!(report.verdict.violations.len(), 2, "{}", report.summary());
    }

    /// The guaranteed kill stays pending while runs end before its step,
    /// as a run that fails as under stale listings does; then kills are
    /// random.
    #[test]
    fn a_guaranteed_kill_waits_for_a_run_killed_at_it() {
        let mut rng: StdRng = rand::make_rng();
        for direction in [Direction::FromStandard, Direction::ToStandard] {
            let mut kills = Kills::new(direction, true, &mut rng);
            let step = kills.choose(&mut rng, 0.0).unwrap();
            if direction == Direction::ToStandard {
                assert!(matches!(
                    step,
                    PointName::ToStandardChange | PointName::ToStandardPublish
                ));
            }
            // Run 0 ended before reaching it: run 1 inherits it.
            assert_eq!(kills.choose(&mut rng, 0.0), Some(step));
            kills.killed(step);
            assert_eq!(kills.choose(&mut rng, 0.0), None);
            assert!(kills.choose(&mut rng, 1.0).is_some());
        }
        let mut kills = Kills::new(Direction::ToStandard, false, &mut rng);
        assert_eq!(kills.choose(&mut rng, 0.0), None);
        for _ in 0..MAX_KILLS {
            let step = kills.choose(&mut rng, 1.0).unwrap();
            kills.killed(step);
        }
        assert_eq!(kills.choose(&mut rng, 1.0), None, "at most MAX_KILLS");
    }

    /// The late commit's outcome comes first in the converter's log.
    #[test]
    fn the_conversion_outcome_is_its_intents() {
        let (convert, late) = (uuid::Uuid::new_v4(), uuid::Uuid::new_v4());
        let records = [
            Record::Intent {
                t_ms: 1,
                id: convert,
                op: Op::Convert { direction: Direction::ToStandard },
            },
            Record::Intent { t_ms: 2, id: late, op: Op::MainCommit },
            Record::Outcome { t_ms: 3, id: late, result: OpResult::Conflict },
            Record::Outcome {
                t_ms: 4,
                id: convert,
                result: OpResult::Ack { snapshot: None },
            },
        ];
        assert_eq!(conversion_outcome(&records), Some(OpResult::Ack { snapshot: None }));
        assert_eq!(conversion_outcome(&records[..3]), None);
    }

    pub(crate) fn config() -> ScenarioConfig {
        ScenarioConfig {
            rounds: 2,
            stress: None,
            workers: 1,
            kill_probability: 0.0,
            standard_commits: 2,
            fsync: false,
            sabotage: None,
            plant_standard_write: false,
            late_commit: false,
            guaranteed_kills: false,
        }
    }

    async fn run(config: &ScenarioConfig) -> (tempfile::TempDir, ScenarioRoot, Report) {
        let dir = tempfile::tempdir().unwrap();
        let root = ScenarioRoot::new(dir.path().join("scenario"));
        let report =
            convert_scenario(&root, config, &Converter::InProcess, Duration::ZERO)
                .await
                .unwrap();
        (dir, root, report)
    }

    fn kinds(report: &Report) -> Vec<String> {
        report
            .verdict
            .violations
            .iter()
            .map(|v| {
                serde_json::to_value(v).unwrap()["violation"].as_str().unwrap().to_owned()
            })
            .collect()
    }

    #[tokio::test]
    async fn round_trips_without_stress_phases_pass() {
        let (_dir, root, report) = run(&config()).await;
        assert!(report.passed, "{}", report.summary());
        assert_eq!(Report::load(&root.top()).unwrap(), report);
        assert_eq!(standard_overwritten(&root.storage()).unwrap(), None);
        // Two commits in each of the three standard eras, and two conversions
        // per round, every one checked.
        assert_eq!(report.verdict.stats.acks, 3 * 2 + 2 * 2, "{}", report.summary());
    }

    #[tokio::test]
    async fn every_step_of_both_conversions_can_be_killed_and_rerun() {
        let mut config = config();
        config.rounds = 3;
        config.kill_probability = 1.0;
        let (_dir, _root, report) = run(&config).await;
        assert!(report.passed, "{}", report.summary());
        assert!(
            report.verdict.stats.faults.is_empty(),
            "unwinding isn't logged as a kill"
        );
    }

    #[tokio::test]
    async fn a_planted_standard_write_stops_the_rerun() {
        let mut config = config();
        config.plant_standard_write = true;
        let (_dir, _root, report) = run(&config).await;
        assert!(report.passed, "{}", report.summary());
        assert!(
            report.notes.iter().any(|note| note.contains("stopped its rerun")),
            "{:?}",
            report.notes
        );
        config.sabotage = Some(Sabotage::FromStandardWithoutProvenance);
        let (_dir, _root, report) = run(&config).await;
        let kinds = kinds(&report);
        assert!(kinds.contains(&"ConversionEndState".to_owned()), "{}", report.summary());
        assert!(kinds.contains(&"SilentForkLoss".to_owned()), "{}", report.summary());
        assert!(report.verdict.stats.faults["sabotage"] > 0);
    }

    #[tokio::test]
    async fn a_commit_hidden_from_the_conversion_is_converted() {
        let mut config = config();
        config.rounds = 1;
        config.late_commit = true;
        let (_dir, _root, report) = run(&config).await;
        assert!(report.passed, "{}", report.summary());
        config.sabotage = Some(Sabotage::ToStandardWithoutChange);
        let (_dir, _root, report) = run(&config).await;
        assert_eq!(kinds(&report), ["LostUpdate"], "{}", report.summary());
    }

    #[tokio::test]
    async fn deleting_repo_d_before_the_publication_is_caught() {
        let mut config = config();
        config.sabotage = Some(Sabotage::DeleteDirBeforePublish);
        let (_dir, _root, report) = run(&config).await;
        let kinds = kinds(&report);
        assert!(kinds.contains(&"DeletedHistory".to_owned()), "{}", report.summary());
        assert!(kinds.contains(&"ConversionEndState".to_owned()), "{}", report.summary());
        // Counted over the logs of the round it stopped in: two commits and
        // the conversion from the standard layout.
        assert_eq!(report.verdict.stats.acks, 3, "{}", report.summary());
    }

    #[tokio::test]
    async fn a_writer_during_an_interrupted_conversion_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = ScenarioRoot::new(dir.path().join("scenario"));
        let config = ScenarioConfig { rounds: 1, ..config() };
        create_root(&root, &config).unwrap();
        let phase = root.phase(1);
        phase.create_dirs().unwrap();
        create_standard(&phase).await.unwrap();
        let mut scenario = Scenario {
            root: &root,
            config: &config,
            converter: &Converter::InProcess,
            network_fs: false,
            verdict: Verdict::default(),
            notes: vec![],
            earlier: vec![],
            rng: rand::make_rng(),
            killed: vec![],
            c7_writers: 0,
            kill_plan: std::collections::VecDeque::new(),
        };
        assert!(scenario.convert(1, Direction::FromStandard).await.unwrap());
        scenario.kill_plan = [Some(PointName::ToStandardPublish)].into();
        let run = Converter::InProcess
            .run(&root, 1, Direction::ToStandard, 0, Some(PointName::ToStandardChange))
            .await
            .unwrap();
        assert_eq!(run, Run::Killed);
        scenario.writer_during_conversion(1).await.unwrap();
        assert_eq!(scenario.verdict.violations, []);
        let writer = read_log(&phase.worker_log(SCENARIO_TAG, WRITER_SLOT, 0)).unwrap();
        assert!(
            writer.iter().any(|record| matches!(
                record,
                Record::Outcome { result: OpResult::ConversionInProgress { .. }, .. }
            )),
            "{writer:?}"
        );
        // The reruns, the first killed after its publication, which also
        // tries a writer, finish the conversion.
        assert!(scenario.convert(1, Direction::ToStandard).await.unwrap());
        assert_eq!(scenario.verdict.violations, []);
        assert!(
            phase.worker_log(SCENARIO_TAG, WRITER_SLOT, 1).exists(),
            "no second writer"
        );
    }
}
