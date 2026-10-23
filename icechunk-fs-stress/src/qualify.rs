//! `qualify`: the fixed qualification of a filesystem that anyone runs
//! unchanged, on one host or several. A storage-mode run, then a
//! repository-mode run, each an ordinary run root under one root, verified
//! together into one result.

use std::{fmt, fmt::Write as _, fs, path::Path, time::Duration};

use anyhow::{Context as _, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    config::{
        DEFAULT_READONLY_HOLD, FaultFloor, GcConfig, Mode, ReadOnlyConfig, RetryConfig,
        RunConfig, millis, option_millis, write_synced,
    },
    control::{self, read_go},
    env_info::{Build, HostDone, default_tag},
    layout::{QualifyRoot, qualify_run_dir},
    next_steps::qualify_next_steps,
    report::{Outcome, Report, replace_file},
    supervisor::{self, LateHost},
    verify::{self, AlreadyVerified, DEFAULT_DONE_TIMEOUT},
};

/// The runs, in the order they run.
pub const MODES: [Mode; 2] = [Mode::Storage, Mode::Repository];

/// How long a host waits in the second run's barrier, from its registration,
/// before it starts with the hosts registered by then: a host that died in
/// the first run doesn't hold up the second.
pub const SECOND_RUN_GO_AFTER: Duration = Duration::from_secs(10 * 60);

/// The free space a qualification needs, whatever the hosts and workers:
/// mostly the backups of the repo info that every change keeps in
/// `storage/overwritten/`.
pub const DISK_NEEDED: &str = "needs up to about 10 GB of free space on the filesystem \
     under test for the whole qualification; usually under 1 GB on network filesystems";

/// The fixed settings of the runs; every other setting keeps its default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Preset {
    #[serde(with = "millis")]
    pub storage_duration: Duration,
    /// With GC at its default margin and interval.
    #[serde(with = "millis")]
    pub repository_duration: Duration,
    #[serde(with = "millis")]
    pub external_kill_every: Duration,
    #[serde(with = "millis")]
    pub mirror_sample_every: Duration,
    /// Repository mode only.
    #[serde(with = "millis")]
    pub readonly_every: Duration,
}

pub const PRESET: Preset = Preset {
    storage_duration: Duration::from_secs(5 * 60),
    repository_duration: Duration::from_secs(10 * 60),
    external_kill_every: Duration::from_secs(10),
    mirror_sample_every: Duration::from_millis(25),
    readonly_every: Duration::from_secs(10),
};

/// Changes to the preset for short test runs. A result with any of them is
/// no qualification.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Overrides {
    /// Both runs'.
    #[serde(with = "option_millis")]
    pub duration: Option<Duration>,
    #[serde(with = "option_millis")]
    pub gc_margin: Option<Duration>,
    /// No minimum counts of fired faults.
    pub no_fault_floor: bool,
}

impl Overrides {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    fn describe(&self) -> String {
        let duration = self.duration.map(|d| format!("duration {}", human_duration(d)));
        let gc_margin =
            self.gc_margin.map(|d| format!("GC margin {}", human_duration(d)));
        let floor = self.no_fault_floor.then(|| "no fault floor".to_owned());
        [duration, gc_margin, floor].into_iter().flatten().collect::<Vec<_>>().join(", ")
    }
}

/// `qualify.json`, written by `init`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Qualify {
    pub hosts: u32,
    pub workers_per_host: u32,
    pub max_backoff_ms: u32,
    pub preset: Preset,
    pub overrides: Overrides,
    /// The binary that ran `init`.
    pub build: Build,
}

impl Qualify {
    /// The preset with the given values; `None` takes the default.
    pub fn new(
        hosts: u32,
        workers_per_host: Option<u32>,
        max_backoff_ms: Option<u32>,
        overrides: Overrides,
    ) -> Self {
        let defaults = RunConfig::defaults(Mode::Storage, hosts);
        Self {
            hosts,
            workers_per_host: workers_per_host
                .unwrap_or(defaults.workers_per_host_default),
            max_backoff_ms: max_backoff_ms
                .unwrap_or(RetryConfig::default().max_backoff_ms),
            preset: PRESET,
            overrides,
            build: Build::current(),
        }
    }

    /// The configuration of `mode`'s run, with `init`'s checks.
    pub fn run_config(&self, mode: Mode) -> anyhow::Result<RunConfig> {
        let (preset, overrides) = (&self.preset, &self.overrides);
        let mut config = RunConfig::defaults(mode, self.hosts);
        config.workers_per_host_default = self.workers_per_host;
        config.repo_update_retries.max_backoff_ms = self.max_backoff_ms;
        config.external_kill_every = Some(preset.external_kill_every);
        config.mirror_sample_every = Some(preset.mirror_sample_every);
        config.duration = match mode {
            Mode::Storage => preset.storage_duration,
            Mode::Repository => preset.repository_duration,
        };
        if mode == Mode::Repository {
            config.readonly = Some(ReadOnlyConfig {
                every: preset.readonly_every,
                hold: DEFAULT_READONLY_HOLD,
            });
            let gc = GcConfig::run_defaults(config.faults.pause);
            config.gc =
                Some(GcConfig { margin: overrides.gc_margin.unwrap_or(gc.margin), ..gc });
        }
        config.duration = overrides.duration.unwrap_or(config.duration);
        if overrides.no_fault_floor {
            config.fault_floor = FaultFloor::ZERO;
        }
        config.check_gc_margin()?;
        config.validate()?;
        Ok(config)
    }

    pub fn load(root: &QualifyRoot) -> anyhow::Result<Self> {
        let path = root.qualify_json();
        let bytes =
            fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))
    }

    fn write_new(&self, root: &QualifyRoot) -> anyhow::Result<()> {
        let path = root.qualify_json();
        write_synced(&path, &serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("writing {}", path.display()))
    }
}

/// Creates the root, which must be missing or empty, and both run roots,
/// then `qualify.json`, so no host can join a half-initialized
/// qualification.
pub async fn init(root: &QualifyRoot, qualify: &Qualify) -> anyhow::Result<()> {
    let configs = MODES
        .into_iter()
        .map(|mode| qualify.run_config(mode).map(|config| (mode, config)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    root.create_dir()?;
    for (mode, config) in &configs {
        supervisor::init(&root.run(*mode), config).await?;
    }
    qualify.write_new(root)
}

/// This host's part of each run, in order: `workers` on the storage-mode run,
/// then on the repository-mode run, whatever happened in the first. The
/// second run's barrier starts without the missing hosts after `go_after`.
/// `workers` overrides the runs' workers per host.
pub async fn workers(
    root: &QualifyRoot,
    tag: &str,
    workers: Option<u32>,
    progress_every: Duration,
    go_after: Duration,
) -> anyhow::Result<Vec<(Mode, anyhow::Result<HostDone>)>> {
    Qualify::load(root)?;
    let mut results = Vec::new();
    for (i, mode) in MODES.into_iter().enumerate() {
        let run = root.run(mode);
        eprintln!(
            "starting the {} run in {}",
            qualify_run_dir(mode),
            run.root().display()
        );
        let go_after = (i > 0).then_some(go_after);
        let done = async {
            let workers = match workers {
                Some(workers) => workers,
                None => RunConfig::load(&run)?.workers_per_host_default,
            };
            supervisor::workers_with_go_after(
                &run,
                tag.to_owned(),
                workers,
                progress_every,
                go_after,
            )
            .await
        }
        .await;
        let done = match (go_after, done) {
            (Some(go_after), Err(err))
                if err.downcast_ref::<LateHost>().is_some()
                    && matches!(
                        control::read_control(&run),
                        Ok(control::Control::Go { .. })
                    ) =>
            {
                Err(LateForSecondRun { go_after }.into())
            }
            (_, done) => done,
        };
        results.push((mode, done));
    }
    Ok(results)
}

/// A host that missed the second run's go: the other hosts started it
/// without this one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LateForSecondRun {
    pub go_after: Duration,
}

impl fmt::Display for LateForSecondRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the other hosts started the {} run without this host, because it didn't \
             register within {} of them; the qualification goes on without it",
            qualify_run_dir(MODES[1]),
            human_duration(self.go_after)
        )
    }
}

impl std::error::Error for LateForSecondRun {}

/// `qualify workers`' exit code: 1 if either run hit an error, else 2 if
/// this host was late for either, else 0.
pub fn workers_exit_code<'a>(
    results: impl IntoIterator<Item = &'a anyhow::Result<HostDone>>,
) -> u8 {
    let errors: Vec<_> = results.into_iter().filter_map(|r| r.as_ref().err()).collect();
    let late = |err: &&anyhow::Error| {
        err.downcast_ref::<LateHost>().is_some()
            || err.downcast_ref::<LateForSecondRun>().is_some()
    };
    if !errors.iter().all(late) {
        1
    } else if errors.is_empty() {
        0
    } else {
        2
    }
}

/// One run's result.
#[derive(Debug, Clone, PartialEq)]
pub enum RunResult {
    Report(Box<Report>),
    /// Its control still says wait: not every host registered for it yet.
    /// Like a run with hosts still missing, it needs another verify.
    NotStarted,
    /// Why the run has no report.
    Error(String),
}

impl RunResult {
    pub fn outcome(&self) -> Outcome {
        match self {
            Self::Report(report) => report.outcome(),
            Self::NotStarted => Outcome::Provisional,
            Self::Error(_) => Outcome::Error,
        }
    }
}

/// Both runs' results, checked together.
#[derive(Debug, Clone, PartialEq)]
pub struct Combined {
    pub runs: Vec<(Mode, RunResult)>,
    /// The hosts that took part in the first run but not in the second.
    pub missing: Vec<String>,
    pub overrides: Overrides,
    /// The build that ran `init`.
    pub init_build: Build,
}

impl Combined {
    /// A failure wins, then a provisional verdict, then an error.
    pub fn outcome(&self) -> Outcome {
        let outcomes: Vec<_> = self.runs.iter().map(|(_, run)| run.outcome()).collect();
        [Outcome::Failed, Outcome::Provisional, Outcome::Error]
            .into_iter()
            .find(|worst| outcomes.contains(worst))
            .unwrap_or(Outcome::Passed)
    }

    /// The combined summary and what to do next, for the root `root` as the
    /// user gave it.
    pub fn summary(&self, root: &Path) -> String {
        let mut out = String::new();
        // Writing to a `String` can't fail.
        let _ = self.write_summary(root, &mut out);
        out
    }

    fn write_summary(&self, root: &Path, out: &mut String) -> fmt::Result {
        let outcome = self.outcome();
        let result = match outcome {
            Outcome::Passed => "PASS",
            Outcome::Failed => "FAIL",
            Outcome::Provisional => "PROVISIONAL",
            Outcome::Error => "ERROR",
        };
        let build = Build::current();
        writeln!(out, "{result}: fs-stress qualification, {build}")?;
        if self.init_build != build {
            writeln!(out, "note: another build initialized it: {}", self.init_build)?;
        }
        if !self.overrides.is_empty() {
            writeln!(
                out,
                "note: the preset was changed for a test ({}), so this result is not a \
                 qualification",
                self.overrides.describe()
            )?;
        }
        for (mode, run) in &self.runs {
            let dir = root.join(qualify_run_dir(*mode));
            writeln!(out, "\n{} run, in {}:", qualify_run_dir(*mode), dir.display())?;
            match run {
                RunResult::Report(report) => write!(out, "{}", report.summary())?,
                RunResult::NotStarted => writeln!(
                    out,
                    "not started yet: it starts once its hosts have registered"
                )?,
                RunResult::Error(why) => writeln!(out, "ERROR: {why}")?,
            }
        }
        if !self.missing.is_empty() {
            writeln!(
                out,
                "\nhosts in the first run but not in the second: {}",
                self.missing.join(", ")
            )?;
        }
        write!(out, "\n{}", qualify_next_steps(outcome, root))
    }
}

/// Writes `summary.txt` in the root with `text`, replacing any earlier one.
pub fn write_summary_file(root: &QualifyRoot, text: &str) -> anyhow::Result<()> {
    replace_file(&root.summary(), text.as_bytes())
}

/// Verifies both runs, as `verify` does each, and combines the results. A run
/// verified fully before is refused, as `verify` refuses it, and its
/// `report.json` read again.
pub async fn verify(
    root: &QualifyRoot,
    done_timeout: Duration,
) -> anyhow::Result<Combined> {
    let qualify = Qualify::load(root)?;
    let mut runs = Vec::new();
    for mode in MODES {
        runs.push((mode, verify_run(root, mode, done_timeout).await));
    }
    Ok(combined(root, &qualify, runs))
}

/// `init` for one host, this host's workers on both runs, then `verify`. A
/// run whose workers failed isn't verified.
pub async fn run(
    root: &QualifyRoot,
    qualify: &Qualify,
    progress_every: Duration,
) -> anyhow::Result<Combined> {
    ensure!(qualify.hosts == 1, "run is for one host, got hosts = {}", qualify.hosts);
    init(root, qualify).await?;
    let tag = default_tag();
    let host_runs =
        workers(root, &tag, None, progress_every, SECOND_RUN_GO_AFTER).await?;
    let mut runs = Vec::new();
    for (mode, done) in host_runs {
        let result = match done {
            Ok(_) => verify_run(root, mode, DEFAULT_DONE_TIMEOUT).await,
            Err(err) => {
                eprintln!("the {} run failed: {err:#}", qualify_run_dir(mode));
                RunResult::Error(format!("this host's workers failed: {err:#}"))
            }
        };
        runs.push((mode, result));
    }
    Ok(combined(root, qualify, runs))
}

fn combined(
    root: &QualifyRoot,
    qualify: &Qualify,
    runs: Vec<(Mode, RunResult)>,
) -> Combined {
    Combined {
        runs,
        missing: missing_from_second_run(root),
        overrides: qualify.overrides,
        init_build: qualify.build.clone(),
    }
}

async fn verify_run(root: &QualifyRoot, mode: Mode, done_timeout: Duration) -> RunResult {
    let run = root.run(mode);
    // Verify would report it as never started, an error.
    if matches!(control::read_control(&run), Ok(control::Control::Wait)) {
        eprintln!("the {} run hasn't started yet", qualify_run_dir(mode));
        return RunResult::NotStarted;
    }
    eprintln!("verifying the {} run in {}", qualify_run_dir(mode), run.root().display());
    match verify::verify(&run, done_timeout).await {
        Ok(report) => RunResult::Report(Box::new(report)),
        Err(err) if err.downcast_ref::<AlreadyVerified>().is_some() => {
            eprintln!("{err:#}; reading its report.json");
            match Report::load(&run) {
                Ok(report) => RunResult::Report(Box::new(report)),
                Err(load) => RunResult::Error(format!("{err:#}; {load:#}")),
            }
        }
        Err(err) => RunResult::Error(format!("verify hit an error: {err:#}")),
    }
}

/// The hosts in the first run's go list but not in the second's; none if
/// either run never started.
fn missing_from_second_run(root: &QualifyRoot) -> Vec<String> {
    match (read_go(&root.run(MODES[0])), read_go(&root.run(MODES[1]))) {
        (Ok(first), Ok(second)) => first.difference(&second).cloned().collect(),
        _ => Vec::new(),
    }
}

/// Tells every host to stop, in both runs.
pub fn stop(root: &QualifyRoot) -> anyhow::Result<()> {
    let errors: Vec<_> = MODES
        .into_iter()
        .filter_map(|mode| {
            control::stop(&root.run(mode))
                .with_context(|| format!("stopping the {} run", qualify_run_dir(mode)))
                .err()
        })
        .map(|err| format!("{err:#}"))
        .collect();
    ensure!(errors.is_empty(), "{}", errors.join("; "));
    Ok(())
}

/// `5m`, `90s` or `1500ms`.
pub fn human_duration(d: Duration) -> String {
    if d.subsec_millis() != 0 {
        format!("{}ms", d.as_millis())
    } else if d.as_secs().is_multiple_of(60) && !d.is_zero() {
        format!("{}m", d.as_secs() / 60)
    } else {
        format!("{}s", d.as_secs())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::{
        config::ReadOnlyConfig,
        layout::RunRoot,
        log::LogWriter,
        next_steps::{ISSUES_URL, SLACK_INVITE_URL},
        oracle::{Verdict, Violation},
        recovery::recovery_worker,
        report::Participants,
    };

    #[test]
    fn the_preset_makes_a_storage_run_of_5m() {
        let qualify = Qualify::new(3, None, None, Overrides::default());
        let mut expected = RunConfig::defaults(Mode::Storage, 3);
        expected.duration = Duration::from_secs(300);
        expected.external_kill_every = Some(Duration::from_secs(10));
        expected.mirror_sample_every = Some(Duration::from_millis(25));
        assert_eq!(qualify.run_config(Mode::Storage).unwrap(), expected);
        assert_eq!(expected.workers_per_host_default, 8);
        assert_eq!(expected.fault_floor, FaultFloor::default());
        assert_eq!((expected.gc, expected.readonly, expected.fork), (None, None, None));
    }

    #[test]
    fn the_preset_makes_a_repository_run_of_10m_with_gc_and_readonly_holds() {
        let qualify = Qualify::new(2, Some(4), Some(1000), Overrides::default());
        let mut expected = RunConfig::defaults(Mode::Repository, 2);
        expected.duration = Duration::from_secs(600);
        expected.workers_per_host_default = 4;
        expected.repo_update_retries.max_backoff_ms = 1000;
        expected.external_kill_every = Some(Duration::from_secs(10));
        expected.mirror_sample_every = Some(Duration::from_millis(25));
        expected.readonly = Some(ReadOnlyConfig {
            every: Duration::from_secs(10),
            hold: DEFAULT_READONLY_HOLD,
        });
        expected.gc = Some(GcConfig::run_defaults(expected.faults.pause));
        assert_eq!(qualify.run_config(Mode::Repository).unwrap(), expected);
        assert_eq!(expected.gc.unwrap().margin, Duration::from_secs(186));
        assert_eq!(expected.fork, None);
    }

    #[test]
    fn overrides_shorten_both_runs() {
        let overrides = Overrides {
            duration: Some(Duration::from_secs(20)),
            gc_margin: Some(Duration::from_secs(5)),
            no_fault_floor: true,
        };
        let qualify = Qualify::new(1, None, None, overrides);
        for mode in MODES {
            let config = qualify.run_config(mode).unwrap();
            assert_eq!(config.duration, Duration::from_secs(20));
            assert_eq!(config.fault_floor, FaultFloor::ZERO);
        }
        let gc = qualify.run_config(Mode::Repository).unwrap().gc.unwrap();
        assert_eq!(gc.margin, Duration::from_secs(5));

        let short =
            Overrides { duration: Some(Duration::from_secs(20)), ..Overrides::default() };
        let err =
            Qualify::new(1, None, None, short).run_config(Mode::Repository).unwrap_err();
        assert!(format!("{err:#}").contains("deletes nothing"), "{err:#}");
        let backoff = Qualify::new(1, None, Some(5), Overrides::default());
        assert!(backoff.run_config(Mode::Storage).is_err());
    }

    fn short() -> Overrides {
        Overrides {
            duration: Some(Duration::from_secs(20)),
            gc_margin: Some(Duration::from_secs(5)),
            no_fault_floor: true,
        }
    }

    #[tokio::test]
    async fn init_creates_both_runs_and_qualify_json_and_refuses_a_used_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = QualifyRoot::new(dir.path().join("q"));
        let qualify = Qualify::new(2, Some(3), None, short());
        init(&root, &qualify).await.unwrap();
        assert_eq!(Qualify::load(&root).unwrap(), qualify);
        for mode in MODES {
            let run = root.run(mode);
            assert_eq!(run.root(), root.root().join(qualify_run_dir(mode)));
            assert_eq!(RunConfig::load(&run).unwrap(), qualify.run_config(mode).unwrap());
            assert_eq!(control::read_control(&run).unwrap(), control::Control::Wait);
        }
        assert!(init(&root, &qualify).await.is_err());
        stop(&root).unwrap();
        for mode in MODES {
            let control = control::read_control(&root.run(mode)).unwrap();
            assert_eq!(control, control::Control::Stop);
        }

        let bad = QualifyRoot::new(dir.path().join("bad"));
        let gc_too_late = Overrides { gc_margin: None, ..short() };
        assert!(init(&bad, &Qualify::new(1, None, None, gc_too_late)).await.is_err());
        assert!(!bad.root().exists());
        assert!(stop(&bad).is_err());
    }

    #[tokio::test]
    async fn workers_needs_qualify_json() {
        let dir = tempfile::tempdir().unwrap();
        let root = QualifyRoot::new(dir.path().to_path_buf());
        let err = workers(&root, "a", None, Duration::ZERO, SECOND_RUN_GO_AFTER)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("qualify.json"), "{err:#}");
    }

    #[test]
    fn workers_exits_1_on_an_error_else_2_if_late() {
        let done = || {
            Ok(HostDone {
                tag: "a".to_owned(),
                go_seen_at_ms: 0,
                stopped_at_ms: 0,
                workers: 1,
                incarnations_spawned: 1,
                external_kills: 0,
                stragglers: 0,
                sampler_error: None,
            })
        };
        let late = || Err(LateHost { hosts: 2 }.into());
        let error = || Err(anyhow::anyhow!("boom"));
        let code = |runs: [anyhow::Result<HostDone>; 2]| workers_exit_code(&runs);
        assert_eq!(code([done(), done()]), 0);
        assert_eq!(code([late(), done()]), 2);
        assert_eq!(code([done(), late()]), 2);
        assert_eq!(code([late(), late()]), 2);
        assert_eq!(code([error(), done()]), 1);
        assert_eq!(code([late(), error()]), 1);
        assert_eq!(code([error(), error()]), 1);
        let go_after = Duration::from_secs(600);
        let missed = || Err(LateForSecondRun { go_after }.into());
        assert_eq!(code([done(), missed()]), 2);
        assert_eq!(code([late(), missed()]), 2);
        assert_eq!(code([error(), missed()]), 1);
        let text = LateForSecondRun { go_after }.to_string();
        assert_eq!(
            text,
            "the other hosts started the repository-mode run without this host, because \
             it didn't register within 10m of them; the qualification goes on without it"
        );
    }

    fn report(mode: Mode, outcome: Outcome) -> RunResult {
        let violations = match outcome {
            Outcome::Passed => vec![],
            Outcome::Failed => vec![Violation::Phantom { what: "x".to_owned() }],
            Outcome::Provisional | Outcome::Error => {
                vec![Violation::Harness { detail: "y".to_owned() }]
            }
        };
        let missing = match outcome {
            Outcome::Provisional => vec!["b".to_owned()],
            _ => vec![],
        };
        let participants = Participants {
            tags: BTreeSet::from(["a".to_owned(), "b".to_owned()]),
            missing,
        };
        let verdict = Verdict { violations, ..Verdict::default() };
        let qualify = Qualify::new(2, None, None, Overrides::default());
        let config = qualify.run_config(mode).unwrap();
        let report =
            Report::new(verdict, config, participants, vec![], vec![], None, vec![]);
        assert_eq!(report.outcome(), outcome);
        RunResult::Report(Box::new(report))
    }

    fn combined(storage: RunResult, repository: RunResult) -> Combined {
        Combined {
            runs: vec![(Mode::Storage, storage), (Mode::Repository, repository)],
            missing: vec![],
            overrides: Overrides::default(),
            init_build: Build::current(),
        }
    }

    #[test]
    fn a_failure_wins_then_a_provisional_verdict_then_an_error() {
        use Outcome::{Error, Failed, Passed, Provisional};
        let error = || RunResult::Error("verify hit an error: z".to_owned());
        let result = |outcome| match outcome {
            Error => error(),
            outcome => report(Mode::Storage, outcome),
        };
        let all = [Passed, Failed, Provisional, Error];
        for first in all {
            for second in all {
                let both = [first, second];
                let expected = if both.contains(&Failed) {
                    (Failed, 1)
                } else if both.contains(&Provisional) {
                    (Provisional, 3)
                } else if both.contains(&Error) {
                    (Error, 3)
                } else {
                    (Passed, 0)
                };
                let combined = combined(result(first), result(second));
                let outcome = combined.outcome();
                assert_eq!((outcome, outcome.exit_code()), expected, "{both:?}");
            }
        }
    }

    const ROOT: &str = "/mnt/shared/q1";

    #[test]
    fn the_summary_shows_the_result_both_runs_and_what_to_do_next() {
        let mut both = combined(
            report(Mode::Storage, Outcome::Passed),
            report(Mode::Repository, Outcome::Failed),
        );
        both.missing = vec!["b".to_owned()];
        let text = both.summary(Path::new(ROOT));
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(
            lines[0],
            format!("FAIL: fs-stress qualification, {}", Build::current())
        );
        let storage = format!("storage-mode run, in {ROOT}/storage-mode:");
        let repository = format!("repository-mode run, in {ROOT}/repository-mode:");
        let at = |line: &str| lines.iter().position(|l| *l == line).unwrap_or(usize::MAX);
        assert!(at(&storage) < at(&repository), "{text}");
        assert!(
            lines[at(&storage) + 1].starts_with("PASS: storage mode, 2 host(s), 300s")
        );
        assert!(lines[at(&repository) + 1].starts_with("FAIL: repository mode"));
        let missing = "hosts in the first run but not in the second: b";
        assert!(at(&repository) < at(missing), "{text}");
        assert!(text.contains("Phantom: x"), "{text}");
        assert!(!text.contains("preset"), "{text}");
        let next = qualify_next_steps(Outcome::Failed, Path::new(ROOT));
        assert!(text.ends_with(&format!("\n\n{next}")), "{text}");
        for expected in
            [&format!("fs-stress bundle --root {ROOT}`"), SLACK_INVITE_URL, ISSUES_URL]
        {
            assert!(text.contains(expected), "{expected} missing from {text}");
        }
    }

    #[test]
    fn the_summary_says_when_the_preset_was_changed_and_shows_errors() {
        let mut both = combined(
            RunResult::Error("verify hit an error: no run.json".to_owned()),
            report(Mode::Repository, Outcome::Provisional),
        );
        both.overrides = short();
        let text = both.summary(Path::new(ROOT));
        let lines: Vec<_> = text.lines().collect();
        assert!(lines[0].starts_with("PROVISIONAL: fs-stress qualification"), "{text}");
        assert_eq!(
            lines[1],
            "note: the preset was changed for a test (duration 20s, GC margin 5s, no fault \
             floor), so this result is not a qualification"
        );
        assert!(text.contains("\nERROR: verify hit an error: no run.json\n"), "{text}");
        assert!(text.contains("provisional: hosts still missing (b)"), "{text}");
        assert!(!text.contains("first run but not"), "{text}");
        assert!(
            text.contains(&format!("fs-stress qualify verify --root {ROOT}")),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_run_verified_before_is_refused_and_its_report_read_again() {
        let dir = tempfile::tempdir().unwrap();
        let root = QualifyRoot::new(dir.path().join("q"));
        init(&root, &Qualify::new(1, None, None, short())).await.unwrap();
        let run: RunRoot = root.run(Mode::Storage);
        let RunResult::Report(earlier) = report(Mode::Storage, Outcome::Failed) else {
            panic!("not a report");
        };
        earlier.write(&run).unwrap();
        let participants = BTreeSet::from(["a".to_owned()]);
        control::write_control(&run, &control::Control::Go { participants }).unwrap();
        let who = recovery_worker();
        LogWriter::create(&run.worker_log(&who.tag, &who.slot, who.incarnation)).unwrap();
        let err = verify::verify(&run, Duration::ZERO).await.unwrap_err();
        assert!(err.downcast_ref::<AlreadyVerified>().is_some(), "{err:#}");

        let result = verify_run(&root, Mode::Storage, Duration::ZERO).await;
        assert_eq!(result, RunResult::Report(earlier));
        assert_eq!(missing_from_second_run(&root), Vec::<String>::new());
    }

    #[tokio::test]
    async fn a_run_not_started_yet_is_provisional_and_can_be_verified_later() {
        let dir = tempfile::tempdir().unwrap();
        let root = QualifyRoot::new(dir.path().join("q"));
        init(&root, &Qualify::new(2, None, None, short())).await.unwrap();
        let run = root.run(Mode::Repository);
        let not_started = verify_run(&root, Mode::Repository, Duration::ZERO).await;
        assert_eq!(not_started, RunResult::NotStarted);
        assert!(!run.report_json().exists());
        assert!(!run.summary().exists());

        let both = combined(report(Mode::Storage, Outcome::Passed), not_started);
        assert_eq!(
            (both.outcome(), both.outcome().exit_code()),
            (Outcome::Provisional, 3)
        );
        let text = both.summary(Path::new(ROOT));
        assert!(text.starts_with("PROVISIONAL: fs-stress qualification"), "{text}");
        let section = format!(
            "repository-mode run, in {ROOT}/repository-mode:\n\
             not started yet: it starts once its hosts have registered\n"
        );
        assert!(text.contains(&section), "{text}");
        assert!(!text.contains("ERROR"), "{text}");
        let again = format!("Run `fs-stress qualify verify --root {ROOT}` again");
        assert!(text.contains(&again), "{text}");

        // Nothing written, so a later verify isn't refused.
        let report = verify::verify(&run, Duration::ZERO).await.unwrap();
        assert!(run.report_json().exists());
        assert_eq!(report.outcome(), Outcome::Error);
    }

    #[test]
    fn the_summary_names_another_build_that_initialized_it() {
        let mut both = combined(
            report(Mode::Storage, Outcome::Passed),
            report(Mode::Repository, Outcome::Passed),
        );
        let text = both.summary(Path::new(ROOT));
        assert!(!text.contains("another build"), "{text}");
        both.init_build =
            Build { icechunk_version: "2.2.0".to_owned(), commit: "abc".to_owned() };
        let text = both.summary(Path::new(ROOT));
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(
            lines[1],
            "note: another build initialized it: Icechunk 2.2.0, commit abc"
        );
    }

    #[test]
    fn missing_hosts_are_those_of_the_first_go_not_in_the_second() {
        let dir = tempfile::tempdir().unwrap();
        let root = QualifyRoot::new(dir.path().to_path_buf());
        for (mode, go) in [(Mode::Storage, "a\nb\nc\n"), (Mode::Repository, "a\nd\n")] {
            let run = root.run(mode);
            fs::create_dir_all(run.root()).unwrap();
            fs::write(run.go(), go).unwrap();
        }
        assert_eq!(missing_from_second_run(&root), ["b", "c"]);
    }

    #[test]
    fn durations_read_as_people_write_them() {
        assert_eq!(human_duration(Duration::from_secs(300)), "5m");
        assert_eq!(human_duration(Duration::from_secs(90)), "90s");
        assert_eq!(human_duration(Duration::from_millis(1500)), "1500ms");
        assert_eq!(human_duration(Duration::ZERO), "0s");
    }
}
