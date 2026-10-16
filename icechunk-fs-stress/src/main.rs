use std::process::ExitCode;

#[cfg(unix)]
fn main() -> ExitCode {
    cli::main()
}

#[cfg(not(unix))]
fn main() -> ExitCode {
    eprintln!("fs-stress is only supported on Unix platforms");
    ExitCode::FAILURE
}

#[cfg(unix)]
mod cli {
    use std::{
        path::PathBuf,
        process::ExitCode,
        time::{Duration, SystemTime},
    };

    use anyhow::{Context as _, ensure};
    use clap::{Args, Parser, Subcommand, ValueEnum};
    use icechunk_fs_stress::{
        config::{
            Backend, DEFAULT_READONLY_HOLD, FaultConfig, ForkConfig, GcConfig, Mode,
            PointName, ReadOnlyConfig, RunConfig, WorkerId, parse_duration,
        },
        control,
        convert::{self, Converter, ScenarioConfig},
        env_info::default_tag,
        fork::{self, FORK_SLOT},
        layout::{RunRoot, ScenarioRoot, is_valid_slot, is_valid_tag},
        log::Direction,
        repo_mode,
        report::Report,
        selftest::{self, SelftestOptions},
        storage_mode,
        supervisor::{
            self, ASSUMED_OPS_PER_SECOND_PER_WORKER, GC_SLOT, LateHost,
            data_volume_estimate,
        },
        toggler::{self, TOGGLER_SLOT, storage_error_counter},
        verify,
    };
    use tracing_subscriber::{
        Layer as _,
        filter::{EnvFilter, LevelFilter},
        layer::SubscriberExt as _,
        util::SubscriberInitExt as _,
    };

    /// Stress test for icechunk's filesystem storage.
    #[derive(Debug, Parser)]
    #[command(name = "fs-stress", version, args_override_self = true)]
    struct Cli {
        #[command(subcommand)]
        command: Command,
    }

    #[derive(Debug, Subcommand)]
    enum Command {
        /// Create a run root.
        Init(InitArgs),
        /// Register this host, wait for the go signal, then run workers.
        Workers(WorkersArgs),
        /// Run one worker; spawned by `workers`.
        #[command(hide = true)]
        Worker(WorkerArgs),
        /// Tell every host to stop.
        Stop(StopArgs),
        /// Check a finished run and write its report.
        Verify(VerifyArgs),
        /// Init, run workers on this host and verify, in one command.
        Run(RunArgs),
        /// Run with sabotaged protocol steps, and on the unsafe legacy local
        /// backend, and expect verify to fail.
        Selftest(SelftestArgs),
        /// Convert a repository between the standard and the filesystem
        /// layout in rounds, killing and rerunning the conversions, with a
        /// stress phase in the filesystem layout and commits in the standard
        /// one in every round, and check every conversion.
        ConvertScenario(ScenarioArgs),
        /// Run one conversion of the scenario; spawned by `convert-scenario`.
        #[command(hide = true)]
        Conversion(ConversionArgs),
    }

    impl Command {
        /// Whether an error is a harness or setup error (exit code 3) rather
        /// than a plain failure.
        fn exits_3_on_error(&self) -> bool {
            matches!(
                self,
                Self::Verify(_)
                    | Self::Run(_)
                    | Self::Selftest(_)
                    | Self::ConvertScenario(_)
            )
        }
    }

    #[derive(Debug, Args)]
    struct InitArgs {
        /// The run root, which must be missing or empty.
        #[arg(long)]
        root: PathBuf,
        /// How many hosts run `workers`; the run starts once they registered.
        #[arg(long)]
        hosts: u32,
        #[command(flatten)]
        config: ConfigArgs,
        /// `legacy-local` is `selftest`'s unsafe control: it turns faults off.
        #[arg(long, hide = true, value_enum, default_value_t = BackendArg::Filesystem)]
        backend: BackendArg,
    }

    /// The run configuration, apart from the host count.
    #[derive(Debug, Args)]
    struct ConfigArgs {
        #[arg(long)]
        mode: ModeArg,
        /// [default: 1m]
        #[arg(long, value_parser = parse_duration)]
        duration: Option<Duration>,
        /// Don't fsync storage writes.
        #[arg(long)]
        no_fsync: bool,
        /// Workers per host unless `workers --workers` says otherwise [default: 8]
        #[arg(long)]
        workers: Option<u32>,
        /// Probability of a kill at each fault point an attempt passes
        /// [default: 0.005]
        #[arg(long)]
        kill_p: Option<f64>,
        /// Probability of a pause at each fault point [default: 0.005]
        #[arg(long)]
        pause_p: Option<f64>,
        /// Length of a pause [default: 2s]
        #[arg(long, value_parser = parse_duration)]
        pause: Option<Duration>,
        /// Where kills and pauses happen [default: Linked,BeforeRename,BeforeTidyUp,
        /// BeforePublish,BeforeMirrorRename]
        /// [possible values: Staged,Linked,BeforeRename,Renamed,BeforeTidyUp,
        /// BeforePublish,MirrorStaged,BeforeMirrorRename,MirrorRenamed]
        #[arg(long, value_delimiter = ',', value_parser = parse_point)]
        fault_points: Option<Vec<PointName>>,
        /// How often each host SIGKILLs one of its workers, never the status
        /// toggler, or "off" [default: 3s]
        #[arg(long, value_parser = parse_kill_every)]
        external_kill_every: Option<KillEvery>,
        /// Storage mode: padding per payload [default: 1024]
        #[arg(long)]
        payload_bytes: Option<usize>,
        /// Repository mode: chunk size [default: 1024]
        #[arg(long)]
        chunk_bytes: Option<usize>,
        /// Repository mode: run garbage collection during the run.
        #[arg(long)]
        gc: bool,
        /// [default: 5s]
        #[arg(long, value_parser = parse_duration, requires = "gc")]
        gc_every: Option<Duration>,
        /// GC deletes what is older than this; shorter than --duration
        /// [default: 3 × pause + 3m]
        #[arg(long, value_parser = parse_duration, requires = "gc")]
        gc_margin: Option<Duration>,
        /// Repository mode: first retry back-off of a conflicting `repo`
        /// update [default: 10ms]
        #[arg(long = "initial-backoff", value_parser = parse_millis)]
        initial_backoff_ms: Option<u32>,
        /// Repository mode: longest retry back-off of a conflicting `repo`
        /// update [default: 100ms]
        #[arg(long = "max-backoff", value_parser = parse_millis)]
        max_backoff_ms: Option<u32>,
        /// Repository mode: how often the status toggler sets the status
        /// ReadOnly, holds it, then sets it Online [default: off]
        #[arg(long, value_parser = parse_duration)]
        readonly_every: Option<Duration>,
        /// How long the status toggler holds ReadOnly [default: 3s]
        #[arg(long, value_parser = parse_duration, requires = "readonly_every")]
        readonly_hold: Option<Duration>,
        /// How often every host reads repo and logs what changed, e.g. 25ms
        /// [default: off]
        #[arg(long, value_parser = parse_duration)]
        mirror_sample_every: Option<Duration>,
        /// When, after the go, the fork actor replaces repo as Icechunk 2.3
        /// does when it sets the status [default: off]
        #[arg(long, value_parser = parse_duration)]
        fork_at: Option<Duration>,
        /// Fork again 500ms after each erasure of the fork.
        #[arg(long, requires = "fork_at")]
        fork_repeat: bool,
    }

    #[derive(Debug, Clone, Copy, ValueEnum)]
    enum ModeArg {
        Storage,
        Repository,
    }

    #[derive(Debug, Clone, Copy, ValueEnum)]
    enum BackendArg {
        Filesystem,
        LegacyLocal,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
    enum SelftestMode {
        Storage,
        Repository,
        Both,
    }

    #[derive(Debug, Clone, Copy)]
    struct KillEvery(Option<Duration>);

    #[derive(Debug, Args)]
    struct WorkersArgs {
        #[arg(long)]
        root: PathBuf,
        /// This host's name in the run [default: <hostname>-<pid>]
        #[arg(long)]
        tag: Option<String>,
        /// [default: the run's workers per host]
        #[arg(long)]
        workers: Option<u32>,
        #[command(flatten)]
        progress: ProgressArgs,
    }

    #[derive(Debug, Clone, Copy, Args)]
    struct ProgressArgs {
        /// How often to print a status line to stderr; 0 for never.
        #[arg(long, value_parser = parse_progress_every, default_value = "5s")]
        progress_every: Duration,
    }

    #[derive(Debug, Args)]
    struct VerifyArgs {
        #[arg(long)]
        root: PathBuf,
        /// How long to wait for every host's done/ entry.
        #[arg(long, value_parser = parse_duration, default_value = "10m")]
        done_timeout: Duration,
    }

    #[derive(Debug, Args)]
    struct RunArgs {
        /// The run root, which must be missing or empty.
        #[arg(long)]
        root: PathBuf,
        #[command(flatten)]
        config: ConfigArgs,
        #[command(flatten)]
        progress: ProgressArgs,
    }

    #[derive(Debug, Args)]
    struct SelftestArgs {
        /// Each case runs in a new directory below this one, deleted if the
        /// case was detected and kept otherwise.
        #[arg(long)]
        root_parent: PathBuf,
        /// Duration of each case; legacy-backend cases run at most 10s.
        #[arg(long, value_parser = parse_duration, default_value = "20s")]
        duration: Duration,
        #[arg(long, value_enum, default_value_t = SelftestMode::Both)]
        mode: SelftestMode,
        /// Workers per case.
        #[arg(long, default_value_t = 8)]
        workers: u32,
        #[command(flatten)]
        progress: ProgressArgs,
    }

    #[derive(Debug, Args)]
    struct ScenarioArgs {
        /// The scenario's root, which must be missing or empty.
        #[arg(long)]
        root: PathBuf,
        #[arg(long, default_value_t = 2)]
        rounds: u32,
        /// Duration of each round's stress phase, which runs repository mode
        /// with GC; 0s for none.
        #[arg(long, value_parser = parse_progress_every, default_value = "60s")]
        duration: Duration,
        /// Workers of each stress phase.
        #[arg(long, default_value_t = 8)]
        workers: u32,
        /// Probability that a run of a conversion is killed at one of its
        /// steps, chosen at random.
        #[arg(long, default_value_t = 0.3)]
        conversion_kill_p: f64,
        /// Commits in the standard layout in each round.
        #[arg(long, default_value_t = 3)]
        standard_commits: u32,
        /// Don't fsync storage writes.
        #[arg(long)]
        no_fsync: bool,
        #[command(flatten)]
        progress: ProgressArgs,
    }

    #[derive(Debug, Args)]
    struct ConversionArgs {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        round: u32,
        #[arg(long, value_parser = parse_serde::<Direction>)]
        direction: Direction,
        #[arg(long)]
        attempt: u32,
        #[arg(long, value_parser = parse_serde::<PointName>)]
        kill_at: Option<PointName>,
    }

    #[derive(Debug, Args)]
    struct StopArgs {
        #[arg(long)]
        root: PathBuf,
    }

    #[derive(Debug, Args)]
    struct WorkerArgs {
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        tag: String,
        #[arg(long)]
        slot: String,
        #[arg(long)]
        incarnation: u32,
        /// Stop at this wall-clock time, in Unix milliseconds.
        #[arg(long)]
        until_ms: u64,
    }

    /// `workers` exits with this when the run started without it.
    const LATE_EXIT: u8 = 2;

    /// `verify`, `run` and `selftest` exit with this on a harness or setup
    /// error.
    const HARNESS_EXIT: u8 = 3;

    pub(super) fn main() -> ExitCode {
        let cli = Cli::parse();
        match init_tracing().and_then(|()| run(&cli)) {
            Ok(code) => code,
            Err(err) => {
                eprintln!("fs-stress: {err:#}");
                if err.downcast_ref::<LateHost>().is_some() {
                    ExitCode::from(LATE_EXIT)
                } else if cli.command.exits_3_on_error() {
                    ExitCode::from(HARNESS_EXIT)
                } else {
                    ExitCode::FAILURE
                }
            }
        }
    }

    /// Logs to stderr, filtered by `FS_STRESS_LOG`, and counts the storage's
    /// errors for the status toggler, whatever the filter.
    fn init_tracing() -> anyhow::Result<()> {
        let filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::WARN.into())
            .with_env_var("FS_STRESS_LOG")
            .from_env()
            .context("invalid FS_STRESS_LOG")?;
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .with(storage_error_counter())
            .init();
        Ok(())
    }

    fn run(cli: &Cli) -> anyhow::Result<ExitCode> {
        let success = |()| ExitCode::SUCCESS;
        match &cli.command {
            Command::Init(args) => init(args).map(success),
            Command::Workers(args) => workers(args).map(success),
            Command::Worker(args) => worker(args).map(success),
            Command::Stop(args) => {
                control::stop(&RunRoot::new(args.root.clone())).map(success)
            }
            Command::Verify(args) => {
                let root = RunRoot::new(args.root.clone());
                report(&runtime()?.block_on(verify::verify(&root, args.done_timeout))?)
            }
            Command::Run(args) => run_one_host(args),
            Command::Selftest(args) => selftest(args),
            Command::ConvertScenario(args) => {
                let root = ScenarioRoot::new(args.root.clone());
                let exe =
                    std::env::current_exe().context("locating the fs-stress binary")?;
                let report = runtime()?.block_on(convert::convert_scenario(
                    &root,
                    &scenario_config(args),
                    &Converter::Process(exe),
                    args.progress.progress_every,
                ))?;
                report_scenario(&root, &report)
            }
            Command::Conversion(args) => {
                let root = ScenarioRoot::new(args.root.clone());
                runtime()?
                    .block_on(convert::converter(
                        &root,
                        args.round,
                        args.direction,
                        args.attempt,
                        args.kill_at,
                    ))
                    .map(success)
            }
        }
    }

    fn report_scenario(root: &ScenarioRoot, report: &Report) -> anyhow::Result<ExitCode> {
        print!("{}", report.summary());
        println!(
            "reports: {}/report.json and phase-*/report.json",
            root.root().display()
        );
        Ok(ExitCode::from(report.exit_code()))
    }

    fn scenario_config(args: &ScenarioArgs) -> ScenarioConfig {
        let stress = (!args.duration.is_zero()).then(|| {
            let mut config = RunConfig::defaults(Mode::Repository, 1);
            config.duration = args.duration;
            config.fsync = !args.no_fsync;
            config.workers_per_host_default = args.workers;
            config.gc = Some(GcConfig::defaults(config.faults.pause));
            config
        });
        ScenarioConfig {
            rounds: args.rounds,
            stress,
            workers: args.workers,
            kill_probability: args.conversion_kill_p,
            standard_commits: args.standard_commits,
            fsync: !args.no_fsync,
            sabotage: None,
            plant_standard_write: false,
            late_commit: false,
            guaranteed_kills: true,
        }
    }

    /// A value as `serde` names it, e.g. `to_standard` or `to_standard_change`.
    fn parse_serde<T: serde::de::DeserializeOwned>(s: &str) -> anyhow::Result<T> {
        serde_json::from_value(serde_json::Value::String(s.to_owned()))
            .with_context(|| format!("unknown value {s:?}"))
    }

    fn report(report: &Report) -> anyhow::Result<ExitCode> {
        print!("{}", report.summary());
        Ok(ExitCode::from(report.exit_code()))
    }

    fn run_one_host(args: &RunArgs) -> anyhow::Result<ExitCode> {
        let config = run_config(&args.config, 1)?;
        let root = RunRoot::new(args.root.clone());
        let workers = config.workers_per_host_default;
        let progress_every = args.progress.progress_every;
        eprintln!("{}: {}", root.root().display(), volume_estimate(&config));
        report(&runtime()?.block_on(verify::run(
            &root,
            &config,
            workers,
            progress_every,
        ))?)
    }

    fn selftest(args: &SelftestArgs) -> anyhow::Result<ExitCode> {
        let modes = match args.mode {
            SelftestMode::Storage => vec![Mode::Storage],
            SelftestMode::Repository => vec![Mode::Repository],
            SelftestMode::Both => vec![Mode::Storage, Mode::Repository],
        };
        let opts = SelftestOptions {
            duration: args.duration,
            modes,
            workers: args.workers,
            progress_every: args.progress.progress_every,
        };
        let report = runtime()?.block_on(selftest::selftest(&args.root_parent, &opts))?;
        println!("{}", report.summary());
        Ok(ExitCode::from(report.exit_code()))
    }

    fn runtime() -> anyhow::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("starting the tokio runtime")
    }

    fn init(args: &InitArgs) -> anyhow::Result<()> {
        let config = init_config(args)?;
        let root = RunRoot::new(args.root.clone());
        runtime()?.block_on(supervisor::init(&root, &config))?;
        let backend = match config.backend {
            Backend::Filesystem => String::new(),
            backend => format!(" with backend: {}, unsafe", backend.name()),
        };
        println!(
            "initialized {}{backend} ({} hosts expected); {}",
            root.root().display(),
            config.hosts,
            volume_estimate(&config),
        );
        Ok(())
    }

    fn volume_estimate(config: &RunConfig) -> String {
        format!(
            "estimated data volume: at most {} (assuming \
             {ASSUMED_OPS_PER_SECOND_PER_WORKER} operations per second per worker)",
            human_bytes(data_volume_estimate(config)),
        )
    }

    fn init_config(args: &InitArgs) -> anyhow::Result<RunConfig> {
        let config = run_config(&args.config, args.hosts)?;
        Ok(match args.backend {
            BackendArg::Filesystem => config,
            BackendArg::LegacyLocal => config.with_legacy_local_backend(),
        })
    }

    fn run_config(args: &ConfigArgs, hosts: u32) -> anyhow::Result<RunConfig> {
        let mode = match args.mode {
            ModeArg::Storage => Mode::Storage,
            ModeArg::Repository => Mode::Repository,
        };
        ensure!(!args.gc || mode == Mode::Repository, "--gc needs repository mode");
        let mut config = RunConfig::defaults(mode, hosts);
        let defaults = FaultConfig::default();
        let faults = FaultConfig {
            points: args.fault_points.clone().unwrap_or(defaults.points),
            kill_probability: args.kill_p.unwrap_or(defaults.kill_probability),
            pause_probability: args.pause_p.unwrap_or(defaults.pause_probability),
            pause: args.pause.unwrap_or(defaults.pause),
        };
        config.readonly = args.readonly_every.map(|every| ReadOnlyConfig {
            every,
            hold: args.readonly_hold.unwrap_or(DEFAULT_READONLY_HOLD),
        });
        config.mirror_sample_every = args.mirror_sample_every;
        config.fork = args.fork_at.map(|at| ForkConfig {
            at,
            repeat: args.fork_repeat,
            during_gc: false,
        });
        config.gc = args.gc.then(|| {
            let defaults = GcConfig::run_defaults(faults.pause);
            GcConfig {
                every: args.gc_every.unwrap_or(defaults.every),
                margin: args.gc_margin.unwrap_or(defaults.margin),
            }
        });
        config.faults = faults;
        config.duration = args.duration.unwrap_or(config.duration);
        if let Some(gc) = &config.gc {
            ensure!(
                config.duration > gc.margin,
                "--gc deletes only what is older than --gc-margin ({}s), so a run of {}s \
                 deletes nothing: raise --duration or lower --gc-margin",
                gc.margin.as_secs(),
                config.duration.as_secs(),
            );
        }
        config.fsync = !args.no_fsync;
        config.workers_per_host_default =
            args.workers.unwrap_or(config.workers_per_host_default);
        if let Some(KillEvery(every)) = args.external_kill_every {
            config.external_kill_every = every;
        }
        config.payload_bytes = args.payload_bytes.unwrap_or(config.payload_bytes);
        config.chunk_bytes = args.chunk_bytes.unwrap_or(config.chunk_bytes);
        let retries = &mut config.repo_update_retries;
        retries.initial_backoff_ms =
            args.initial_backoff_ms.unwrap_or(retries.initial_backoff_ms);
        retries.max_backoff_ms = args.max_backoff_ms.unwrap_or(retries.max_backoff_ms);
        Ok(config)
    }

    fn parse_point(s: &str) -> anyhow::Result<PointName> {
        let points = [
            ("Staged", PointName::Staged),
            ("Linked", PointName::Linked),
            ("BeforeRename", PointName::BeforeRename),
            ("Renamed", PointName::Renamed),
            ("BeforeTidyUp", PointName::BeforeTidyUp),
            ("BeforePublish", PointName::BeforePublish),
            ("MirrorStaged", PointName::MirrorStaged),
            ("BeforeMirrorRename", PointName::BeforeMirrorRename),
            ("MirrorRenamed", PointName::MirrorRenamed),
        ];
        let key = |name: &str| name.replace('_', "").to_ascii_lowercase();
        points
            .iter()
            .find(|(name, _)| key(name) == key(s))
            .map(|&(_, point)| point)
            .with_context(|| {
                let names: Vec<_> = points.iter().map(|(name, _)| *name).collect();
                format!("unknown fault point {s:?}, expected one of {}", names.join(", "))
            })
    }

    /// A duration in whole milliseconds.
    fn parse_millis(s: &str) -> anyhow::Result<u32> {
        let millis = parse_duration(s)?.as_millis();
        u32::try_from(millis).with_context(|| format!("{s:?} is too long"))
    }

    fn parse_progress_every(s: &str) -> anyhow::Result<Duration> {
        if s == "0" {
            return Ok(Duration::ZERO);
        }
        parse_duration(s)
    }

    fn parse_kill_every(s: &str) -> anyhow::Result<KillEvery> {
        if s == "off" {
            return Ok(KillEvery(None));
        }
        Ok(KillEvery(Some(parse_duration(s)?)))
    }

    fn human_bytes(bytes: u64) -> String {
        const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
        let mut value = bytes as f64;
        let mut unit = 0;
        while value >= 1024.0 && unit + 1 < UNITS.len() {
            value /= 1024.0;
            unit += 1;
        }
        format!("{value:.1} {}", UNITS[unit])
    }

    fn workers(args: &WorkersArgs) -> anyhow::Result<()> {
        let root = RunRoot::new(args.root.clone());
        let tag = args.tag.clone().unwrap_or_else(default_tag);
        let workers = match args.workers {
            Some(workers) => workers,
            None => RunConfig::load(&root)?.workers_per_host_default,
        };
        let progress_every = args.progress.progress_every;
        let done = runtime()?.block_on(supervisor::workers(
            &root,
            tag,
            workers,
            progress_every,
        ))?;
        println!(
            "{}: {} worker incarnations, {} external kills, {} stragglers",
            done.tag, done.incarnations_spawned, done.external_kills, done.stragglers
        );
        Ok(())
    }

    fn worker(args: &WorkerArgs) -> anyhow::Result<()> {
        ensure!(is_valid_tag(&args.tag), "invalid tag {:?}", args.tag);
        ensure!(is_valid_slot(&args.slot), "invalid slot {:?}", args.slot);
        let (root, config) = RunConfig::load_resolved(&RunRoot::new(args.root.clone()))?;
        let who = WorkerId {
            tag: args.tag.clone(),
            slot: args.slot.clone(),
            incarnation: args.incarnation,
        };
        let until = SystemTime::UNIX_EPOCH + Duration::from_millis(args.until_ms);
        let runtime = runtime()?;
        if who.slot == TOGGLER_SLOT {
            ensure!(
                config.readonly.is_some(),
                "slot {TOGGLER_SLOT} needs a run with --readonly-every"
            );
            return runtime.block_on(toggler::toggler(&root, &config, &who, until));
        }
        if who.slot == FORK_SLOT {
            ensure!(config.fork.is_some(), "slot {FORK_SLOT} needs a run with --fork-at");
            return runtime.block_on(fork::fork_actor(&root, &config, &who, until));
        }
        if who.slot == GC_SLOT {
            ensure!(
                config.mode == Mode::Repository && config.gc.is_some(),
                "slot {GC_SLOT} needs a repository-mode run with --gc"
            );
            return runtime.block_on(repo_mode::gc_worker(&root, &config, &who, until));
        }
        match config.mode {
            Mode::Storage => {
                runtime.block_on(storage_mode::worker(&root, &config, &who, until))
            }
            Mode::Repository => {
                runtime.block_on(repo_mode::worker(&root, &config, &who, until))
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use icechunk_fs_stress::{config::FaultFloor, verify::DEFAULT_DONE_TIMEOUT};

        use super::*;

        fn init_args(args: &[&str]) -> InitArgs {
            let argv = ["fs-stress", "init", "--root", "r"].iter().chain(args);
            match Cli::try_parse_from(argv).unwrap().command {
                Command::Init(args) => args,
                other => panic!("{other:?}"),
            }
        }

        fn config(args: &InitArgs) -> anyhow::Result<RunConfig> {
            init_config(args)
        }

        #[test]
        fn init_defaults_match_run_config_defaults() {
            let args = init_args(&["--mode", "storage", "--hosts", "3"]);
            assert_eq!(config(&args).unwrap(), RunConfig::defaults(Mode::Storage, 3));
        }

        #[test]
        fn init_flags_set_config_and_derived_defaults_follow() {
            let args = init_args(&[
                "--mode",
                "repository",
                "--hosts",
                "2",
                "--duration",
                "10m",
                "--no-fsync",
                "--workers",
                "4",
                "--kill-p",
                "0.5",
                "--pause-p",
                "0",
                "--pause",
                "3s",
                "--fault-points",
                "Renamed,before_tidy_up,STAGED",
                "--external-kill-every",
                "off",
                "--payload-bytes",
                "10",
                "--chunk-bytes",
                "2048",
                "--gc",
                "--gc-every",
                "2s",
                "--initial-backoff",
                "5ms",
                "--max-backoff",
                "1s",
                "--readonly-every",
                "10s",
                "--readonly-hold",
                "4s",
                "--mirror-sample-every",
                "25ms",
                "--fork-at",
                "30s",
                "--fork-repeat",
            ]);
            let config = config(&args).unwrap();
            let pause = Duration::from_secs(3);
            let mut expected = RunConfig::defaults(Mode::Repository, 2);
            expected.duration = Duration::from_secs(600);
            expected.fsync = false;
            expected.workers_per_host_default = 4;
            expected.faults = FaultConfig {
                points: vec![
                    PointName::Renamed,
                    PointName::BeforeTidyUp,
                    PointName::Staged,
                ],
                kill_probability: 0.5,
                pause_probability: 0.0,
                pause,
            };
            expected.external_kill_every = None;
            expected.payload_bytes = 10;
            expected.chunk_bytes = 2048;
            expected.gc = Some(GcConfig {
                every: Duration::from_secs(2),
                margin: 3 * pause + Duration::from_secs(180),
            });
            expected.repo_update_retries.initial_backoff_ms = 5;
            expected.repo_update_retries.max_backoff_ms = 1000;
            expected.readonly = Some(ReadOnlyConfig {
                every: Duration::from_secs(10),
                hold: Duration::from_secs(4),
            });
            expected.mirror_sample_every = Some(Duration::from_millis(25));
            expected.fork = Some(ForkConfig {
                at: Duration::from_secs(30),
                repeat: true,
                during_gc: false,
            });
            assert_eq!(config, expected);
            config.validate().unwrap();
        }

        #[test]
        fn init_rejects_gc_that_deletes_nothing() {
            let gc = |flags: &[&str]| {
                let base = ["--mode", "repository", "--hosts", "1", "--gc"];
                config(&init_args(&[&base[..], flags].concat()))
            };
            assert!(gc(&[]).is_err());
            assert!(gc(&["--duration", "186s"]).is_err());
            assert!(gc(&["--duration", "187s"]).is_ok());
            assert!(gc(&["--gc-margin", "30s"]).is_ok());
            assert!(gc(&["--gc-margin", "1m"]).is_err());
        }

        #[test]
        fn later_flags_override_earlier_ones() {
            let args = init_args(&[
                "--pause",
                "1s",
                "--mode",
                "storage",
                "--hosts",
                "1",
                "--pause",
                "7s",
                "--external-kill-every",
                "off",
                "--external-kill-every",
                "2s",
            ]);
            let config = config(&args).unwrap();
            assert_eq!(config.faults.pause, Duration::from_secs(7));
            assert_eq!(config.stall_threshold, Duration::from_secs(30));
            assert_eq!(config.external_kill_every, Some(Duration::from_secs(2)));
        }

        #[test]
        fn init_rejects_bad_flags() {
            let base = ["fs-stress", "init", "--root", "r", "--hosts", "1"];
            for bad in [
                &["--mode", "nfs"][..],
                &["--mode", "storage", "--fault-points", "Nope"],
                &["--mode", "storage", "--fault-points", "Compared"],
                &["--mode", "storage", "--stale-after", "1s"],
                &["--mode", "storage", "--external-kill-every", "never"],
                &["--mode", "storage", "--duration", "5"],
                &["--mode", "repository", "--gc-every", "5s"],
                &["--mode", "repository", "--readonly-hold", "5s"],
                &["--mode", "repository", "--mirror-sample-every", "25"],
                &["--mode", "repository", "--fork-repeat"],
                &["--mode", "repository", "--fork-at", "5"],
                &["--mode", "repository", "--max-backoff", "5"],
                &["--mode", "repository", "--initial-backoff", "2000h"],
            ] {
                let argv = base.iter().chain(bad);
                assert!(Cli::try_parse_from(argv).is_err(), "{bad:?}");
            }
            let args = init_args(&["--mode", "storage", "--hosts", "1", "--gc"]);
            assert!(config(&args).is_err());
            let args = init_args(&[
                "--mode",
                "repository",
                "--hosts",
                "1",
                "--readonly-every",
                "5s",
            ]);
            assert_eq!(
                config(&args).unwrap().readonly,
                Some(ReadOnlyConfig {
                    every: Duration::from_secs(5),
                    hold: DEFAULT_READONLY_HOLD
                })
            );
            let args = init_args(&[
                "--mode",
                "storage",
                "--hosts",
                "1",
                "--readonly-every",
                "5s",
            ]);
            assert!(config(&args).unwrap().validate().is_err());
            for invalid in [
                &["--initial-backoff", "0ms"][..],
                &["--initial-backoff", "1s", "--max-backoff", "500ms"],
                &["--max-backoff", "5ms"],
            ] {
                let flags = ["--mode", "repository", "--hosts", "1"];
                let args = init_args(&[&flags[..], invalid].concat());
                assert!(config(&args).unwrap().validate().is_err(), "{invalid:?}");
            }
        }

        #[test]
        fn hidden_legacy_backend_forces_faults_off() {
            let args = init_args(&[
                "--mode",
                "repository",
                "--hosts",
                "1",
                "--backend",
                "legacy-local",
                "--kill-p",
                "0.5",
                "--pause-p",
                "0.1",
                "--external-kill-every",
                "2s",
            ]);
            let legacy = config(&args).unwrap();
            assert_eq!(legacy.backend, Backend::LegacyLocal);
            assert_eq!(legacy.faults.kill_probability, 0.0);
            assert_eq!(legacy.faults.pause_probability, 0.0);
            assert_eq!(legacy.fault_floor, FaultFloor::ZERO);
            assert_eq!(legacy.external_kill_every, Some(Duration::from_secs(2)));
            legacy.validate().unwrap();

            let args = init_args(&["--mode", "storage", "--hosts", "1"]);
            assert_eq!(config(&args).unwrap().backend, Backend::Filesystem);
            let explicit = init_args(&[
                "--mode",
                "storage",
                "--hosts",
                "1",
                "--backend",
                "filesystem",
            ]);
            assert_eq!(config(&args).unwrap(), config(&explicit).unwrap());
        }

        #[test]
        fn backend_is_hidden_and_init_only() {
            let mut cli = <Cli as clap::CommandFactory>::command();
            let help =
                cli.find_subcommand_mut("init").unwrap().render_long_help().to_string();
            assert!(help.contains("--hosts") && !help.contains("backend"), "{help}");
            let argv = [
                "fs-stress",
                "run",
                "--root",
                "r",
                "--mode",
                "storage",
                "--backend",
                "legacy-local",
            ];
            assert!(Cli::try_parse_from(argv).is_err());
        }

        fn command(args: &[&str]) -> Command {
            Cli::try_parse_from(["fs-stress"].iter().chain(args)).unwrap().command
        }

        #[test]
        fn run_takes_init_flags_without_hosts() {
            let Command::Run(args) = command(&[
                "run",
                "--root",
                "r",
                "--mode",
                "repository",
                "--gc",
                "--duration",
                "10m",
                "--workers",
                "3",
                "--progress-every",
                "0",
                "--initial-backoff",
                "20ms",
                "--max-backoff",
                "2s",
            ]) else {
                panic!("not run");
            };
            let config = run_config(&args.config, 1).unwrap();
            assert_eq!(config.repo_update_retries.initial_backoff_ms, 20);
            assert_eq!(config.repo_update_retries.max_backoff_ms, 2000);
            assert_eq!(config.hosts, 1);
            assert_eq!(config.workers_per_host_default, 3);
            assert!(config.gc.is_some());
            assert_eq!(args.progress.progress_every, Duration::ZERO);
            let argv =
                ["fs-stress", "run", "--root", "r", "--mode", "storage", "--hosts", "2"];
            assert!(Cli::try_parse_from(argv).is_err());
        }

        #[test]
        fn verify_selftest_and_workers_have_defaults() {
            let Command::Verify(verify) = command(&["verify", "--root", "r"]) else {
                panic!("not verify");
            };
            assert_eq!(verify.done_timeout, DEFAULT_DONE_TIMEOUT);
            let Command::Selftest(selftest) =
                command(&["selftest", "--root-parent", "p", "--mode", "storage"])
            else {
                panic!("not selftest");
            };
            assert_eq!(selftest.duration, Duration::from_secs(20));
            assert_eq!(selftest.mode, SelftestMode::Storage);
            assert_eq!(selftest.progress.progress_every, Duration::from_secs(5));
            let Command::Workers(workers) =
                command(&["workers", "--root", "r", "--progress-every", "1m"])
            else {
                panic!("not workers");
            };
            assert_eq!(workers.progress.progress_every, Duration::from_secs(60));
            assert!(
                Cli::try_parse_from([
                    "fs-stress",
                    "workers",
                    "--root",
                    "r",
                    "--progress-every",
                    "5"
                ])
                .is_err()
            );
        }

        #[test]
        fn convert_scenario_has_defaults_and_conversion_takes_serde_names() {
            let Command::ConvertScenario(args) =
                command(&["convert-scenario", "--root", "r"])
            else {
                panic!("not convert-scenario");
            };
            let config = scenario_config(&args);
            config.validate().unwrap();
            assert_eq!((config.rounds, config.standard_commits), (2, 3));
            assert_eq!(config.kill_probability, 0.3);
            assert!(config.guaranteed_kills);
            let stress = config.stress.unwrap();
            assert_eq!(stress.duration, Duration::from_secs(60));
            assert_eq!(stress.mode, Mode::Repository);
            assert!(stress.gc.is_some());
            let Command::ConvertScenario(args) =
                command(&["convert-scenario", "--root", "r", "--duration", "0s"])
            else {
                panic!("not convert-scenario");
            };
            assert_eq!(scenario_config(&args).stress, None);

            let Command::Conversion(args) = command(&[
                "conversion",
                "--root",
                "r",
                "--round",
                "2",
                "--direction",
                "to_standard",
                "--attempt",
                "3",
                "--kill-at",
                "to_standard_change",
            ]) else {
                panic!("not conversion");
            };
            assert_eq!(args.direction, Direction::ToStandard);
            assert_eq!(args.kill_at, Some(PointName::ToStandardChange));
            let bad = [
                "fs-stress",
                "conversion",
                "--root",
                "r",
                "--round",
                "1",
                "--attempt",
                "0",
            ];
            assert!(
                Cli::try_parse_from(bad.iter().chain(&["--direction", "sideways"]))
                    .is_err()
            );
            let mut cli = <Cli as clap::CommandFactory>::command();
            let help = cli.render_long_help().to_string();
            assert!(
                help.contains("convert-scenario") && !help.contains("conversion "),
                "{help}"
            );
        }

        #[test]
        fn human_bytes_picks_a_unit() {
            assert_eq!(human_bytes(0), "0.0 B");
            assert_eq!(human_bytes(1536), "1.5 KiB");
            assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.0 GiB");
            assert_eq!(human_bytes(u64::MAX), "16777216.0 TiB");
        }
    }
}
