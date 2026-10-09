//! The run configuration, `run.json`, written once by `init` and read by every
//! other command.

use std::{fs, num::NonZeroU16, path::Path, time::Duration};

use anyhow::{Context as _, bail, ensure};
use icechunk_fs::fault_injection::Point;
use serde::{Deserialize, Serialize};

use crate::layout::RunRoot;

pub const RUN_CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunConfig {
    pub version: u32,
    pub mode: Mode,
    pub backend: Backend,
    /// Expected supervisor registrations, at least 1.
    pub hosts: u32,
    #[serde(with = "millis")]
    pub duration: Duration,
    #[serde(with = "millis")]
    pub stale_after: Duration,
    pub fsync: bool,
    pub workers_per_host_default: u32,
    pub faults: FaultConfig,
    #[serde(with = "option_millis")]
    pub external_kill_every: Option<Duration>,
    #[serde(with = "millis")]
    pub stall_threshold: Duration,
    pub fault_floor: FaultFloor,
    /// Storage mode: padding added to every payload.
    pub payload_bytes: usize,
    /// Repository mode: size of the chunk every commit writes.
    pub chunk_bytes: usize,
    /// Repository mode only.
    #[serde(deserialize_with = "Option::deserialize")]
    pub gc: Option<GcConfig>,
    pub repo_update_retries: RetryConfig,
    /// Set only by `selftest`.
    #[serde(deserialize_with = "Option::deserialize")]
    pub sabotage: Option<Sabotage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Storage,
    Repository,
}

/// The storage the run's workers write through.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Backend {
    #[default]
    Filesystem,
    /// `object_store`'s local filesystem storage, which overwrites `repo`
    /// unconditionally: a positive control that `selftest` expects to lose
    /// updates. It takes no fault hooks.
    LegacyLocal,
}

impl Backend {
    pub fn name(self) -> &'static str {
        match self {
            Self::Filesystem => "filesystem",
            Self::LegacyLocal => "legacy-local",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultConfig {
    pub points: Vec<PointName>,
    /// Per attempt per point.
    pub kill_probability: f64,
    pub pause_probability: f64,
    #[serde(with = "millis")]
    pub pause: Duration,
}

/// A [`Point`] without its data, as named in the configuration and in fault
/// records.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PointName {
    LockInfoWritten,
    TicketWritten,
    LockTaken,
    Fenced,
    Compared,
    Renamed,
}

/// Minimum fired-fault counts for a run to count as exercised. Counts are per
/// action, summed over points; `per_point` applies to every configured point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultFloor {
    pub in_window_kill: u64,
    pub pause: u64,
    pub external_kill: u64,
    pub per_point: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcConfig {
    #[serde(with = "millis")]
    pub every: Duration,
    /// GC cutoff is `now - margin`.
    #[serde(with = "millis")]
    pub margin: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    pub max_tries: NonZeroU16,
    pub initial_backoff_ms: u32,
    pub max_backoff_ms: u32,
}

/// One incarnation of a worker: its log is `logs/<tag>.<slot>.<incarnation>.log`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerId {
    pub tag: String,
    pub slot: String,
    pub incarnation: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sabotage {
    SkipFence,
    SkipCompare,
    TrustRenameEnoent,
}

const DEFAULT_DURATION: Duration = Duration::from_secs(60);
const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(5);
const DEFAULT_WORKERS_PER_HOST: u32 = 8;
const DEFAULT_EXTERNAL_KILL_EVERY: Duration = Duration::from_secs(3);
const DEFAULT_PAYLOAD_BYTES: usize = 1024;
const DEFAULT_CHUNK_BYTES: usize = 1024;
const DEFAULT_GC_EVERY: Duration = Duration::from_secs(5);
const GC_MARGIN_SLACK: Duration = Duration::from_secs(30);

impl RunConfig {
    pub fn defaults(mode: Mode, hosts: u32) -> Self {
        let stale_after = DEFAULT_STALE_AFTER;
        let faults = FaultConfig::defaults(stale_after);
        Self {
            version: RUN_CONFIG_VERSION,
            mode,
            backend: Backend::default(),
            hosts,
            duration: DEFAULT_DURATION,
            stale_after,
            fsync: true,
            workers_per_host_default: DEFAULT_WORKERS_PER_HOST,
            stall_threshold: default_stall_threshold(stale_after, faults.pause),
            faults,
            external_kill_every: Some(DEFAULT_EXTERNAL_KILL_EVERY),
            fault_floor: FaultFloor::default(),
            payload_bytes: DEFAULT_PAYLOAD_BYTES,
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            gc: None,
            repo_update_retries: RetryConfig::default(),
            sabotage: None,
        }
    }

    /// This configuration on the legacy-local backend, which takes no fault
    /// hooks: no in-window faults and no fault floor.
    pub fn with_legacy_local_backend(mut self) -> Self {
        self.backend = Backend::LegacyLocal;
        self.faults.kill_probability = 0.0;
        self.faults.pause_probability = 0.0;
        self.fault_floor = FaultFloor::ZERO;
        self
    }

    pub fn load(root: &RunRoot) -> anyhow::Result<Self> {
        let path = root.run_json();
        let bytes =
            fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        let config: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        config.validate().with_context(|| format!("invalid {}", path.display()))?;
        Ok(config)
    }

    /// Writes `run.json`, which must not exist yet. Readers never see a
    /// partial file.
    pub fn write_new(&self, root: &RunRoot) -> anyhow::Result<()> {
        self.validate()?;
        let path = root.run_json();
        let bytes = serde_json::to_vec_pretty(self)?;
        let staging = root.root().join(format!("run.json.{}", uuid::Uuid::new_v4()));
        let linked = write_synced(&staging, &bytes)
            .and_then(|()| fs::hard_link(&staging, &path))
            .with_context(|| format!("writing {}", path.display()));
        let removed = fs::remove_file(&staging)
            .with_context(|| format!("removing {}", staging.display()));
        linked.and(removed)
    }

    /// Rejects every value later code could not use, so that a bad
    /// `run.json` is an error here rather than a panic in a worker.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.version == RUN_CONFIG_VERSION,
            "unsupported version {}, expected {RUN_CONFIG_VERSION}",
            self.version
        );
        ensure!(self.hosts >= 1, "hosts must be at least 1");
        for (name, duration) in [
            ("duration", self.duration),
            ("stale_after", self.stale_after),
            ("faults.pause", self.faults.pause),
            ("stall_threshold", self.stall_threshold),
        ] {
            check_period(name, duration)?;
        }
        if let Some(every) = self.external_kill_every {
            check_period("external_kill_every", every)?;
        }
        if let Some(gc) = &self.gc {
            check_period("gc.every", gc.every)?;
            ensure!(
                gc.margin <= MAX_DURATION,
                "gc.margin must be at most {MAX_DURATION:?}"
            );
        }
        self.faults.validate()?;
        if self.backend == Backend::LegacyLocal {
            ensure!(
                self.faults.kill_probability == 0.0
                    && self.faults.pause_probability == 0.0
                    && self.sabotage.is_none(),
                "the legacy-local backend takes no fault hooks: kill and pause \
                 probabilities must be 0 and no sabotage set"
            );
        }
        for (name, bytes) in
            [("payload_bytes", self.payload_bytes), ("chunk_bytes", self.chunk_bytes)]
        {
            ensure!(bytes <= MAX_BYTES, "{name} must be at most {MAX_BYTES}");
        }
        let retries = &self.repo_update_retries;
        // Zero would make the retry loop spin.
        ensure!(
            retries.initial_backoff_ms > 0,
            "repo_update_retries.initial_backoff_ms must be positive"
        );
        ensure!(
            retries.initial_backoff_ms <= retries.max_backoff_ms,
            "repo_update_retries.initial_backoff_ms must not exceed max_backoff_ms"
        );
        Ok(())
    }
}

/// Longer durations risk overflowing `Instant` arithmetic.
const MAX_DURATION: Duration = Duration::from_secs(30 * 24 * 3600);
const MAX_BYTES: usize = 64 * 1024 * 1024;

fn check_period(name: &str, duration: Duration) -> anyhow::Result<()> {
    ensure!(
        !duration.is_zero() && duration <= MAX_DURATION,
        "{name} must be positive and at most {MAX_DURATION:?}, got {duration:?}"
    );
    Ok(())
}

/// Creates `path`, which must not exist, with `bytes`, and fsyncs it.
pub(crate) fn write_synced(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut file = fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

pub fn default_stall_threshold(stale_after: Duration, pause: Duration) -> Duration {
    4 * stale_after + pause
}

impl FaultConfig {
    pub fn defaults(stale_after: Duration) -> Self {
        Self {
            points: vec![PointName::LockTaken, PointName::Fenced, PointName::Compared],
            kill_probability: 0.005,
            pause_probability: 0.005,
            pause: 2 * stale_after,
        }
    }
}

impl FaultConfig {
    fn validate(&self) -> anyhow::Result<()> {
        for (name, p) in [
            ("kill_probability", self.kill_probability),
            ("pause_probability", self.pause_probability),
        ] {
            ensure!((0.0..=1.0).contains(&p), "faults.{name} must be in [0, 1], got {p}");
        }
        ensure!(
            self.kill_probability + self.pause_probability <= 1.0,
            "faults.kill_probability + faults.pause_probability must be at most 1"
        );
        let mut seen = std::collections::HashSet::new();
        if let Some(dup) = self.points.iter().find(|&&point| !seen.insert(point)) {
            bail!("faults.points lists {dup:?} twice");
        }
        Ok(())
    }
}

impl FaultFloor {
    pub const ZERO: Self =
        Self { in_window_kill: 0, pause: 0, external_kill: 0, per_point: 0 };
}

impl Default for FaultFloor {
    fn default() -> Self {
        Self { in_window_kill: 3, pause: 3, external_kill: 10, per_point: 1 }
    }
}

impl GcConfig {
    pub fn defaults(pause: Duration) -> Self {
        Self { every: DEFAULT_GC_EVERY, margin: 3 * pause + GC_MARGIN_SLACK }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_tries: NonZeroU16::new(1000).unwrap_or(NonZeroU16::MAX),
            initial_backoff_ms: 10,
            max_backoff_ms: 100,
        }
    }
}

impl From<Point> for PointName {
    fn from(point: Point) -> Self {
        match point {
            Point::LockInfoWritten => Self::LockInfoWritten,
            Point::TicketWritten => Self::TicketWritten,
            Point::LockTaken { .. } => Self::LockTaken,
            Point::Fenced => Self::Fenced,
            Point::Compared => Self::Compared,
            Point::Renamed => Self::Renamed,
        }
    }
}

impl PointName {
    pub fn matches(self, point: Point) -> bool {
        self == Self::from(point)
    }
}

/// Parses a non-negative integer with a unit: `500ms`, `5s`, `2m` or `1h`.
pub fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (digits, unit) = s.split_at(split);
    let invalid = || format!("invalid duration {s:?}, expected e.g. 500ms, 5s, 2m or 1h");
    if digits.is_empty() {
        bail!(invalid());
    }
    let n: u64 = digits.parse().with_context(invalid)?;
    let millis_per_unit = match unit {
        "ms" => 1,
        "s" => 1000,
        "m" => 60_000,
        "h" => 3_600_000,
        _ => bail!(invalid()),
    };
    let millis = n.checked_mul(millis_per_unit).with_context(invalid)?;
    Ok(Duration::from_millis(millis))
}

mod millis {
    use std::time::Duration;

    use serde::{Deserialize as _, Deserializer, Serializer, ser::Error as _};

    pub(super) fn serialize<S: Serializer>(
        duration: &Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let millis = u64::try_from(duration.as_millis())
            .map_err(|_| S::Error::custom("duration too long"))?;
        serializer.serialize_u64(millis)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        u64::deserialize(deserializer).map(Duration::from_millis)
    }
}

mod option_millis {
    use std::time::Duration;

    use serde::{Deserialize as _, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        duration: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match duration {
            Some(duration) => super::millis::serialize(duration, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        Option::<u64>::deserialize(deserializer).map(|ms| ms.map(Duration::from_millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_root() -> (tempfile::TempDir, RunRoot) {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().to_path_buf());
        (dir, root)
    }

    #[test]
    fn defaults_round_trip_through_run_json() {
        let (_dir, root) = run_root();
        let mut repo = RunConfig::defaults(Mode::Repository, 3);
        repo.gc = Some(GcConfig::defaults(repo.faults.pause));
        repo.sabotage = Some(Sabotage::TrustRenameEnoent);
        repo.external_kill_every = None;
        repo.write_new(&root).unwrap();
        assert_eq!(RunConfig::load(&root).unwrap(), repo);

        let (_dir, root) = run_root();
        let storage = RunConfig::defaults(Mode::Storage, 1);
        storage.write_new(&root).unwrap();
        assert_eq!(RunConfig::load(&root).unwrap(), storage);
    }

    #[test]
    fn run_json_is_written_once() {
        let (dir, root) = run_root();
        let config = RunConfig::defaults(Mode::Storage, 1);
        config.write_new(&root).unwrap();
        assert!(config.write_new(&root).is_err());
        let names: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, ["run.json"]);
    }

    #[test]
    fn durations_are_milliseconds_and_every_field_is_required() {
        let config = RunConfig::defaults(Mode::Storage, 2);
        let json = serde_json::to_value(&config).unwrap();
        assert_eq!(json["stale_after"], 5000);
        assert_eq!(json["faults"]["pause"], 10_000);
        assert_eq!(json["external_kill_every"], 3000);
        assert_eq!(json["gc"], serde_json::Value::Null);

        assert_eq!(json["backend"], "filesystem");
        assert_eq!(
            json["repo_update_retries"],
            serde_json::json!({
                "max_tries": 1000,
                "initial_backoff_ms": 10,
                "max_backoff_ms": 100,
            })
        );
        for field in ["gc", "sabotage", "external_kill_every", "fsync", "backend"] {
            let mut json = json.clone();
            json.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<RunConfig>(json).is_err(), "{field}");
        }
    }

    #[test]
    fn load_rejects_malformed_run_json() {
        let (_dir, root) = run_root();
        fs::write(root.run_json(), b"{\"version\": 1").unwrap();
        let err = format!("{:#}", RunConfig::load(&root).unwrap_err());
        assert!(err.contains("run.json"), "{err}");

        let mut config =
            serde_json::to_value(RunConfig::defaults(Mode::Storage, 1)).unwrap();
        config["version"] = 2.into();
        fs::write(root.run_json(), serde_json::to_vec(&config).unwrap()).unwrap();
        assert!(RunConfig::load(&root).is_err());
    }

    #[test]
    fn defaults_validate() {
        for mode in [Mode::Storage, Mode::Repository] {
            let mut config = RunConfig::defaults(mode, 1);
            config.validate().unwrap();
            config.gc = Some(GcConfig::defaults(config.faults.pause));
            config.sabotage = Some(Sabotage::SkipFence);
            config.validate().unwrap();
        }
    }

    #[test]
    fn validate_rejects_unusable_values() {
        const DAY: Duration = Duration::from_secs(24 * 3600);
        type Mutate = fn(&mut RunConfig);
        let cases: &[(&str, Mutate)] = &[
            ("hosts 0", |c| c.hosts = 0),
            ("version", |c| c.version = 2),
            ("zero duration", |c| c.duration = Duration::ZERO),
            ("huge duration", |c| c.duration = 31 * DAY),
            ("zero stale_after", |c| c.stale_after = Duration::ZERO),
            ("huge stale_after", |c| c.stale_after = Duration::MAX),
            ("zero pause", |c| c.faults.pause = Duration::ZERO),
            ("huge pause", |c| c.faults.pause = 31 * DAY),
            ("zero stall", |c| c.stall_threshold = Duration::ZERO),
            ("huge stall", |c| c.stall_threshold = 31 * DAY),
            ("zero kill every", |c| c.external_kill_every = Some(Duration::ZERO)),
            ("huge kill every", |c| c.external_kill_every = Some(31 * DAY)),
            ("zero gc every", |c| {
                c.gc = Some(GcConfig { every: Duration::ZERO, margin: Duration::ZERO });
            }),
            ("huge gc margin", |c| {
                c.gc = Some(GcConfig { every: Duration::from_secs(5), margin: 31 * DAY });
            }),
            ("kill > 1", |c| c.faults.kill_probability = 1.5),
            ("kill < 0", |c| c.faults.kill_probability = -0.1),
            ("kill NaN", |c| c.faults.kill_probability = f64::NAN),
            ("pause > 1", |c| c.faults.pause_probability = 1.01),
            ("pause < 0", |c| c.faults.pause_probability = -1.0),
            ("pause NaN", |c| c.faults.pause_probability = f64::NAN),
            ("sum > 1", |c| {
                c.faults.kill_probability = 0.6;
                c.faults.pause_probability = 0.5;
            }),
            ("duplicate point", |c| {
                c.faults.points = vec![PointName::Fenced, PointName::Fenced];
            }),
            ("huge payload", |c| c.payload_bytes = MAX_BYTES + 1),
            ("huge chunk", |c| c.chunk_bytes = MAX_BYTES + 1),
            ("zero initial backoff", |c| c.repo_update_retries.initial_backoff_ms = 0),
            ("initial > max backoff", |c| {
                c.repo_update_retries.initial_backoff_ms = 2000;
                c.repo_update_retries.max_backoff_ms = 1000;
            }),
        ];
        for (name, mutate) in cases {
            let mut config = RunConfig::defaults(Mode::Repository, 1);
            mutate(&mut config);
            assert!(config.validate().is_err(), "{name}");
        }

        let mut edge = RunConfig::defaults(Mode::Repository, 1);
        edge.faults.kill_probability = 0.5;
        edge.faults.pause_probability = 0.5;
        edge.faults.points = vec![];
        edge.external_kill_every = None;
        edge.gc = Some(GcConfig { every: MAX_DURATION, margin: Duration::ZERO });
        edge.duration = MAX_DURATION;
        edge.payload_bytes = MAX_BYTES;
        edge.repo_update_retries.initial_backoff_ms =
            edge.repo_update_retries.max_backoff_ms;
        edge.validate().unwrap();
    }

    fn legacy(mode: Mode) -> RunConfig {
        RunConfig::defaults(mode, 1).with_legacy_local_backend()
    }

    #[test]
    fn legacy_backend_round_trips_and_validates_without_faults() {
        let (_dir, root) = run_root();
        let config = legacy(Mode::Repository);
        assert_eq!(serde_json::to_value(&config).unwrap()["backend"], "legacy-local");
        config.write_new(&root).unwrap();
        assert_eq!(RunConfig::load(&root).unwrap(), config);
        legacy(Mode::Storage).validate().unwrap();
    }

    #[test]
    fn validate_rejects_legacy_backend_with_faults_or_sabotage() {
        type Mutate = fn(&mut RunConfig);
        let cases: &[(&str, Mutate)] = &[
            ("kill", |c| c.faults.kill_probability = 0.1),
            ("pause", |c| c.faults.pause_probability = 0.1),
            ("sabotage", |c| c.sabotage = Some(Sabotage::SkipCompare)),
        ];
        for (name, mutate) in cases {
            for mode in [Mode::Storage, Mode::Repository] {
                let mut config = legacy(mode);
                mutate(&mut config);
                let err = format!("{:#}", config.validate().unwrap_err());
                assert!(err.contains("legacy-local"), "{name}: {err}");
            }
        }
    }

    #[test]
    fn load_reports_out_of_range_values() {
        let (_dir, root) = run_root();
        let mut config =
            serde_json::to_value(RunConfig::defaults(Mode::Storage, 1)).unwrap();
        config["faults"]["kill_probability"] = 1.5.into();
        fs::write(root.run_json(), serde_json::to_vec(&config).unwrap()).unwrap();
        let err = format!("{:#}", RunConfig::load(&root).unwrap_err());
        assert!(err.contains("run.json") && err.contains("kill_probability"), "{err}");
    }

    #[test]
    fn stall_threshold_defaults_to_four_stale_after_plus_pause() {
        let config = RunConfig::defaults(Mode::Storage, 1);
        assert_eq!(config.faults.pause, 2 * config.stale_after);
        assert_eq!(config.stall_threshold, 4 * config.stale_after + config.faults.pause);
        assert_eq!(config.stall_threshold, Duration::from_secs(30));
    }

    #[test]
    fn parse_duration_accepts_units() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("0s").unwrap(), Duration::ZERO);
    }

    #[test]
    fn parse_duration_rejects_malformed() {
        for bad in [
            "5",
            "-1s",
            "abc",
            "",
            "s",
            "1.5s",
            "5 s",
            "+5s",
            "5sec",
            "99999999999999999999h",
        ] {
            assert!(parse_duration(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn point_names_ignore_generation() {
        assert_eq!(
            PointName::from(Point::LockTaken { generation: 7 }),
            PointName::LockTaken
        );
        assert!(PointName::LockTaken.matches(Point::LockTaken { generation: 0 }));
        assert!(!PointName::Fenced.matches(Point::Compared));
        assert!(PointName::Renamed.matches(Point::Renamed));
    }
}
