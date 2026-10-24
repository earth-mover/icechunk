//! The run configuration, `run.json`, written once by `init` and read by every
//! other command.

use std::{
    fs,
    num::NonZeroU16,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context as _, bail, ensure};
use icechunk_fs::fault_injection::{
    ConversionStep, FromStandardStep, Point, Sabotage as ProtocolSabotage, ToStandardStep,
};
use serde::{Deserialize, Serialize};

use crate::layout::RunRoot;

pub const RUN_CONFIG_VERSION: u32 = 6;

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
    /// Repository mode on the filesystem backend: the status toggler.
    #[serde(deserialize_with = "Option::deserialize")]
    pub readonly: Option<ReadOnlyConfig>,
    /// Filesystem backend: how often each host's sampler reads `$ROOT/repo`.
    #[serde(with = "option_millis")]
    pub mirror_sample_every: Option<Duration>,
    /// Filesystem backend: the fork actor.
    #[serde(deserialize_with = "Option::deserialize")]
    pub fork: Option<ForkConfig>,
    /// The storage root when it isn't the run root's `storage/`: a phase of
    /// the conversion scenario names the scenario's shared one. Absolute.
    #[serde(deserialize_with = "Option::deserialize")]
    pub storage: Option<PathBuf>,
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

/// A [`Point`], as named in the configuration and in fault records.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum PointName {
    Staged,
    Linked,
    BeforeRename,
    Renamed,
    BeforeTidyUp,
    BeforePublish,
    MirrorStaged,
    BeforeMirrorRename,
    MirrorRenamed,
    /// The conversions' steps, where the conversion scenario kills its
    /// converter; never in a run's fault points.
    FromStandardBackup,
    FromStandardBuild,
    FromStandardRenameDir,
    FromStandardPublish,
    ToStandardForkCheck,
    ToStandardChange,
    ToStandardPublish,
    ToStandardRenameDir,
    ToStandardDeleteDir,
}

impl PointName {
    pub fn is_conversion_step(self) -> bool {
        Point::try_from(self).is_ok_and(|point| matches!(point, Point::Conversion(_)))
    }
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

/// Every `every`, the status toggler sets the status `ReadOnly`, holds it for
/// `hold`, then sets it `Online`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOnlyConfig {
    #[serde(with = "millis")]
    pub every: Duration,
    #[serde(with = "millis")]
    pub hold: Duration,
}

/// `at` after the go, the fork actor writes a fork: `$ROOT/repo` replaced, as
/// Icechunk 2.0 to 2.3 replace it, by a repo info without the writer feature.
/// With `repeat` it forks again 500 ms after each erasure. With `during_gc`
/// (repository mode with GC, selftest only) the GC worker writes it instead,
/// once, during the first GC that starts after `at`: right after its change's
/// publication, before its hook, which must find it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkConfig {
    #[serde(with = "millis")]
    pub at: Duration,
    pub repeat: bool,
    pub during_gc: bool,
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

/// A [`ProtocolSabotage`], as named in the configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sabotage {
    SkipLink,
    RenameFromListedMarker,
    TrustRenameEnoent,
    AckUnproven,
    TidyUpDeletesCurrent,
    DeleteAboveMarker,
    UnlinkBaseBeforeRename,
    LinkAfterRename,
    DeleteWinnerFile,
    AckSupersededWithoutProof,
    PublishWithoutForkCheck,
    PublishNonCurrent,
    CommitWithoutPublish,
    GcWithoutForkCheck,
    GcPublishesListed,
    FromStandardWithoutProvenance,
    ToStandardWithoutChange,
    DeleteDirBeforePublish,
}

const DEFAULT_DURATION: Duration = Duration::from_secs(60);
const DEFAULT_PAUSE: Duration = Duration::from_secs(2);
/// No writer holds anything across steps, so a pause stalls only the paused
/// writer: the threshold doesn't depend on it.
const DEFAULT_STALL_THRESHOLD: Duration = Duration::from_secs(30);
const DEFAULT_WORKERS_PER_HOST: u32 = 8;
const DEFAULT_EXTERNAL_KILL_EVERY: Duration = Duration::from_secs(3);
const DEFAULT_PAYLOAD_BYTES: usize = 1024;
const DEFAULT_CHUNK_BYTES: usize = 1024;
const DEFAULT_GC_EVERY: Duration = Duration::from_secs(5);
/// Longer than the default pause, so that publications paused before the
/// hold end within it.
pub const DEFAULT_READONLY_HOLD: Duration = Duration::from_secs(3);
const GC_MARGIN_SLACK: Duration = Duration::from_secs(30);
/// `init`'s: a commit on a shared filesystem under load can take over a
/// minute, and GC deletes the objects of one that started before its cutoff.
const RUN_GC_MARGIN_SLACK: Duration = Duration::from_secs(180);

impl RunConfig {
    pub fn defaults(mode: Mode, hosts: u32) -> Self {
        Self {
            version: RUN_CONFIG_VERSION,
            mode,
            backend: Backend::default(),
            hosts,
            duration: DEFAULT_DURATION,
            fsync: true,
            workers_per_host_default: DEFAULT_WORKERS_PER_HOST,
            stall_threshold: DEFAULT_STALL_THRESHOLD,
            faults: FaultConfig::default(),
            external_kill_every: Some(DEFAULT_EXTERNAL_KILL_EVERY),
            fault_floor: FaultFloor::default(),
            payload_bytes: DEFAULT_PAYLOAD_BYTES,
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            gc: None,
            repo_update_retries: RetryConfig::default(),
            sabotage: None,
            readonly: None,
            mirror_sample_every: None,
            fork: None,
            storage: None,
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

    /// `run.json` of the run root at `root`, and the run root with the
    /// storage root it names: what every process that only has the run
    /// root's path works with.
    pub fn load_resolved(root: &RunRoot) -> anyhow::Result<(RunRoot, Self)> {
        let config = Self::load(root)?;
        let resolved = match &config.storage {
            None => root.clone(),
            Some(storage) => {
                ensure!(
                    root.storage_override().is_none_or(|ours| ours == storage),
                    "{} names the storage root {}, not {}",
                    root.run_json().display(),
                    storage.display(),
                    root.storage().display()
                );
                RunRoot::with_storage(root.root().to_path_buf(), storage.clone())
            }
        };
        Ok((resolved, config))
    }

    pub fn load(root: &RunRoot) -> anyhow::Result<Self> {
        let path = root.run_json();
        let bytes =
            fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        // Before the fields: another version's fields differ.
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))?;
        let version = value.get("version").and_then(serde_json::Value::as_u64);
        ensure!(
            version == Some(u64::from(RUN_CONFIG_VERSION)),
            "{}: unsupported version {}, expected {RUN_CONFIG_VERSION}: written by another \
             fs-stress build",
            path.display(),
            version.map_or_else(|| "none".to_owned(), |v| v.to_string())
        );
        let config: Self = serde_json::from_value(value)
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

    /// `init`'s check that GC, if on, can delete anything during the run.
    pub fn check_gc_margin(&self) -> anyhow::Result<()> {
        if let Some(gc) = &self.gc {
            ensure!(
                self.duration > gc.margin,
                "--gc deletes only what is older than --gc-margin ({}s), so a run of {}s \
                 deletes nothing: raise --duration or lower --gc-margin",
                gc.margin.as_secs(),
                self.duration.as_secs(),
            );
        }
        Ok(())
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
        if let Some(fork) = &self.fork {
            ensure!(
                self.backend == Backend::Filesystem,
                "the fork actor needs the filesystem backend"
            );
            ensure!(fork.at < self.duration, "fork.at must be shorter than duration");
            ensure!(
                !fork.during_gc || (self.mode == Mode::Repository && self.gc.is_some()),
                "fork.during_gc needs a repository-mode run with GC"
            );
        }
        if let Some(readonly) = &self.readonly {
            ensure!(
                self.mode == Mode::Repository && self.backend == Backend::Filesystem,
                "the status toggler needs repository mode on the filesystem backend"
            );
            check_period("readonly.every", readonly.every)?;
            check_period("readonly.hold", readonly.hold)?;
            // A hold is no stall.
            ensure!(
                readonly.hold < self.stall_threshold,
                "readonly.hold must be shorter than stall_threshold"
            );
            // Otherwise the holds run back to back.
            ensure!(
                readonly.hold < readonly.every,
                "readonly.hold must be shorter than readonly.every"
            );
        }
        if let Some(storage) = &self.storage {
            ensure!(storage.is_absolute(), "storage must be an absolute path");
        }
        if let Some(every) = self.mirror_sample_every {
            ensure!(
                self.backend == Backend::Filesystem,
                "the mirror sampler needs the filesystem backend"
            );
            check_period("mirror_sample_every", every)?;
        }
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

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            points: vec![
                PointName::Linked,
                PointName::BeforeRename,
                PointName::BeforeTidyUp,
                PointName::BeforePublish,
                PointName::BeforeMirrorRename,
            ],
            kill_probability: 0.005,
            pause_probability: 0.005,
            pause: DEFAULT_PAUSE,
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
        if let Some(step) = self.points.iter().find(|point| point.is_conversion_step()) {
            bail!("faults.points lists the conversion step {step:?}: no run converts");
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
    /// For the selftest and the conversion scenario: short runs on one host.
    pub fn defaults(pause: Duration) -> Self {
        Self { every: DEFAULT_GC_EVERY, margin: 3 * pause + GC_MARGIN_SLACK }
    }

    /// For `init`.
    pub fn run_defaults(pause: Duration) -> Self {
        Self { margin: 3 * pause + RUN_GC_MARGIN_SLACK, ..Self::defaults(pause) }
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

/// The creation's points and the fork check's reads aren't fault points: the
/// error.
impl TryFrom<Point> for PointName {
    type Error = Point;

    fn try_from(point: Point) -> Result<Self, Point> {
        use ConversionStep::{FromStandard, ToStandard};
        match point {
            Point::Staged => Ok(Self::Staged),
            Point::Linked => Ok(Self::Linked),
            Point::BeforeRename => Ok(Self::BeforeRename),
            Point::Renamed => Ok(Self::Renamed),
            Point::BeforeTidyUp => Ok(Self::BeforeTidyUp),
            Point::BeforePublish => Ok(Self::BeforePublish),
            Point::MirrorStaged => Ok(Self::MirrorStaged),
            Point::BeforeMirrorRename => Ok(Self::BeforeMirrorRename),
            Point::MirrorRenamed => Ok(Self::MirrorRenamed),
            Point::Conversion(step) => Ok(match step {
                FromStandard(FromStandardStep::Backup) => Self::FromStandardBackup,
                FromStandard(FromStandardStep::Build) => Self::FromStandardBuild,
                FromStandard(FromStandardStep::RenameDir) => Self::FromStandardRenameDir,
                FromStandard(FromStandardStep::Publish) => Self::FromStandardPublish,
                ToStandard(ToStandardStep::ForkCheck) => Self::ToStandardForkCheck,
                ToStandard(ToStandardStep::Change) => Self::ToStandardChange,
                ToStandard(ToStandardStep::Publish) => Self::ToStandardPublish,
                ToStandard(ToStandardStep::RenameDir) => Self::ToStandardRenameDir,
                ToStandard(ToStandardStep::DeleteDir) => Self::ToStandardDeleteDir,
            }),
            Point::Creation(_) | Point::BeforeMirrorRead => Err(point),
        }
    }
}

/// The protocol point a [`PointName`] names; the error for those that aren't
/// a conversion's step, which [`FaultHooks`](crate::faults::FaultHooks)
/// matches by name.
impl TryFrom<PointName> for Point {
    type Error = PointName;

    fn try_from(name: PointName) -> Result<Self, PointName> {
        use ConversionStep::{FromStandard, ToStandard};
        let step = match name {
            PointName::FromStandardBackup => FromStandard(FromStandardStep::Backup),
            PointName::FromStandardBuild => FromStandard(FromStandardStep::Build),
            PointName::FromStandardRenameDir => FromStandard(FromStandardStep::RenameDir),
            PointName::FromStandardPublish => FromStandard(FromStandardStep::Publish),
            PointName::ToStandardForkCheck => ToStandard(ToStandardStep::ForkCheck),
            PointName::ToStandardChange => ToStandard(ToStandardStep::Change),
            PointName::ToStandardPublish => ToStandard(ToStandardStep::Publish),
            PointName::ToStandardRenameDir => ToStandard(ToStandardStep::RenameDir),
            PointName::ToStandardDeleteDir => ToStandard(ToStandardStep::DeleteDir),
            _ => return Err(name),
        };
        Ok(Self::Conversion(step))
    }
}

impl From<ProtocolSabotage> for Sabotage {
    fn from(sabotage: ProtocolSabotage) -> Self {
        match sabotage {
            ProtocolSabotage::SkipLink => Self::SkipLink,
            ProtocolSabotage::RenameFromListedMarker => Self::RenameFromListedMarker,
            ProtocolSabotage::TrustRenameEnoent => Self::TrustRenameEnoent,
            ProtocolSabotage::AckUnproven => Self::AckUnproven,
            ProtocolSabotage::TidyUpDeletesCurrent => Self::TidyUpDeletesCurrent,
            ProtocolSabotage::DeleteAboveMarker => Self::DeleteAboveMarker,
            ProtocolSabotage::UnlinkBaseBeforeRename => Self::UnlinkBaseBeforeRename,
            ProtocolSabotage::LinkAfterRename => Self::LinkAfterRename,
            ProtocolSabotage::DeleteWinnerFile => Self::DeleteWinnerFile,
            ProtocolSabotage::AckSupersededWithoutProof => {
                Self::AckSupersededWithoutProof
            }
            ProtocolSabotage::PublishWithoutForkCheck => Self::PublishWithoutForkCheck,
            ProtocolSabotage::PublishNonCurrent => Self::PublishNonCurrent,
            ProtocolSabotage::CommitWithoutPublish => Self::CommitWithoutPublish,
            ProtocolSabotage::GcWithoutForkCheck => Self::GcWithoutForkCheck,
            ProtocolSabotage::GcPublishesListed => Self::GcPublishesListed,
            ProtocolSabotage::FromStandardWithoutProvenance => {
                Self::FromStandardWithoutProvenance
            }
            ProtocolSabotage::ToStandardWithoutChange => Self::ToStandardWithoutChange,
            ProtocolSabotage::DeleteDirBeforePublish => Self::DeleteDirBeforePublish,
        }
    }
}

impl Sabotage {
    /// The protocol point nearest to where the sabotage changes behaviour,
    /// named in its fault records.
    pub fn point(self) -> PointName {
        match self {
            Self::SkipLink => PointName::Linked,
            Self::RenameFromListedMarker | Self::UnlinkBaseBeforeRename => {
                PointName::BeforeRename
            }
            Self::TrustRenameEnoent
            | Self::AckUnproven
            | Self::LinkAfterRename
            | Self::DeleteWinnerFile => PointName::Renamed,
            Self::TidyUpDeletesCurrent | Self::DeleteAboveMarker => {
                PointName::BeforeTidyUp
            }
            Self::AckSupersededWithoutProof => PointName::Renamed,
            Self::PublishWithoutForkCheck => PointName::MirrorStaged,
            Self::PublishNonCurrent
            | Self::CommitWithoutPublish
            | Self::GcWithoutForkCheck
            | Self::GcPublishesListed => PointName::BeforePublish,
            Self::FromStandardWithoutProvenance => PointName::FromStandardRenameDir,
            Self::ToStandardWithoutChange => PointName::ToStandardChange,
            Self::DeleteDirBeforePublish => PointName::ToStandardRenameDir,
        }
    }

    /// Whether the sabotage breaks a conversion: the conversion scenario
    /// detects it, not a run.
    pub fn is_conversion(self) -> bool {
        matches!(
            self,
            Self::FromStandardWithoutProvenance
                | Self::ToStandardWithoutChange
                | Self::DeleteDirBeforePublish
        )
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

pub(crate) mod millis {
    use std::time::Duration;

    use serde::{Deserialize as _, Deserializer, Serializer};

    /// Saturates at `u64::MAX` milliseconds, which parsed durations never reach.
    pub(crate) fn serialize<S: Serializer>(
        duration: &Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        serializer.serialize_u64(millis)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        u64::deserialize(deserializer).map(Duration::from_millis)
    }
}

pub(crate) mod option_millis {
    use std::time::Duration;

    use serde::{Deserialize as _, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        duration: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match duration {
            Some(duration) => super::millis::serialize(duration, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
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
        repo.sabotage = Some(Sabotage::DeleteAboveMarker);
        repo.external_kill_every = None;
        repo.readonly = Some(ReadOnlyConfig {
            every: Duration::from_secs(10),
            hold: DEFAULT_READONLY_HOLD,
        });
        repo.mirror_sample_every = Some(Duration::from_millis(25));
        repo.fork = Some(ForkConfig {
            at: Duration::from_secs(20),
            repeat: true,
            during_gc: true,
        });
        repo.write_new(&root).unwrap();
        assert_eq!(RunConfig::load(&root).unwrap(), repo);

        let (_dir, root) = run_root();
        let storage = RunConfig::defaults(Mode::Storage, 1);
        storage.write_new(&root).unwrap();
        assert_eq!(RunConfig::load(&root).unwrap(), storage);
    }

    #[test]
    fn a_run_json_naming_a_storage_root_resolves_to_it() {
        let (dir, root) = run_root();
        let shared = dir.path().join("shared");
        let mut config = RunConfig::defaults(Mode::Repository, 1);
        config.storage = Some(shared.clone());
        config.write_new(&root).unwrap();
        // As a process that has only the run root's path.
        let (resolved, loaded) =
            RunConfig::load_resolved(&RunRoot::new(root.root().to_path_buf())).unwrap();
        assert_eq!(loaded, config);
        assert_eq!(resolved.storage(), shared);
        assert_eq!(resolved.logs_dir(), root.logs_dir());
        let other =
            RunRoot::with_storage(root.root().to_path_buf(), dir.path().join("other"));
        assert!(RunConfig::load_resolved(&other).is_err());

        let (_dir, root) = run_root();
        RunConfig::defaults(Mode::Storage, 1).write_new(&root).unwrap();
        assert_eq!(RunConfig::load_resolved(&root).unwrap().0, root);
    }

    /// A `run.json` of version 5, from before `storage`, is another
    /// version, not a malformed file.
    #[test]
    fn an_older_run_json_is_an_unsupported_version() {
        let (_dir, root) = run_root();
        let mut json =
            serde_json::to_value(RunConfig::defaults(Mode::Storage, 1)).unwrap();
        json.as_object_mut().unwrap().remove("storage");
        json["version"] = 5.into();
        fs::write(root.run_json(), serde_json::to_vec(&json).unwrap()).unwrap();
        let err = format!("{:#}", RunConfig::load(&root).unwrap_err());
        assert!(err.contains("unsupported version 5, expected 6"), "{err}");
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
        assert_eq!(json["version"], 6);
        assert_eq!(json["faults"]["pause"], 2000);
        assert_eq!(json["stall_threshold"], 30_000);
        assert_eq!(json["external_kill_every"], 3000);
        assert_eq!(json["gc"], serde_json::Value::Null);
        assert_eq!(json["readonly"], serde_json::Value::Null);
        assert_eq!(json["mirror_sample_every"], serde_json::Value::Null);
        assert_eq!(json["fork"], serde_json::Value::Null);

        assert_eq!(json["backend"], "filesystem");
        assert_eq!(
            json["repo_update_retries"],
            serde_json::json!({
                "max_tries": 1000,
                "initial_backoff_ms": 10,
                "max_backoff_ms": 100,
            })
        );
        for field in [
            "gc",
            "sabotage",
            "external_kill_every",
            "fsync",
            "backend",
            "readonly",
            "mirror_sample_every",
            "fork",
            "storage",
        ] {
            let mut json = json.clone();
            json.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<RunConfig>(json).is_err(), "{field}");
        }
    }

    #[test]
    fn a_run_json_of_the_lock_protocol_is_rejected() {
        let mut json =
            serde_json::to_value(RunConfig::defaults(Mode::Storage, 1)).unwrap();
        json["stale_after"] = 5000.into();
        assert!(serde_json::from_value::<RunConfig>(json.clone()).is_err());
        json.as_object_mut().unwrap().remove("stale_after");
        json["version"] = 1.into();
        let config: RunConfig = serde_json::from_value(json).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn load_rejects_malformed_run_json() {
        let (_dir, root) = run_root();
        fs::write(root.run_json(), b"{\"version\": 1").unwrap();
        let err = format!("{:#}", RunConfig::load(&root).unwrap_err());
        assert!(err.contains("run.json"), "{err}");

        let mut config =
            serde_json::to_value(RunConfig::defaults(Mode::Storage, 1)).unwrap();
        config["version"] = (RUN_CONFIG_VERSION + 1).into();
        fs::write(root.run_json(), serde_json::to_vec(&config).unwrap()).unwrap();
        assert!(RunConfig::load(&root).is_err());
    }

    #[test]
    fn defaults_validate() {
        for mode in [Mode::Storage, Mode::Repository] {
            let mut config = RunConfig::defaults(mode, 1);
            config.validate().unwrap();
            config.gc = Some(GcConfig::defaults(config.faults.pause));
            config.sabotage = Some(Sabotage::SkipLink);
            config.mirror_sample_every = Some(Duration::from_millis(25));
            config.fork = Some(ForkConfig {
                at: Duration::from_secs(5),
                repeat: true,
                during_gc: false,
            });
            config.validate().unwrap();
        }
        let mut config = RunConfig::defaults(Mode::Repository, 1);
        config.readonly = Some(ReadOnlyConfig {
            every: Duration::from_secs(10),
            hold: DEFAULT_READONLY_HOLD,
        });
        config.validate().unwrap();
    }

    #[test]
    fn validate_rejects_unusable_values() {
        const DAY: Duration = Duration::from_secs(24 * 3600);
        type Mutate = fn(&mut RunConfig);
        let cases: &[(&str, Mutate)] = &[
            ("hosts 0", |c| c.hosts = 0),
            ("version", |c| c.version = 1),
            ("zero duration", |c| c.duration = Duration::ZERO),
            ("huge duration", |c| c.duration = 31 * DAY),
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
                c.faults.points = vec![PointName::Linked, PointName::Linked];
            }),
            ("huge payload", |c| c.payload_bytes = MAX_BYTES + 1),
            ("huge chunk", |c| c.chunk_bytes = MAX_BYTES + 1),
            ("zero initial backoff", |c| c.repo_update_retries.initial_backoff_ms = 0),
            ("initial > max backoff", |c| {
                c.repo_update_retries.initial_backoff_ms = 2000;
                c.repo_update_retries.max_backoff_ms = 1000;
            }),
            ("zero readonly every", |c| {
                c.readonly = Some(ReadOnlyConfig {
                    every: Duration::ZERO,
                    hold: DEFAULT_READONLY_HOLD,
                });
            }),
            ("zero readonly hold", |c| {
                c.readonly = Some(ReadOnlyConfig {
                    every: Duration::from_secs(5),
                    hold: Duration::ZERO,
                });
            }),
            ("hold as long as the period", |c| {
                c.readonly = Some(ReadOnlyConfig {
                    every: DEFAULT_READONLY_HOLD,
                    hold: DEFAULT_READONLY_HOLD,
                });
            }),
            ("hold as long as a stall", |c| {
                c.readonly = Some(ReadOnlyConfig {
                    every: Duration::from_secs(5),
                    hold: c.stall_threshold,
                });
            }),
            ("readonly in storage mode", |c| {
                c.mode = Mode::Storage;
                c.readonly = Some(ReadOnlyConfig {
                    every: Duration::from_secs(5),
                    hold: DEFAULT_READONLY_HOLD,
                });
            }),
            ("zero sample every", |c| c.mirror_sample_every = Some(Duration::ZERO)),
            ("fork at the end", |c| {
                c.fork =
                    Some(ForkConfig { at: c.duration, repeat: false, during_gc: false });
            }),
            ("fork during GC without GC", |c| {
                c.fork = Some(ForkConfig {
                    at: Duration::from_secs(1),
                    repeat: false,
                    during_gc: true,
                });
            }),
            ("fork during GC in storage mode", |c| {
                c.mode = Mode::Storage;
                c.gc = Some(GcConfig::defaults(c.faults.pause));
                c.fork = Some(ForkConfig {
                    at: Duration::from_secs(1),
                    repeat: false,
                    during_gc: true,
                });
            }),
            ("relative storage", |c| c.storage = Some(PathBuf::from("storage"))),
            ("conversion step in the fault points", |c| {
                c.faults.points = vec![PointName::Linked, PointName::ToStandardChange];
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
            ("sabotage", |c| c.sabotage = Some(Sabotage::AckUnproven)),
        ];
        let sampler_or_toggler: &[(&str, Mutate)] = &[
            ("sampler", |c| c.mirror_sample_every = Some(Duration::from_millis(25))),
            ("fork actor", |c| {
                c.fork = Some(ForkConfig {
                    at: Duration::from_secs(1),
                    repeat: false,
                    during_gc: false,
                });
            }),
            ("toggler", |c| {
                c.readonly = Some(ReadOnlyConfig {
                    every: Duration::from_secs(5),
                    hold: DEFAULT_READONLY_HOLD,
                });
            }),
        ];
        for (name, mutate) in sampler_or_toggler {
            let mut config = legacy(Mode::Repository);
            mutate(&mut config);
            assert!(config.validate().is_err(), "{name}");
        }
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
    fn fault_and_stall_defaults() {
        let config = RunConfig::defaults(Mode::Storage, 1);
        assert_eq!(config.faults.pause, Duration::from_secs(2));
        assert_eq!(config.stall_threshold, Duration::from_secs(30));
        assert_eq!(
            config.faults.points,
            [
                PointName::Linked,
                PointName::BeforeRename,
                PointName::BeforeTidyUp,
                PointName::BeforePublish,
                PointName::BeforeMirrorRename,
            ]
        );
        assert_eq!((config.readonly, config.mirror_sample_every), (None, None));
        assert_eq!(config.fork, None);
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
    fn point_names_mirror_the_protocol_points() {
        let points = [
            (Point::Staged, PointName::Staged, "staged"),
            (Point::Linked, PointName::Linked, "linked"),
            (Point::BeforeRename, PointName::BeforeRename, "before_rename"),
            (Point::Renamed, PointName::Renamed, "renamed"),
            (Point::BeforeTidyUp, PointName::BeforeTidyUp, "before_tidy_up"),
            (Point::BeforePublish, PointName::BeforePublish, "before_publish"),
            (Point::MirrorStaged, PointName::MirrorStaged, "mirror_staged"),
            (
                Point::BeforeMirrorRename,
                PointName::BeforeMirrorRename,
                "before_mirror_rename",
            ),
            (Point::MirrorRenamed, PointName::MirrorRenamed, "mirror_renamed"),
        ];
        for (point, name, json) in points {
            assert_eq!(PointName::try_from(point), Ok(name));
            assert_eq!(Point::try_from(name), Err(name));
            assert!(!name.is_conversion_step(), "{name:?}");
            assert_eq!(serde_json::to_value(name).unwrap(), json);
        }
        use ConversionStep::{FromStandard, ToStandard};
        let steps = [
            (FromStandard(FromStandardStep::Backup), PointName::FromStandardBackup),
            (FromStandard(FromStandardStep::Build), PointName::FromStandardBuild),
            (FromStandard(FromStandardStep::RenameDir), PointName::FromStandardRenameDir),
            (FromStandard(FromStandardStep::Publish), PointName::FromStandardPublish),
            (ToStandard(ToStandardStep::ForkCheck), PointName::ToStandardForkCheck),
            (ToStandard(ToStandardStep::Change), PointName::ToStandardChange),
            (ToStandard(ToStandardStep::Publish), PointName::ToStandardPublish),
            (ToStandard(ToStandardStep::RenameDir), PointName::ToStandardRenameDir),
            (ToStandard(ToStandardStep::DeleteDir), PointName::ToStandardDeleteDir),
        ];
        for (step, name) in steps {
            assert_eq!(PointName::try_from(Point::Conversion(step)), Ok(name));
            assert_eq!(Point::try_from(name), Ok(Point::Conversion(step)));
            assert!(name.is_conversion_step(), "{name:?}");
        }
        assert_eq!(
            serde_json::to_value(PointName::ToStandardDeleteDir).unwrap(),
            "to_standard_delete_dir"
        );
        let creation = Point::Creation(icechunk_fs::fault_injection::CreationStep::Built);
        assert_eq!(PointName::try_from(creation), Err(creation));
        let read = Point::BeforeMirrorRead;
        assert_eq!(PointName::try_from(read), Err(read));
    }

    #[test]
    fn sabotages_mirror_the_protocol_sabotages() {
        use ProtocolSabotage as P;
        use Sabotage as S;
        let sabotages = [
            (P::SkipLink, S::SkipLink, "skip_link", PointName::Linked),
            (
                P::RenameFromListedMarker,
                S::RenameFromListedMarker,
                "rename_from_listed_marker",
                PointName::BeforeRename,
            ),
            (
                P::TrustRenameEnoent,
                S::TrustRenameEnoent,
                "trust_rename_enoent",
                PointName::Renamed,
            ),
            (P::AckUnproven, S::AckUnproven, "ack_unproven", PointName::Renamed),
            (
                P::TidyUpDeletesCurrent,
                S::TidyUpDeletesCurrent,
                "tidy_up_deletes_current",
                PointName::BeforeTidyUp,
            ),
            (
                P::DeleteAboveMarker,
                S::DeleteAboveMarker,
                "delete_above_marker",
                PointName::BeforeTidyUp,
            ),
            (
                P::UnlinkBaseBeforeRename,
                S::UnlinkBaseBeforeRename,
                "unlink_base_before_rename",
                PointName::BeforeRename,
            ),
            (
                P::LinkAfterRename,
                S::LinkAfterRename,
                "link_after_rename",
                PointName::Renamed,
            ),
            (
                P::DeleteWinnerFile,
                S::DeleteWinnerFile,
                "delete_winner_file",
                PointName::Renamed,
            ),
            (
                P::AckSupersededWithoutProof,
                S::AckSupersededWithoutProof,
                "ack_superseded_without_proof",
                PointName::Renamed,
            ),
            (
                P::PublishWithoutForkCheck,
                S::PublishWithoutForkCheck,
                "publish_without_fork_check",
                PointName::MirrorStaged,
            ),
            (
                P::PublishNonCurrent,
                S::PublishNonCurrent,
                "publish_non_current",
                PointName::BeforePublish,
            ),
            (
                P::CommitWithoutPublish,
                S::CommitWithoutPublish,
                "commit_without_publish",
                PointName::BeforePublish,
            ),
            (
                P::GcWithoutForkCheck,
                S::GcWithoutForkCheck,
                "gc_without_fork_check",
                PointName::BeforePublish,
            ),
            (
                P::GcPublishesListed,
                S::GcPublishesListed,
                "gc_publishes_listed",
                PointName::BeforePublish,
            ),
            (
                P::FromStandardWithoutProvenance,
                S::FromStandardWithoutProvenance,
                "from_standard_without_provenance",
                PointName::FromStandardRenameDir,
            ),
            (
                P::ToStandardWithoutChange,
                S::ToStandardWithoutChange,
                "to_standard_without_change",
                PointName::ToStandardChange,
            ),
            (
                P::DeleteDirBeforePublish,
                S::DeleteDirBeforePublish,
                "delete_dir_before_publish",
                PointName::ToStandardRenameDir,
            ),
        ];
        for (protocol, sabotage, json, point) in sabotages {
            assert_eq!(Sabotage::from(protocol), sabotage);
            assert_eq!(serde_json::to_value(sabotage).unwrap(), json);
            assert_eq!(sabotage.point(), point, "{sabotage:?}");
            assert_eq!(
                sabotage.is_conversion(),
                point.is_conversion_step(),
                "{sabotage:?}"
            );
        }
    }
}
