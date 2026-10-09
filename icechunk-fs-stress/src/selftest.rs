//! `selftest`: runs that `verify` must fail with a lost update. Each mode
//! runs once per sabotaged protocol step, and once on the legacy-local
//! backend, which overwrites `repo` unconditionally.

use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Context as _;
use uuid::Uuid;

use crate::{
    config::{
        Backend, FaultConfig, FaultFloor, Mode, PointName, RunConfig, Sabotage,
        default_stall_threshold,
    },
    env_info::default_tag,
    layout::RunRoot,
    log::{FaultAction, Record, read_all_logs},
    oracle::{RepositoryUnreadable, Violation},
    report::Report,
    supervisor,
    verify::{DEFAULT_DONE_TIMEOUT, verify},
};

pub const SABOTAGES: [Sabotage; 3] =
    [Sabotage::SkipFence, Sabotage::SkipCompare, Sabotage::TrustRenameEnoent];

/// What makes a case lose updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseKind {
    Sabotage(Sabotage),
    LegacyBackend,
}

/// Every case of one mode, in the order they run.
pub fn cases() -> impl Iterator<Item = CaseKind> {
    SABOTAGES.into_iter().map(CaseKind::Sabotage).chain([CaseKind::LegacyBackend])
}

/// The legacy backend is fast and unthrottled, so its cases write gigabytes
/// unless kept short and small.
const LEGACY_MAX_DURATION: Duration = Duration::from_secs(10);
const LEGACY_PAYLOAD_BYTES: usize = 64;

pub const ROOT_POLICY: &str = "selftest: the run roots of detected cases are deleted; \
                               those of other cases are kept and printed";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelftestOptions {
    pub duration: Duration,
    pub modes: Vec<Mode>,
    pub workers: u32,
    pub progress_every: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaseOutcome {
    Detected(String),
    /// The run passed, or failed without a lost update, or the sabotage
    /// never fired.
    Missed(String),
    /// The case couldn't be run or checked.
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelftestCase {
    pub mode: Mode,
    pub kind: CaseKind,
    /// The case's run root, unless it was deleted.
    pub kept_root: Option<PathBuf>,
    pub outcome: CaseOutcome,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelftestReport {
    pub cases: Vec<SelftestCase>,
}

impl SelftestReport {
    pub fn passed(&self) -> bool {
        self.cases.iter().all(|case| matches!(case.outcome, CaseOutcome::Detected(_)))
    }

    /// 0 if every case was detected, 1 if one was missed, 3 otherwise.
    pub fn exit_code(&self) -> u8 {
        if self.passed() {
            0
        } else if self
            .cases
            .iter()
            .any(|case| matches!(case.outcome, CaseOutcome::Missed(_)))
        {
            1
        } else {
            3
        }
    }

    pub fn summary(&self) -> String {
        let mut out = String::new();
        for case in &self.cases {
            // Writing to a `String` can't fail.
            let _ = writeln!(out, "{}", case_line(case));
        }
        out.push_str(if self.passed() { "PASS" } else { "FAIL" });
        out
    }
}

fn case_line(case: &SelftestCase) -> String {
    let (result, detail) = match &case.outcome {
        CaseOutcome::Detected(detail) => ("detected", detail),
        CaseOutcome::Missed(detail) => ("MISSED", detail),
        CaseOutcome::Error(detail) => ("ERROR", detail),
    };
    let root = case
        .kept_root
        .as_ref()
        .map(|root| format!(" ({})", root.display()))
        .unwrap_or_default();
    format!("{} {}: {result}: {detail}{root}", mode_name(case.mode), case_name(case.kind))
}

/// Whether a case's run root is kept for inspection: only if it didn't
/// behave as expected.
pub fn keep_root(outcome: &CaseOutcome) -> bool {
    !matches!(outcome, CaseOutcome::Detected(_))
}

pub fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Storage => "storage",
        Mode::Repository => "repository",
    }
}

pub fn sabotage_name(sabotage: Sabotage) -> &'static str {
    match sabotage {
        Sabotage::SkipFence => "skip-fence",
        Sabotage::SkipCompare => "skip-compare",
        Sabotage::TrustRenameEnoent => "trust-rename-enoent",
    }
}

pub fn case_name(kind: CaseKind) -> &'static str {
    match kind {
        CaseKind::Sabotage(sabotage) => sabotage_name(sabotage),
        CaseKind::LegacyBackend => Backend::LegacyLocal.name(),
    }
}

/// A one-host run with `sabotage`, pauses at the compare step long enough
/// for other writers to steal the lock, and no fault floor.
pub fn selftest_config(mode: Mode, sabotage: Sabotage, duration: Duration) -> RunConfig {
    let stale_after = Duration::from_secs(1);
    let pause = Duration::from_secs(2);
    let mut config = RunConfig::defaults(mode, 1);
    config.duration = duration;
    config.stale_after = stale_after;
    config.faults = FaultConfig {
        points: vec![PointName::Compared],
        kill_probability: 0.0,
        pause_probability: 0.05,
        pause,
    };
    config.stall_threshold = default_stall_threshold(stale_after, pause);
    config.external_kill_every = None;
    config.fault_floor = FaultFloor::ZERO;
    config.sabotage = Some(sabotage);
    config
}

/// A one-host run on the legacy-local backend, without faults or external
/// kills, of at most [`LEGACY_MAX_DURATION`] with small payloads.
pub fn legacy_config(mode: Mode, duration: Duration) -> RunConfig {
    let mut config = RunConfig::defaults(mode, 1).with_legacy_local_backend();
    config.duration = duration.min(LEGACY_MAX_DURATION);
    config.payload_bytes = LEGACY_PAYLOAD_BYTES;
    config.faults.points = vec![];
    config.external_kill_every = None;
    config
}

fn case_config(mode: Mode, kind: CaseKind, duration: Duration) -> RunConfig {
    match kind {
        CaseKind::Sabotage(sabotage) => selftest_config(mode, sabotage, duration),
        CaseKind::LegacyBackend => legacy_config(mode, duration),
    }
}

/// Runs every case in every mode of `opts`, each in a new run root under
/// `root_parent`. Only the roots of cases that weren't detected are kept.
pub async fn selftest(
    root_parent: &Path,
    opts: &SelftestOptions,
) -> anyhow::Result<SelftestReport> {
    fs::create_dir_all(root_parent)
        .with_context(|| format!("creating {}", root_parent.display()))?;
    eprintln!("{ROOT_POLICY}");
    let mut report = SelftestReport::default();
    for &mode in &opts.modes {
        for kind in cases() {
            let root = RunRoot::new(root_parent.join(format!(
                "selftest-{}-{}-{}",
                mode_name(mode),
                case_name(kind),
                Uuid::new_v4()
            )));
            let config = case_config(mode, kind, opts.duration);
            eprintln!(
                "selftest: {} {} in {}",
                mode_name(mode),
                case_name(kind),
                root.root().display()
            );
            let outcome = run_case(&root, &config, kind, opts).await;
            let kept_root = retain(root.root(), &outcome);
            let case = SelftestCase { mode, kind, kept_root, outcome };
            eprintln!("{}", case_line(&case));
            report.cases.push(case);
        }
    }
    Ok(report)
}

/// Deletes `root` unless [`keep_root`]; the root if it is still there.
fn retain(root: &Path, outcome: &CaseOutcome) -> Option<PathBuf> {
    if !keep_root(outcome) {
        match fs::remove_dir_all(root) {
            Ok(()) => return None,
            Err(err) => eprintln!("selftest: deleting {}: {err}", root.display()),
        }
    }
    Some(root.to_path_buf())
}

async fn run_case(
    root: &RunRoot,
    config: &RunConfig,
    kind: CaseKind,
    opts: &SelftestOptions,
) -> CaseOutcome {
    if let Err(err) = supervisor::init(root, config).await {
        return CaseOutcome::Error(format!("init: {err:#}"));
    }
    let tag = default_tag();
    if let Err(err) =
        supervisor::workers(root, tag, opts.workers, opts.progress_every).await
    {
        return CaseOutcome::Error(format!("workers: {err:#}"));
    }
    let verified = verify(root, DEFAULT_DONE_TIMEOUT).await;
    match (kind, verified) {
        (CaseKind::Sabotage(_), Ok(report)) => judge(&report),
        (CaseKind::LegacyBackend, Ok(report)) => judge_legacy(&report),
        (CaseKind::Sabotage(_), Err(err)) => match sabotage_fired(root) {
            Ok(fired) => judge_verify_error(&err, fired),
            Err(read) => CaseOutcome::Error(format!(
                "verify: {err:#}; counting sabotage faults: {read:#}"
            )),
        },
        // Every file the legacy backend writes is atomic, so it loses updates
        // but can't make the repository unreadable: an error is the harness's.
        (CaseKind::LegacyBackend, Err(err)) => {
            CaseOutcome::Error(format!("verify: {err:#}"))
        }
    }
}

fn lost_updates(report: &Report) -> usize {
    let violations = &report.verdict.violations;
    violations.iter().filter(|v| matches!(v, Violation::LostUpdate { .. })).count()
}

/// Detected only as a failed run with a lost update, after the sabotage fired.
pub fn judge(report: &Report) -> CaseOutcome {
    let fired = report.verdict.stats.faults.get("sabotage").copied().unwrap_or(0);
    let lost = lost_updates(report);
    if report.passed {
        CaseOutcome::Missed(format!("the run passed; the sabotage fired {fired} times"))
    } else if fired == 0 {
        CaseOutcome::Missed("the sabotage never fired".to_owned())
    } else if lost == 0 {
        CaseOutcome::Missed(format!(
            "no LostUpdate among {} violations; the sabotage fired {fired} times",
            report.verdict.violations.len()
        ))
    } else {
        CaseOutcome::Detected(format!(
            "{lost} LostUpdate; the sabotage fired {fired} times"
        ))
    }
}

/// Detected only as a failed run with a lost update. The backend loses
/// updates whenever writers race, so there is no fault to count.
pub fn judge_legacy(report: &Report) -> CaseOutcome {
    let lost = lost_updates(report);
    if report.passed {
        CaseOutcome::Missed(
            "the oracle did not detect the unsafe backend's lost updates on this \
             filesystem"
                .to_owned(),
        )
    } else if lost == 0 {
        CaseOutcome::Missed(format!(
            "no LostUpdate among {} violations",
            report.verdict.violations.len()
        ))
    } else {
        CaseOutcome::Detected(format!("{lost} LostUpdate"))
    }
}

/// A sabotaged repository can be too broken for the oracle to read; that
/// counts as detected once the sabotage fired. Any other `verify` error is the
/// harness's.
pub fn judge_verify_error(err: &anyhow::Error, fired: u64) -> CaseOutcome {
    if err.downcast_ref::<RepositoryUnreadable>().is_some() && fired > 0 {
        CaseOutcome::Detected(format!(
            "verify failed: {err:#}; the sabotage fired {fired} times"
        ))
    } else {
        CaseOutcome::Error(format!("verify: {err:#}"))
    }
}

fn sabotage_fired(root: &RunRoot) -> anyhow::Result<u64> {
    let logs = read_all_logs(root)?;
    let fired = logs
        .values()
        .flatten()
        .filter(|record| {
            matches!(record, Record::Fault { action: FaultAction::Sabotage, .. })
        })
        .count();
    Ok(u64::try_from(fired)?)
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{config::Backend, oracle::Verdict, report::Participants};

    fn report(violations: Vec<Violation>, fired: u64) -> Report {
        let mut verdict = Verdict { violations, ..Verdict::default() };
        verdict.stats.faults.insert("sabotage".to_owned(), fired);
        Report::new(
            verdict,
            selftest_config(Mode::Storage, Sabotage::SkipFence, Duration::from_secs(1)),
            Participants::default(),
            vec![],
            vec![],
            None,
            vec![],
        )
    }

    fn lost() -> Violation {
        Violation::LostUpdate {
            id: Uuid::new_v4(),
            detail: "not on the chain".to_owned(),
        }
    }

    fn stall() -> Violation {
        Violation::Stall { from_ms: 0, to_ms: 10_000 }
    }

    #[test]
    fn only_a_lost_update_after_the_sabotage_fired_is_detected() {
        assert!(matches!(
            judge(&report(vec![lost(), stall()], 2)),
            CaseOutcome::Detected(_)
        ));
        assert!(matches!(judge(&report(vec![], 2)), CaseOutcome::Missed(_)));
        assert!(matches!(judge(&report(vec![stall()], 2)), CaseOutcome::Missed(_)));
        assert!(matches!(judge(&report(vec![lost()], 0)), CaseOutcome::Missed(_)));
    }

    #[test]
    fn the_legacy_backend_is_detected_by_a_lost_update_alone() {
        assert!(matches!(
            judge_legacy(&report(vec![lost(), stall()], 0)),
            CaseOutcome::Detected(_)
        ));
        assert_eq!(
            judge_legacy(&report(vec![], 0)),
            CaseOutcome::Missed(
                "the oracle did not detect the unsafe backend's lost updates on this \
                 filesystem"
                    .to_owned()
            )
        );
        assert!(matches!(
            judge_legacy(&report(vec![stall()], 0)),
            CaseOutcome::Missed(_)
        ));
    }

    #[test]
    fn only_an_unreadable_repository_counts_as_detected() {
        let unreadable =
            anyhow::anyhow!("listing branches").context(RepositoryUnreadable);
        assert!(matches!(judge_verify_error(&unreadable, 1), CaseOutcome::Detected(_)));
        assert!(matches!(judge_verify_error(&unreadable, 0), CaseOutcome::Error(_)));
        let wrapped = unreadable.context("verifying");
        assert!(matches!(judge_verify_error(&wrapped, 1), CaseOutcome::Detected(_)));
        let other = anyhow::anyhow!("writing report.json");
        assert!(matches!(judge_verify_error(&other, 1), CaseOutcome::Error(_)));
    }

    #[test]
    fn case_configs_are_valid() {
        for mode in [Mode::Storage, Mode::Repository] {
            for sabotage in SABOTAGES {
                let config = selftest_config(mode, sabotage, Duration::from_secs(15));
                config.validate().unwrap();
                assert_eq!(config.sabotage, Some(sabotage));
                assert_eq!(config.backend, Backend::Filesystem);
                assert_eq!(config.gc, None);
            }
            let config = legacy_config(mode, Duration::from_secs(15));
            config.validate().unwrap();
            assert_eq!(config.mode, mode);
            assert_eq!(config.backend, Backend::LegacyLocal);
            assert_eq!(config.sabotage, None);
            assert_eq!(config.duration, Duration::from_secs(10));
            assert_eq!(config.payload_bytes, 64);
            assert_eq!(config.external_kill_every, None);
            assert_eq!(config.faults.kill_probability, 0.0);
            assert_eq!(config.faults.pause_probability, 0.0);
            assert_eq!(config.fault_floor, FaultFloor::ZERO);
            assert_eq!(config.gc, None);
        }
    }

    #[test]
    fn legacy_cases_run_at_most_10_seconds_with_small_payloads() {
        for (requested, expected) in [(3, 3), (10, 10), (20, 10)] {
            let duration = Duration::from_secs(requested);
            for mode in [Mode::Storage, Mode::Repository] {
                let config = case_config(mode, CaseKind::LegacyBackend, duration);
                assert_eq!(config.duration, Duration::from_secs(expected));
                assert_eq!(config.payload_bytes, 64);
                let sabotaged =
                    case_config(mode, CaseKind::Sabotage(Sabotage::SkipFence), duration);
                assert_eq!(sabotaged.duration, duration);
                assert_eq!(sabotaged.payload_bytes, 1024);
            }
        }
    }

    #[test]
    fn cases_cover_every_sabotage_then_the_legacy_backend() {
        let mut expected: Vec<_> = SABOTAGES.map(CaseKind::Sabotage).to_vec();
        expected.push(CaseKind::LegacyBackend);
        assert_eq!(cases().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn only_detected_cases_lose_their_root() {
        assert!(!keep_root(&CaseOutcome::Detected("3 LostUpdate".to_owned())));
        assert!(keep_root(&CaseOutcome::Missed("the run passed".to_owned())));
        assert!(keep_root(&CaseOutcome::Missed("no LostUpdate".to_owned())));
        assert!(keep_root(&CaseOutcome::Error("verify: boom".to_owned())));
    }

    #[test]
    fn exit_code_and_summary_cover_every_case() {
        let case = |kind, outcome: CaseOutcome| SelftestCase {
            mode: Mode::Storage,
            kind,
            kept_root: keep_root(&outcome).then(|| PathBuf::from("kept-root")),
            outcome,
        };
        let detected = |kind| case(kind, CaseOutcome::Detected("ok".to_owned()));
        let all = SelftestReport { cases: cases().map(detected).collect() };
        assert_eq!(all.exit_code(), 0);
        let summary = all.summary();
        assert_eq!(summary.lines().count(), 5, "{summary}");
        assert!(summary.ends_with("PASS"), "{summary}");
        assert!(summary.contains("storage trust-rename-enoent: detected"), "{summary}");
        assert!(summary.contains("storage legacy-local: detected"), "{summary}");
        assert!(!summary.contains("kept-root"), "{summary}");

        let mut missed = all.clone();
        missed.cases[1] = case(
            CaseKind::Sabotage(Sabotage::SkipCompare),
            CaseOutcome::Missed("the run passed".to_owned()),
        );
        missed.cases[2].outcome = CaseOutcome::Error("boom".to_owned());
        assert_eq!(missed.exit_code(), 1);
        let summary = missed.summary();
        assert!(summary.ends_with("FAIL"), "{summary}");
        assert!(
            summary.contains("storage skip-compare: MISSED: the run passed (kept-root)"),
            "{summary}"
        );

        let mut errored = all;
        errored.cases[0].outcome = CaseOutcome::Error("boom".to_owned());
        assert_eq!(errored.exit_code(), 3);
    }
}
