//! `selftest`: runs that `verify` must fail. Each case turns on one
//! sabotaged step of the protocol and names the violations that must detect
//! it; it runs in each mode it can be detected in. The conversions' cases
//! run the conversion scenario instead of a run. Each mode also runs once on
//! the legacy-local backend, which overwrites `repo` unconditionally and must
//! lose updates.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Context as _;
use uuid::Uuid;

use crate::{
    config::{Backend, FaultConfig, FaultFloor, Mode, PointName, RunConfig, Sabotage},
    convert::{Converter, ScenarioConfig, convert_scenario},
    env_info::default_tag,
    layout::{RunRoot, ScenarioRoot},
    log::{FaultAction, Record, read_all_logs},
    oracle::RepositoryUnreadable,
    report::Report,
    supervisor,
    verify::{DEFAULT_DONE_TIMEOUT, verify},
};

pub const SABOTAGES: [Sabotage; 13] = [
    Sabotage::SkipLink,
    Sabotage::RenameFromListedMarker,
    Sabotage::TrustRenameEnoent,
    Sabotage::AckUnproven,
    Sabotage::TidyUpDeletesCurrent,
    Sabotage::DeleteAboveMarker,
    Sabotage::UnlinkBaseBeforeRename,
    Sabotage::LinkAfterRename,
    Sabotage::DeleteWinnerFile,
    Sabotage::AckSupersededWithoutProof,
    Sabotage::FromObjectStoreWithoutProvenance,
    Sabotage::ToObjectStoreWithoutChange,
    Sabotage::DeleteDirBeforeRepoWrite,
];

/// What a case breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseKind {
    Sabotage(Sabotage),
    LegacyBackend,
}

/// The violations that detect `sabotage`: a run with it must fail with one
/// of them after it fired.
pub fn detected_by(sabotage: Sabotage) -> &'static [&'static str] {
    match sabotage {
        Sabotage::SkipLink
        | Sabotage::RenameFromListedMarker
        | Sabotage::TrustRenameEnoent
        | Sabotage::AckUnproven
        | Sabotage::TidyUpDeletesCurrent
        | Sabotage::DeleteAboveMarker
        | Sabotage::UnlinkBaseBeforeRename
        | Sabotage::LinkAfterRename
        | Sabotage::DeleteWinnerFile => &["LostUpdate", "BrokenHistory"],
        Sabotage::AckSupersededWithoutProof | Sabotage::ToObjectStoreWithoutChange => {
            &["LostUpdate"]
        }
        Sabotage::FromObjectStoreWithoutProvenance => &["ConversionEndState"],
        Sabotage::DeleteDirBeforeRepoWrite => {
            &["ConversionEndState", "DeletedHistory", "LostUpdate"]
        }
    }
}

/// The modes a sabotage's case runs in: repository mode alone for those the
/// conversion scenario detects.
pub fn modes(sabotage: Sabotage) -> &'static [Mode] {
    if sabotage.is_conversion() {
        &[Mode::Repository]
    } else {
        &[Mode::Storage, Mode::Repository]
    }
}

/// Every case of `mode`, in the order they run.
pub fn cases(mode: Mode) -> impl Iterator<Item = CaseKind> {
    SABOTAGES
        .into_iter()
        .filter(move |&sabotage| modes(sabotage).contains(&mode))
        .map(CaseKind::Sabotage)
        .chain([CaseKind::LegacyBackend])
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
    /// The run passed, or failed without a detecting violation, or the
    /// sabotage never fired.
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
    /// Every case was detected.
    pub fn passed(&self) -> bool {
        self.cases.iter().all(|case| !keep_root(&case.outcome))
    }

    /// 0 if every case passed, 1 if one was missed, 3 otherwise.
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
    matches!(outcome, CaseOutcome::Missed(_) | CaseOutcome::Error(_))
}

pub fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::Storage => "storage",
        Mode::Repository => "repository",
    }
}

pub fn sabotage_name(sabotage: Sabotage) -> &'static str {
    match sabotage {
        Sabotage::SkipLink => "skip-link",
        Sabotage::RenameFromListedMarker => "rename-from-listed-marker",
        Sabotage::TrustRenameEnoent => "trust-rename-enoent",
        Sabotage::AckUnproven => "ack-unproven",
        Sabotage::TidyUpDeletesCurrent => "tidy-up-deletes-current",
        Sabotage::DeleteAboveMarker => "delete-above-marker",
        Sabotage::UnlinkBaseBeforeRename => "unlink-base-before-rename",
        Sabotage::LinkAfterRename => "link-after-rename",
        Sabotage::DeleteWinnerFile => "delete-winner-file",
        Sabotage::AckSupersededWithoutProof => "ack-superseded-without-proof",
        Sabotage::FromObjectStoreWithoutProvenance => {
            "from-object-store-without-provenance"
        }
        Sabotage::ToObjectStoreWithoutChange => "to-object-store-without-change",
        Sabotage::DeleteDirBeforeRepoWrite => "delete-dir-before-repo-write",
    }
}

pub fn case_name(kind: CaseKind) -> &'static str {
    match kind {
        CaseKind::Sabotage(sabotage) => sabotage_name(sabotage),
        CaseKind::LegacyBackend => Backend::LegacyLocal.name(),
    }
}

/// A one-host run with `sabotage` and no fault floor. Writers pause before
/// the rename, so that others commit in between and they race on stale
/// bases. A sabotage that breaks the repository only if the writer dies in
/// its window also gets kills there, and the supervisor respawns the killed
/// workers.
pub fn selftest_config(mode: Mode, sabotage: Sabotage, duration: Duration) -> RunConfig {
    let (points, kill_probability) = match sabotage {
        // Unlinked the base, not yet renamed. Often, because a kill only
        // breaks the repository when no other writer on that base survives
        // to rename past it; see `case_workers`.
        Sabotage::UnlinkBaseBeforeRename => (vec![PointName::BeforeRename], 0.3),
        // Renamed, not yet linked.
        Sabotage::LinkAfterRename => {
            (vec![PointName::BeforeRename, PointName::Renamed], 0.05)
        }
        // Commits land while the paused committer has yet to tidy up, so its
        // tidy-up finds the new current version above its own.
        Sabotage::DeleteAboveMarker => {
            (vec![PointName::BeforeRename, PointName::BeforeTidyUp], 0.0)
        }
        _ => (vec![PointName::BeforeRename], 0.0),
    };
    let mut config = RunConfig::defaults(mode, 1);
    config.duration = duration;
    config.faults = FaultConfig {
        points,
        kill_probability,
        pause_probability: 0.05,
        pause: Duration::from_secs(2),
    };
    config.external_kill_every = None;
    config.fault_floor = FaultFloor::ZERO;
    config.sabotage = Some(sabotage);
    config
}

/// The conversion scenario of a conversion's sabotage: one round without a
/// stress phase or random kills, with the emulation that makes the sabotage
/// matter.
pub fn scenario_config(sabotage: Sabotage, workers: u32) -> ScenarioConfig {
    ScenarioConfig {
        rounds: 1,
        stress: None,
        workers,
        kill_probability: 0.0,
        object_store_commits: 3,
        fsync: true,
        sabotage: Some(sabotage),
        plant_object_store_write: sabotage == Sabotage::FromObjectStoreWithoutProvenance,
        late_commit: sabotage == Sabotage::ToObjectStoreWithoutChange,
        kill_after_deleting_repo_d: sabotage == Sabotage::DeleteDirBeforeRepoWrite,
        guaranteed_kills: false,
    }
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
        for kind in cases(mode) {
            let root = root_parent.join(format!(
                "selftest-{}-{}-{}",
                mode_name(mode),
                case_name(kind),
                Uuid::new_v4()
            ));
            eprintln!(
                "selftest: {} {} in {}",
                mode_name(mode),
                case_name(kind),
                root.display()
            );
            let outcome = match kind {
                CaseKind::Sabotage(sabotage) if sabotage.is_conversion() => {
                    run_scenario_case(&ScenarioRoot::new(root.clone()), sabotage, opts)
                        .await
                }
                kind => {
                    let config = case_config(mode, kind, opts.duration);
                    run_case(&RunRoot::new(root.clone()), &config, kind, opts).await
                }
            };
            let kept_root = retain(&root, &outcome);
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

/// The writers of a case: at most two for `UnlinkBaseBeforeRename`: a writer
/// killed after unlinking its base leaves the marker without bytes only until
/// another writer that read that base renames past it, which under many
/// writers one almost always does.
pub fn case_workers(kind: CaseKind, workers: u32) -> u32 {
    match kind {
        CaseKind::Sabotage(Sabotage::UnlinkBaseBeforeRename) => workers.min(2),
        _ => workers,
    }
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
    let workers = case_workers(kind, opts.workers);
    if let Err(err) = supervisor::workers(root, tag, workers, opts.progress_every).await {
        return CaseOutcome::Error(format!("workers: {err:#}"));
    }
    let verified = verify(root, DEFAULT_DONE_TIMEOUT).await;
    match (kind, verified) {
        (CaseKind::Sabotage(sabotage), Ok(report)) => judge(&report, sabotage),
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

async fn run_scenario_case(
    root: &ScenarioRoot,
    sabotage: Sabotage,
    opts: &SelftestOptions,
) -> CaseOutcome {
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(err) => {
            return CaseOutcome::Error(format!("locating the fs-stress binary: {err}"));
        }
    };
    let config = scenario_config(sabotage, opts.workers);
    match convert_scenario(root, &config, &Converter::Process(exe), opts.progress_every)
        .await
    {
        Ok(report) => judge(&report, sabotage),
        Err(err) => CaseOutcome::Error(format!("convert-scenario: {err:#}")),
    }
}

/// Detected only as a failed run, after the sabotage fired, with one of the
/// violations that [`detected_by`] names.
pub fn judge(report: &Report, sabotage: Sabotage) -> CaseOutcome {
    let fired = report.verdict.stats.faults.get("sabotage").copied().unwrap_or(0);
    let expected = detected_by(sabotage);
    let mut found: BTreeMap<&str, usize> = BTreeMap::new();
    for violation in &report.verdict.violations {
        if expected.contains(&violation.kind()) {
            *found.entry(violation.kind()).or_default() += 1;
        }
    }
    if fired > 0 && !found.is_empty() {
        let found: Vec<_> = found.iter().map(|(kind, n)| format!("{n} {kind}")).collect();
        return CaseOutcome::Detected(format!(
            "{}; the sabotage fired {fired} times",
            found.join(", ")
        ));
    }
    if report.passed {
        CaseOutcome::Missed(format!("the run passed; the sabotage fired {fired} times"))
    } else if fired == 0 {
        CaseOutcome::Missed("the sabotage never fired".to_owned())
    } else {
        CaseOutcome::Missed(format!(
            "no {} among {} violations; the sabotage fired {fired} times",
            expected.join(" or "),
            report.verdict.violations.len()
        ))
    }
}

/// Detected only as a failed run with a lost update. The backend loses
/// updates whenever writers race, so there is no fault to count.
pub fn judge_legacy(report: &Report) -> CaseOutcome {
    let lost = report
        .verdict
        .violations
        .iter()
        .filter(|violation| violation.kind() == "LostUpdate")
        .count();
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
    use crate::{
        config::Backend,
        log::Direction,
        oracle::{Verdict, Violation},
        report::Participants,
    };

    fn report(violations: Vec<Violation>, fired: u64) -> Report {
        let mut verdict = Verdict { violations, ..Verdict::default() };
        verdict.stats.faults.insert("sabotage".to_owned(), fired);
        Report::new(
            verdict,
            selftest_config(Mode::Storage, Sabotage::SkipLink, Duration::from_secs(1)),
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

    fn broken() -> Violation {
        Violation::BrokenHistory { detail: "reading repo: no consistent view".to_owned() }
    }

    /// A violation of `kind`, one of those [`detected_by`] names.
    fn violation(kind: &str) -> Violation {
        let detail = String::new;
        let violation = match kind {
            "LostUpdate" => lost(),
            "BrokenHistory" => broken(),
            "ConversionEndState" => Violation::ConversionEndState {
                direction: Direction::ToObjectStore,
                detail: detail(),
            },
            "DeletedHistory" => Violation::DeletedHistory { detail: detail() },
            other => panic!("no example of {other}"),
        };
        assert_eq!(violation.kind(), kind);
        violation
    }

    #[test]
    fn each_sabotage_is_detected_by_its_violations_after_it_fired() {
        for sabotage in SABOTAGES {
            for &kind in detected_by(sabotage) {
                assert!(
                    matches!(
                        judge(&report(vec![violation(kind), stall()], 2), sabotage),
                        CaseOutcome::Detected(detail) if detail.contains(kind) && detail.contains("fired 2 times")
                    ),
                    "{sabotage:?} {kind}"
                );
                let never_fired = judge(&report(vec![violation(kind)], 0), sabotage);
                assert!(
                    !matches!(never_fired, CaseOutcome::Detected(_)),
                    "{sabotage:?} {kind}"
                );
            }
            for (others, fired) in [
                (vec![], 0),
                (vec![], 2),
                (vec![stall()], 2),
                (vec![Violation::Recovery { detail: "no".to_owned() }], 2),
            ] {
                let outcome = judge(&report(others, fired), sabotage);
                assert!(
                    matches!(outcome, CaseOutcome::Missed(_)),
                    "{sabotage:?}: {outcome:?}"
                );
            }
        }
        // Another sabotage's violation detects nothing.
        let outcome =
            judge(&report(vec![violation("DeletedHistory")], 2), Sabotage::SkipLink);
        assert!(
            matches!(outcome, CaseOutcome::Missed(detail) if detail.contains("LostUpdate or BrokenHistory"))
        );
        assert_eq!(
            judge(&report(vec![lost(), stall(), broken()], 2), Sabotage::SkipLink),
            CaseOutcome::Detected(
                "1 BrokenHistory, 1 LostUpdate; the sabotage fired 2 times".to_owned()
            )
        );
    }

    #[test]
    fn the_legacy_backend_is_detected_by_a_lost_update_alone() {
        assert!(matches!(
            judge_legacy(&report(vec![lost(), stall()], 0)),
            CaseOutcome::Detected(_)
        ));
        assert!(matches!(
            judge_legacy(&report(vec![broken()], 0)),
            CaseOutcome::Missed(_)
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
        let duration = Duration::from_secs(15);
        for mode in [Mode::Storage, Mode::Repository] {
            for sabotage in SABOTAGES.into_iter().filter(|s| modes(*s).contains(&mode)) {
                if sabotage.is_conversion() {
                    let config = scenario_config(sabotage, 8);
                    config.validate().unwrap();
                    assert_eq!(config.sabotage, Some(sabotage));
                    assert_eq!(config.stress, None);
                    assert_eq!(config.kill_probability, 0.0);
                    continue;
                }
                let config = selftest_config(mode, sabotage, duration);
                config.validate().unwrap();
                assert_eq!(config.sabotage, Some(sabotage));
                assert_eq!(config.backend, Backend::Filesystem);
                assert_eq!(config.external_kill_every, None);
                assert_eq!(config.fault_floor, FaultFloor::ZERO);
                assert!(config.faults.points.contains(&PointName::BeforeRename));
                assert!(config.faults.pause_probability > 0.0);
                let kill_point = match sabotage {
                    Sabotage::UnlinkBaseBeforeRename => Some(PointName::BeforeRename),
                    Sabotage::LinkAfterRename => Some(PointName::Renamed),
                    _ => None,
                };
                match kill_point {
                    Some(point) => {
                        assert!(config.faults.points.contains(&point), "{sabotage:?}");
                        assert!(config.faults.kill_probability >= 0.05, "{sabotage:?}");
                    }
                    None => assert_eq!(config.faults.kill_probability, 0.0),
                }
                assert_eq!(
                    config.faults.points.contains(&PointName::BeforeTidyUp),
                    sabotage == Sabotage::DeleteAboveMarker,
                    "{sabotage:?}"
                );
                assert_eq!(config.gc, None, "{sabotage:?}");
                assert_eq!(config.readonly, None, "{sabotage:?}");
            }
            let config = legacy_config(mode, duration);
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
        let flags = |config: ScenarioConfig| {
            (
                config.plant_object_store_write,
                config.late_commit,
                config.kill_after_deleting_repo_d,
            )
        };
        let plant = scenario_config(Sabotage::FromObjectStoreWithoutProvenance, 8);
        assert_eq!(flags(plant), (true, false, false));
        let late = scenario_config(Sabotage::ToObjectStoreWithoutChange, 8);
        assert_eq!(flags(late), (false, true, false));
        let deleting = scenario_config(Sabotage::DeleteDirBeforeRepoWrite, 8);
        assert_eq!(flags(deleting), (false, false, true));
    }

    #[test]
    fn unlink_base_cases_run_few_writers() {
        for sabotage in SABOTAGES {
            let expected = match sabotage {
                Sabotage::UnlinkBaseBeforeRename => 2,
                _ => 8,
            };
            assert_eq!(
                case_workers(CaseKind::Sabotage(sabotage), 8),
                expected,
                "{sabotage:?}"
            );
        }
        assert_eq!(
            case_workers(CaseKind::Sabotage(Sabotage::UnlinkBaseBeforeRename), 1),
            1
        );
        assert_eq!(case_workers(CaseKind::LegacyBackend, 8), 8);
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
                    case_config(mode, CaseKind::Sabotage(Sabotage::SkipLink), duration);
                assert_eq!(sabotaged.duration, duration);
                assert_eq!(sabotaged.payload_bytes, 1024);
            }
        }
    }

    #[test]
    fn cases_cover_every_sabotage_in_its_modes_then_the_legacy_backend() {
        let distinct: std::collections::HashSet<_> = SABOTAGES.into_iter().collect();
        assert_eq!(distinct.len(), SABOTAGES.len());
        for sabotage in SABOTAGES {
            assert!(!modes(sabotage).is_empty(), "{sabotage:?}");
        }
        let names = |mode| cases(mode).map(case_name).collect::<Vec<_>>();
        assert_eq!(
            names(Mode::Storage),
            [
                "skip-link",
                "rename-from-listed-marker",
                "trust-rename-enoent",
                "ack-unproven",
                "tidy-up-deletes-current",
                "delete-above-marker",
                "unlink-base-before-rename",
                "link-after-rename",
                "delete-winner-file",
                "ack-superseded-without-proof",
                "legacy-local",
            ]
        );
        let repository = names(Mode::Repository);
        assert_eq!(repository.len(), SABOTAGES.len() + 1);
        assert_eq!(
            repository[repository.len() - 4..],
            [
                "from-object-store-without-provenance",
                "to-object-store-without-change",
                "delete-dir-before-repo-write",
                "legacy-local",
            ]
        );
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
        let all = SelftestReport { cases: cases(Mode::Storage).map(detected).collect() };
        assert_eq!(all.exit_code(), 0);
        let summary = all.summary();
        assert_eq!(summary.lines().count(), 12, "{summary}");
        assert!(summary.ends_with("PASS"), "{summary}");
        assert!(summary.contains("storage trust-rename-enoent: detected"), "{summary}");
        assert!(summary.contains("storage legacy-local: detected"), "{summary}");
        assert!(!summary.contains("kept-root"), "{summary}");

        let mut missed = all.clone();
        missed.cases[1] = case(
            CaseKind::Sabotage(Sabotage::RenameFromListedMarker),
            CaseOutcome::Missed("the run passed".to_owned()),
        );
        missed.cases[2].outcome = CaseOutcome::Error("boom".to_owned());
        assert_eq!(missed.exit_code(), 1);
        let summary = missed.summary();
        assert!(summary.ends_with("FAIL"), "{summary}");
        assert!(
            summary.contains(
                "storage rename-from-listed-marker: MISSED: the run passed (kept-root)"
            ),
            "{summary}"
        );

        let mut errored = all;
        errored.cases[0].outcome = CaseOutcome::Error("boom".to_owned());
        assert_eq!(errored.exit_code(), 3);
    }
}
