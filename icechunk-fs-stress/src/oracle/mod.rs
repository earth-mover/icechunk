//! Checks a finished run against what its workers logged.

pub mod conversion;
pub mod gc_cutoff;
pub mod layout;
pub mod liveness;
pub mod repo;
pub mod storage;

use std::collections::{BTreeMap, HashSet, btree_map::Entry};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    config::{Backend, Mode, RunConfig},
    env_info::{HostDone, HostInfo},
    layout::RunRoot,
    log::{Direction, LogName, Op, OpResult, Record},
    oracle::layout::{RunStatuses, check_init_version, check_readonly_holds},
    repo_mode, storage_mode,
};

/// A failed run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "violation")]
pub enum Violation {
    /// Acknowledged, but not on the chain, after the wrong predecessor or
    /// missing from the ancestry.
    LostUpdate {
        id: Uuid,
        detail: String,
    },
    /// A chain element, snapshot or tag that no intent explains.
    Phantom {
        what: String,
    },
    /// On a local filesystem, a failed outcome whose update landed, or a main
    /// commit that landed twice: its rebase conflicted with its own first
    /// landing.
    FalseConflict {
        id: Uuid,
        landed_twice: bool,
    },
    /// A missing backup, a cycle, an unparsable payload or message.
    BrokenHistory {
        detail: String,
    },
    Stall {
        from_ms: i64,
        to_ms: i64,
    },
    FaultFloor {
        kind: String,
        fired: u64,
        required: u64,
    },
    Recovery {
        detail: String,
    },
    Leftovers {
        names: Vec<String>,
    },
    DataMismatch {
        detail: String,
    },
    /// The harness misbehaved: malformed logs, missing `done/` files, …
    Harness {
        detail: String,
    },
    /// M7: the current version references a snapshot file, transaction log
    /// or manifest that doesn't exist.
    DanglingReference {
        detail: String,
    },
    /// A version that was current without exactly `filesystem-layout-v1`
    /// and a status the implementation writes.
    FeatureMissing {
        what: String,
        detail: String,
    },
    /// The first version's `N` isn't within its creation's milliseconds.
    InitialVersion {
        n: u64,
        t_before_ms: i64,
        t_after_ms: i64,
    },
    /// The sweep or GC deleted an entry it doesn't recognize, which it must
    /// never do: a planted canary.
    DeletedUnrecognized {
        name: String,
    },
    /// M4: a change landed during a hold of the status `ReadOnly`, or right
    /// after the `ReadOnly` change (`version` is `None` then).
    ChangeDuringReadOnlyHold {
        version: Option<String>,
        detail: String,
    },
    /// C2: a conversion changed more than the writer features and the ops
    /// log, or didn't back up the repo info it converted.
    ConversionMismatch {
        direction: Direction,
        detail: String,
    },
    /// C3: a conversion didn't finish, or didn't leave the target layout and
    /// nothing else; or a planted object-store-layout write didn't stop the rerun
    /// of an interrupted conversion from the object store layout.
    ConversionEndState {
        direction: Direction,
        detail: String,
    },
    /// C5: something overwrote the converted `$ROOT/repo` after a conversion
    /// to the object store layout.
    OverwroteObjectStore {
        detail: String,
    },
    /// C6: `repo.d` was deleted before `$ROOT/repo` held the conversion's
    /// version without the feature.
    DeletedHistory {
        detail: String,
    },
    /// C7: a writer attempt during an interrupted conversion to the object
    /// store layout didn't fail with the error naming the conversion to rerun.
    WriterDuringConversion {
        id: Uuid,
        detail: String,
    },
    /// On a local filesystem, a rerun of a conversion failed as only stale
    /// listings make it fail: relisting ran out.
    ConversionRetried {
        direction: Direction,
        detail: String,
    },
}

impl Violation {
    /// The violation's name, as `report.json` tags it.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::LostUpdate { .. } => "LostUpdate",
            Self::Phantom { .. } => "Phantom",
            Self::FalseConflict { .. } => "FalseConflict",
            Self::BrokenHistory { .. } => "BrokenHistory",
            Self::Stall { .. } => "Stall",
            Self::FaultFloor { .. } => "FaultFloor",
            Self::Recovery { .. } => "Recovery",
            Self::Leftovers { .. } => "Leftovers",
            Self::DataMismatch { .. } => "DataMismatch",
            Self::Harness { .. } => "Harness",
            Self::DanglingReference { .. } => "DanglingReference",
            Self::FeatureMissing { .. } => "FeatureMissing",
            Self::InitialVersion { .. } => "InitialVersion",
            Self::DeletedUnrecognized { .. } => "DeletedUnrecognized",
            Self::ChangeDuringReadOnlyHold { .. } => "ChangeDuringReadOnlyHold",
            Self::ConversionMismatch { .. } => "ConversionMismatch",
            Self::ConversionEndState { .. } => "ConversionEndState",
            Self::OverwroteObjectStore { .. } => "OverwroteObjectStore",
            Self::DeletedHistory { .. } => "DeletedHistory",
            Self::WriterDuringConversion { .. } => "WriterDuringConversion",
            Self::ConversionRetried { .. } => "ConversionRetried",
        }
    }
}

/// Worth a look, but not a failed run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "warning")]
pub enum Warning {
    LostReplyConflict {
        id: Uuid,
    },
    AmbiguousErrorLanded {
        id: Uuid,
    },
    Stall {
        from_ms: i64,
        to_ms: i64,
    },
    StartSkew {
        ms: i64,
    },
    Starved {
        tag: String,
        longest_gap_ms: i64,
    },
    SubdirStaging {
        count: u64,
    },
    Stragglers {
        tag: String,
        count: u32,
    },
    /// Storage mode: attempts that failed reading or backing up `repo`, with
    /// the first few messages.
    AttemptFailures {
        count: u64,
        samples: Vec<String>,
    },
    /// Entries of the storage root or `repo.d` that the layout never
    /// creates and GC never deletes: someone else's, or a bug.
    UnknownEntries {
        names: Vec<String>,
    },
    /// [`Violation::ConversionRetried`] on a network filesystem.
    ConversionRetried {
        direction: Direction,
        detail: String,
    },
    /// On a network filesystem, run `attempt` of the conversion to the
    /// object store layout returned `NothingToConvert` and left `repo.d`, with
    /// `$ROOT/repo` a copy of its current version without the feature: its
    /// listing of the storage root missed `repo.d`, an accepted limit. The
    /// conversion runs again after `rerun_after_ms`.
    StaleRootListing {
        attempt: u32,
        rerun_after_ms: u64,
    },
    /// The commit `id` started before the cutoff of the GC `gc` by more than
    /// the clock allowance and may still have committed when that GC, which
    /// may have landed, began: the run broke GC's precondition (spec 2.2,
    /// "Garbage collection": the cutoff "MUST be earlier than the start of
    /// every session that may still commit"), as a storage stall longer than
    /// `gc.margin` does, so that GC may have deleted the commit's objects.
    /// `missing` lists the objects of its acknowledged `snapshot` that the
    /// oracle found missing and excused: its snapshot file and transaction
    /// log, and, only as the failure of its chunk's read-back, the manifest
    /// or chunk the commit wrote that the read-back found missing. M7 still
    /// judges a missing manifest, that one included: when it checks it, it
    /// reports [`Violation::DanglingReference`].
    GcCutoffAfterSessionStart {
        id: Uuid,
        snapshot: Option<String>,
        intent_ms: i64,
        /// When its outcome was logged, `None` if never.
        outcome_ms: Option<i64>,
        gc: Uuid,
        gc_intent_ms: i64,
        cutoff_ms: i64,
        missing: Vec<String>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Verdict {
    pub violations: Vec<Violation>,
    pub warnings: Vec<Warning>,
    pub stats: Stats,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stats {
    pub intents: u64,
    pub acks: u64,
    pub conflicts: u64,
    pub errors: u64,
    /// Outcomes the repository's status or layout refused.
    pub refused: u64,
    /// Storage mode: attempts that failed before their intent.
    pub failures: u64,
    /// Doubtful outcomes the commits resolved, by
    /// [`observed_name`](crate::faults::observed_name).
    pub observed: BTreeMap<String, u64>,
    /// Intents without an outcome.
    pub unresolved: u64,
    /// Storage mode: payloads on the rebuilt chain, the initial one included.
    pub chain_len: u64,
    pub faults: BTreeMap<String, u64>,
    pub external_kills: u64,
    /// Holds of the status `ReadOnly` the status toggler logged.
    pub readonly_holds: u64,
}

/// Everything a run left under its root, except the repository itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    pub logs: BTreeMap<LogName, Vec<Record>>,
    pub hosts: Vec<HostInfo>,
    pub done: Vec<HostDone>,
}

impl History {
    /// The history of a phase of the conversion scenario, `self`, after the
    /// operations of its `earlier` phases on the same repository, oldest
    /// first: the intents, outcomes and starts of their worker logs. A worker
    /// log whose host tag and slot is already there gets later incarnations,
    /// so that log order stays the order of the phases.
    pub fn after_earlier_phases(
        self,
        earlier: &[BTreeMap<LogName, Vec<Record>>],
    ) -> Self {
        let is_op = |record: &Record| {
            matches!(
                record,
                Record::Start { .. } | Record::Intent { .. } | Record::Outcome { .. }
            )
        };
        let mut logs: BTreeMap<LogName, Vec<Record>> = BTreeMap::new();
        let phases = earlier.iter().map(|logs| (logs, true)).chain([(&self.logs, false)]);
        for (phase, ops_only) in phases {
            let mut offsets: BTreeMap<(&str, &str), u32> = BTreeMap::new();
            for (name, records) in phase {
                let Some(incarnation) = name.incarnation else {
                    if !ops_only {
                        logs.insert(name.clone(), records.clone());
                    }
                    continue;
                };
                let offset =
                    *offsets.entry((&name.tag, &name.slot)).or_insert_with(|| {
                        logs.keys()
                            .filter(|other| {
                                other.tag == name.tag && other.slot == name.slot
                            })
                            .filter_map(|other| other.incarnation)
                            .max()
                            .map_or(0, |max| max + 1)
                    });
                let records = records
                    .iter()
                    .filter(|record| !ops_only || is_op(record))
                    .cloned()
                    .collect();
                logs.insert(
                    LogName { incarnation: Some(incarnation + offset), ..name.clone() },
                    records,
                );
            }
        }
        Self { logs, ..self }
    }

    /// Every record of every worker log, with the log it is in.
    pub fn worker_records(&self) -> impl Iterator<Item = (&LogName, &Record)> {
        self.logs
            .iter()
            .filter(|(name, _)| name.incarnation.is_some())
            .flat_map(|(name, records)| records.iter().map(move |record| (name, record)))
    }
}

/// Checks the run's repository against `history` with the oracle of its
/// mode, and on the filesystem backend the writer-feature rule L1, I1 and
/// M4. `earlier_eras` are the backups the conversion scenario's earlier eras
/// wrote, which L1 doesn't check.
/// In storage mode an unreadable `repo` is `BrokenHistory`, and `Err` only
/// if the storage can't be opened or the backups can't be read; in
/// repository mode `Err` if the repository can't be read, with the context
/// [`RepositoryUnreadable`]. Both read `repo` through the run's storage.
pub async fn check_mode(
    root: &RunRoot,
    config: &RunConfig,
    history: &History,
    network_fs: bool,
    earlier_eras: &HashSet<String>,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    let layout = config.backend == Backend::Filesystem;
    let statuses = RunStatuses::of(config);
    match config.mode {
        Mode::Storage => {
            let storage = storage_mode::storage(root, config, None).await?;
            let final_repo = storage_mode::read_repo(&*storage)
                .await
                .map_err(|err| {
                    verdict.violations.push(Violation::BrokenHistory {
                        detail: format!("reading {}: {err:#}", storage_mode::REPO_KEY),
                    });
                })
                .ok();
            let backups = storage::read_backups(&root.storage())?;
            storage::check_chain(
                final_repo.as_deref(),
                &backups,
                history,
                network_fs,
                verdict,
            );
            if layout {
                storage::check_versions(final_repo.as_deref(), &backups, verdict);
            }
        }
        Mode::Repository => {
            let repo = repo_mode::open_repo(root, config, None)
                .await
                .map_err(|err| err.context(RepositoryUnreadable))?;
            repo::check_repo(&repo, history, config, network_fs, verdict)
                .await
                .map_err(|err| err.context(RepositoryUnreadable))?;
            if layout {
                let current = storage_mode::read_repo(repo.storage().as_ref())
                    .await
                    .map_err(|err| err.context(RepositoryUnreadable))?;
                layout::versions_that_were_current(
                    &root.storage(),
                    &current,
                    statuses,
                    earlier_eras,
                    verdict,
                )?;
            }
        }
    }
    if layout {
        check_readonly_holds(history, verdict);
        check_init_version(history, verdict);
    }
    Ok(())
}

/// The clock allowance between hosts for rules that compare their times: the
/// larger of [`MIN_CLOCK_ALLOWANCE_MS`] and the start skew the `done/`
/// records show.
pub fn clock_allowance_ms(history: &History) -> i64 {
    let go = history.done.iter().map(|done| done.go_seen_at_ms);
    let skew = go.clone().max().zip(go.min()).map_or(0, |(last, first)| last - first);
    skew.max(MIN_CLOCK_ALLOWANCE_MS)
}

pub const MIN_CLOCK_ALLOWANCE_MS: i64 = 50;

/// The context of [`check_mode`]'s errors from opening or reading the
/// repository, as opposed to the harness's own I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepositoryUnreadable;

impl std::fmt::Display for RepositoryUnreadable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the oracle couldn't read the repository")
    }
}

/// Every logged intent by id, with its outcome if one was logged and the
/// intent's `t_ms`.
pub type Intents = BTreeMap<Uuid, (Op, Option<OpResult>, i64)>;

/// The [`Intents`] of every worker log. A worker logs each intent once and
/// its outcome at most once, after it in the same log; anything else is a
/// `Harness` violation.
pub fn intents(history: &History, verdict: &mut Verdict) -> Intents {
    let mut harness = |detail| verdict.violations.push(Violation::Harness { detail });
    let mut intents = BTreeMap::new();
    for (name, records) in
        history.logs.iter().filter(|(name, _)| name.incarnation.is_some())
    {
        let mut open = HashSet::new();
        for record in records {
            match record {
                Record::Intent { t_ms, id, op } => match intents.entry(*id) {
                    Entry::Vacant(entry) => {
                        entry.insert((op.clone(), None, *t_ms));
                        open.insert(*id);
                    }
                    Entry::Occupied(_) => {
                        harness(format!("intent {id} logged again in {}", label(name)));
                    }
                },
                Record::Outcome { id, result, .. } => {
                    if open.remove(id)
                        && let Some((_, outcome, _)) = intents.get_mut(id)
                    {
                        *outcome = Some(result.clone());
                    } else if intents
                        .get(id)
                        .is_some_and(|(_, outcome, _)| outcome.is_some())
                    {
                        harness(format!("outcome {id} logged again in {}", label(name)));
                    } else {
                        harness(format!(
                            "outcome {id} in {} without an earlier intent in that log",
                            label(name)
                        ));
                    }
                }
                _ => {}
            }
        }
    }
    intents
}

/// How many [`AttemptFailures`](Warning::AttemptFailures) samples a report
/// keeps.
const FAILURE_SAMPLES: usize = 3;

/// Counts the attempts that failed before their intent and warns about them.
/// No intent was logged for them, so the other checks don't see them.
pub fn check_failures(history: &History, verdict: &mut Verdict) {
    let messages: Vec<&String> = history
        .worker_records()
        .filter_map(|(_, record)| match record {
            Record::Failure { message, .. } => Some(message),
            _ => None,
        })
        .collect();
    let count = messages.len() as u64;
    verdict.stats.failures = count;
    if count > 0 {
        let samples =
            messages.into_iter().take(FAILURE_SAMPLES).map(String::clone).collect();
        verdict.warnings.push(Warning::AttemptFailures { count, samples });
    }
}

/// Counts the doubtful outcomes the workers' commits resolved: they show
/// whether lost replies and races reached the protocol's ambiguous branches.
pub fn count_observed(history: &History, verdict: &mut Verdict) {
    for (_, record) in history.worker_records() {
        if let Record::Observed { what, .. } = record {
            *verdict.stats.observed.entry(what.clone()).or_default() += 1;
        }
    }
}

fn label(name: &LogName) -> String {
    match name.incarnation {
        Some(incarnation) => format!("{}.{}.{incarnation}", name.tag, name.slot),
        None => format!("{}.{}", name.tag, name.slot),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GC: Op = Op::Gc { older_than_ms: None };

    fn name(slot: &str, incarnation: Option<u32>) -> LogName {
        LogName { tag: "h".to_owned(), slot: slot.to_owned(), incarnation }
    }

    fn worker(slot: &str) -> LogName {
        name(slot, Some(0))
    }

    fn intent(t_ms: i64, id: Uuid) -> Record {
        Record::Intent { t_ms, id, op: GC }
    }

    fn conflict(id: Uuid) -> Record {
        Record::Outcome { t_ms: 0, id, result: OpResult::Conflict }
    }

    #[test]
    fn a_violations_kind_is_its_tag_in_report_json() {
        let violations = [
            Violation::LostUpdate { id: Uuid::nil(), detail: String::new() },
            Violation::DanglingReference { detail: String::new() },
            Violation::ConversionEndState {
                direction: Direction::ToObjectStore,
                detail: String::new(),
            },
            Violation::ChangeDuringReadOnlyHold { version: None, detail: String::new() },
        ];
        for violation in violations {
            let json = serde_json::to_value(&violation).unwrap();
            assert_eq!(json["violation"], violation.kind());
        }
    }

    #[test]
    fn earlier_phases_come_first_with_their_operations_only() {
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let renamed = Record::Observed { t_ms: 1, what: "link_existed".to_owned() };
        let first = BTreeMap::from([
            (worker("0"), vec![intent(1, a), renamed.clone()]),
            (name("supervisor", None), vec![renamed.clone()]),
        ]);
        let second = BTreeMap::from([
            (worker("0"), vec![intent(2, b)]),
            (name("0", Some(1)), vec![intent(3, c)]),
        ]);
        let own = History {
            logs: BTreeMap::from([(
                worker("0"),
                vec![intent(4, Uuid::new_v4()), renamed.clone()],
            )]),
            ..History::default()
        };
        let merged = own.clone().after_earlier_phases(&[first, second]);
        let ids: Vec<(Option<u32>, usize)> = merged
            .logs
            .iter()
            .map(|(name, records)| (name.incarnation, records.len()))
            .collect();
        assert_eq!(ids, [(Some(0), 1), (Some(1), 1), (Some(2), 1), (Some(3), 2)]);
        assert_eq!(merged.logs[&name("0", Some(3))], own.logs[&worker("0")]);
        let order: Vec<i64> = merged
            .worker_records()
            .filter_map(|(_, record)| match record {
                Record::Intent { t_ms, .. } => Some(*t_ms),
                _ => None,
            })
            .collect();
        assert_eq!(order, [1, 2, 3, 4]);
    }

    fn checked(logs: Vec<(LogName, Vec<Record>)>) -> (Intents, Verdict) {
        let history = History { logs: logs.into_iter().collect(), ..History::default() };
        let mut verdict = Verdict::default();
        (intents(&history, &mut verdict), verdict)
    }

    #[test]
    fn intents_pairs_outcomes_from_worker_logs_only() {
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let cas = Op::Cas {
            base: "e0".to_owned(),
            new: "e1".to_owned(),
            backup: "repo.1.X".to_owned(),
        };
        let (intents, verdict) = checked(vec![
            (
                worker("0"),
                vec![
                    Record::Intent { t_ms: 1, id: a, op: cas.clone() },
                    Record::Intent { t_ms: 3, id: b, op: Op::MainCommit },
                    conflict(a),
                ],
            ),
            (name("supervisor", None), vec![intent(5, c), conflict(c)]),
        ]);
        assert_eq!(verdict.violations, []);
        assert_eq!(
            intents,
            BTreeMap::from([
                (a, (cas, Some(OpResult::Conflict), 1)),
                (b, (Op::MainCommit, None, 3)),
            ])
        );
    }

    fn assert_one_harness(verdict: &Verdict, id: Uuid, what: &str) {
        assert!(
            matches!(&verdict.violations[..], [Violation::Harness { detail }]
                if detail.contains(&id.to_string()) && detail.contains(what)),
            "{verdict:?}"
        );
    }

    #[test]
    fn outcome_without_an_earlier_intent_in_its_log_is_a_harness_error() {
        let a = Uuid::new_v4();
        let (intents, verdict) = checked(vec![(worker("0"), vec![conflict(a)])]);
        assert_one_harness(&verdict, a, "without an earlier intent");
        assert_eq!(intents, BTreeMap::new());

        let (intents, verdict) =
            checked(vec![(worker("0"), vec![conflict(a), intent(1, a)])]);
        assert_one_harness(&verdict, a, "without an earlier intent");
        assert_eq!(intents, BTreeMap::from([(a, (GC, None, 1))]));

        let (_, verdict) = checked(vec![
            (worker("0"), vec![intent(1, a)]),
            (worker("1"), vec![conflict(a)]),
        ]);
        assert_one_harness(&verdict, a, "h.1.0");
    }

    #[test]
    fn duplicate_intent_is_a_harness_error() {
        let a = Uuid::new_v4();
        let (intents, verdict) =
            checked(vec![(worker("0"), vec![intent(1, a), conflict(a), intent(2, a)])]);
        assert_one_harness(&verdict, a, "intent");
        assert_eq!(intents, BTreeMap::from([(a, (GC, Some(OpResult::Conflict), 1))]));

        let (_, verdict) = checked(vec![
            (worker("0"), vec![intent(1, a)]),
            (worker("1"), vec![intent(2, a)]),
        ]);
        assert_one_harness(&verdict, a, "h.1.0");
    }

    #[test]
    fn failures_are_counted_and_sampled_from_worker_logs_only() {
        let failure =
            |message: &str| Record::Failure { t_ms: 0, message: message.to_owned() };
        let history = History {
            logs: [
                (
                    worker("0"),
                    vec![failure("a"), intent(1, Uuid::new_v4()), failure("b")],
                ),
                (worker("1"), vec![failure("c"), failure("d")]),
                (name("supervisor", None), vec![failure("e")]),
            ]
            .into(),
            ..History::default()
        };
        let mut verdict = Verdict::default();
        check_failures(&history, &mut verdict);
        assert_eq!(verdict.stats.failures, 4);
        assert_eq!(
            verdict.warnings,
            [Warning::AttemptFailures {
                count: 4,
                samples: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            }]
        );

        let mut verdict = Verdict::default();
        check_failures(&History::default(), &mut verdict);
        assert_eq!(verdict, Verdict::default());
    }

    #[test]
    fn observed_outcomes_are_counted_from_worker_logs_only() {
        let observed = |what: &str| Record::Observed { t_ms: 0, what: what.to_owned() };
        let history = History {
            logs: [
                (worker("0"), vec![observed("a"), observed("b"), observed("a")]),
                (name("supervisor", None), vec![observed("a")]),
            ]
            .into(),
            ..History::default()
        };
        let mut verdict = Verdict::default();
        count_observed(&history, &mut verdict);
        assert_eq!(
            verdict.stats.observed,
            BTreeMap::from([("a".to_owned(), 2), ("b".to_owned(), 1)])
        );
        assert_eq!(verdict.warnings, []);
    }

    #[test]
    fn duplicate_outcome_is_a_harness_error() {
        let a = Uuid::new_v4();
        let ack =
            Record::Outcome { t_ms: 0, id: a, result: OpResult::Ack { snapshot: None } };
        let (intents, verdict) =
            checked(vec![(worker("0"), vec![intent(1, a), conflict(a), ack])]);
        assert_one_harness(&verdict, a, "outcome");
        assert_eq!(intents, BTreeMap::from([(a, (GC, Some(OpResult::Conflict), 1))]));
    }
}
