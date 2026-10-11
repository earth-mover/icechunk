//! Checks a finished run against what its workers logged.

pub mod liveness;
pub mod repo;
pub mod storage;

use std::collections::{BTreeMap, HashSet, btree_map::Entry};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    config::{Mode, RunConfig},
    env_info::{HostDone, HostInfo},
    layout::RunRoot,
    log::{LogName, Op, OpResult, Record},
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
}

/// Worth a look, but not a failed run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "warning")]
pub enum Warning {
    LostReplyConflict { id: Uuid },
    AmbiguousErrorLanded { id: Uuid },
    Stall { from_ms: i64, to_ms: i64 },
    StartSkew { ms: i64 },
    Starved { tag: String, longest_gap_ms: i64 },
    SubdirStaging { count: u64 },
    Stragglers { tag: String, count: u32 },
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
    /// Intents without an outcome.
    pub unresolved: u64,
    /// Storage mode: payloads on the rebuilt chain, the initial one included.
    pub chain_len: u64,
    pub faults: BTreeMap<String, u64>,
    pub external_kills: u64,
}

/// Everything a run left under its root, except the repository itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    pub logs: BTreeMap<LogName, Vec<Record>>,
    pub hosts: Vec<HostInfo>,
    pub done: Vec<HostDone>,
}

impl History {
    /// Every record of every worker log, with the log it is in.
    pub fn worker_records(&self) -> impl Iterator<Item = (&LogName, &Record)> {
        self.logs
            .iter()
            .filter(|(name, _)| name.incarnation.is_some())
            .flat_map(|(name, records)| records.iter().map(move |record| (name, record)))
    }
}

/// Checks the run's repository against `history` with the oracle of its
/// mode. In storage mode an unreadable `repo` is `BrokenHistory`, and `Err`
/// only if the storage can't be opened or the backups can't be read; in
/// repository mode `Err` if the repository can't be read, with the context
/// [`RepositoryUnreadable`]. Both read `repo` through the run's storage.
pub async fn check_mode(
    root: &RunRoot,
    config: &RunConfig,
    history: &History,
    network_fs: bool,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
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
            Ok(())
        }
        Mode::Repository => {
            let repo = repo_mode::open_repo(root, config, None)
                .await
                .map_err(|err| err.context(RepositoryUnreadable))?;
            repo::check_repo(&repo, history, config, network_fs, verdict)
                .await
                .map_err(|err| err.context(RepositoryUnreadable))
        }
    }
}

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

fn label(name: &LogName) -> String {
    match name.incarnation {
        Some(incarnation) => format!("{}.{}.{incarnation}", name.tag, name.slot),
        None => format!("{}.{}", name.tag, name.slot),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(slot: &str, incarnation: Option<u32>) -> LogName {
        LogName { tag: "h".to_owned(), slot: slot.to_owned(), incarnation }
    }

    fn worker(slot: &str) -> LogName {
        name(slot, Some(0))
    }

    fn intent(t_ms: i64, id: Uuid) -> Record {
        Record::Intent { t_ms, id, op: Op::Gc }
    }

    fn conflict(id: Uuid) -> Record {
        Record::Outcome { t_ms: 0, id, result: OpResult::Conflict }
    }

    fn checked(logs: Vec<(LogName, Vec<Record>)>) -> (Intents, Verdict) {
        let history = History { logs: logs.into_iter().collect(), ..History::default() };
        let mut verdict = Verdict::default();
        (intents(&history, &mut verdict), verdict)
    }

    #[test]
    fn intents_pairs_outcomes_from_worker_logs_only() {
        let (a, b, c) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let cas = Op::Cas { base: "e0".to_owned(), new: "e1".to_owned() };
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
        assert_eq!(intents, BTreeMap::from([(a, (Op::Gc, None, 1))]));

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
        assert_eq!(intents, BTreeMap::from([(a, (Op::Gc, Some(OpResult::Conflict), 1))]));

        let (_, verdict) = checked(vec![
            (worker("0"), vec![intent(1, a)]),
            (worker("1"), vec![intent(2, a)]),
        ]);
        assert_one_harness(&verdict, a, "h.1.0");
    }

    #[test]
    fn duplicate_outcome_is_a_harness_error() {
        let a = Uuid::new_v4();
        let ack =
            Record::Outcome { t_ms: 0, id: a, result: OpResult::Ack { snapshot: None } };
        let (intents, verdict) =
            checked(vec![(worker("0"), vec![intent(1, a), conflict(a), ack])]);
        assert_one_harness(&verdict, a, "outcome");
        assert_eq!(intents, BTreeMap::from([(a, (Op::Gc, Some(OpResult::Conflict), 1))]));
    }
}
