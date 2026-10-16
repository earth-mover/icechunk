//! Repository mode: commits that a GC's cutoff came after, breaking GC's
//! precondition, as [`Warning::GcCutoffAfterSessionStart`]s.

use std::collections::HashMap;

use uuid::Uuid;

use crate::{
    log::{Op, OpResult, Record},
    oracle::{History, Verdict, Warning, clock_allowance_ms, mirror::died_by_ms},
};

/// A GC that may have landed: not a conflict, which is its change running
/// out of attempts, nor a refusal.
#[derive(Debug)]
struct Gc {
    id: Uuid,
    intent_ms: i64,
    cutoff_ms: i64,
}

/// Warns about every commit that started before the cutoff of a GC that may
/// have landed, by more than the clock allowance ε, and whose outcome, or
/// its process's death, came after that GC began, by more than ε: it may
/// have landed after that GC's change, which then didn't keep its objects.
/// Each commit names the first such GC.
pub fn check_gc_cutoffs(history: &History, verdict: &mut Verdict) {
    let epsilon = clock_allowance_ms(history);
    let outcomes: HashMap<Uuid, (i64, &OpResult)> = history
        .worker_records()
        .filter_map(|(_, record)| match record {
            Record::Outcome { t_ms, id, result } => Some((*id, (*t_ms, result))),
            _ => None,
        })
        .collect();
    let may_have_landed = |id: &Uuid| {
        outcomes.get(id).is_none_or(|(_, result)| {
            !(matches!(result, OpResult::Conflict) || result.is_refusal())
        })
    };
    let mut gcs = Vec::new();
    let mut commits = Vec::new();
    for (name, records) in
        history.logs.iter().filter(|(name, _)| name.incarnation.is_some())
    {
        // A GC worker round has one cutoff, for its expiration and its GC;
        // logs from before GC intents carried it have it in the former only.
        let mut round_cutoff = None;
        for record in records {
            let Record::Intent { t_ms, id, op } = record else { continue };
            match op {
                Op::Expire { older_than_ms } => round_cutoff = Some(*older_than_ms),
                Op::Gc { older_than_ms } => {
                    if let Some(cutoff_ms) = older_than_ms.or(round_cutoff)
                        && may_have_landed(id)
                    {
                        gcs.push(Gc { id: *id, intent_ms: *t_ms, cutoff_ms });
                    }
                }
                Op::BranchCommit { .. } | Op::MainCommit => {
                    commits.push((name, *id, *t_ms))
                }
                _ => {}
            }
        }
    }
    gcs.sort_by_key(|gc| gc.intent_ms);
    for (name, id, intent_ms) in commits {
        let outcome = outcomes.get(&id);
        let outcome_ms = outcome.map(|&(t_ms, _)| t_ms);
        let end_ms = outcome_ms.or_else(|| died_by_ms(history, name));
        let Some(gc) = gcs.iter().find(|gc| {
            intent_ms + epsilon < gc.cutoff_ms
                && end_ms.is_none_or(|end_ms| end_ms > gc.intent_ms + epsilon)
        }) else {
            continue;
        };
        let snapshot = outcome.and_then(|(_, result)| result.landed()).flatten();
        verdict.warnings.push(Warning::GcCutoffAfterSessionStart {
            id,
            snapshot: snapshot.map(str::to_owned),
            intent_ms,
            outcome_ms,
            gc: gc.id,
            gc_intent_ms: gc.intent_ms,
            cutoff_ms: gc.cutoff_ms,
            missing: vec![],
        });
    }
}

/// Records `key`, an object of `snapshot` found missing, in the
/// [`Warning::GcCutoffAfterSessionStart`] of the commit acknowledged as
/// `snapshot`: a GC may have deleted it. `false` if there is none.
pub fn missing_after_gc_cutoff(
    warnings: &mut [Warning],
    snapshot: &str,
    key: &str,
) -> bool {
    let missing = warnings.iter_mut().find_map(|warning| match warning {
        Warning::GcCutoffAfterSessionStart {
            snapshot: Some(warned), missing, ..
        } if warned == snapshot => Some(missing),
        _ => None,
    });
    let Some(missing) = missing else { return false };
    if !missing.iter().any(|missing| missing == key) {
        missing.push(key.to_owned());
    }
    true
}

#[cfg(test)]
pub(crate) mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::log::{LogName, Op, OpResult, Record};

    fn log(tag: &str, slot: &str, incarnation: u32) -> LogName {
        LogName {
            tag: tag.to_owned(),
            slot: slot.to_owned(),
            incarnation: Some(incarnation),
        }
    }

    fn intent(t_ms: i64, id: Uuid, op: Op) -> Record {
        Record::Intent { t_ms, id, op }
    }

    fn outcome(t_ms: i64, id: Uuid, result: OpResult) -> Record {
        Record::Outcome { t_ms, id, result }
    }

    fn ack(snapshot: Option<&str>) -> OpResult {
        OpResult::Ack { snapshot: snapshot.map(str::to_owned) }
    }

    /// The incident's GC round, times relative to 1792559000000: an
    /// expiration with the round's cutoff, then the GC.
    pub(crate) const GC_INTENT_MS: i64 = 1_037_340;
    pub(crate) const CUTOFF_MS: i64 = 991_157;

    fn gc_round(gc: Uuid, logged_cutoff: bool, result: Option<OpResult>) -> Vec<Record> {
        let expire = Uuid::new_v4();
        let older_than_ms = logged_cutoff.then_some(CUTOFF_MS);
        let mut records = vec![
            intent(1_027_157, expire, Op::Expire { older_than_ms: CUTOFF_MS }),
            outcome(GC_INTENT_MS, expire, ack(None)),
            intent(GC_INTENT_MS, gc, Op::Gc { older_than_ms }),
        ];
        records.extend(result.map(|result| outcome(1_064_705, gc, result)));
        records
    }

    fn branch_commit() -> Op {
        Op::BranchCommit { branch: "w.client5.2".to_owned() }
    }

    /// The incident: a commit that started at `intent_ms` (973998 in the
    /// run) and was acked as `snapshot` at 1059380, and the GC whose round's
    /// cutoff was 991157, acked.
    pub(crate) fn incident(
        commit: Uuid,
        gc: Uuid,
        snapshot: &str,
        intent_ms: i64,
    ) -> History {
        let commit_records = vec![
            intent(intent_ms, commit, branch_commit()),
            outcome(1_059_380, commit, ack(Some(snapshot))),
        ];
        History {
            logs: [
                (log("client5", "2", 3), commit_records),
                (log("client1", "gc", 2), gc_round(gc, true, Some(ack(None)))),
            ]
            .into(),
            ..History::default()
        }
    }

    fn warnings(history: &History) -> Vec<Warning> {
        let mut verdict = Verdict::default();
        check_gc_cutoffs(history, &mut verdict);
        assert_eq!(verdict.violations, []);
        verdict.warnings
    }

    fn warning(
        commit: Uuid,
        gc: Uuid,
        snapshot: Option<&str>,
        outcome_ms: Option<i64>,
    ) -> Warning {
        Warning::GcCutoffAfterSessionStart {
            id: commit,
            snapshot: snapshot.map(str::to_owned),
            intent_ms: 973_998,
            outcome_ms,
            gc,
            gc_intent_ms: GC_INTENT_MS,
            cutoff_ms: CUTOFF_MS,
            missing: vec![],
        }
    }

    #[test]
    fn a_commit_that_started_before_a_gcs_cutoff_and_outlived_its_start_is_warned() {
        let (commit, gc) = (Uuid::new_v4(), Uuid::new_v4());
        let history = incident(commit, gc, "XBD15", 973_998);
        assert_eq!(
            warnings(&history),
            [warning(commit, gc, Some("XBD15"), Some(1_059_380))]
        );

        // Logs from before the cutoff was logged: the round's expiration's.
        let mut old = history.clone();
        old.logs.insert(log("client1", "gc", 2), gc_round(gc, false, Some(ack(None))));
        assert_eq!(warnings(&old), warnings(&history));

        // A GC that may have landed: killed, or failed ambiguously.
        for result in [None, Some(OpResult::Error { message: "boom".to_owned() })] {
            let mut run = history.clone();
            run.logs.insert(log("client1", "gc", 2), gc_round(gc, true, result.clone()));
            assert_eq!(warnings(&run).len(), 1, "{result:?}");
        }

        // A commit never resolved may still commit: until its death, if
        // logged, here never.
        let mut unresolved = history.clone();
        unresolved.logs.insert(
            log("client5", "2", 3),
            vec![intent(973_998, commit, branch_commit())],
        );
        assert_eq!(warnings(&unresolved), [warning(commit, gc, None, None)]);
    }

    #[test]
    fn a_commit_within_the_clock_allowance_or_a_gc_that_did_not_land_is_not_warned() {
        let (commit, gc) = (Uuid::new_v4(), Uuid::new_v4());
        let epsilon = crate::oracle::MIN_CLOCK_ALLOWANCE_MS;
        // Started after the cutoff, or before it by no more than ε.
        for intent_ms in [CUTOFF_MS + 1, CUTOFF_MS - epsilon] {
            assert_eq!(
                warnings(&incident(commit, gc, "S", intent_ms)),
                [],
                "{intent_ms}"
            );
        }
        assert_eq!(
            warnings(&incident(commit, gc, "S", CUTOFF_MS - epsilon - 1)).len(),
            1
        );

        // Ended before the GC began, or after it by no more than ε.
        for outcome_ms in [GC_INTENT_MS - 1, GC_INTENT_MS + epsilon] {
            let mut run = incident(commit, gc, "S", 973_998);
            run.logs.insert(
                log("client5", "2", 3),
                vec![
                    intent(973_998, commit, branch_commit()),
                    outcome(outcome_ms, commit, ack(Some("S"))),
                ],
            );
            assert_eq!(warnings(&run), [], "{outcome_ms}");
        }

        // A GC that didn't land deletes nothing.
        for result in
            [OpResult::Conflict, OpResult::ReadOnly { message: "held".to_owned() }]
        {
            let mut run = incident(commit, gc, "S", 973_998);
            run.logs.insert(
                log("client1", "gc", 2),
                gc_round(gc, true, Some(result.clone())),
            );
            assert_eq!(warnings(&run), [], "{result:?}");
        }

        // Not a commit.
        let mut run = incident(commit, gc, "S", 973_998);
        run.logs.insert(
            log("client5", "2", 3),
            vec![
                intent(
                    973_998,
                    commit,
                    Op::CreateBranch { branch: "w.client5.2".to_owned() },
                ),
                outcome(1_059_380, commit, ack(None)),
            ],
        );
        assert_eq!(warnings(&run), []);
    }

    #[test]
    fn missing_objects_are_recorded_in_the_warning_of_their_snapshot() {
        let (commit, gc) = (Uuid::new_v4(), Uuid::new_v4());
        let mut warned = warnings(&incident(commit, gc, "XBD15", 973_998));
        assert!(missing_after_gc_cutoff(&mut warned, "XBD15", "transactions/XBD15"));
        assert!(!missing_after_gc_cutoff(&mut warned, "OTHER", "transactions/OTHER"));
        let Warning::GcCutoffAfterSessionStart { missing, .. } = &warned[0] else {
            panic!("{warned:?}")
        };
        assert_eq!(missing, &["transactions/XBD15"]);
    }
}
