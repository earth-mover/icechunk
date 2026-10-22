//! Forks, written by the fork actor (or, with `fork.during_gc`, by the GC
//! worker): every operation that landed after a fork reported it (F2), no GC
//! that the fork preceded was acknowledged (F3), a fork is erased only by a
//! publication whose fork check read `$ROOT/repo` before it (F4), and a fork
//! that persisted left both histories for manual recovery (F5).

use std::{
    collections::{BTreeMap, HashSet},
    fs, io,
    path::Path,
};

use anyhow::Context as _;
use icechunk::{Repository, format::OVERWRITTEN_FILES_PATH};
use uuid::Uuid;

use crate::{
    log::{LogName, Op, OpResult, Record, Sample},
    oracle::{
        History, Verdict, Violation, Warning, clock_allowance_ms,
        mirror::{
            LoggedRename, RenameTime, UnresolvedEnd, check_mirror_objects, logged_renames,
        },
    },
    storage_mode::{REPO_KEY, payload_etag},
};

/// A fork a run wrote, and what replaced it if that was seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggedFork {
    /// The host tag of the log, whose clock its times are on.
    pub tag: String,
    pub t_before_ms: i64,
    pub t_after_ms: i64,
    pub base_hash: String,
    pub fork_hash: String,
    pub backup: String,
    pub gc: Option<Uuid>,
    /// When `$ROOT/repo` was first seen without it, and what it held.
    pub gone: Option<(i64, Sample)>,
}

/// Every fork of `history`, in the order they were written, each with the
/// first [`Record::ForkGone`] of its hash.
pub fn logged_forks(history: &History) -> Vec<LoggedFork> {
    let mut gone: BTreeMap<&str, (i64, &Sample)> = BTreeMap::new();
    let mut forks = Vec::new();
    for (name, record) in history
        .logs
        .iter()
        .flat_map(|(name, records)| records.iter().map(move |r| (name, r)))
    {
        match record {
            Record::ForkWritten {
                t_before_ms,
                t_after_ms,
                base_hash,
                fork_hash,
                backup,
                gc,
            } => {
                forks.push(LoggedFork {
                    tag: name.tag.clone(),
                    t_before_ms: *t_before_ms,
                    t_after_ms: *t_after_ms,
                    base_hash: base_hash.clone(),
                    fork_hash: fork_hash.clone(),
                    backup: backup.clone(),
                    gc: *gc,
                    gone: None,
                });
            }
            Record::ForkGone { t_ms, fork_hash, found } => {
                let first = gone.entry(fork_hash).or_insert((*t_ms, found));
                if *t_ms < first.0 {
                    *first = (*t_ms, found);
                }
            }
            _ => {}
        }
    }
    for fork in &mut forks {
        fork.gone =
            gone.get(fork.fork_hash.as_str()).map(|&(t_ms, found)| (t_ms, found.clone()));
    }
    forks.sort_by_key(|fork| fork.t_after_ms);
    forks
}

/// The last fork, if it was never seen erased: it is still in `$ROOT/repo`.
pub fn persisted_fork(forks: &[LoggedFork]) -> Option<LoggedFork> {
    forks.last().filter(|fork| fork.gone.is_none()).cloned()
}

/// The backups that fork writes announced ([`Record::ForkBackup`]) and that
/// no fork they wrote names: copies of `$ROOT/repo` that may have been
/// interrupted, which no ops log references.
pub fn unused_fork_backups(history: &History) -> HashSet<&str> {
    let records = || history.logs.values().flatten();
    let written: HashSet<&str> = records()
        .filter_map(|record| match record {
            Record::ForkWritten { backup, .. } => Some(backup.as_str()),
            _ => None,
        })
        .collect();
    records()
        .filter_map(|record| match record {
            Record::ForkBackup { backup, .. } => Some(backup.as_str()),
            _ => None,
        })
        .filter(|backup| !written.contains(backup))
        .collect()
}

/// The publication that erased `fork` in the accepted window: a mirror
/// rename of the bytes that replaced it, whose fork check read `$ROOT/repo`
/// before the fork was written ([`checked_at`]) and which may have renamed
/// after the fork's write began: between its announcement and its return or,
/// never recorded done, until its call returned or its process died. Of
/// several, the one done soonest after its check, so that neither the verdict
/// nor the warning depends on the logs' order. Times logged on another host
/// are compared with the clock allowance; on the same host, whose processes
/// share its clock, exactly: publications follow each other within
/// milliseconds, and an allowance would excuse the next one too.
fn explaining_rename<'a>(
    fork: &LoggedFork,
    renames: &'a [LoggedRename<'a>],
    epsilon: i64,
) -> Option<&'a LoggedRename<'a>> {
    replacing_renames(fork, renames, epsilon)
        .filter(|replacing| replacing.checked_before_fork && replacing.in_window)
        .map(|replacing| replacing.rename)
        .min_by_key(|rename| {
            let landed = landed_ms_after_check(rename);
            (landed.is_none(), landed.unwrap_or_default())
        })
}

/// When the rename returned after its fork check, if it was recorded done.
fn landed_ms_after_check(rename: &LoggedRename<'_>) -> Option<i64> {
    match rename.time {
        RenameTime::Done { done_ms, .. } => rename.t_check_ms.map(|t| done_ms - t),
        RenameTime::Unresolved { .. } => None,
    }
}

/// A publication that would explain `fork`'s erasure had its rename been
/// applied after its process died: one whose fork check read `$ROOT/repo`
/// before the fork and whose process died after announcing the rename of
/// the bytes that replaced it, while no publication whose fork check didn't
/// read it before the fork may have renamed them then. Once no rename
/// explains the erasure, that death was seen before the fork's write began.
fn killed_publisher<'a>(
    fork: &LoggedFork,
    renames: &'a [LoggedRename<'a>],
    epsilon: i64,
) -> Option<&'a LoggedRename<'a>> {
    let mut replacing = replacing_renames(fork, renames, epsilon);
    if replacing
        .clone()
        .any(|replacing| !replacing.checked_before_fork && replacing.in_window)
    {
        return None;
    }
    replacing
        .find(|replacing| {
            replacing.checked_before_fork
                && matches!(
                    replacing.rename.time,
                    RenameTime::Unresolved { announced_ms, end: UnresolvedEnd::Died(_) }
                        if announced_ms <= replacing.t_gone + replacing.epsilon
                )
        })
        .map(|replacing| replacing.rename)
}

/// When `rename`'s fork check observed `$ROOT/repo`: when its last read began,
/// or, in logs without that time, when the check passed. A read that began
/// before a fork's rename returned may have seen the bytes before the fork,
/// and the check's listing after the read can take long on a network
/// filesystem. `None` if no check was recorded passing, read or not.
fn checked_at(rename: &LoggedRename<'_>) -> Option<i64> {
    rename.t_check_ms.map(|t_check| rename.t_read_ms.unwrap_or(t_check))
}

/// A rename of the bytes that replaced a fork.
#[derive(Clone, Copy)]
struct Replacing<'a> {
    rename: &'a LoggedRename<'a>,
    /// When the fork was seen gone.
    t_gone: i64,
    /// The clock allowance to compare its times with: none on the fork's
    /// host, see [`explaining_rename`].
    epsilon: i64,
    /// Its fork check read `$ROOT/repo` before the fork was written, see
    /// [`checked_at`].
    checked_before_fork: bool,
    /// It may have renamed between the fork's write and its erasure.
    in_window: bool,
}

fn replacing_renames<'f, 'a>(
    fork: &'f LoggedFork,
    renames: &'a [LoggedRename<'a>],
    epsilon: i64,
) -> impl Iterator<Item = Replacing<'a>> + Clone + use<'f, 'a> {
    let gone = match &fork.gone {
        Some((t_gone, Sample::Hash(found))) => Some((*t_gone, found.as_str())),
        _ => None,
    };
    gone.into_iter().flat_map(move |(t_gone, found)| {
        renames.iter().filter(move |rename| rename.hash == found).map(move |rename| {
            let epsilon = if rename.tag == fork.tag { 0 } else { epsilon };
            Replacing {
                rename,
                t_gone,
                epsilon,
                checked_before_fork: checked_at(rename)
                    .is_some_and(|t| t <= fork.t_after_ms + epsilon),
                in_window: rename
                    .time
                    .may_be_within(fork.t_before_ms - epsilon, t_gone + epsilon),
            }
        })
    })
}

/// When the fork stopped being in `$ROOT/repo`, as far as the records tell:
/// the earliest a rename that installed what replaced it may have done so,
/// or when it was seen gone. `None` if it persisted.
fn erased_at(
    fork: &LoggedFork,
    renames: &[LoggedRename<'_>],
    epsilon: i64,
) -> Option<i64> {
    let (t_gone, found) = fork.gone.as_ref()?;
    let t_gone = *t_gone;
    let from = fork.t_before_ms - epsilon;
    let renamed = match found {
        Sample::Hash(found) => renames
            .iter()
            .filter(|rename| {
                rename.hash == found && rename.time.may_be_within(from, t_gone + epsilon)
            })
            .map(|rename| rename.time.earliest().max(from))
            .min(),
        Sample::Missing | Sample::Error(_) => None,
    };
    Some(renamed.unwrap_or(t_gone))
}

/// F2, F3 and F4. An operation is after a fork if its intent came later than
/// the fork's write, by the clock allowance ε, and its outcome earlier than
/// the fork's erasure, by ε. After a fork, an operation that landed must
/// report it (F2): acknowledged, its publication missed the fork. A GC after a
/// fork, or during whose change's publication the fork was written, must not
/// be acknowledged (F3): it deleted while the fork was in place. F2 and F3
/// are violations on a local filesystem and warnings on a network one, whose
/// reads of `$ROOT/repo` may be stale. A fork erased by anything but a
/// publication whose fork check passed before it is a violation everywhere
/// (F4); one erased in that accepted window is a warning. So is, on a network
/// filesystem, one that only a publication whose fork check passed before it
/// may have erased, but whose process was seen dead before the fork, while no
/// publication whose check didn't pass before the fork may have renamed the
/// same bytes since: the server may apply a killed process's rename after its
/// death.
pub fn check_forks(history: &History, network_fs: bool, verdict: &mut Verdict) {
    let forks = logged_forks(history);
    if forks.is_empty() {
        return;
    }
    let epsilon = clock_allowance_ms(history);
    let renames = logged_renames(history);
    for fork in &forks {
        let Some((_, found)) = &fork.gone else { continue };
        match explaining_rename(fork, &renames, epsilon) {
            Some(rename) => verdict.warnings.push(Warning::ForkErasedInWindow {
                fork_hash: fork.fork_hash.clone(),
                landed_ms_after_check: landed_ms_after_check(rename),
            }),
            None if network_fs
                && let Some(rename) = killed_publisher(fork, &renames, epsilon) =>
            {
                verdict.warnings.push(Warning::ForkErasedAfterKilledPublisher {
                    fork_hash: fork.fork_hash.clone(),
                    replaced_by: rename.hash.to_owned(),
                });
            }
            None => {
                let erased_by = match found {
                    Sample::Hash(hash)
                        if renames.iter().any(|rename| rename.hash == hash) =>
                    {
                        format!(
                            "{hash}, which no publication whose fork check read repo \
                             before the fork (its last read began by the end of the \
                             fork's write, or, in logs without the read's time, it \
                             passed by then) may have renamed into place while it was \
                             there"
                        )
                    }
                    Sample::Hash(hash) => format!("{hash}, which no publication renamed"),
                    Sample::Missing => "nothing: repo went missing".to_owned(),
                    Sample::Error(err) => format!("an unreadable repo ({err})"),
                };
                verdict.violations.push(Violation::SilentForkLoss {
                    fork_hash: fork.fork_hash.clone(),
                    erased_by,
                });
            }
        }
    }

    let windows: Vec<(i64, Option<i64>)> = forks
        .iter()
        .map(|fork| (fork.t_after_ms + epsilon, erased_at(fork, &renames, epsilon)))
        .collect();
    let during_gc: Vec<Uuid> = forks.iter().filter_map(|fork| fork.gc).collect();
    for (_, id, op, intent_ms, outcome) in operations(history) {
        let Some((OpResult::Ack { .. }, outcome_ms)) = outcome else { continue };
        let after_fork = windows.iter().any(|&(from, until)| {
            intent_ms > from && until.is_none_or(|until| outcome_ms < until - epsilon)
        });
        match op {
            Op::Gc { .. } if after_fork || during_gc.contains(&id) => {
                if network_fs {
                    verdict.warnings.push(Warning::GcAfterFork { id });
                } else {
                    verdict.violations.push(Violation::GcAfterFork { id });
                }
            }
            Op::Convert { .. } | Op::Gc { .. } => {}
            op if after_fork => {
                let op = format!("{op:?}");
                if network_fs {
                    verdict.warnings.push(Warning::UnreportedFork { id, op });
                } else {
                    verdict.violations.push(Violation::UnreportedFork { id, op });
                }
            }
            _ => {}
        }
    }
}

/// An intent: its log, id, op and time, and its outcome with the outcome's time.
type Operation<'a> = (&'a LogName, Uuid, &'a Op, i64, Option<(&'a OpResult, i64)>);

/// Every intent of every worker log, with its time and its outcome's.
fn operations(history: &History) -> Vec<Operation<'_>> {
    let mut outcomes: BTreeMap<Uuid, (&OpResult, i64)> = BTreeMap::new();
    for (_, record) in history.worker_records() {
        if let Record::Outcome { t_ms, id, result } = record {
            outcomes.insert(*id, (result, *t_ms));
        }
    }
    history
        .worker_records()
        .filter_map(|(name, record)| match record {
            Record::Intent { t_ms, id, op } => {
                Some((name, *id, op, *t_ms, outcomes.get(id).copied()))
            }
            _ => None,
        })
        .collect()
}

/// F5, after a run whose last fork persisted: `$ROOT/repo` still holds it,
/// the backup of what it replaced exists, and in repository mode (`repo`)
/// every object it references exists, for a recovery that keeps `repo`. The
/// side of `repo.d` is the oracle's, through the filesystem storage. `Err`
/// only if the storage root can't be read.
pub async fn check_fork_sides(
    storage_root: &Path,
    fork: &LoggedFork,
    repo: Option<&Repository>,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    let mut damaged =
        |detail: String| verdict.violations.push(Violation::ForkDamaged { detail });
    let mirror = storage_root.join(REPO_KEY);
    match fs::read(&mirror) {
        Ok(bytes) if payload_etag(&bytes) == fork.fork_hash => {}
        Ok(bytes) => damaged(format!(
            "repo is {}, not the fork {}",
            payload_etag(&bytes),
            fork.fork_hash
        )),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            damaged(format!("repo is missing, not the fork {}", fork.fork_hash));
        }
        Err(err) => {
            return Err(err).with_context(|| format!("reading {}", mirror.display()));
        }
    }
    let backup = storage_root.join(OVERWRITTEN_FILES_PATH).join(&fork.backup);
    match fs::read(&backup) {
        Ok(bytes) if payload_etag(&bytes) == fork.base_hash => {}
        Ok(bytes) => damaged(format!(
            "the fork's backup {OVERWRITTEN_FILES_PATH}/{} is {}, not the {} it replaced",
            fork.backup,
            payload_etag(&bytes),
            fork.base_hash
        )),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            damaged(format!(
                "the fork's backup {OVERWRITTEN_FILES_PATH}/{} is missing",
                fork.backup
            ));
        }
        Err(err) => {
            return Err(err).with_context(|| format!("reading {}", backup.display()));
        }
    }
    if let Some(repo) = repo {
        // With the warnings of the commits a GC's cutoff came after.
        let warnings = verdict.warnings.clone();
        let mut objects = Verdict { warnings, ..Verdict::default() };
        check_mirror_objects(repo, storage_root, &mut objects).await?;
        verdict.warnings = objects.warnings;
        for violation in objects.violations {
            if let Violation::MirrorDangling { detail } = violation {
                damaged(format!("the fork side: {detail}"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{env_info::HostDone, layout::SUPERVISOR_SLOT, log::LogName};

    const GC: Op = Op::Gc { older_than_ms: None };

    fn worker(slot: &str) -> LogName {
        LogName { tag: "h".to_owned(), slot: slot.to_owned(), incarnation: Some(0) }
    }

    fn written(t_ms: i64, fork_hash: &str, gc: Option<Uuid>) -> Record {
        Record::ForkWritten {
            t_before_ms: t_ms - 5,
            t_after_ms: t_ms,
            base_hash: "base".to_owned(),
            fork_hash: fork_hash.to_owned(),
            backup: "repo.1.X".to_owned(),
            gc,
        }
    }

    fn gone(t_ms: i64, fork_hash: &str, found: &str) -> Record {
        Record::ForkGone {
            t_ms,
            fork_hash: fork_hash.to_owned(),
            found: Sample::Hash(found.to_owned()),
        }
    }

    fn rename(t_check_ms: Option<i64>, t_rename_ms: i64, hash: &str) -> Record {
        Record::MirrorRename {
            t_check_ms,
            t_read_ms: None,
            t_rename_ms,
            hash: hash.to_owned(),
        }
    }

    fn op(t_ms: i64, op: Op, result: Option<OpResult>) -> (Uuid, Vec<Record>) {
        let id = Uuid::new_v4();
        let mut records = vec![Record::Intent { t_ms, id, op }];
        if let Some(result) = result {
            records.push(Record::Outcome { t_ms: t_ms + 10, id, result });
        }
        (id, records)
    }

    fn ack() -> Option<OpResult> {
        Some(OpResult::Ack { snapshot: None })
    }

    fn durable() -> Option<OpResult> {
        Some(OpResult::DurableButForked { snapshot: None, message: "fork".to_owned() })
    }

    fn history(logs: Vec<(LogName, Vec<Record>)>) -> History {
        let done = HostDone {
            tag: "h".to_owned(),
            go_seen_at_ms: 0,
            stopped_at_ms: 100_000,
            workers: 1,
            incarnations_spawned: 1,
            external_kills: 0,
            stragglers: 0,
            sampler_error: None,
        };
        History {
            logs: logs.into_iter().collect(),
            done: vec![done],
            ..History::default()
        }
    }

    fn checked(history: &History, network_fs: bool) -> Verdict {
        let mut verdict = Verdict::default();
        check_forks(history, network_fs, &mut verdict);
        verdict
    }

    #[test]
    fn an_erasure_is_explained_only_by_a_publication_checked_before_the_fork() {
        // Checked at 990, before the fork at 1000; renamed at 1020.
        let explained = history(vec![
            (worker("fork"), vec![written(1000, "f", None), gone(1030, "f", "v")]),
            (worker("0"), vec![rename(Some(990), 1020, "v")]),
        ]);
        let verdict = checked(&explained, false);
        assert_eq!(verdict.violations, []);
        assert_eq!(
            verdict.warnings,
            [Warning::ForkErasedInWindow {
                fork_hash: "f".to_owned(),
                landed_ms_after_check: Some(30)
            }]
        );

        for (name, renames) in [
            ("checked after the fork", vec![rename(Some(1100), 1200, "v")]),
            ("without its check's time", vec![rename(None, 1020, "v")]),
            ("renamed before the fork", vec![rename(Some(850), 900, "v")]),
            ("no publication of those bytes", vec![rename(Some(990), 1020, "w")]),
        ] {
            let unexplained = history(vec![
                (worker("fork"), vec![written(1000, "f", None), gone(1300, "f", "v")]),
                (worker("0"), renames),
            ]);
            let verdict = checked(&unexplained, true);
            assert!(
                matches!(&verdict.violations[..], [Violation::SilentForkLoss { fork_hash, .. }]
                    if fork_hash == "f"),
                "{name}: {verdict:?}"
            );
        }
        // Across hosts, within the clock allowance; on one host, exactly.
        let other_host =
            LogName { tag: "g".to_owned(), slot: "0".to_owned(), incarnation: Some(0) };
        for (log, explained) in [(other_host, true), (worker("0"), false)] {
            let run = history(vec![
                (worker("fork"), vec![written(1000, "f", None), gone(1100, "f", "v")]),
                (log, vec![rename(Some(1040), 1060, "v")]),
            ]);
            assert_eq!(checked(&run, false).violations.is_empty(), explained);
        }
        // A missing repo is never explained.
        let missing = history(vec![(
            worker("fork"),
            vec![
                written(1000, "f", None),
                Record::ForkGone {
                    t_ms: 1010,
                    fork_hash: "f".to_owned(),
                    found: Sample::Missing,
                },
            ],
        )]);
        assert!(matches!(
            &checked(&missing, false).violations[..],
            [Violation::SilentForkLoss { .. }]
        ));
    }

    /// The I-forkloss run on a network filesystem: the fork was written in [263, 320]; a
    /// publication's fork check began reading `$ROOT/repo` at `t_read_ms`,
    /// passed at 334 after its slow listing, and its rename ran from 338 to
    /// 434, erasing the fork. A read that began by the end of the fork's
    /// write may have seen the bytes before it: the accepted window.
    #[test]
    fn a_fork_check_is_judged_by_when_its_read_began() {
        let run = |t_read_ms: Option<i64>, t_check_ms: Option<i64>| {
            let fork = Record::ForkWritten {
                t_before_ms: 263,
                t_after_ms: 320,
                base_hash: "base".to_owned(),
                fork_hash: "f".to_owned(),
                backup: "repo.1.X".to_owned(),
                gc: None,
            };
            let hash = "v".to_owned();
            let publication = vec![
                Record::MirrorRenaming {
                    t_check_ms,
                    t_read_ms,
                    t_ms: 338,
                    hash: hash.clone(),
                },
                Record::MirrorRename { t_check_ms, t_read_ms, t_rename_ms: 434, hash },
            ];
            checked(
                &history(vec![
                    (worker("fork"), vec![fork, gone(451, "f", "v")]),
                    (worker("3"), publication),
                ]),
                true,
            )
        };
        let verdict = run(Some(300), Some(334));
        assert_eq!(verdict.violations, []);
        assert_eq!(
            verdict.warnings,
            [Warning::ForkErasedInWindow {
                fork_hash: "f".to_owned(),
                landed_ms_after_check: Some(100)
            }]
        );
        // Read after the fork's write; in a log without the read's time,
        // checked after it; or read in time without a check that passed.
        for stamps in [(Some(330), Some(334)), (None, Some(334)), (Some(300), None)] {
            let verdict = run(stamps.0, stamps.1);
            assert!(
                matches!(&verdict.violations[..], [Violation::SilentForkLoss { fork_hash, .. }]
                    if fork_hash == "f"),
                "{stamps:?}: {verdict:?}"
            );
            assert_eq!(verdict.warnings, [], "{stamps:?}");
        }
    }

    fn announced(t_check_ms: i64, t_ms: i64, hash: &str) -> Record {
        Record::MirrorRenaming {
            t_check_ms: Some(t_check_ms),
            t_read_ms: None,
            t_ms,
            hash: hash.to_owned(),
        }
    }

    fn supervisor(tag: &str) -> LogName {
        LogName {
            tag: tag.to_owned(),
            slot: SUPERVISOR_SLOT.to_owned(),
            incarnation: None,
        }
    }

    fn killed(t_ms: i64, slot: &str, incarnation: u32) -> Record {
        Record::ExternalKill { t_ms, slot: slot.to_owned(), incarnation }
    }

    /// A publication whose fork check passed before the fork, killed before
    /// it could record its rename, may have renamed until its process died.
    #[test]
    fn an_unresolved_rename_explains_an_erasure_until_its_process_died() {
        #[derive(Clone, Copy)]
        enum Death {
            Killed(i64),
            NextStarted(i64),
            Unrecorded,
        }
        let log = |tag: &str, slot: &str, incarnation| LogName {
            tag: tag.to_owned(),
            slot: slot.to_owned(),
            incarnation: Some(incarnation),
        };
        let run = |tag: &str, death: Death, network_fs| {
            let fork = Record::ForkWritten {
                t_before_ms: 1129,
                t_after_ms: 1172,
                base_hash: "base".to_owned(),
                fork_hash: "f".to_owned(),
                backup: "repo.1.X".to_owned(),
                gc: None,
            };
            let mut logs = vec![
                (log("a", "fork", 1), vec![fork, gone(1640, "f", "v")]),
                (log(tag, "6", 5), vec![announced(872, 875, "v")]),
                // Another publication of those bytes, killed before the fork.
                (log("b", "5", 3), vec![announced(917, 923, "v")]),
                (supervisor("b"), vec![killed(952, "5", 3)]),
            ];
            match death {
                Death::Killed(t_ms) => {
                    logs.push((supervisor(tag), vec![killed(t_ms, "6", 5)]));
                }
                Death::NextStarted(t_ms) => {
                    let start = Record::Start {
                        t_ms,
                        host: tag.to_owned(),
                        slot: "6".to_owned(),
                        incarnation: 6,
                        pid: 1,
                    };
                    logs.push((log(tag, "6", 6), vec![start]));
                }
                Death::Unrecorded => {}
            }
            checked(&history(logs), network_fs)
        };
        for (case, tag, death, explained) in [
            ("killed after it was seen gone", "e", Death::Killed(1700), true),
            ("killed before it was seen gone", "e", Death::Killed(1629), true),
            ("killed before the fork", "e", Death::Killed(1000), false),
            ("respawned after the fork", "e", Death::NextStarted(1300), true),
            ("respawned before the fork", "e", Death::NextStarted(1000), false),
            ("no record of its death", "e", Death::Unrecorded, true),
            // Within the clock allowance before the fork: across hosts only.
            ("killed just before the fork", "e", Death::Killed(1100), true),
            ("killed just before the fork, on its host", "a", Death::Killed(1100), false),
        ] {
            let local = run(tag, death, false);
            let network = run(tag, death, true);
            if explained {
                for verdict in [local, network] {
                    assert_eq!(verdict.violations, [], "{case}");
                    assert_eq!(
                        verdict.warnings,
                        [Warning::ForkErasedInWindow {
                            fork_hash: "f".to_owned(),
                            landed_ms_after_check: None
                        }],
                        "{case}"
                    );
                }
            } else {
                assert!(
                    matches!(&local.violations[..], [Violation::SilentForkLoss { fork_hash, .. }]
                        if fork_hash == "f"),
                    "{case}: {local:?}"
                );
                // A killed process's rename RPC may land after its death.
                assert_eq!(network.violations, [], "{case}");
                assert_eq!(
                    network.warnings,
                    [Warning::ForkErasedAfterKilledPublisher {
                        fork_hash: "f".to_owned(),
                        replaced_by: "v".to_owned()
                    }],
                    "{case}"
                );
            }
        }
    }

    /// On a network filesystem too, an erasure no publication whose fork
    /// check passed before the fork may explain is a violation.
    #[test]
    fn a_publisher_killed_before_the_fork_excuses_only_its_own_bytes() {
        let other_host =
            LogName { tag: "g".to_owned(), slot: "0".to_owned(), incarnation: Some(0) };
        let killed_before = || vec![announced(900, 910, "v")];
        let observed = |t_ms| Record::Observed { t_ms, what: "x".to_owned() };
        for (case, records, others, excused) in [
            ("killed before the fork", killed_before(), vec![], true),
            ("another hash", vec![announced(900, 910, "w")], vec![], false),
            ("checked after the fork", vec![announced(1100, 1110, "v")], vec![], false),
            (
                "announced after it was seen gone",
                vec![announced(990, 1400, "v")],
                vec![],
                false,
            ),
            ("done before the fork", vec![rename(Some(900), 910, "v")], vec![], false),
            (
                "returned before the fork",
                vec![announced(900, 910, "v"), observed(915)],
                vec![],
                false,
            ),
            ("no publication", vec![], vec![], false),
            // Only bytes no other publication may have renamed then.
            (
                "with a publication checked after the fork",
                killed_before(),
                vec![announced(1100, 1150, "v"), rename(Some(1100), 1160, "v")],
                false,
            ),
            (
                "with one without its check's time",
                killed_before(),
                vec![rename(None, 1160, "v")],
                false,
            ),
            (
                "with one checked after the fork, before it",
                killed_before(),
                vec![rename(Some(1100), 940, "v")],
                true,
            ),
        ] {
            let run = history(vec![
                (worker("fork"), vec![written(1000, "f", None), gone(1300, "f", "v")]),
                (other_host.clone(), records),
                (supervisor("g"), vec![killed(920, "0", 0)]),
                (worker("1"), others),
            ]);
            let verdict = checked(&run, true);
            if excused {
                assert_eq!(verdict.violations, [], "{case}");
            } else {
                assert!(
                    matches!(&verdict.violations[..], [Violation::SilentForkLoss { .. }]),
                    "{case}: {verdict:?}"
                );
                assert_eq!(verdict.warnings, [], "{case}");
            }
        }
    }

    /// Of several publications that may have erased a fork, the one whose
    /// rename is known to have returned soonest after its check explains it,
    /// whatever the logs' order.
    #[test]
    fn the_explaining_rename_is_the_one_done_soonest_after_its_check() {
        let soon = vec![announced(990, 1010, "v"), rename(Some(990), 1020, "v")];
        let late = vec![announced(980, 1005, "v"), rename(Some(980), 1250, "v")];
        let unresolved = vec![announced(980, 1005, "v")];
        for (first, second) in [
            (soon.clone(), late.clone()),
            (late, soon.clone()),
            (unresolved.clone(), soon.clone()),
            (soon, unresolved),
        ] {
            let run = history(vec![
                (worker("fork"), vec![written(1000, "f", None), gone(1300, "f", "v")]),
                (worker("0"), first),
                (worker("1"), second),
            ]);
            let verdict = checked(&run, false);
            assert_eq!(verdict.violations, []);
            assert_eq!(
                verdict.warnings,
                [Warning::ForkErasedInWindow {
                    fork_hash: "f".to_owned(),
                    landed_ms_after_check: Some(30)
                }]
            );
        }
    }

    /// A rename call that failed may hide a rename that landed, as `ENOENT`
    /// after a lost reply: it may have renamed until its log's next record.
    #[test]
    fn a_rename_announced_and_never_done_may_have_landed_until_the_next_record() {
        let observed = |t_ms| Record::Observed { t_ms, what: "x".to_owned() };
        for (records, explained) in [
            (vec![announced(990, 1010, "v"), observed(1040)], true),
            (
                vec![
                    announced(990, 1010, "v"),
                    observed(1020),
                    rename(Some(985), 1030, "w"),
                ],
                true,
            ),
            (vec![announced(980, 985, "v"), observed(990)], false),
        ] {
            let run = history(vec![
                (worker("fork"), vec![written(1000, "f", None), gone(1030, "f", "v")]),
                (worker("0"), records.clone()),
            ]);
            for network_fs in [false, true] {
                let verdict = checked(&run, network_fs);
                if explained {
                    assert_eq!(verdict.violations, [], "{records:?}");
                    assert_eq!(
                        verdict.warnings,
                        [Warning::ForkErasedInWindow {
                            fork_hash: "f".to_owned(),
                            landed_ms_after_check: None
                        }]
                    );
                } else {
                    // Its process lived on: no late RPC of a killed one.
                    assert!(
                        matches!(
                            &verdict.violations[..],
                            [Violation::SilentForkLoss { .. }]
                        ),
                        "{records:?}: {verdict:?}"
                    );
                }
            }
        }
    }

    /// A rename recorded done may have landed any time after its
    /// announcement: it explains an erasure seen before it returned, and
    /// erased the fork as early as then.
    #[test]
    fn a_done_rename_may_have_landed_from_its_announcement() {
        let run = history(vec![
            (worker("fork"), vec![written(1000, "f", None), gone(1030, "f", "v")]),
            (worker("0"), vec![announced(990, 1020, "v"), rename(Some(990), 1200, "v")]),
        ]);
        let verdict = checked(&run, false);
        assert_eq!(verdict.violations, []);
        assert_eq!(
            verdict.warnings,
            [Warning::ForkErasedInWindow {
                fork_hash: "f".to_owned(),
                landed_ms_after_check: Some(210)
            }]
        );

        let (_, acked) = op(1060, Op::MainCommit, ack());
        let run = history(vec![
            (worker("fork"), vec![written(1000, "f", None), gone(1300, "f", "v")]),
            (worker("1"), vec![announced(940, 960, "v"), rename(Some(940), 1200, "v")]),
            (worker("0"), acked),
        ]);
        let forks = logged_forks(&run);
        let renames = logged_renames(&run);
        // The fork's write began at 995.
        assert_eq!(erased_at(&forks[0], &renames, 50), Some(960));
        assert_eq!(checked(&run, false).violations, []);
    }

    /// F2 and F3 judge no operation after a fork past the earliest time an
    /// unresolved rename may have erased it.
    #[test]
    fn an_unresolved_rename_erases_a_fork_as_early_as_it_may_have() {
        let (_, acked) = op(1060, Op::MainCommit, ack());
        let run = history(vec![
            (worker("fork"), vec![written(1000, "f", None), gone(1300, "f", "v")]),
            (worker("1"), vec![announced(890, 900, "v")]),
            (supervisor("h"), vec![killed(1200, "1", 0)]),
            (worker("0"), acked),
        ]);
        let forks = logged_forks(&run);
        let renames = logged_renames(&run);
        // The fork's write began at 995.
        assert_eq!(erased_at(&forks[0], &renames, 50), Some(945));
        assert_eq!(checked(&run, false).violations, []);
    }

    #[test]
    fn an_operation_that_landed_after_a_fork_reports_it() {
        let (reported, reported_records) = op(2000, Op::MainCommit, durable());
        let (unreported, unreported_records) = op(2000, Op::MainCommit, ack());
        let (before, before_records) = op(900, Op::MainCommit, ack());
        let (failed, failed_records) = op(2000, Op::MainCommit, Some(OpResult::Conflict));
        let (unresolved, unresolved_records) = op(2000, Op::MainCommit, None);
        let mut records = vec![];
        for r in [
            reported_records,
            unreported_records,
            before_records,
            failed_records,
            unresolved_records,
        ] {
            records.extend(r);
        }
        let run = history(vec![
            (worker("fork"), vec![written(1000, "f", None)]),
            (worker("0"), records),
        ]);
        assert_eq!(
            checked(&run, false).violations,
            [Violation::UnreportedFork { id: unreported, op: "MainCommit".to_owned() }]
        );
        let verdict = checked(&run, true);
        assert_eq!(verdict.violations, []);
        assert_eq!(
            verdict.warnings,
            [Warning::UnreportedFork { id: unreported, op: "MainCommit".to_owned() }]
        );
        let _ = (reported, before, failed, unresolved);

        // Within ε of the fork, or ending after its erasure: no rule.
        let (_, near) = op(1040, Op::MainCommit, ack());
        let (_, late) = op(1500, Op::MainCommit, ack());
        let erased = history(vec![
            (worker("fork"), vec![written(1000, "f", None), gone(1505, "f", "v")]),
            (worker("1"), vec![rename(Some(990), 1500, "v")]),
            (worker("0"), [near, late].concat()),
        ]);
        assert_eq!(checked(&erased, false).violations, []);
    }

    #[test]
    fn no_gc_is_acknowledged_after_a_fork_or_with_one_written_during_it() {
        let (after, after_records) = op(2000, GC, ack());
        let (before, before_records) = op(500, GC, ack());
        let (refused, refused_records) =
            op(3000, GC, Some(OpResult::Forked { message: "fork".to_owned() }));
        let run = history(vec![
            (worker("fork"), vec![written(1000, "f", None)]),
            (worker("gc"), [after_records, before_records, refused_records].concat()),
        ]);
        assert_eq!(
            checked(&run, false).violations,
            [Violation::GcAfterFork { id: after }]
        );
        assert_eq!(checked(&run, true).warnings, [Warning::GcAfterFork { id: after }]);
        let _ = (before, refused);

        // Written by the GC worker during the change of a GC that started
        // before it.
        let (during, during_records) = op(990, GC, ack());
        let (_, refused_records) =
            op(995, GC, Some(OpResult::Forked { message: "fork".to_owned() }));
        let run = history(vec![(
            worker("gc"),
            [vec![written(1000, "f", Some(during))], during_records, refused_records]
                .concat(),
        )]);
        assert_eq!(
            checked(&run, false).violations,
            [Violation::GcAfterFork { id: during }]
        );
    }

    #[test]
    fn the_last_fork_persists_unless_seen_gone() {
        let run = history(vec![(
            worker("fork"),
            vec![
                written(1000, "f", None),
                gone(1100, "f", "v"),
                written(1700, "g", None),
            ],
        )]);
        let forks = logged_forks(&run);
        assert_eq!(forks.len(), 2);
        assert_eq!(
            persisted_fork(&forks).map(|fork| fork.fork_hash),
            Some("g".to_owned())
        );
        let run = history(vec![(
            worker("fork"),
            vec![written(1000, "f", None), gone(1100, "f", "v")],
        )]);
        assert_eq!(persisted_fork(&logged_forks(&run)), None);
    }

    #[tokio::test]
    async fn a_persisted_fork_keeps_its_bytes_and_its_backup() {
        let dir = tempfile::tempdir().unwrap();
        let storage = dir.path();
        fs::create_dir_all(storage.join(OVERWRITTEN_FILES_PATH)).unwrap();
        fs::write(storage.join(REPO_KEY), b"fork").unwrap();
        fs::write(storage.join(OVERWRITTEN_FILES_PATH).join("repo.1.X"), b"base")
            .unwrap();
        let fork = LoggedFork {
            tag: "h".to_owned(),
            t_before_ms: 0,
            t_after_ms: 1,
            base_hash: payload_etag(b"base"),
            fork_hash: payload_etag(b"fork"),
            backup: "repo.1.X".to_owned(),
            gc: None,
            gone: None,
        };
        let mut verdict = Verdict::default();
        check_fork_sides(storage, &fork, None, &mut verdict).await.unwrap();
        assert_eq!(verdict.violations, []);

        fs::write(storage.join(REPO_KEY), b"other").unwrap();
        fs::remove_file(storage.join(OVERWRITTEN_FILES_PATH).join("repo.1.X")).unwrap();
        let mut verdict = Verdict::default();
        check_fork_sides(storage, &fork, None, &mut verdict).await.unwrap();
        assert_eq!(verdict.violations.len(), 2, "{verdict:?}");
        assert!(
            verdict.violations.iter().all(|v| matches!(v, Violation::ForkDamaged { .. }))
        );
    }

    /// F5 excuses the fork side's missing transaction log of a commit that a
    /// GC's cutoff came after as M7 does, and nothing else.
    #[tokio::test]
    async fn the_fork_side_may_miss_the_objects_of_a_commit_a_gc_cutoff_came_after() {
        use crate::{
            oracle::gc_cutoff::{check_gc_cutoffs, tests::incident},
            repo_mode::{
                open_repo,
                tests::{repo_root, repo_worker, who},
            },
        };
        let (_dir, root, config) = repo_root().await;
        let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert!(worker.ensure_branch().await.unwrap());
        let mut snapshots = Vec::new();
        for _ in 0..2 {
            match worker.branch_commit().await.unwrap() {
                Some(OpResult::Ack { snapshot: Some(snapshot) }) => {
                    snapshots.push(snapshot);
                }
                other => panic!("expected an ack, got {other:?}"),
            }
        }
        let storage = root.storage();
        let backup = storage.join(OVERWRITTEN_FILES_PATH).join("repo.1.X");
        fs::write(&backup, b"base").unwrap();
        let fork = LoggedFork {
            tag: "h".to_owned(),
            t_before_ms: 0,
            t_after_ms: 1,
            base_hash: payload_etag(b"base"),
            fork_hash: payload_etag(&fs::read(storage.join(REPO_KEY)).unwrap()),
            backup: "repo.1.X".to_owned(),
            gc: None,
            gone: None,
        };
        let repo = open_repo(&root, &config, None).await.unwrap();
        let check = |snapshot: &str| {
            let tx_log = storage.join("transactions").join(snapshot);
            let bytes = fs::read(&tx_log).unwrap();
            fs::remove_file(&tx_log).unwrap();
            let history =
                incident(Uuid::new_v4(), Uuid::new_v4(), &snapshots[0], 973_998);
            let (storage, fork, repo) = (&storage, &fork, &repo);
            async move {
                let mut verdict = Verdict::default();
                check_gc_cutoffs(&history, &mut verdict);
                check_fork_sides(storage, fork, Some(repo), &mut verdict).await.unwrap();
                fs::write(&tx_log, bytes).unwrap();
                verdict
            }
        };

        let verdict = check(&snapshots[0]).await;
        assert_eq!(verdict.violations, []);
        assert!(
            matches!(&verdict.warnings[..], [Warning::GcCutoffAfterSessionStart { missing, .. }]
                if *missing == [format!("transactions/{}", snapshots[0])]),
            "{verdict:?}"
        );
        let verdict = check(&snapshots[1]).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::ForkDamaged { detail }]
                if detail.contains(&snapshots[1])),
            "{verdict:?}"
        );
    }
}
