//! Repository mode: checks the branches, `main` and the tags of the final
//! repository against every logged intent, and reads back the chunk of every
//! acknowledged commit.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use bytes::Bytes;
use futures::TryStreamExt as _;
use icechunk::{
    Repository,
    error::ICError,
    format::{
        ByteRange, ChunkIndices, Path, SNAPSHOTS_FILE_PATH, SnapshotId,
        snapshot::{Snapshot, SnapshotInfo},
    },
    repository::VersionInfo,
    session::get_chunk,
    storage::{StorageContext, StorageErrorKind},
};
use uuid::Uuid;

use crate::{
    config::RunConfig,
    log::{LogName, Op, OpResult, Record},
    oracle::{History, Intents, Verdict, Violation, Warning, intents},
    repo_mode::{
        MAIN_BRANCH, array_path_on_branch, array_path_on_main, chunk_bytes,
        parse_branch_name, parse_commit_message,
    },
};

/// Checks the repository against `history`. `Err` only if the repository's
/// branches or tags can't be listed, or a snapshot file can't be opened for a
/// reason other than its absence.
pub async fn check_repo(
    repo: &Repository,
    history: &History,
    config: &RunConfig,
    network_fs: bool,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    let intents = intents(history, verdict);
    let ops = in_log_order(history, &intents);
    count(&ops, verdict);

    let branches = repo.list_branches().await.map_err(read_error("listing branches"))?;
    let mut creates: HashMap<&str, Vec<(Uuid, Option<&OpResult>)>> = HashMap::new();
    for &(_, id, op, result) in &ops {
        if let Op::CreateBranch { branch } = op {
            creates.entry(branch).or_default().push((id, result));
        }
    }
    let mut landed: HashMap<&str, Landed> = HashMap::new();
    // Branches whose ancestry couldn't be walked: their commits go unchecked.
    let mut unreadable: HashSet<&str> = HashSet::new();
    let mut relanded: HashSet<Uuid> = HashSet::new();
    let mut opened: HashSet<SnapshotId> = HashSet::new();
    for branch in &branches {
        if branch != MAIN_BRANCH {
            if parse_branch_name(branch).is_none() {
                phantom(verdict, format!("branch {branch}"));
                continue;
            }
            check_creation(branch, creates.get(branch.as_str()), network_fs, verdict);
        }
        let ancestry = match ancestry(repo, branch).await {
            Ok(ancestry) => ancestry,
            Err(err) => {
                verdict.violations.push(Violation::BrokenHistory {
                    detail: format!("reading the ancestry of {branch}: {err:#}"),
                });
                unreadable.insert(branch);
                continue;
            }
        };
        for info in &ancestry {
            if opened.insert(info.id.clone())
                && !snapshot_file_exists(repo, &info.id).await?
            {
                verdict.violations.push(Violation::BrokenHistory {
                    detail: format!(
                        "snapshot {} on {branch} (message {:?}) has no snapshot file",
                        info.id, info.message
                    ),
                });
            }
        }
        let commits = landed_commits(branch, &ancestry, &intents, &mut relanded, verdict);
        landed.insert(branch, commits);
    }
    verdict.stats.chain_len = landed.values().map(|commits| commits.len() as u64).sum();

    let tags = repo.list_tags().await.map_err(read_error("listing tags"))?;
    let mut acks = BranchAcks::default();
    // The acked commits that landed as acked, with the array they wrote.
    let mut read_backs: Vec<(Uuid, &SnapshotId, Path)> = Vec::new();
    for &(writer, id, op, result) in &ops {
        let is_commit = matches!(op, Op::BranchCommit { .. } | Op::MainCommit);
        if is_commit && unreadable.contains(commit_branch(op)) {
            continue;
        }
        // A commit that landed more than once reported a conflict to its own
        // rebase, so it is a false conflict even if acked. A failed outcome
        // that landed gets the usual rule below instead.
        if relanded.contains(&id) && matches!(result, None | Some(OpResult::Ack { .. })) {
            conflict_but_landed(id, true, network_fs, verdict);
        }
        match (op, result) {
            (_, None) => {}
            (Op::CreateBranch { branch }, Some(OpResult::Ack { .. })) => {
                if !branches.contains(branch) {
                    lost(verdict, id, format!("branch {branch} doesn't exist"));
                }
            }
            (
                Op::BranchCommit { .. } | Op::MainCommit,
                Some(OpResult::Ack { snapshot }),
            ) => {
                let Some(snapshot) =
                    acks.check(id, op, snapshot.as_deref(), &landed, verdict)
                else {
                    continue;
                };
                let array = match op {
                    Op::BranchCommit { branch } => match parse_branch_name(branch) {
                        Some((tag, slot)) => array_path_on_branch(tag, slot)?,
                        None => continue,
                    },
                    _ => array_path_on_main(&writer.tag, &writer.slot)?,
                };
                read_backs.push((id, snapshot, array));
            }
            (Op::Tag { name, snapshot }, Some(OpResult::Ack { .. })) => {
                if !tags.contains(name) {
                    lost(verdict, id, format!("tag {name} doesn't exist"));
                    continue;
                }
                match repo.lookup_tag(name).await {
                    Ok(target) if target.to_string() == *snapshot => {}
                    Ok(target) => {
                        lost(verdict, id, format!("tag {name} points at {target}"));
                    }
                    Err(err) => verdict.violations.push(Violation::BrokenHistory {
                        detail: format!("looking up tag {name}: {}", err.kind()),
                    }),
                }
            }
            (_, Some(failed)) => {
                let landed_anyway = match op {
                    Op::BranchCommit { .. } | Op::MainCommit => landed
                        .get(commit_branch(op))
                        .is_some_and(|commits| commits.contains_key(&id)),
                    Op::Tag { name, .. } => tags.contains(name),
                    _ => false,
                };
                if landed_anyway {
                    failed_but_landed(id, failed, network_fs, verdict);
                }
            }
        }
    }

    let tag_intents: HashSet<&str> = ops
        .iter()
        .filter_map(|(_, _, op, _)| match op {
            Op::Tag { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    for tag in tags.iter().filter(|tag| !tag_intents.contains(tag.as_str())) {
        phantom(verdict, format!("tag {tag}"));
    }

    // Without expiry every acked snapshot stays reachable, so GC must keep
    // every one of these chunks.
    for (id, snapshot, array) in read_backs {
        let detail = match read_chunk(repo, snapshot, &array).await {
            Ok(Some(chunk)) if chunk == chunk_bytes(id, config.chunk_bytes) => continue,
            Ok(Some(_)) => {
                format!("chunk [0] of {array} at {snapshot} isn't the one {id} wrote")
            }
            Ok(None) => format!("no chunk [0] in {array} at {snapshot}, written by {id}"),
            Err(err) => format!("reading chunk [0] of {array} at {snapshot}: {err:#}"),
        };
        verdict.violations.push(Violation::DataMismatch { detail });
    }
    Ok(())
}

/// The position of the last acked commit on each worker branch, in log order.
#[derive(Debug, Default)]
struct BranchAcks<'a> {
    last_position: HashMap<&'a str, usize>,
}

impl<'a> BranchAcks<'a> {
    /// Checks that the acked commit `id` landed as `snapshot` on its branch,
    /// and on a worker branch after the commits acked before it. Returns the
    /// snapshot if it landed as acked.
    fn check(
        &mut self,
        id: Uuid,
        op: &'a Op,
        snapshot: Option<&str>,
        landed: &'a HashMap<&str, Landed>,
        verdict: &mut Verdict,
    ) -> Option<&'a SnapshotId> {
        let branch = commit_branch(op);
        let checked = match landed.get(branch).and_then(|commits| commits.get(&id)) {
            None => {
                lost(verdict, id, format!("not in the ancestry of {branch}"));
                None
            }
            Some(commit) if Some(commit.snapshot.to_string().as_str()) == snapshot => {
                Some(commit)
            }
            Some(commit) => {
                let detail = format!(
                    "acknowledged as snapshot {snapshot:?} but landed on {branch} as {}",
                    commit.snapshot
                );
                lost(verdict, id, detail);
                None
            }
        };
        if let (Op::BranchCommit { .. }, Some(commit)) = (op, checked) {
            if self.last_position.get(branch).is_some_and(|&p| p >= commit.position) {
                verdict.violations.push(Violation::BrokenHistory {
                    detail: format!(
                        "{id} landed on {branch} before a commit acknowledged earlier"
                    ),
                });
            }
            self.last_position.insert(branch, commit.position);
        }
        checked.map(|commit| &commit.snapshot)
    }
}

/// A failed outcome whose operation landed: a violation for a conflict on a
/// local filesystem, a warning otherwise.
fn failed_but_landed(
    id: Uuid,
    failed: &OpResult,
    network_fs: bool,
    verdict: &mut Verdict,
) {
    match failed {
        OpResult::Conflict => conflict_but_landed(id, false, network_fs, verdict),
        OpResult::Error { .. } => {
            verdict.warnings.push(Warning::AmbiguousErrorLanded { id });
        }
        OpResult::Ack { .. } => {}
    }
}

/// A conflict reported for an update that landed: a violation on a local
/// filesystem, a warning otherwise.
fn conflict_but_landed(
    id: Uuid,
    landed_twice: bool,
    network_fs: bool,
    verdict: &mut Verdict,
) {
    if network_fs {
        verdict.warnings.push(Warning::LostReplyConflict { id });
    } else {
        verdict.violations.push(Violation::FalseConflict { id, landed_twice });
    }
}

/// An intent with the log it is in and its outcome.
type LoggedOp<'a> = (&'a LogName, Uuid, &'a Op, Option<&'a OpResult>);

/// Each intent once, in the order its worker logged it, incarnations in
/// order.
fn in_log_order<'a>(history: &'a History, intents: &'a Intents) -> Vec<LoggedOp<'a>> {
    let mut seen = HashSet::new();
    history
        .worker_records()
        .filter_map(|(name, record)| match record {
            Record::Intent { id, .. } if seen.insert(*id) => {
                intents.get(id).map(|(op, result, _)| (name, *id, op, result.as_ref()))
            }
            _ => None,
        })
        .collect()
}

fn count(ops: &[LoggedOp<'_>], verdict: &mut Verdict) {
    for &(_, id, op, result) in ops {
        if let Op::Cas { .. } = op {
            verdict.violations.push(Violation::Harness {
                detail: format!("repository-mode intent {id} is {op:?}"),
            });
            continue;
        }
        let stats = &mut verdict.stats;
        stats.intents += 1;
        match result {
            Some(OpResult::Ack { .. }) => stats.acks += 1,
            Some(OpResult::Conflict) => stats.conflicts += 1,
            Some(OpResult::Error { .. }) => stats.errors += 1,
            None => stats.unresolved += 1,
        }
    }
}

/// The branch a commit intent targets.
fn commit_branch(op: &Op) -> &str {
    match op {
        Op::BranchCommit { branch } => branch,
        _ => MAIN_BRANCH,
    }
}

#[derive(Debug)]
struct Commit {
    /// From the root: the first commit after the initial snapshot is 0.
    position: usize,
    snapshot: SnapshotId,
}

/// A branch's stress commits by intent id.
type Landed = HashMap<Uuid, Commit>;

/// A worker branch exists, so one of its `CreateBranch` intents landed. If
/// every one failed, the one that landed reported a failure.
fn check_creation(
    branch: &str,
    creates: Option<&Vec<(Uuid, Option<&OpResult>)>>,
    network_fs: bool,
    verdict: &mut Verdict,
) {
    let Some(creates) = creates.filter(|creates| !creates.is_empty()) else {
        phantom(verdict, format!("branch {branch}, created without an intent"));
        return;
    };
    if creates
        .iter()
        .any(|(_, result)| matches!(result, None | Some(OpResult::Ack { .. })))
    {
        return;
    }
    let errored = creates
        .iter()
        .rev()
        .find(|(_, result)| matches!(result, Some(OpResult::Error { .. })));
    match (errored, creates.last()) {
        (Some(&(id, Some(error))), _) => {
            failed_but_landed(id, error, network_fs, verdict);
        }
        (_, Some(&(id, _))) => {
            failed_but_landed(id, &OpResult::Conflict, network_fs, verdict);
        }
        _ => {}
    }
}

/// `branch`'s ancestry, tip first.
async fn ancestry(repo: &Repository, branch: &str) -> anyhow::Result<Vec<SnapshotInfo>> {
    repo.ancestry(&VersionInfo::BranchTipRef(branch.to_owned()))
        .await
        .map_err(read_error("starting the walk"))?
        .try_collect()
        .await
        .map_err(read_error("walking"))
}

/// The commits in `ancestry` (tip first), each of which must carry out a
/// commit intent for `branch`. A commit that landed twice is
/// added to `relanded` on `main`; elsewhere, without rebase, it is broken
/// history.
fn landed_commits(
    branch: &str,
    ancestry: &[SnapshotInfo],
    intents: &Intents,
    relanded: &mut HashSet<Uuid>,
    verdict: &mut Verdict,
) -> Landed {
    let mut broken =
        |detail| verdict.violations.push(Violation::BrokenHistory { detail });
    let Some((root, commits)) = ancestry.split_last() else {
        broken(format!("{branch} has no snapshots"));
        return Landed::new();
    };
    if root.id != Snapshot::INITIAL_SNAPSHOT_ID {
        broken(format!(
            "the ancestry of {branch} ends at {}, not the initial snapshot",
            root.id
        ));
    }
    let mut landed = Landed::new();
    for (position, info) in commits.iter().rev().enumerate() {
        let id = parse_commit_message(&info.message).filter(|id| {
            intents.get(id).is_some_and(|(op, _, _)| match op {
                Op::BranchCommit { .. } | Op::MainCommit => commit_branch(op) == branch,
                _ => false,
            })
        });
        let Some(id) = id else {
            phantom(
                verdict,
                format!(
                    "snapshot {} on {branch} with message {:?}",
                    info.id, info.message
                ),
            );
            continue;
        };
        let commit = Commit { position, snapshot: info.id.clone() };
        // The later landing is the one acked.
        if let Some(earlier) = landed.insert(id, commit) {
            if branch == MAIN_BRANCH {
                relanded.insert(id);
                continue;
            }
            verdict.violations.push(Violation::BrokenHistory {
                detail: format!(
                    "{id} landed on {branch} twice, first as {}",
                    earlier.snapshot
                ),
            });
        }
    }
    landed
}

/// Whether `snapshots/<id>` exists, by a fresh open of that name: a
/// directory listing may be stale on a network filesystem.
async fn snapshot_file_exists(
    repo: &Repository,
    id: &SnapshotId,
) -> anyhow::Result<bool> {
    let ctx = StorageContext::unattributed(repo.storage_settings());
    let key = format!("{SNAPSHOTS_FILE_PATH}/{id}");
    // `object_store` rejects empty ranges; snapshot files are never empty.
    match repo.storage().get_object_range(&ctx, &key, Some(&(0..1))).await {
        Ok(_) => Ok(true),
        Err(err) if matches!(err.kind(), StorageErrorKind::ObjectNotFound) => Ok(false),
        Err(err) => Err(anyhow::anyhow!("opening {key}: {}", err.kind())),
    }
}

async fn read_chunk(
    repo: &Repository,
    snapshot: &SnapshotId,
    array: &Path,
) -> anyhow::Result<Option<Bytes>> {
    let session = repo
        .readonly_session(&VersionInfo::SnapshotId(snapshot.clone()))
        .await
        .map_err(read_error("opening a session"))?;
    let index = ChunkIndices(vec![0]);
    let reader = session
        .get_chunk_reader(array, &index, &ByteRange::ALL)
        .await
        .map_err(read_error("locating the chunk"))?;
    get_chunk(reader).await.map_err(read_error("reading the chunk"))
}

fn phantom(verdict: &mut Verdict, what: String) {
    verdict.violations.push(Violation::Phantom { what });
}

fn lost(verdict: &mut Verdict, id: Uuid, detail: String) {
    verdict.violations.push(Violation::LostUpdate { id, detail });
}

/// Leaves out the error's span trace.
fn read_error<E: Display>(what: &str) -> impl FnOnce(ICError<E>) -> anyhow::Error {
    let what = what.to_owned();
    move |err| anyhow::anyhow!("{what}: {}", err.kind())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path as FsPath, PathBuf},
    };

    use bytes::Bytes;
    use icechunk::format::{ChunkIndices, SnapshotId, snapshot::Snapshot};
    use uuid::Uuid;

    use super::*;
    use crate::{
        layout::RunRoot,
        log::{LogWriter, Op, OpResult, Record, read_all_logs, read_log},
        oracle::{Violation, Warning},
        repo_mode::{
            MAIN_BRANCH, RepoWorker, array_path_on_branch, array_path_on_main,
            branch_name, chunk_bytes, commit_message, open_repo, tag_name,
            tests::{repo_root, repo_worker, who},
        },
    };

    fn step(result: anyhow::Result<Option<OpResult>>) -> String {
        match result.unwrap() {
            Some(OpResult::Ack { snapshot: Some(snapshot) }) => snapshot,
            other => panic!("expected an ack, got {other:?}"),
        }
    }

    /// Two workers on host `h`: slot 0 commits twice to its branch, once to
    /// `main` and tags; slot 1 commits once to each.
    async fn clean_run(root: &RunRoot, config: &RunConfig) -> (RepoWorker, RepoWorker) {
        let mut zero = repo_worker(root, config, &who("h", "0", 0)).await;
        let mut one = repo_worker(root, config, &who("h", "1", 0)).await;
        for worker in [&zero, &one] {
            assert!(worker.ensure_branch().await.unwrap());
        }
        step(zero.branch_commit().await);
        step(one.main_commit().await);
        step(zero.main_commit().await);
        step(one.branch_commit().await);
        step(zero.branch_commit().await);
        step(zero.tag().await);
        (zero, one)
    }

    async fn check(root: &RunRoot, config: &RunConfig, network_fs: bool) -> Verdict {
        let repo = open_repo(root, config, None).await.unwrap();
        let history =
            History { logs: read_all_logs(root).unwrap(), ..History::default() };
        let mut verdict = Verdict::default();
        check_repo(&repo, &history, config, network_fs, &mut verdict).await.unwrap();
        verdict
    }

    /// A log of an incarnation that never ran, for records no worker wrote.
    fn forged_log(root: &RunRoot, slot: &str, incarnation: u32) -> LogWriter {
        LogWriter::create(&root.worker_log("h", slot, incarnation)).unwrap()
    }

    fn intent(id: Uuid, op: Op) -> Record {
        Record::Intent { t_ms: 0, id, op }
    }

    fn outcome(id: Uuid, result: OpResult) -> Record {
        Record::Outcome { t_ms: 0, id, result }
    }

    fn ack(id: Uuid, snapshot: &str) -> Record {
        outcome(id, OpResult::Ack { snapshot: Some(snapshot.to_owned()) })
    }

    fn branch_op(slot: &str) -> Op {
        Op::BranchCommit { branch: branch_name("h", slot) }
    }

    /// Commits a chunk to slot `slot`'s branch array as `id` would, without
    /// logging anything.
    async fn unlogged_commit(
        root: &RunRoot,
        config: &RunConfig,
        slot: &str,
        id: Uuid,
    ) -> String {
        let repo = open_repo(root, config, None).await.unwrap();
        let mut session = repo.writable_session(&branch_name("h", slot)).await.unwrap();
        let array = array_path_on_branch("h", slot).unwrap();
        let index = ChunkIndices(vec![0]);
        let payload = session.get_chunk_writer(&array, &index).unwrap()(chunk_bytes(
            id,
            config.chunk_bytes,
        ))
        .await
        .unwrap();
        session.set_chunk_ref(array, index, Some(payload)).await.unwrap();
        session.commit(commit_message(id)).execute().await.unwrap().to_string()
    }

    /// Commits a chunk to slot `slot`'s array on `main` as `id` would,
    /// without logging anything.
    async fn unlogged_main_commit(
        root: &RunRoot,
        config: &RunConfig,
        slot: &str,
        id: Uuid,
    ) -> String {
        let repo = open_repo(root, config, None).await.unwrap();
        let mut session = repo.writable_session(MAIN_BRANCH).await.unwrap();
        let array = array_path_on_main("h", slot).unwrap();
        let index = ChunkIndices(vec![0]);
        let payload = session.get_chunk_writer(&array, &index).unwrap()(chunk_bytes(
            id,
            config.chunk_bytes,
        ))
        .await
        .unwrap();
        session.set_chunk_ref(array, index, Some(payload)).await.unwrap();
        session.commit(commit_message(id)).execute().await.unwrap().to_string()
    }

    #[tokio::test]
    async fn main_commit_landing_twice_follows_the_false_conflict_rule() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let id = Uuid::new_v4();
        unlogged_main_commit(&root, &config, "1", id).await;
        let second = unlogged_main_commit(&root, &config, "1", id).await;
        let log = forged_log(&root, "1", 1);
        log.append(&intent(id, Op::MainCommit)).unwrap();
        log.append(&ack(id, &second)).unwrap();

        let local = check(&root, &config, false).await;
        assert_eq!(
            local.violations,
            [Violation::FalseConflict { id, landed_twice: true }]
        );
        assert_eq!(local.warnings, []);

        let network = check(&root, &config, true).await;
        assert_eq!(network.violations, []);
        assert_eq!(network.warnings, [Warning::LostReplyConflict { id }]);
    }

    #[tokio::test]
    async fn errored_main_commit_landing_twice_is_reported_once() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let id = Uuid::new_v4();
        unlogged_main_commit(&root, &config, "1", id).await;
        unlogged_main_commit(&root, &config, "1", id).await;
        let log = forged_log(&root, "1", 1);
        log.append(&intent(id, Op::MainCommit)).unwrap();
        let error = OpResult::Error { message: "timed out".to_owned() };
        log.append(&outcome(id, error)).unwrap();
        for network_fs in [false, true] {
            let verdict = check(&root, &config, network_fs).await;
            assert_eq!(verdict.violations, []);
            assert_eq!(verdict.warnings, [Warning::AmbiguousErrorLanded { id }]);
        }
    }

    #[tokio::test]
    async fn branch_commit_landing_twice_breaks_history() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let id = Uuid::new_v4();
        unlogged_commit(&root, &config, "1", id).await;
        let second = unlogged_commit(&root, &config, "1", id).await;
        let log = forged_log(&root, "1", 1);
        log.append(&intent(id, branch_op("1"))).unwrap();
        log.append(&ack(id, &second)).unwrap();
        for network_fs in [false, true] {
            let verdict = check(&root, &config, network_fs).await;
            assert!(
                matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                    if detail.contains("twice")),
                "{verdict:?}"
            );
        }
    }

    #[tokio::test]
    async fn failed_branch_creation_that_landed_follows_the_false_conflict_rule() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let repo = open_repo(&root, &config, None).await.unwrap();
        let [conflicted, errored] = [Uuid::new_v4(), Uuid::new_v4()];
        for (slot, id, result) in [
            ("7", conflicted, OpResult::Conflict),
            ("8", errored, OpResult::Error { message: "timed out".to_owned() }),
        ] {
            let branch = branch_name("h", slot);
            repo.create_branch(&branch, &Snapshot::INITIAL_SNAPSHOT_ID).await.unwrap();
            let log = forged_log(&root, slot, 0);
            log.append(&intent(id, Op::CreateBranch { branch })).unwrap();
            log.append(&outcome(id, result)).unwrap();
        }

        let local = check(&root, &config, false).await;
        assert_eq!(
            local.violations,
            [Violation::FalseConflict { id: conflicted, landed_twice: false }]
        );
        assert_eq!(local.warnings, [Warning::AmbiguousErrorLanded { id: errored }]);

        let network = check(&root, &config, true).await;
        assert_eq!(network.violations, []);
        assert_eq!(
            network.warnings,
            [
                Warning::LostReplyConflict { id: conflicted },
                Warning::AmbiguousErrorLanded { id: errored }
            ]
        );
    }

    #[tokio::test]
    async fn failed_branch_creation_followed_by_an_ack_is_fine() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let [failed, acked] = [Uuid::new_v4(), Uuid::new_v4()];
        let log = forged_log(&root, "0", 1);
        log.append(&intent(failed, Op::CreateBranch { branch: branch_name("h", "0") }))
            .unwrap();
        log.append(&outcome(failed, OpResult::Conflict)).unwrap();
        log.append(&intent(acked, Op::CreateBranch { branch: branch_name("h", "0") }))
            .unwrap();
        log.append(&outcome(acked, OpResult::Ack { snapshot: None })).unwrap();
        let verdict = check(&root, &config, false).await;
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.warnings, []);
    }

    #[tokio::test]
    async fn missing_snapshot_file_breaks_history() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let first = read_log(&root.worker_log("h", "0", 0))
            .unwrap()
            .into_iter()
            .find_map(|record| match record {
                Record::Outcome {
                    result: OpResult::Ack { snapshot: Some(s) }, ..
                } => Some(s),
                _ => None,
            })
            .unwrap();
        let file = root.storage().join("snapshots").join(&first);
        assert!(file.exists(), "{}", file.display());
        fs::remove_file(&file).unwrap();
        let verdict = check(&root, &config, false).await;
        // Its chunk can't be read back either.
        assert!(
            matches!(&verdict.violations[..], [
                Violation::BrokenHistory { detail },
                Violation::DataMismatch { detail: unread },
            ] if detail.contains(&first) && unread.contains(&first)),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn clean_history_passes() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let verdict = check(&root, &config, false).await;
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.warnings, []);
        let stats = &verdict.stats;
        // 2 branch creations, 5 commits and a tag.
        assert_eq!(
            (stats.intents, stats.acks, stats.conflicts, stats.errors, stats.unresolved),
            (8, 8, 0, 0, 0)
        );
        assert_eq!(stats.chain_len, 5);
    }

    #[tokio::test]
    async fn ack_missing_from_the_ancestry_is_a_lost_update() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let id = Uuid::new_v4();
        let log = forged_log(&root, "0", 1);
        log.append(&intent(id, branch_op("0"))).unwrap();
        log.append(&ack(id, &SnapshotId::random().to_string())).unwrap();
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::LostUpdate { id: lost, .. }]
                if *lost == id),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn acked_main_commit_missing_from_main_is_a_lost_update() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let id = Uuid::new_v4();
        let log = forged_log(&root, "1", 1);
        log.append(&intent(id, Op::MainCommit)).unwrap();
        log.append(&ack(id, &SnapshotId::random().to_string())).unwrap();
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::LostUpdate { id: lost, .. }]
                if *lost == id),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn stress_commit_without_intent_is_a_phantom() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let id = Uuid::new_v4();
        unlogged_commit(&root, &config, "1", id).await;
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::Phantom { what }]
                if what.contains(&id.to_string())),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn commit_with_a_foreign_message_is_a_phantom() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let repo = open_repo(&root, &config, None).await.unwrap();
        let mut session = repo.writable_session(MAIN_BRANCH).await.unwrap();
        session.add_group("/g".try_into().unwrap(), Bytes::new()).await.unwrap();
        session.commit("hand-made").execute().await.unwrap();
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::Phantom { what }]
                if what.contains("hand-made")),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn commit_landing_on_the_wrong_branch_is_a_phantom() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let id = Uuid::new_v4();
        let snapshot = unlogged_commit(&root, &config, "1", id).await;
        let log = forged_log(&root, "0", 1);
        log.append(&intent(id, branch_op("0"))).unwrap();
        log.append(&ack(id, &snapshot)).unwrap();
        let verdict = check(&root, &config, false).await;
        assert!(
            verdict.violations.iter().any(|v| matches!(v, Violation::Phantom { what }
                if what.contains(&id.to_string()))),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn acks_out_of_order_break_history() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let [a, b] = [Uuid::new_v4(), Uuid::new_v4()];
        let snapshot_a = unlogged_commit(&root, &config, "1", a).await;
        let snapshot_b = unlogged_commit(&root, &config, "1", b).await;
        let log = forged_log(&root, "1", 1);
        for (id, snapshot) in [(b, &snapshot_b), (a, &snapshot_a)] {
            log.append(&intent(id, branch_op("1"))).unwrap();
            log.append(&ack(id, snapshot)).unwrap();
        }
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::BrokenHistory { detail }]
                if detail.contains(&a.to_string())),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn ack_naming_another_snapshot_is_a_lost_update() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let id = Uuid::new_v4();
        unlogged_commit(&root, &config, "1", id).await;
        let log = forged_log(&root, "1", 1);
        log.append(&intent(id, branch_op("1"))).unwrap();
        log.append(&ack(id, &SnapshotId::random().to_string())).unwrap();
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::LostUpdate { id: lost, .. }]
                if *lost == id),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn failed_commit_that_landed_follows_the_false_conflict_rule() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let [conflicted, errored] = [Uuid::new_v4(), Uuid::new_v4()];
        unlogged_commit(&root, &config, "1", conflicted).await;
        unlogged_commit(&root, &config, "1", errored).await;
        let log = forged_log(&root, "1", 1);
        log.append(&intent(conflicted, branch_op("1"))).unwrap();
        log.append(&outcome(conflicted, OpResult::Conflict)).unwrap();
        log.append(&intent(errored, branch_op("1"))).unwrap();
        let error = OpResult::Error { message: "timed out".to_owned() };
        log.append(&outcome(errored, error)).unwrap();

        let local = check(&root, &config, false).await;
        assert_eq!(
            local.violations,
            [Violation::FalseConflict { id: conflicted, landed_twice: false }]
        );
        assert_eq!(local.warnings, [Warning::AmbiguousErrorLanded { id: errored }]);

        let network = check(&root, &config, true).await;
        assert_eq!(network.violations, []);
        assert_eq!(
            network.warnings,
            [
                Warning::LostReplyConflict { id: conflicted },
                Warning::AmbiguousErrorLanded { id: errored }
            ]
        );
    }

    #[tokio::test]
    async fn tag_without_intent_is_a_phantom() {
        let (_dir, root, config) = repo_root().await;
        let (zero, _) = clean_run(&root, &config).await;
        let repo = open_repo(&root, &config, None).await.unwrap();
        let tip = repo.lookup_branch(zero.branch()).await.unwrap();
        let name = tag_name(Uuid::new_v4());
        repo.create_tag(&name, &tip).await.unwrap();
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::Phantom { what }]
                if what.contains(&name)),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn conflicted_tag_that_exists_is_a_false_conflict() {
        let (_dir, root, config) = repo_root().await;
        let (zero, _) = clean_run(&root, &config).await;
        let repo = open_repo(&root, &config, None).await.unwrap();
        let tip = repo.lookup_branch(zero.branch()).await.unwrap();
        let id = Uuid::new_v4();
        let name = tag_name(id);
        repo.create_tag(&name, &tip).await.unwrap();
        let log = forged_log(&root, "0", 1);
        log.append(&intent(id, Op::Tag { name, snapshot: tip.to_string() })).unwrap();
        log.append(&outcome(id, OpResult::Conflict)).unwrap();
        let verdict = check(&root, &config, false).await;
        assert_eq!(
            verdict.violations,
            [Violation::FalseConflict { id, landed_twice: false }]
        );
    }

    #[tokio::test]
    async fn acked_tag_must_exist_and_point_at_its_snapshot() {
        let (_dir, root, config) = repo_root().await;
        let (zero, _) = clean_run(&root, &config).await;
        let repo = open_repo(&root, &config, None).await.unwrap();
        let tip = repo.lookup_branch(zero.branch()).await.unwrap().to_string();
        let [missing, moved] = [Uuid::new_v4(), Uuid::new_v4()];
        repo.create_tag(&tag_name(moved), &Snapshot::INITIAL_SNAPSHOT_ID).await.unwrap();
        let log = forged_log(&root, "0", 1);
        for id in [missing, moved] {
            let name = tag_name(id);
            log.append(&intent(id, Op::Tag { name, snapshot: tip.clone() })).unwrap();
            log.append(&ack(id, &tip)).unwrap();
        }
        let verdict = check(&root, &config, false).await;
        let mut lost: Vec<Uuid> = verdict
            .violations
            .iter()
            .map(|v| match v {
                Violation::LostUpdate { id, .. } => *id,
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        lost.sort();
        let mut expected = vec![missing, moved];
        expected.sort();
        assert_eq!(lost, expected, "{verdict:?}");
    }

    #[tokio::test]
    async fn overwritten_chunk_is_a_data_mismatch() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let chunks = root.storage().join("chunks");
        overwrite_all(&chunks);
        let verdict = check(&root, &config, false).await;
        // One per acked commit: 3 branch and 2 main commits.
        assert_eq!(verdict.violations.len(), 5, "{verdict:?}");
        assert!(
            verdict
                .violations
                .iter()
                .all(|v| matches!(v, Violation::DataMismatch { .. })),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn missing_chunk_of_an_older_acked_commit_is_a_data_mismatch() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let branch_commits: Vec<_> = acked_commits(&root, "0")
            .into_iter()
            .filter(|(_, op, _)| matches!(op, Op::BranchCommit { .. }))
            .collect();
        let [(first, _, snapshot), _] = &branch_commits[..] else {
            panic!("{branch_commits:?}");
        };
        fs::remove_file(chunk_file(&root, &config, *first)).unwrap();
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::DataMismatch { detail }]
                if detail.contains(snapshot.as_str())),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn main_commit_chunk_mismatch_is_a_data_mismatch() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let main_commits: Vec<_> = acked_commits(&root, "1")
            .into_iter()
            .filter(|(_, op, _)| matches!(op, Op::MainCommit))
            .collect();
        let [(id, _, snapshot)] = &main_commits[..] else {
            panic!("{main_commits:?}");
        };
        overwrite(&chunk_file(&root, &config, *id));
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::DataMismatch { detail }]
                if detail.contains(snapshot.as_str()) && detail.contains("/m.h.1")),
            "{verdict:?}"
        );
    }

    /// The acked commits of slot `slot`'s first incarnation, in log order.
    fn acked_commits(root: &RunRoot, slot: &str) -> Vec<(Uuid, Op, String)> {
        let mut ops = HashMap::new();
        let mut acked = Vec::new();
        for record in read_log(&root.worker_log("h", slot, 0)).unwrap() {
            match record {
                Record::Intent { id, op, .. } => {
                    ops.insert(id, op);
                }
                Record::Outcome {
                    id,
                    result: OpResult::Ack { snapshot: Some(snapshot) },
                    ..
                } => {
                    let op = ops.remove(&id).unwrap();
                    if matches!(op, Op::BranchCommit { .. } | Op::MainCommit) {
                        acked.push((id, op, snapshot));
                    }
                }
                _ => {}
            }
        }
        acked
    }

    /// The chunk file holding the chunk `id` wrote.
    fn chunk_file(root: &RunRoot, config: &RunConfig, id: Uuid) -> PathBuf {
        let expected = chunk_bytes(id, config.chunk_bytes);
        fs::read_dir(root.storage().join("chunks"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| fs::read(path).unwrap() == expected)
            .unwrap_or_else(|| panic!("no chunk file of {id}"))
    }

    fn overwrite_all(dir: &FsPath) {
        let mut overwritten = 0;
        for entry in fs::read_dir(dir).unwrap() {
            overwrite(&entry.unwrap().path());
            overwritten += 1;
        }
        assert!(overwritten > 0);
    }

    fn overwrite(path: &FsPath) {
        let len = fs::metadata(path).unwrap().len();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        #[expect(clippy::permissions_set_readonly_false, reason = "test")]
        permissions.set_readonly(false);
        fs::set_permissions(path, permissions).unwrap();
        fs::write(path, vec![b'!'; usize::try_from(len).unwrap()]).unwrap();
    }

    #[tokio::test]
    async fn acked_branch_creation_must_exist_and_branches_need_an_intent() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let id = Uuid::new_v4();
        let log = forged_log(&root, "7", 0);
        let branch = branch_name("h", "7");
        log.append(&intent(id, Op::CreateBranch { branch })).unwrap();
        log.append(&outcome(id, OpResult::Ack { snapshot: None })).unwrap();
        let repo = open_repo(&root, &config, None).await.unwrap();
        let unlogged = branch_name("h", "8");
        repo.create_branch(&unlogged, &Snapshot::INITIAL_SNAPSHOT_ID).await.unwrap();
        let verdict = check(&root, &config, false).await;
        assert_eq!(verdict.violations.len(), 2, "{verdict:?}");
        assert!(
            verdict
                .violations
                .iter()
                .any(|v| matches!(v, Violation::LostUpdate { id: lost, .. }
                if *lost == id)),
            "{verdict:?}"
        );
        assert!(
            verdict.violations.iter().any(|v| matches!(v, Violation::Phantom { what }
                if what.contains(&unlogged))),
            "{verdict:?}"
        );
    }

    #[tokio::test]
    async fn storage_mode_intent_is_a_harness_error() {
        let (_dir, root, config) = repo_root().await;
        let log = forged_log(&root, "0", 0);
        let op = Op::Cas { base: "a".to_owned(), new: "b".to_owned() };
        log.append(&intent(Uuid::new_v4(), op)).unwrap();
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::Harness { .. }]),
            "{verdict:?}"
        );
    }
}
