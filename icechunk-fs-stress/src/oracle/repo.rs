//! Repository mode: checks the branches, `main` and the tags of the final
//! repository against every logged intent, and reads back the chunk of every
//! acknowledged commit, after warning about the commits a GC's cutoff came
//! after.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Display,
};

use bytes::Bytes;
use futures::TryStreamExt as _;
use icechunk::{
    Repository,
    error::ICError,
    format::{
        ByteRange, CHUNKS_FILE_PATH, ChunkIndices, MANIFESTS_FILE_PATH, Path,
        SNAPSHOTS_FILE_PATH, SnapshotId,
        manifest::{ChunkPayload, ChunkRef},
        snapshot::{NodeData, Snapshot, SnapshotInfo},
    },
    repository::{RepositoryErrorKind, VersionInfo},
    session::{SessionError, SessionErrorKind, get_chunk},
    storage::{StorageContext, StorageErrorKind},
};
use uuid::Uuid;

use crate::{
    config::RunConfig,
    log::{LogName, Op, OpResult, Record},
    oracle::{
        History, Intents, Stats, Verdict, Violation, Warning,
        gc_cutoff::{check_gc_cutoffs, missing_after_gc_cutoff},
        intents,
        mirror::object_exists,
    },
    repo_mode::{
        MAIN_BRANCH, array_path_on_branch, array_path_on_main, chunk_bytes,
        parse_branch_name, parse_commit_message,
    },
};

/// Checks the repository against `history`. `Err` only if the repository's
/// branches or tags can't be listed, or a snapshot file can't be opened for a
/// reason other than its absence. The missing snapshot file of a commit that
/// a GC's cutoff came after, and so its read-back, is no violation: it goes
/// to the commit's [`Warning::GcCutoffAfterSessionStart`]. So is a read-back
/// of such a commit's snapshot that finds the manifest or the chunk the
/// commit wrote missing; M7 still judges a missing manifest.
pub async fn check_repo(
    repo: &Repository,
    history: &History,
    config: &RunConfig,
    network_fs: bool,
    verdict: &mut Verdict,
) -> anyhow::Result<()> {
    check_gc_cutoffs(history, verdict);
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
    // Snapshot files a GC whose cutoff came after their commit's start may
    // have deleted.
    let mut excused: HashSet<SnapshotId> = HashSet::new();
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
                let key = format!("{SNAPSHOTS_FILE_PATH}/{}", info.id);
                if missing_after_gc_cutoff(
                    &mut verdict.warnings,
                    &info.id.to_string(),
                    &key,
                ) {
                    excused.insert(info.id.clone());
                    continue;
                }
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
    let removals = Removals::new(&ops, &landed);
    let mut landed_deletions = BTreeMap::new();
    let mut acks = BranchAcks::default();
    // The acked commits that landed as acked, with the array they wrote.
    let mut read_backs: Vec<(Uuid, &SnapshotId, Path)> = Vec::new();
    for (index, &(writer, id, op, result)) in ops.iter().enumerate() {
        let is_commit = matches!(op, Op::BranchCommit { .. } | Op::MainCommit);
        if is_commit && unreadable.contains(commit_branch(op)) {
            continue;
        }
        // A commit that landed more than once reported a conflict to its own
        // rebase, so it is a false conflict even if acked. A failed outcome
        // that landed gets the usual rule below instead.
        if relanded.contains(&id) && result.is_none_or(|result| result.landed().is_some())
        {
            conflict_but_landed(id, true, network_fs, verdict);
        }
        let Some(result) = result else { continue };
        let Some(snapshot) = result.landed() else {
            let landed_anyway = match op {
                Op::BranchCommit { .. } | Op::MainCommit => landed
                    .get(commit_branch(op))
                    .is_some_and(|commits| commits.contains_key(&id)),
                Op::Tag { name, .. } => tags.contains(name),
                _ => false,
            };
            if landed_anyway {
                failed_but_landed(id, result, network_fs, verdict);
            }
            continue;
        };
        match op {
            Op::CreateBranch { branch } => {
                if !branches.contains(branch)
                    && !removals.branch_deleted_after(
                        index,
                        branch,
                        &mut landed_deletions,
                    )
                {
                    lost(verdict, id, format!("branch {branch} doesn't exist"));
                }
            }
            Op::BranchCommit { .. } | Op::MainCommit => {
                let branch = commit_branch(op);
                let found =
                    landed.get(branch).is_some_and(|commits| commits.contains_key(&id));
                if !found
                    && removals.removed_commit(index, writer, op, &mut landed_deletions)
                {
                    continue;
                }
                let Some(snapshot) = acks.check(id, op, snapshot, &landed, verdict)
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
            Op::Tag { name, snapshot } => {
                if !tags.contains(name) {
                    if !removals.tag_deleted(name, &mut landed_deletions) {
                        lost(verdict, id, format!("tag {name} doesn't exist"));
                    }
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
            Op::DeleteTag { name } => {
                if tags.contains(name) {
                    lost(verdict, id, format!("tag {name} still exists"));
                }
            }
            Op::DeleteBranch { branch } => {
                if let Some(commit) = removals.landed_before(index, branch) {
                    lost(
                        verdict,
                        id,
                        format!(
                            "{branch} still holds {commit}, committed before the deletion"
                        ),
                    );
                }
            }
            Op::Cas { .. }
            | Op::Gc { .. }
            | Op::Expire { .. }
            | Op::SetStatus { .. }
            | Op::Convert { .. } => {}
        }
    }
    for (id, result) in landed_deletions.into_values() {
        failed_but_landed(id, result, network_fs, verdict);
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

    // Every acked snapshot that landed is reachable, so GC must keep every one
    // of these chunks.
    for (id, snapshot, array) in read_backs {
        if excused.contains(snapshot) {
            continue;
        }
        let detail = match read_chunk(repo, snapshot, &array).await {
            Ok(Some(chunk)) if chunk == chunk_bytes(id, config.chunk_bytes) => continue,
            Ok(Some(_)) => {
                format!("chunk [0] of {array} at {snapshot} isn't the one {id} wrote")
            }
            Ok(None) => format!("no chunk [0] in {array} at {snapshot}, written by {id}"),
            Err(failure) => {
                let excused = failure.not_found
                    && match missing_read_back_object(repo, snapshot, &array).await {
                        Ok(Some(key)) => missing_after_gc_cutoff(
                            &mut verdict.warnings,
                            &snapshot.to_string(),
                            &key,
                        ),
                        Ok(None) | Err(_) => false,
                    };
                if excused {
                    continue;
                }
                format!("reading chunk [0] of {array} at {snapshot}: {:#}", failure.err)
            }
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

/// Deletions by name, in log order, with their positions.
type Deletions<'a> = HashMap<&'a str, Vec<(usize, Uuid, Option<&'a OpResult>)>>;

/// What expirations and ref deletions may have removed. Every logged intent
/// counts, whatever its outcome: a failed one may have landed.
#[derive(Debug)]
struct Removals<'a> {
    /// Some expiration was logged.
    expired: bool,
    /// Each branch's deletions in log order, with their positions.
    branch_deletions: Deletions<'a>,
    tag_deletions: Deletions<'a>,
    /// Each branch's commits in the final repository, by log position.
    landed_positions: HashMap<&'a str, Vec<(usize, Uuid)>>,
    /// The log positions of the commits each slot (host tag and slot) made
    /// to each branch that are on it in the final repository.
    slot_landed: HashMap<(&'a str, &'a str, &'a str), Vec<usize>>,
}

impl<'a> Removals<'a> {
    fn new(ops: &[LoggedOp<'a>], landed: &HashMap<&str, Landed>) -> Self {
        let mut removals = Self {
            expired: false,
            branch_deletions: HashMap::new(),
            tag_deletions: HashMap::new(),
            landed_positions: HashMap::new(),
            slot_landed: HashMap::new(),
        };
        let is_landed = |branch: &str, id: &Uuid| {
            landed.get(branch).is_some_and(|c| c.contains_key(id))
        };
        for (index, &(writer, id, op, result)) in ops.iter().enumerate() {
            if matches!(op, Op::BranchCommit { .. } | Op::MainCommit) {
                let branch = commit_branch(op);
                if is_landed(branch, &id) {
                    let slot = (writer.tag.as_str(), writer.slot.as_str(), branch);
                    removals.slot_landed.entry(slot).or_default().push(index);
                }
            }
            match op {
                Op::Expire { .. } => removals.expired = true,
                Op::DeleteBranch { branch } => {
                    removals
                        .branch_deletions
                        .entry(branch)
                        .or_default()
                        .push((index, id, result));
                }
                Op::DeleteTag { name } => {
                    removals
                        .tag_deletions
                        .entry(name)
                        .or_default()
                        .push((index, id, result));
                }
                Op::BranchCommit { branch } if is_landed(branch, &id) => {
                    removals
                        .landed_positions
                        .entry(branch)
                        .or_default()
                        .push((index, id));
                }
                _ => {}
            }
        }
        removals
    }

    /// Whether an expiration or a deletion of its branch logged after it
    /// explains why the commit `writer` logged at `index` isn't on its
    /// branch. Expiration drops a prefix of an ancestry: it can't have
    /// dropped a commit above one the same slot made to the same branch
    /// before it, which ended before its intent, that is still there.
    fn removed_commit(
        &self,
        index: usize,
        writer: &LogName,
        op: &Op,
        landed_deletions: &mut LandedDeletions<'a>,
    ) -> bool {
        let slot = (writer.tag.as_str(), writer.slot.as_str(), commit_branch(op));
        let below_a_kept_one = self
            .slot_landed
            .get(&slot)
            .is_some_and(|landed| landed.iter().any(|&at| at < index));
        if self.expired && !below_a_kept_one {
            return true;
        }
        match op {
            Op::BranchCommit { branch } => {
                self.branch_deleted_after(index, branch, landed_deletions)
            }
            _ => false,
        }
    }

    /// Whether a deletion of `branch` logged after `index` explains its
    /// absence. A deletion that landed removed every commit logged before
    /// it, since the branch is recreated at the initial snapshot: one that
    /// left such a commit on the branch didn't land and explains nothing.
    fn branch_deleted_after(
        &self,
        index: usize,
        branch: &str,
        landed_deletions: &mut LandedDeletions<'a>,
    ) -> bool {
        let deletions = self.branch_deletions.get(branch).into_iter().flatten();
        explains(
            deletions
                .filter(|(at, ..)| {
                    *at > index && self.landed_before(*at, branch).is_none()
                })
                .copied(),
            landed_deletions,
        )
    }

    /// Whether a deletion of tag `name` explains its absence.
    fn tag_deleted(
        &self,
        name: &str,
        landed_deletions: &mut LandedDeletions<'a>,
    ) -> bool {
        explains(
            self.tag_deletions.get(name).into_iter().flatten().copied(),
            landed_deletions,
        )
    }

    /// A commit to `branch` logged before `index` that is on the branch.
    fn landed_before(&self, index: usize, branch: &str) -> Option<Uuid> {
        let landed = self.landed_positions.get(branch)?;
        landed.iter().find(|(at, _)| *at < index).map(|(_, id)| *id)
    }
}

/// Whether one of `deletions`, with their log positions, may have removed
/// something that is gone: one acknowledged or unresolved. If every one
/// failed, one of them landed: with a single candidate, it is added to
/// `landed_deletions`.
fn explains<'a>(
    deletions: impl Iterator<Item = (usize, Uuid, Option<&'a OpResult>)>,
    landed_deletions: &mut LandedDeletions<'a>,
) -> bool {
    let deletions: Vec<_> = deletions.collect();
    if deletions.iter().any(|(.., result)| result.is_none_or(|r| r.landed().is_some())) {
        return true;
    }
    if let [(at, id, Some(failed))] = deletions[..] {
        landed_deletions.insert(at, (id, failed));
    }
    !deletions.is_empty()
}

/// Failed deletions that landed, by log position.
type LandedDeletions<'a> = BTreeMap<usize, (Uuid, &'a OpResult)>;

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
        OpResult::Ack { .. } | OpResult::DurableButForked { .. } => {}
        OpResult::Error { .. }
        | OpResult::ReadOnly { .. }
        | OpResult::Forked { .. }
        | OpResult::ConversionInProgress { .. } => {
            verdict.warnings.push(Warning::AmbiguousErrorLanded { id });
        }
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

/// The operation counts of `history`'s intents and outcomes, as
/// [`check_repo`] counts them.
pub fn count_operations(history: &History) -> Stats {
    let mut verdict = Verdict::default();
    let intents = intents(history, &mut verdict);
    count(&in_log_order(history, &intents), &mut verdict);
    verdict.stats
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
            Some(result) if result.landed().is_some() => stats.acks += 1,
            Some(OpResult::Conflict) => stats.conflicts += 1,
            Some(result) if result.is_refusal() => stats.refused += 1,
            Some(_) => stats.errors += 1,
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

/// A failed read-back.
struct ReadFailure {
    err: anyhow::Error,
    /// The storage found an object missing while locating or reading the
    /// chunk.
    not_found: bool,
}

async fn read_chunk(
    repo: &Repository,
    snapshot: &SnapshotId,
    array: &Path,
) -> Result<Option<Bytes>, ReadFailure> {
    let session = repo
        .readonly_session(&VersionInfo::SnapshotId(snapshot.clone()))
        .await
        .map_err(|err| ReadFailure {
            err: read_error("opening a session")(err),
            not_found: false,
        })?;
    let index = ChunkIndices(vec![0]);
    let reader = session
        .get_chunk_reader(array, &index, &ByteRange::ALL)
        .await
        .map_err(session_failure("locating the chunk"))?;
    get_chunk(reader).await.map_err(session_failure("reading the chunk"))
}

fn session_failure(what: &str) -> impl FnOnce(SessionError) -> ReadFailure {
    let what = what.to_owned();
    move |err| ReadFailure {
        not_found: matches!(
            err.kind(),
            SessionErrorKind::StorageError(StorageErrorKind::ObjectNotFound)
                | SessionErrorKind::RepositoryError(RepositoryErrorKind::StorageError(
                    StorageErrorKind::ObjectNotFound
                ))
        ),
        err: read_error(&what)(err),
    }
}

/// The object that the read-back of chunk [0] of `array` at `snapshot` needs
/// and that is missing: the manifest holding the chunk's reference or, if it
/// exists, the chunk. The commit that wrote that chunk and made `snapshot`
/// wrote both.
async fn missing_read_back_object(
    repo: &Repository,
    snapshot: &SnapshotId,
    array: &Path,
) -> anyhow::Result<Option<String>> {
    let session = repo
        .readonly_session(&VersionInfo::SnapshotId(snapshot.clone()))
        .await
        .map_err(read_error("opening a session"))?;
    let index = ChunkIndices(vec![0]);
    let node = session.get_node(array).await.map_err(read_error("reading the array"))?;
    let NodeData::Array { manifests, .. } = node.node_data else { return Ok(None) };
    for manifest in manifests.iter().filter(|m| m.extents.contains(&index.0)) {
        let key = format!("{MANIFESTS_FILE_PATH}/{}", manifest.object_id);
        if !object_exists(repo, &key).await? {
            return Ok(Some(key));
        }
    }
    let chunk = session
        .get_chunk_ref(array, &index)
        .await
        .map_err(read_error("locating the chunk"))?;
    match chunk {
        Some(ChunkPayload::Ref(ChunkRef { id, .. })) => {
            let key = format!("{CHUNKS_FILE_PATH}/{id}");
            Ok((!object_exists(repo, &key).await?).then_some(key))
        }
        _ => Ok(None),
    }
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
            branch_name, chunk_bytes, commit_message, expire_snapshots, open_repo,
            tag_name,
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

    /// The snapshot file of an acked commit that a GC's cutoff came after,
    /// missing: part of the commit's warning, and so is its read-back; if
    /// the commit started after the cutoff, both are violations.
    #[tokio::test]
    async fn the_snapshot_file_of_a_commit_a_gc_cutoff_came_after_is_warned() {
        use crate::oracle::gc_cutoff::tests::{CUTOFF_MS, GC_INTENT_MS};
        for (intent_ms, excused) in [(973_998, true), (CUTOFF_MS + 1, false)] {
            let (_dir, root, config) = repo_root().await;
            let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
            assert!(worker.ensure_branch().await.unwrap());
            step(worker.branch_commit().await);
            let id = Uuid::new_v4();
            let snapshot = unlogged_commit(&root, &config, "0", id).await;
            let commit = forged_log(&root, "0", 1);
            commit
                .append(&Record::Intent { t_ms: intent_ms, id, op: branch_op("0") })
                .unwrap();
            let result = OpResult::Ack { snapshot: Some(snapshot.clone()) };
            commit.append(&Record::Outcome { t_ms: 1_059_380, id, result }).unwrap();
            let gc = Uuid::new_v4();
            let collector = forged_log(&root, "gc", 0);
            let op = Op::Gc { older_than_ms: Some(CUTOFF_MS) };
            collector.append(&Record::Intent { t_ms: GC_INTENT_MS, id: gc, op }).unwrap();
            let result = OpResult::Ack { snapshot: None };
            collector
                .append(&Record::Outcome { t_ms: 1_064_705, id: gc, result })
                .unwrap();
            let key = format!("{SNAPSHOTS_FILE_PATH}/{snapshot}");
            fs::remove_file(root.storage().join(&key)).unwrap();

            let verdict = check(&root, &config, false).await;
            if excused {
                assert_eq!(verdict.violations, []);
                assert!(
                    matches!(&verdict.warnings[..], [Warning::GcCutoffAfterSessionStart {
                        id: warned, missing, ..
                    }] if *warned == id && *missing == [key]),
                    "{verdict:?}"
                );
            } else {
                assert_eq!(verdict.warnings, []);
                assert!(
                    matches!(&verdict.violations[..], [
                        Violation::BrokenHistory { detail },
                        Violation::DataMismatch { detail: unread },
                    ] if detail.contains(&snapshot) && unread.contains(&snapshot)),
                    "{verdict:?}"
                );
            }
        }
    }

    /// A branch commit acked as the returned snapshot, logged as starting at
    /// `intent_ms`, and an acked GC whose cutoff came after that by more than
    /// ε for the incident's 973998, with the keys of the manifest and the
    /// chunk the commit wrote.
    async fn commit_and_gc(
        intent_ms: i64,
    ) -> (tempfile::TempDir, RunRoot, RunConfig, String, [String; 2]) {
        use crate::oracle::gc_cutoff::tests::{CUTOFF_MS, GC_INTENT_MS};
        let (dir, root, config) = repo_root().await;
        let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert!(worker.ensure_branch().await.unwrap());
        step(worker.branch_commit().await);
        let manifests = root.storage().join(MANIFESTS_FILE_PATH);
        let before: HashSet<_> = fs::read_dir(&manifests)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        let id = Uuid::new_v4();
        let snapshot = unlogged_commit(&root, &config, "0", id).await;
        let [manifest]: [_; 1] = fs::read_dir(&manifests)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .filter(|name| !before.contains(name))
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        let manifest = format!("{MANIFESTS_FILE_PATH}/{}", manifest.to_str().unwrap());
        let chunk = chunk_file(&root, &config, id);
        let chunk = format!(
            "{CHUNKS_FILE_PATH}/{}",
            chunk.file_name().unwrap().to_str().unwrap()
        );
        let commit = forged_log(&root, "0", 1);
        let op = branch_op("0");
        commit.append(&Record::Intent { t_ms: intent_ms, id, op }).unwrap();
        let result = OpResult::Ack { snapshot: Some(snapshot.clone()) };
        commit.append(&Record::Outcome { t_ms: 1_059_380, id, result }).unwrap();
        let gc = Uuid::new_v4();
        let collector = forged_log(&root, "gc", 0);
        let op = Op::Gc { older_than_ms: Some(CUTOFF_MS) };
        collector.append(&Record::Intent { t_ms: GC_INTENT_MS, id: gc, op }).unwrap();
        let result = OpResult::Ack { snapshot: None };
        collector.append(&Record::Outcome { t_ms: 1_064_705, id: gc, result }).unwrap();
        (dir, root, config, snapshot, [manifest, chunk])
    }

    /// The `FSx` run: GC deleted the chunk of a commit its cutoff came after
    /// and kept the snapshot file. A read-back that finds the commit's own
    /// manifest or chunk missing is part of the commit's warning; one that
    /// finds wrong bytes, or of a commit that started after the cutoff, is a
    /// violation.
    #[tokio::test]
    async fn the_missing_chunk_or_manifest_of_a_commit_a_gc_cutoff_came_after_is_warned()
    {
        use crate::oracle::gc_cutoff::tests::CUTOFF_MS;
        for deleted in 0..2 {
            let (_dir, root, config, snapshot, objects) = commit_and_gc(973_998).await;
            let key = &objects[deleted];
            fs::remove_file(root.storage().join(key)).unwrap();
            let verdict = check(&root, &config, false).await;
            assert_eq!(verdict.violations, [], "{key}");
            assert!(
                matches!(&verdict.warnings[..], [Warning::GcCutoffAfterSessionStart {
                    snapshot: Some(warned), missing, ..
                }] if *warned == snapshot && missing == std::slice::from_ref(key)),
                "{key}: {verdict:?}"
            );
        }

        let (_dir, root, config, snapshot, [_, chunk]) =
            commit_and_gc(CUTOFF_MS + 1).await;
        fs::remove_file(root.storage().join(chunk)).unwrap();
        let verdict = check(&root, &config, false).await;
        assert_eq!(verdict.warnings, []);
        assert!(
            matches!(&verdict.violations[..], [Violation::DataMismatch { detail }]
                if detail.contains(&snapshot) && detail.contains("not found")),
            "{verdict:?}"
        );

        let (_dir, root, config, snapshot, [_, chunk]) = commit_and_gc(973_998).await;
        overwrite(&root.storage().join(chunk));
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.warnings[..], [Warning::GcCutoffAfterSessionStart {
                snapshot: Some(warned), missing, ..
            }] if *warned == snapshot && missing.is_empty()),
            "{verdict:?}"
        );
        assert!(
            matches!(&verdict.violations[..], [Violation::DataMismatch { detail }]
                if detail.contains(&snapshot) && detail.contains("isn't the one")),
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

    /// The name of the tag slot 0's first incarnation logged.
    fn logged_tag(root: &RunRoot) -> String {
        read_log(&root.worker_log("h", "0", 0))
            .unwrap()
            .into_iter()
            .find_map(|record| match record {
                Record::Intent { op: Op::Tag { name, .. }, .. } => Some(name),
                _ => None,
            })
            .unwrap()
    }

    fn is_ack(result: Option<&OpResult>) -> bool {
        matches!(result, Some(OpResult::Ack { .. }))
    }

    /// The lost updates' ids, sorted.
    fn lost_ids(verdict: &Verdict) -> Vec<Uuid> {
        let mut ids: Vec<Uuid> = verdict
            .violations
            .iter()
            .map(|v| match v {
                Violation::LostUpdate { id, .. } => *id,
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        ids.sort();
        ids
    }

    /// Slot 0 deletes its tag and its branch, recreates the branch and
    /// commits to it again: what the deletions removed isn't lost.
    #[tokio::test]
    async fn deletions_excuse_what_they_removed() {
        let (_dir, root, config) = repo_root().await;
        let (mut zero, _) = clean_run(&root, &config).await;
        assert!(is_ack(zero.delete_tag().await.unwrap().as_ref()));
        assert!(is_ack(Some(&zero.delete_branch().await.unwrap())));
        assert!(zero.ensure_branch().await.unwrap());
        step(zero.branch_commit().await);
        let verdict = check(&root, &config, false).await;
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.warnings, []);
        assert_eq!(verdict.stats.chain_len, 4, "{verdict:?}");
    }

    /// An acknowledged deletion whose ref is still there, and a commit
    /// acknowledged after a branch deletion that isn't on the branch.
    #[tokio::test]
    async fn acked_deletions_must_remove_and_later_commits_must_land() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let [branch, tag, after] = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let log = forged_log(&root, "0", 1);
        log.append(&intent(branch, Op::DeleteBranch { branch: branch_name("h", "0") }))
            .unwrap();
        log.append(&outcome(branch, OpResult::Ack { snapshot: None })).unwrap();
        log.append(&intent(tag, Op::DeleteTag { name: logged_tag(&root) })).unwrap();
        log.append(&outcome(tag, OpResult::Ack { snapshot: None })).unwrap();
        log.append(&intent(after, branch_op("0"))).unwrap();
        log.append(&ack(after, &SnapshotId::random().to_string())).unwrap();
        let verdict = check(&root, &config, false).await;
        let mut expected = vec![branch, tag, after];
        expected.sort();
        assert_eq!(lost_ids(&verdict), expected, "{verdict:?}");
    }

    /// Deletions that reported a failure but landed.
    #[tokio::test]
    async fn failed_deletions_that_landed_follow_the_false_conflict_rule() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        let repo = open_repo(&root, &config, None).await.unwrap();
        let name = logged_tag(&root);
        repo.delete_tag(&name).await.unwrap();
        repo.delete_branch(&branch_name("h", "0")).await.unwrap();
        let [tag, branch] = [Uuid::new_v4(), Uuid::new_v4()];
        let log = forged_log(&root, "0", 1);
        log.append(&intent(tag, Op::DeleteTag { name })).unwrap();
        log.append(&outcome(tag, OpResult::Conflict)).unwrap();
        log.append(&intent(branch, Op::DeleteBranch { branch: branch_name("h", "0") }))
            .unwrap();
        let error = OpResult::Error { message: "timed out".to_owned() };
        log.append(&outcome(branch, error)).unwrap();

        let local = check(&root, &config, false).await;
        assert_eq!(
            local.violations,
            [Violation::FalseConflict { id: tag, landed_twice: false }]
        );
        assert_eq!(local.warnings, [Warning::AmbiguousErrorLanded { id: branch }]);
        let network = check(&root, &config, true).await;
        assert_eq!(network.violations, []);
        assert_eq!(
            network.warnings,
            [
                Warning::LostReplyConflict { id: tag },
                Warning::AmbiguousErrorLanded { id: branch }
            ]
        );
    }

    /// Expiration drops commits flushed before its cutoff that no ref
    /// points at: an expiration logged with that cutoff excuses them.
    #[tokio::test]
    async fn expired_commits_may_be_missing() {
        let (_dir, root, config) = repo_root().await;
        clean_run(&root, &config).await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let older_than_ms = crate::log::now_ms();
        let repo = open_repo(&root, &config, None).await.unwrap();
        let expired = expire_snapshots(&repo, &config, older_than_ms).await.unwrap();
        // Slot 1's first `main` commit and slot 0's first branch commit.
        assert_eq!(expired.released_snapshots.len(), 2, "{expired:?}");

        let unlogged = check(&root, &config, false).await;
        assert_eq!(lost_ids(&unlogged).len(), 2, "{unlogged:?}");

        let log = forged_log(&root, "gc", 0);
        let id = Uuid::new_v4();
        log.append(&intent(id, Op::Expire { older_than_ms })).unwrap();
        log.append(&outcome(id, OpResult::Ack { snapshot: None })).unwrap();
        let verdict = check(&root, &config, false).await;
        assert_eq!(verdict.violations, []);
    }

    /// Expiration drops a prefix of each ancestry, so a missing commit whose
    /// slot has an earlier commit to the same branch still on it is lost,
    /// whatever expiration ran; consecutive expired commits are excused.
    #[tokio::test]
    async fn expiration_excuses_only_commits_below_every_kept_one() {
        let (_dir, root, config) = repo_root().await;
        let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert!(worker.ensure_branch().await.unwrap());
        for _ in 0..3 {
            step(worker.branch_commit().await);
        }
        for _ in 0..2 {
            step(worker.main_commit().await);
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let older_than_ms = crate::log::now_ms();
        let repo = open_repo(&root, &config, None).await.unwrap();
        let expired = expire_snapshots(&repo, &config, older_than_ms).await.unwrap();
        // The first two branch commits and the first `main` commit.
        assert_eq!(expired.released_snapshots.len(), 3, "{expired:?}");
        let log = forged_log(&root, "gc", 0);
        let id = Uuid::new_v4();
        log.append(&intent(id, Op::Expire { older_than_ms })).unwrap();
        log.append(&outcome(id, OpResult::Ack { snapshot: None })).unwrap();
        assert_eq!(check(&root, &config, false).await.violations, []);

        // Lost after the kept tips, logged before any expiration's cutoff.
        let [branch, main] = [Uuid::new_v4(), Uuid::new_v4()];
        let log = forged_log(&root, "0", 1);
        log.append(&intent(branch, branch_op("0"))).unwrap();
        log.append(&ack(branch, &SnapshotId::random().to_string())).unwrap();
        log.append(&intent(main, Op::MainCommit)).unwrap();
        log.append(&ack(main, &SnapshotId::random().to_string())).unwrap();
        let log = forged_log(&root, "gc", 1);
        let id = Uuid::new_v4();
        log.append(&intent(id, Op::Expire { older_than_ms: i64::MAX })).unwrap();
        let verdict = check(&root, &config, false).await;
        let mut expected = vec![branch, main];
        expected.sort();
        assert_eq!(lost_ids(&verdict), expected, "{verdict:?}");
    }

    /// A deletion that visibly didn't land, the commits logged before it
    /// still on the branch, excuses nothing and isn't blamed.
    #[tokio::test]
    async fn deletions_that_visibly_did_not_land_excuse_nothing() {
        for deletion in [Some(OpResult::Conflict), None] {
            let (_dir, root, config) = repo_root().await;
            clean_run(&root, &config).await;
            let [lost, delete] = [Uuid::new_v4(), Uuid::new_v4()];
            let log = forged_log(&root, "0", 1);
            log.append(&intent(lost, branch_op("0"))).unwrap();
            log.append(&ack(lost, &SnapshotId::random().to_string())).unwrap();
            log.append(&intent(
                delete,
                Op::DeleteBranch { branch: branch_name("h", "0") },
            ))
            .unwrap();
            if let Some(result) = &deletion {
                log.append(&outcome(delete, result.clone())).unwrap();
            }
            let verdict = check(&root, &config, false).await;
            assert_eq!(lost_ids(&verdict), [lost], "{deletion:?}: {verdict:?}");
            assert_eq!(verdict.warnings, [], "{deletion:?}");
        }
    }

    #[tokio::test]
    async fn storage_mode_intent_is_a_harness_error() {
        let (_dir, root, config) = repo_root().await;
        let log = forged_log(&root, "0", 0);
        let op = Op::Cas {
            base: "a".to_owned(),
            new: "b".to_owned(),
            backup: "repo.1.X".to_owned(),
        };
        log.append(&intent(Uuid::new_v4(), op)).unwrap();
        let verdict = check(&root, &config, false).await;
        assert!(
            matches!(&verdict.violations[..], [Violation::Harness { .. }]),
            "{verdict:?}"
        );
    }
}
