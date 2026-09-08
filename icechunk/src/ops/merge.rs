//! Merge snapshots into one commit on a branch.
//!
//! A source is any snapshot whose parent is an ancestor of the branch tip,
//! usually a detached snapshot written by `Session::flush`. The merge checks
//! the source transaction logs against each other and against the commits
//! between their parents and the tip, then writes one snapshot that applies
//! every source on top of the tip.

use std::collections::{HashMap, HashSet};

use crate::{
    conflicts::Conflict,
    format::{
        ChunkIndices, NodeId, Path, SnapshotId,
        repo_info::RepoInfo,
        snapshot::{Snapshot, SnapshotInfo},
        transaction_log::TransactionLog,
    },
    session::{SessionError, SessionErrorKind, SessionResult},
};
use icechunk_types::error::ICResultCtxExt as _;

/// A conflict between two snapshots found during a merge.
///
/// `first_snapshot` is the log treated as already applied, `second_snapshot`
/// the log applied on top of it. See `detect_conflicts`.
#[derive(Debug, PartialEq, Eq)]
pub struct MergeConflict {
    pub first_snapshot: SnapshotId,
    pub second_snapshot: SnapshotId,
    pub conflict: Conflict,
}

/// One source and the commits it must be checked against.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SourcePlan {
    pub(crate) id: SnapshotId,
    pub(crate) parent: SnapshotId,
    /// Commits strictly between `parent` and the tip, oldest first.
    pub(crate) intervening: Vec<SnapshotInfo>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MergePlan {
    pub(crate) tip: SnapshotId,
    pub(crate) sources: Vec<SourcePlan>,
}

fn not_in_history(snapshot: &SnapshotId, branch: &str) -> SessionError {
    SessionError::capture(SessionErrorKind::SnapshotNotInBranchHistory {
        snapshot: snapshot.clone(),
        branch: branch.to_string(),
    })
}

/// Resolve the branch tip and, for each source, the commits between its
/// parent and the tip. The ancestry walk stops as soon as every parent is found.
pub(crate) fn plan_merge(
    repo_info: &RepoInfo,
    branch: &str,
    snapshots: &[SnapshotId],
) -> SessionResult<MergePlan> {
    if snapshots.is_empty() {
        return Err(SessionError::capture(SessionErrorKind::NoSnapshotsToMerge));
    }
    let tip = repo_info.resolve_branch(branch).inject()?;
    // `ancestry` yields the tip itself first, then its parents.
    let mut ancestry = repo_info.ancestry(&tip).inject()?;
    let mut history: Vec<SnapshotInfo> = Vec::new();
    let mut sources = Vec::with_capacity(snapshots.len());
    for id in snapshots {
        let parent = repo_info
            .find_snapshot(id)
            .inject()?
            .parent_id
            .ok_or_else(|| not_in_history(id, branch))?;
        let parent_index = loop {
            if let Some(index) = history.iter().position(|info| info.id == parent) {
                break index;
            }
            match ancestry.next() {
                Some(info) => history.push(info.inject()?),
                None => return Err(not_in_history(id, branch)),
            }
        };
        // history[0] is the tip, so the commits after the parent are history[..parent_index], newest first.
        let intervening = history[..parent_index].iter().rev().cloned().collect();
        sources.push(SourcePlan { id: id.clone(), parent, intervening });
    }
    Ok(MergePlan { tip, sources })
}

/// Node ids to paths, for conflict reports and new node checks.
pub(crate) struct PathIndex(HashMap<NodeId, Path>);

impl PathIndex {
    /// Index the nodes of several snapshots. The first snapshot that has a node wins.
    pub(crate) fn from_snapshots<'a>(
        snapshots: impl IntoIterator<Item = &'a Snapshot>,
    ) -> SessionResult<Self> {
        let mut index = HashMap::new();
        for snapshot in snapshots {
            for node in snapshot.iter() {
                let node = node.inject()?;
                index.entry(node.id).or_insert(node.path);
            }
        }
        Ok(Self(index))
    }

    pub(crate) fn from_pairs(pairs: impl IntoIterator<Item = (NodeId, Path)>) -> Self {
        Self(pairs.into_iter().collect())
    }

    fn get(&self, id: &NodeId) -> Option<&Path> {
        self.0.get(id)
    }

    fn path(&self, id: &NodeId) -> SessionResult<Path> {
        self.get(id).cloned().ok_or_else(|| {
            SessionError::capture(SessionErrorKind::ConflictingPathNotFound(id.clone()))
        })
    }
}

/// A transaction log and the id of the snapshot that wrote it.
pub(crate) struct LogView<'a> {
    pub(crate) id: &'a SnapshotId,
    pub(crate) log: &'a TransactionLog,
}

/// Conflicts between two logs, in the direction the rebase detector uses:
/// `previous` is already applied, `current` is applied on top of it.
pub(crate) fn detect_conflicts(
    previous: &LogView<'_>,
    current: &LogView<'_>,
    paths: &PathIndex,
) -> SessionResult<Vec<MergeConflict>> {
    let mut found = Vec::new();
    let mut push = |conflict: Conflict| {
        found.push(MergeConflict {
            first_snapshot: previous.id.clone(),
            second_snapshot: current.id.clone(),
            conflict,
        });
    };

    // Nodes `previous` created, with whether each one is an array.
    let previous_new: Vec<(&Path, bool)> = previous
        .log
        .new_groups()
        .map(|id| (id, false))
        .chain(previous.log.new_arrays().map(|id| (id, true)))
        .filter_map(|(id, is_array)| paths.get(&id).map(|path| (path, is_array)))
        .collect();
    for id in current.log.new_groups().chain(current.log.new_arrays()) {
        let Some(path) = paths.get(&id) else { continue };
        for (previous_path, is_array) in &previous_new {
            if *previous_path == path {
                push(Conflict::NewNodeConflictsWithExistingNode(path.clone()));
            } else if *is_array
                && path.ancestors().skip(1).any(|ancestor| ancestor == **previous_path)
            {
                push(Conflict::NewNodeInInvalidGroup((*previous_path).clone()));
            }
        }
    }

    for id in current.log.updated_arrays() {
        if previous.log.array_updated(&id) {
            push(Conflict::ZarrMetadataDoubleUpdate(paths.path(&id)?));
        }
        if previous.log.array_deleted(&id) {
            push(Conflict::ZarrMetadataUpdateOfDeletedArray(paths.path(&id)?));
        }
    }
    for id in current.log.updated_groups() {
        if previous.log.group_updated(&id) {
            push(Conflict::ZarrMetadataDoubleUpdate(paths.path(&id)?));
        }
        if previous.log.group_deleted(&id) {
            push(Conflict::ZarrMetadataUpdateOfDeletedGroup(paths.path(&id)?));
        }
    }

    for (id, coords) in current.log.updated_chunks() {
        if previous.log.array_deleted(&id) {
            push(Conflict::ChunksUpdatedInDeletedArray {
                path: paths.path(&id)?,
                node_id: id.clone(),
            });
            continue;
        }
        if previous.log.array_updated(&id) {
            push(Conflict::ChunksUpdatedInUpdatedArray {
                path: paths.path(&id)?,
                node_id: id.clone(),
            });
        }
        if previous.log.chunks_updated(&id) {
            // Only a node both logs wrote to holds a coordinate set in memory.
            let previous_coords: HashSet<ChunkIndices> =
                previous.log.updated_chunks_for(&id).collect();
            let common: HashSet<ChunkIndices> =
                coords.filter(|coord| previous_coords.contains(coord)).collect();
            if !common.is_empty() {
                push(Conflict::ChunkDoubleUpdate {
                    path: paths.path(&id)?,
                    node_id: id.clone(),
                    chunk_coordinates: common,
                });
            }
        }
    }

    for id in current.log.deleted_arrays() {
        if previous.log.array_updated(&id) || previous.log.chunks_updated(&id) {
            push(Conflict::DeleteOfUpdatedArray { path: paths.path(&id)?, node_id: id });
        }
    }
    for id in current.log.deleted_groups() {
        if previous.log.group_updated(&id) {
            push(Conflict::DeleteOfUpdatedGroup { path: paths.path(&id)?, node_id: id });
        }
    }

    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, error::Error};

    use bytes::Bytes;
    use icechunk_macros::tokio_test;

    use super::*;
    use crate::{
        Repository,
        format::{
            ChunkIndices, Path, format_constants::SpecVersionBin, manifest::ChunkPayload,
            snapshot::ArrayShape,
        },
        new_in_memory_storage,
    };

    async fn create_repo() -> Repository {
        let storage =
            new_in_memory_storage().await.expect("failed to create in-memory store");
        Repository::create(None, storage, HashMap::new(), Some(SpecVersionBin::V2), true)
            .await
            .expect("failed to create repository")
    }

    /// Four chunks along one dimension.
    fn shape() -> ArrayShape {
        ArrayShape::new(vec![(4, 4)]).expect("valid shape")
    }

    /// A repo with a root group and `/array` committed on main.
    async fn repo_with_array() -> Result<(Repository, Path), Box<dyn Error>> {
        let repo = create_repo().await;
        let path: Path = "/array".try_into()?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        session.add_array(path.clone(), shape(), None, Bytes::new()).await?;
        session.commit("create array").execute().await?;
        Ok((repo, path))
    }

    /// Write one inline chunk from the tip of main and flush it as a detached snapshot.
    async fn flush_chunk(
        repo: &Repository,
        path: &Path,
        index: u32,
        value: &str,
    ) -> Result<SnapshotId, Box<dyn Error>> {
        let mut session = repo.writable_session("main").await?;
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![index]),
                Some(ChunkPayload::Inline(value.to_owned().into())),
            )
            .await?;
        Ok(session.commit(format!("chunk {index}")).anonymous().execute().await?)
    }

    /// Write one inline chunk and commit it on main.
    async fn commit_chunk(
        repo: &Repository,
        path: &Path,
        index: u32,
        value: &str,
    ) -> Result<SnapshotId, Box<dyn Error>> {
        let mut session = repo.writable_session("main").await?;
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![index]),
                Some(ChunkPayload::Inline(value.to_owned().into())),
            )
            .await?;
        Ok(session.commit(format!("chunk {index}")).execute().await?)
    }

    #[tokio_test]
    async fn plan_with_sources_at_the_tip() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let tip = repo.lookup_branch("main").await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let b = flush_chunk(&repo, &path, 1, "b").await?;
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;

        let plan = plan_merge(&repo_info, "main", &[a.clone(), b.clone()])?;

        assert_eq!(plan.tip, tip);
        assert_eq!(plan.sources.len(), 2);
        assert_eq!(plan.sources[0].id, a);
        assert_eq!(plan.sources[0].parent, tip);
        assert!(plan.sources[0].intervening.is_empty());
        assert_eq!(plan.sources[1].id, b);
        assert!(plan.sources[1].intervening.is_empty());
        Ok(())
    }

    #[tokio_test]
    async fn plan_lists_intervening_commits_oldest_first() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let parent = repo.lookup_branch("main").await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let c1 = commit_chunk(&repo, &path, 2, "c1").await?;
        let c2 = commit_chunk(&repo, &path, 3, "c2").await?;
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;

        let plan = plan_merge(&repo_info, "main", std::slice::from_ref(&a))?;

        assert_eq!(plan.tip, c2);
        assert_eq!(plan.sources[0].parent, parent);
        let ids: Vec<SnapshotId> =
            plan.sources[0].intervening.iter().map(|info| info.id.clone()).collect();
        assert_eq!(ids, vec![c1, c2]);
        Ok(())
    }

    #[tokio_test]
    async fn plan_rejects_source_outside_branch_history() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let tip = repo.lookup_branch("main").await?;
        repo.create_branch("other", &tip).await?;
        let mut session = repo.writable_session("other").await?;
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline("o".into())),
            )
            .await?;
        session.commit("on other").execute().await?;
        let mut session = repo.writable_session("other").await?;
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![1]),
                Some(ChunkPayload::Inline("o".into())),
            )
            .await?;
        let source = session.commit("flush on other").anonymous().execute().await?;
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;

        let err =
            plan_merge(&repo_info, "main", std::slice::from_ref(&source)).unwrap_err();

        assert!(matches!(
            err.kind,
            SessionErrorKind::SnapshotNotInBranchHistory { ref snapshot, ref branch }
                if snapshot == &source && branch == "main"
        ));
        Ok(())
    }

    #[tokio_test]
    async fn plan_rejects_empty_source_list() -> Result<(), Box<dyn Error>> {
        let (repo, _) = repo_with_array().await?;
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;

        let err = plan_merge(&repo_info, "main", &[]).unwrap_err();

        assert!(matches!(err.kind, SessionErrorKind::NoSnapshotsToMerge));
        Ok(())
    }

    use std::collections::HashSet;

    use crate::{
        change_set::{ArrayData, ChangeSet, transaction_log_from_change_set},
        format::{NodeId, transaction_log::TransactionLog},
    };

    fn path(s: &str) -> Path {
        Path::try_from(s).expect("valid path")
    }

    fn array_data() -> ArrayData {
        ArrayData { shape: shape(), dimension_names: None, user_data: Bytes::new() }
    }

    fn inline(value: &str) -> Option<ChunkPayload> {
        Some(ChunkPayload::Inline(value.to_owned().into()))
    }

    fn log(cs: &ChangeSet, id: &SnapshotId) -> TransactionLog {
        transaction_log_from_change_set(id, cs)
    }

    /// Run detection with `a` as previous and `b` as current.
    fn conflicts(
        a: &ChangeSet,
        b: &ChangeSet,
        paths: &PathIndex,
    ) -> Result<(SnapshotId, SnapshotId, Vec<MergeConflict>), Box<dyn Error>> {
        let (sa, sb) = (SnapshotId::random(), SnapshotId::random());
        let (la, lb) = (log(a, &sa), log(b, &sb));
        let found = detect_conflicts(
            &LogView { id: &sa, log: &la },
            &LogView { id: &sb, log: &lb },
            paths,
        )?;
        Ok((sa, sb, found))
    }

    #[test]
    fn disjoint_chunk_writes_do_not_conflict() -> Result<(), Box<dyn Error>> {
        let id = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.set_chunk_ref(id.clone(), ChunkIndices(vec![0]), inline("a"))?;
        let mut b = ChangeSet::for_edits();
        b.set_chunk_ref(id.clone(), ChunkIndices(vec![1]), inline("b"))?;
        let paths = PathIndex::from_pairs([(id, path("/array"))]);

        let (_, _, found) = conflicts(&a, &b, &paths)?;

        assert!(found.is_empty());
        Ok(())
    }

    #[test]
    fn detects_chunk_double_update() -> Result<(), Box<dyn Error>> {
        let id = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.set_chunk_ref(id.clone(), ChunkIndices(vec![0]), inline("a"))?;
        a.set_chunk_ref(id.clone(), ChunkIndices(vec![1]), inline("a"))?;
        let mut b = ChangeSet::for_edits();
        b.set_chunk_ref(id.clone(), ChunkIndices(vec![1]), inline("b"))?;
        b.set_chunk_ref(id.clone(), ChunkIndices(vec![2]), inline("b"))?;
        let paths = PathIndex::from_pairs([(id.clone(), path("/array"))]);

        let (sa, sb, found) = conflicts(&a, &b, &paths)?;

        assert_eq!(
            found,
            vec![MergeConflict {
                first_snapshot: sa,
                second_snapshot: sb,
                conflict: Conflict::ChunkDoubleUpdate {
                    path: path("/array"),
                    node_id: id,
                    chunk_coordinates: HashSet::from([ChunkIndices(vec![1])]),
                },
            }]
        );
        Ok(())
    }

    #[test]
    fn detects_metadata_double_update() -> Result<(), Box<dyn Error>> {
        let array = NodeId::random();
        let group = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.update_array(&array, &path("/array"), array_data())?;
        a.update_group(&group, &path("/group"), Bytes::from_static(b"a"))?;
        let mut b = ChangeSet::for_edits();
        b.update_array(&array, &path("/array"), array_data())?;
        b.update_group(&group, &path("/group"), Bytes::from_static(b"b"))?;
        let paths =
            PathIndex::from_pairs([(array, path("/array")), (group, path("/group"))]);

        let (_, _, found) = conflicts(&a, &b, &paths)?;

        let kinds: Vec<&Conflict> = found.iter().map(|c| &c.conflict).collect();
        assert_eq!(
            kinds,
            vec![
                &Conflict::ZarrMetadataDoubleUpdate(path("/array")),
                &Conflict::ZarrMetadataDoubleUpdate(path("/group")),
            ]
        );
        Ok(())
    }

    #[test]
    fn detects_update_of_deleted_nodes() -> Result<(), Box<dyn Error>> {
        let array = NodeId::random();
        let group = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.delete_array(path("/array"), &array)?;
        a.delete_group(path("/group"), &group)?;
        let mut b = ChangeSet::for_edits();
        b.update_array(&array, &path("/array"), array_data())?;
        b.update_group(&group, &path("/group"), Bytes::from_static(b"b"))?;
        let paths =
            PathIndex::from_pairs([(array, path("/array")), (group, path("/group"))]);

        let (_, _, found) = conflicts(&a, &b, &paths)?;

        let kinds: Vec<&Conflict> = found.iter().map(|c| &c.conflict).collect();
        assert_eq!(
            kinds,
            vec![
                &Conflict::ZarrMetadataUpdateOfDeletedArray(path("/array")),
                &Conflict::ZarrMetadataUpdateOfDeletedGroup(path("/group")),
            ]
        );
        Ok(())
    }

    #[test]
    fn detects_chunks_in_deleted_and_updated_arrays() -> Result<(), Box<dyn Error>> {
        let deleted = NodeId::random();
        let updated = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.delete_array(path("/deleted"), &deleted)?;
        a.update_array(&updated, &path("/updated"), array_data())?;
        let mut b = ChangeSet::for_edits();
        b.set_chunk_ref(deleted.clone(), ChunkIndices(vec![0]), inline("b"))?;
        b.set_chunk_ref(updated.clone(), ChunkIndices(vec![0]), inline("b"))?;
        let paths = PathIndex::from_pairs([
            (deleted.clone(), path("/deleted")),
            (updated.clone(), path("/updated")),
        ]);

        let (_, _, mut found) = conflicts(&a, &b, &paths)?;

        found.sort_by_key(|c| format!("{:?}", c.conflict));
        let kinds: Vec<&Conflict> = found.iter().map(|c| &c.conflict).collect();
        assert_eq!(
            kinds,
            vec![
                &Conflict::ChunksUpdatedInDeletedArray {
                    path: path("/deleted"),
                    node_id: deleted,
                },
                &Conflict::ChunksUpdatedInUpdatedArray {
                    path: path("/updated"),
                    node_id: updated,
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn detects_delete_of_updated_nodes() -> Result<(), Box<dyn Error>> {
        let array = NodeId::random();
        let written = NodeId::random();
        let group = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.update_array(&array, &path("/array"), array_data())?;
        a.set_chunk_ref(written.clone(), ChunkIndices(vec![0]), inline("a"))?;
        a.update_group(&group, &path("/group"), Bytes::from_static(b"a"))?;
        let mut b = ChangeSet::for_edits();
        b.delete_array(path("/array"), &array)?;
        b.delete_array(path("/written"), &written)?;
        b.delete_group(path("/group"), &group)?;
        let paths = PathIndex::from_pairs([
            (array.clone(), path("/array")),
            (written.clone(), path("/written")),
            (group.clone(), path("/group")),
        ]);

        let (_, _, mut found) = conflicts(&a, &b, &paths)?;

        found.sort_by_key(|c| format!("{:?}", c.conflict));
        let kinds: Vec<&Conflict> = found.iter().map(|c| &c.conflict).collect();
        assert_eq!(
            kinds,
            vec![
                &Conflict::DeleteOfUpdatedArray { path: path("/array"), node_id: array },
                &Conflict::DeleteOfUpdatedArray {
                    path: path("/written"),
                    node_id: written,
                },
                &Conflict::DeleteOfUpdatedGroup { path: path("/group"), node_id: group },
            ]
        );
        Ok(())
    }

    #[test]
    fn detects_new_node_conflicts() -> Result<(), Box<dyn Error>> {
        let (a_same, b_same) = (NodeId::random(), NodeId::random());
        let (a_parent, b_child) = (NodeId::random(), NodeId::random());
        let mut a = ChangeSet::for_edits();
        a.add_group(path("/same"), a_same.clone(), Bytes::new())?;
        a.add_array(path("/parent"), a_parent.clone(), array_data())?;
        let mut b = ChangeSet::for_edits();
        b.add_array(path("/same"), b_same.clone(), array_data())?;
        b.add_group(path("/parent/child"), b_child.clone(), Bytes::new())?;
        let paths = PathIndex::from_pairs([
            (a_same, path("/same")),
            (b_same, path("/same")),
            (a_parent, path("/parent")),
            (b_child, path("/parent/child")),
        ]);

        let (_, _, mut found) = conflicts(&a, &b, &paths)?;

        found.sort_by_key(|c| format!("{:?}", c.conflict));
        let kinds: Vec<&Conflict> = found.iter().map(|c| &c.conflict).collect();
        assert_eq!(
            kinds,
            vec![
                &Conflict::NewNodeConflictsWithExistingNode(path("/same")),
                &Conflict::NewNodeInInvalidGroup(path("/parent")),
            ]
        );
        Ok(())
    }

    #[test]
    fn unresolvable_node_id_is_an_error() -> Result<(), Box<dyn Error>> {
        let id = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.update_array(&id, &path("/array"), array_data())?;
        let mut b = ChangeSet::for_edits();
        b.update_array(&id, &path("/array"), array_data())?;
        let paths = PathIndex::from_pairs([]);
        let (sa, sb) = (SnapshotId::random(), SnapshotId::random());
        let (la, lb) = (log(&a, &sa), log(&b, &sb));

        let err = detect_conflicts(
            &LogView { id: &sa, log: &la },
            &LogView { id: &sb, log: &lb },
            &paths,
        )
        .unwrap_err();

        assert!(
            matches!(err.kind, SessionErrorKind::ConflictingPathNotFound(ref n) if n == &id)
        );
        Ok(())
    }
}
