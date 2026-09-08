//! Merge snapshots into one commit on a branch.
//!
//! A source is any snapshot whose parent is an ancestor of the branch tip,
//! usually a detached snapshot written by `Session::flush`. The merge checks
//! the source transaction logs against each other and against the commits
//! between their parents and the tip, then writes one snapshot that applies
//! every source on top of the tip.

use crate::{
    conflicts::Conflict,
    format::{SnapshotId, repo_info::RepoInfo, snapshot::SnapshotInfo},
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
}
