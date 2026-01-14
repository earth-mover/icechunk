use std::{collections::HashSet, sync::Arc};

use async_stream::try_stream;
use err_into::ErrorInto as _;
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use tokio::pin;
use tracing::instrument;

use crate::{
    asset_manager::AssetManager,
    format::{
        SnapshotId, format_constants::SpecVersionBin, repo_info::RepoInfo,
        snapshot::Snapshot,
    },
    refs::{RefResult, list_refs},
    repository::{RepositoryError, RepositoryResult},
};
pub mod expiration_v1;
pub mod gc;
pub mod manifests;
pub mod stats;

#[instrument(skip_all)]
pub fn all_roots_v2<'a>(
    repo_info: &'a RepoInfo,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RepositoryResult<impl Iterator<Item = RepositoryResult<SnapshotId>> + 'a> {
    let res = repo_info
        .tag_names()?
        .map(|tag| repo_info.resolve_tag(tag))
        .chain(repo_info.branch_names()?.map(|br| repo_info.resolve_branch(br)))
        .chain(extra_roots.iter().cloned().map(Ok))
        .map(|r| r.err_into());
    Ok(res)
}

#[instrument(skip_all)]
pub async fn pointed_snapshots_v2<'a>(
    asset_manager: Arc<AssetManager>,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + 'a> {
    let mut seen: HashSet<SnapshotId> = HashSet::new();
    let (repo_info, _) = asset_manager.fetch_repo_info().await?;
    let res = try_stream! {

        let roots = all_roots_v2(repo_info.as_ref(), extra_roots)?;
        for pointed_snap_id in roots {
            let pointed_snap_id = pointed_snap_id?;
            if ! seen.contains(&pointed_snap_id)
            {
                let parents = repo_info.ancestry(&pointed_snap_id)?;
                for snap_info in parents {
                    let snap_id = snap_info?.id;
                    let snap: Arc<Snapshot> = asset_manager.fetch_snapshot(&snap_id).await?;
                    if seen.insert(snap_id) { // it's a new snapshot
                        yield snap
                    } else {
                        // as soon as we find a repeated snapshot
                        // there is no point in continuing to retrieve
                        // the rest of the ancestry, it must be already
                        // retrieved from other ref
                        break
                    }
                }
            }
        }
    };
    Ok(res)
}

#[instrument(skip_all)]
pub async fn pointed_snapshots_v1<'a>(
    asset_manager: Arc<AssetManager>,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + 'a> {
    let mut seen: HashSet<SnapshotId> = HashSet::new();
    let res = try_stream! {
        let roots = all_roots_v1(Arc::clone(&asset_manager), extra_roots)
            .await?
            .err_into::<RepositoryError>();
        pin!(roots);

        while let Some(pointed_snap_id) = roots.try_next().await? {
            if ! seen.contains(&pointed_snap_id) {
                #[allow(deprecated)]
                let parents = Arc::clone(&asset_manager).snapshot_ancestry_v1(&pointed_snap_id).await?;
                for await parent in parents {
                    let parent = parent?;
                    let snap_id = parent.id();
                    if seen.insert(snap_id) {
                        // it's a new snapshot
                        yield parent
                    } else {
                        // as soon as we find a repeated snapshot
                        // there is no point in continuing to retrieve
                        // the rest of the ancestry, it must be already
                        // retrieved from other ref
                        break
                    }
                }
            }
        }
    };
    Ok(res)
}
pub async fn all_roots_v1<'a>(
    asset_manager: Arc<AssetManager>,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RefResult<impl Stream<Item = RefResult<SnapshotId>> + 'a> {
    #[allow(deprecated)]
    let all_refs =
        list_refs(asset_manager.storage().as_ref(), asset_manager.storage_settings())
            .await?;
    let roots = stream::iter(all_refs)
        .then(move |r| {
            let asset_manager = asset_manager.clone();
            async move {
                #[allow(deprecated)]
                r.fetch(
                    asset_manager.storage().as_ref(),
                    asset_manager.storage_settings(),
                )
                .await
                .map(|ref_data| ref_data.snapshot)
            }
        })
        .err_into()
        .chain(stream::iter(extra_roots.iter().cloned()).map(Ok));
    Ok(roots)
}

pub async fn pointed_snapshots<'a>(
    asset_manager: Arc<AssetManager>,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + 'a> {
    match asset_manager.spec_version() {
        SpecVersionBin::V1dot0 => {
            Ok(pointed_snapshots_v1(asset_manager, extra_roots).await?.left_stream())
        }
        SpecVersionBin::V2dot0 => {
            Ok(pointed_snapshots_v2(asset_manager, extra_roots).await?.right_stream())
        }
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use futures::TryStreamExt as _;
    use std::collections::{HashMap, HashSet};

    use bytes::Bytes;

    use crate::{
        Repository, format::Path, new_in_memory_storage, ops::pointed_snapshots,
    };

    #[tokio::test]
    async fn test_pointed_snapshots_duplicate() -> Result<(), Box<dyn std::error::Error>>
    {
        let storage = new_in_memory_storage().await?;
        let repo =
            Repository::create(None, storage.clone(), HashMap::new(), None).await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        let snap = session.commit("commit", None).await?;
        repo.create_tag("tag1", &snap).await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group("/foo".try_into().unwrap(), Bytes::new()).await?;
        let snap = session.commit("commit", None).await?;
        repo.create_tag("tag2", &snap).await?;

        let all_snaps = pointed_snapshots(repo.asset_manager().clone(), &HashSet::new())
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(all_snaps.len(), 3);
        Ok(())
    }
}
