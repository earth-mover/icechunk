//! Repository maintenance operations.

use std::{collections::HashSet, num::NonZeroU16, sync::Arc};

use async_stream::try_stream;
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
    repository::RepositoryResult,
};
use icechunk_types::error::ICResultCtxExt as _;

/// Expire old snapshots beyond a threshold.
pub mod expiration_v1;
/// Garbage collection to remove unreferenced data.
pub mod gc;
/// Manifest optimization and rebuilding.
pub mod manifests;
/// Repository statistics.
pub mod stats;

#[instrument(skip_all)]
pub fn all_roots_v2<'a>(
    repo_info: &'a RepoInfo,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RepositoryResult<impl Iterator<Item = RepositoryResult<SnapshotId>> + 'a> {
    let res = repo_info
        .tag_names()
        .inject()?
        .map(|tag| repo_info.resolve_tag(tag))
        .chain(repo_info.branch_names().inject()?.map(|br| repo_info.resolve_branch(br)))
        .chain(extra_roots.iter().cloned().map(Ok))
        .map(|r| r.inject());
    Ok(res)
}

/// Ids of every snapshot reachable from a ref or an `extra_roots` entry,
/// computed from `repo_info` alone (no IO).
#[instrument(skip_all)]
pub fn reachable_snapshots_v2(
    repo_info: &RepoInfo,
    extra_roots: &HashSet<SnapshotId>,
) -> RepositoryResult<HashSet<SnapshotId>> {
    let mut seen: HashSet<SnapshotId> = HashSet::new();
    for pointed_snap_id in all_roots_v2(repo_info, extra_roots)? {
        let pointed_snap_id = pointed_snap_id?;
        if !seen.contains(&pointed_snap_id) {
            for snap_info in repo_info.ancestry(&pointed_snap_id).inject()? {
                if !seen.insert(snap_info.inject()?.id) {
                    // the rest of the ancestry came in with the snapshot we already have
                    break;
                }
            }
        }
    }
    Ok(seen)
}

/// Fetches up to `max_concurrent_fetches` snapshots at a time, in no
/// particular order.
#[instrument(skip_all)]
pub fn pointed_snapshots_v2(
    asset_manager: Arc<AssetManager>,
    repo_info: &RepoInfo,
    extra_roots: &HashSet<SnapshotId>,
    max_concurrent_fetches: NonZeroU16,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + use<>> {
    let ids = reachable_snapshots_v2(repo_info, extra_roots)?;
    let res = stream::iter(ids)
        .map(move |id| {
            let asset_manager = Arc::clone(&asset_manager);
            async move { asset_manager.fetch_snapshot(&id).await }
        })
        .buffer_unordered(max_concurrent_fetches.get() as usize);
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
            .await.inject()?
            .map(|r| r.inject());
        pin!(roots);

        while let Some(pointed_snap_id) = roots.try_next().await? {
            if ! seen.contains(&pointed_snap_id) {
                #[expect(deprecated)]
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
    let all_refs =
        list_refs(asset_manager.storage().as_ref(), asset_manager.storage_settings())
            .await?;
    let roots = stream::iter(all_refs)
        .then(move |r| {
            let asset_manager = Arc::clone(&asset_manager);
            async move {
                r.fetch(
                    asset_manager.storage().as_ref(),
                    asset_manager.storage_settings(),
                )
                .await
                .map(|ref_data| ref_data.snapshot)
            }
        })
        .chain(stream::iter(extra_roots.iter().cloned()).map(Ok));
    Ok(roots)
}

/// `repo_info` is ignored for V1 repos, which have no repo info object. For V2
/// it is fetched when not supplied; pass the one you already hold to keep the
/// walk consistent with anything else you derived from it.
///
/// `max_concurrent_fetches` only applies to V2: a V1 ancestry is a pointer
/// chase through the snapshot files, so its reads stay sequential.
pub async fn pointed_snapshots<'a>(
    asset_manager: Arc<AssetManager>,
    repo_info: Option<Arc<RepoInfo>>,
    extra_roots: &'a HashSet<SnapshotId>,
    max_concurrent_fetches: NonZeroU16,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + 'a> {
    match asset_manager.spec_version() {
        SpecVersionBin::V1 => {
            Ok(pointed_snapshots_v1(asset_manager, extra_roots).await?.left_stream())
        }
        SpecVersionBin::V2 => {
            let repo_info = match repo_info {
                Some(repo_info) => repo_info,
                None => asset_manager.fetch_repo_info().await?.0,
            };
            Ok(pointed_snapshots_v2(
                asset_manager,
                repo_info.as_ref(),
                extra_roots,
                max_concurrent_fetches,
            )?
            .right_stream())
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt as _;
    use std::{
        collections::{HashMap, HashSet},
        num::NonZeroU16,
        sync::Arc,
    };

    use bytes::Bytes;

    use crate::{
        Repository, Storage,
        format::{Path, SNAPSHOTS_FILE_PATH, format_constants::SpecVersionBin},
        new_in_memory_storage,
        ops::pointed_snapshots,
        test_utils::{logging_asset_manager, repo_with_converging_refs},
    };

    #[tokio::test]
    async fn test_pointed_snapshots_duplicate() -> Result<(), Box<dyn std::error::Error>>
    {
        let storage = new_in_memory_storage().await?;
        let repo =
            Repository::create(None, Arc::clone(&storage), HashMap::new(), None, true)
                .await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        let snap = session.commit("commit").max_concurrent_nodes(8).execute().await?;
        repo.create_tag("tag1", &snap).await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group("/foo".try_into().unwrap(), Bytes::new()).await?;
        let snap = session.commit("commit").max_concurrent_nodes(8).execute().await?;
        repo.create_tag("tag2", &snap).await?;

        let all_snaps = pointed_snapshots(
            Arc::clone(repo.asset_manager()),
            None,
            &HashSet::new(),
            NonZeroU16::new(10).unwrap(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        assert_eq!(all_snaps.len(), 3);
        Ok(())
    }

    /// Refs whose ancestries converge must not re-read the snapshot they
    /// converge on: every reachable snapshot is fetched exactly once.
    #[tokio::test]
    async fn test_pointed_snapshots_v2_fetches_each_snapshot_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
        let repo = repo_with_converging_refs(&backend).await?;
        let (logging, asset_manager) = logging_asset_manager(
            &backend,
            repo.storage_settings().clone(),
            SpecVersionBin::V2,
        );

        let all_snaps = pointed_snapshots(
            asset_manager,
            None,
            &HashSet::new(),
            NonZeroU16::new(10).unwrap(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        // 5 commits plus the initial snapshot
        assert_eq!(all_snaps.len(), 6);

        let snapshot_reads = logging
            .fetch_operations()
            .into_iter()
            .filter(|(_, path)| path.starts_with(SNAPSHOTS_FILE_PATH))
            .count();
        assert_eq!(snapshot_reads, 6);
        Ok(())
    }
}
