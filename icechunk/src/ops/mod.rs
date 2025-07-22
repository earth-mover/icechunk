use std::{collections::HashSet, sync::Arc};

use async_stream::try_stream;
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use tokio::pin;
use tracing::instrument;

use crate::{
    Storage,
    asset_manager::AssetManager,
    format::{SnapshotId, snapshot::Snapshot},
    refs::{RefResult, list_refs},
    repository::{RepositoryError, RepositoryResult},
    storage,
};
pub mod gc;
pub mod manifests;
pub mod stats;

pub async fn all_roots<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    storage_settings: &'a storage::Settings,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RefResult<impl Stream<Item = RefResult<SnapshotId>> + 'a> {
    let all_refs = list_refs(storage, storage_settings).await?;
    let roots = stream::iter(all_refs)
        .then(move |r| async move {
            r.fetch(storage, storage_settings).await.map(|ref_data| ref_data.snapshot)
        })
        .err_into()
        .chain(stream::iter(extra_roots.iter().cloned()).map(Ok));
    Ok(roots)
}

#[instrument(skip(storage, storage_settings, asset_manager))]
pub async fn pointed_snapshots<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    storage_settings: &'a storage::Settings,
    asset_manager: Arc<AssetManager>,
    extra_roots: &'a HashSet<SnapshotId>,
) -> RepositoryResult<impl Stream<Item = RepositoryResult<Arc<Snapshot>>> + 'a> {
    let mut seen: HashSet<SnapshotId> = HashSet::new();
    let res = try_stream! {
        let roots = all_roots(storage, storage_settings, extra_roots)
            .await?
            .err_into::<RepositoryError>();
        pin!(roots);

        while let Some(pointed_snap_id) = roots.try_next().await? {
            if ! seen.contains(&pointed_snap_id) {
                let parents = Arc::clone(&asset_manager).snapshot_ancestry(&pointed_snap_id).await?;
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
        let repo = Repository::create(None, storage.clone(), HashMap::new()).await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        let snap = session.commit("commit", None).await?;
        repo.create_tag("tag1", &snap).await?;
        let mut session = repo.writable_session("main").await?;
        session.add_group("/foo".try_into().unwrap(), Bytes::new()).await?;
        let snap = session.commit("commit", None).await?;
        repo.create_tag("tag2", &snap).await?;

        let all_snaps = pointed_snapshots(
            storage.as_ref(),
            &storage.default_settings(),
            repo.asset_manager().clone(),
            &HashSet::new(),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;

        assert_eq!(all_snaps.len(), 3);
        Ok(())
    }
}
