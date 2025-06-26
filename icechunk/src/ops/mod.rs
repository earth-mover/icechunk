use std::{collections::HashSet, future::ready, sync::Arc};

use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use tracing::instrument;

use crate::{
    Storage,
    asset_manager::AssetManager,
    format::SnapshotId,
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
    // TODO: this could be optimized by not following the ancestry of snapshots that we have
    // already seen
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
) -> RepositoryResult<impl Stream<Item = RepositoryResult<SnapshotId>> + 'a> {
    let roots = all_roots(storage, storage_settings, extra_roots)
        .await?
        .err_into::<RepositoryError>();
    Ok(roots
        .and_then(move |snap_id| {
            let asset_manager = Arc::clone(&asset_manager.clone());
            async move {
                let snap = asset_manager.fetch_snapshot(&snap_id).await?;
                let parents = Arc::clone(&asset_manager)
                    .snapshot_ancestry(&snap.id())
                    .await?
                    .map_ok(|parent| parent.id)
                    .err_into();
                Ok(stream::once(ready(Ok(snap_id))).chain(parents))
            }
        })
        .try_flatten())
}
