use std::{collections::HashSet, sync::Arc};

use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt, stream};
use tokio::pin;
use tracing::instrument;

use crate::{
    asset_manager::AssetManager,
    format::SnapshotId,
    ops::gc::{ExpireResult, ExpiredRefAction, GCError, GCResult},
    refs::{Ref, delete_branch, delete_tag, list_refs},
    repository::RepositoryErrorKind,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ExpireRefResult {
    NothingToDo {
        ref_is_expired: bool,
    },
    Done {
        released_snapshots: HashSet<SnapshotId>,
        edited_snapshot: SnapshotId,
        ref_is_expired: bool,
    },
}

/// Expire snapshots older than a threshold.
///
/// This only processes snapshots found by navigating `reference`
/// ancestry. Any other snapshots are not touched.
///
/// The operation will edit in place the oldest non-expired snapshot,
/// changing its parent to be the root of the repo.
///
/// For this reasons, it's recommended to invalidate any snapshot
/// caches before traversing history againg. The cache in the
/// passed `asset_manager` is invalidated here, but other caches
/// may exist, for example, in [`Repository`] instances.
///
/// Returns the ids of all snapshots considered expired and skipped
/// from history. Notice that this snapshot are not necessarily
/// available for garbage collection, they could still be pointed by
/// ether refs.
///
/// See: https://github.com/earth-mover/icechunk/blob/main/design-docs/007-basic-expiration.md
#[instrument(skip(asset_manager))]
pub async fn expire_ref(
    asset_manager: Arc<AssetManager>,
    reference: &Ref,
    older_than: DateTime<Utc>,
) -> GCResult<ExpireRefResult> {
    #[allow(deprecated)]
    let snap_id = reference
        .fetch(asset_manager.storage().as_ref(), asset_manager.storage_settings())
        .await
        .map(|ref_data| ref_data.snapshot)?;

    tracing::info!("Starting expiration at ref {}", snap_id);

    // the algorithm works by finding the oldest non expired snap and the root of the repo
    // we do that in a single pass through the ancestry
    // we keep two "pointer" the last editable_snap and the root, and we keep
    // updating them as we navigate the ancestry
    let mut editable_snap = snap_id.clone();
    let mut root = snap_id.clone();

    // here we'll populate the results of every expired snapshot
    let mut released = HashSet::new();
    #[allow(deprecated)]
    let ancestry =
        Arc::clone(&asset_manager).snapshot_ancestry_v1(&snap_id).await?.peekable();

    pin!(ancestry);

    let mut ref_is_expired = false;
    if let Some(Ok(info)) = ancestry.as_mut().peek().await
        && info.flushed_at()? < older_than
    {
        tracing::debug!(flushed_at = %info.flushed_at()?, "Ref flagged as expired");
        ref_is_expired = true;
    }

    while let Some(parent) = ancestry.try_next().await? {
        let flushed_at = parent.flushed_at()?;
        let parent_id = parent.id();
        if parent.flushed_at()? >= older_than {
            tracing::debug!(snap = %parent_id, flushed_at = %flushed_at, "Processing non expired snapshot");
            // we are navigating non-expired snaps, last will be kept in editable_snap
            editable_snap = parent_id;
        } else {
            tracing::debug!(snap = %parent_id, flushed_at = %flushed_at, "Processing expired snapshot");
            released.insert(parent_id.clone());
            root = parent_id;
        }
    }

    // we counted the root as released, but it's not
    released.remove(&root);

    let editable_snap = asset_manager.fetch_snapshot(&editable_snap).await?;

    #[allow(deprecated)]
    let old_parent_id = editable_snap.parent_id();
    if editable_snap.id() == root    // only root can be expired
        || Some(&root) == old_parent_id.as_ref()
        // we never found an expirable snap
        || root == snap_id
    {
        // Either the reference is the root, or it is pointing to the root as first parent
        // Nothing to do
        tracing::info!("Nothing to expire for this ref");
        return Ok(ExpireRefResult::NothingToDo { ref_is_expired });
    }

    let root = asset_manager.fetch_snapshot(&root).await?;
    // we don't want to create loops, so:
    // we never edit the root of a tree
    #[allow(deprecated)]
    {
        assert!(editable_snap.parent_id().is_some());
    }
    // and, we only set a root as parent
    #[allow(deprecated)]
    {
        assert!(root.parent_id().is_none());
    }

    tracing::info!(root = %root.id(), editable_snap=%editable_snap.id(), "Expiration needed for this ref");

    // TODO: add properties to the snapshot that tell us it was history edited
    #[allow(deprecated)]
    let new_snapshot = Arc::new(root.adopt(&editable_snap)?);
    asset_manager.write_snapshot(new_snapshot).await?;
    tracing::info!("Snapshot overwritten");

    Ok(ExpireRefResult::Done {
        released_snapshots: released,
        edited_snapshot: editable_snap.id().clone(),
        ref_is_expired,
    })
}

/// Expire all snapshots older than a threshold.
///
/// This processes snapshots found by navigating all references in
/// the repo, tags first, branches leter, both in lexicographical order.
///
/// The operation will edit in place the oldest non-expired snapshot,
/// in every ancestry, changing its parent to be the root of the repo.
///
/// For this reasons, it's recommended to invalidate any snapshot
/// caches before traversing history againg. The cache in the
/// passed `asset_manager` is invalidated here, but other caches
/// may exist, for example, in [`Repository`] instances.
///
/// Notice that the snapshot returned as released, are not necessarily
/// available for garbage collection, they could still be pointed by
/// ether refs.
///
/// See: https://github.com/earth-mover/icechunk/blob/main/design-docs/007-basic-expiration.md
#[instrument(skip(asset_manager))]
pub async fn expire(
    asset_manager: Arc<AssetManager>,
    older_than: DateTime<Utc>,
    expired_branches: ExpiredRefAction,
    expired_tags: ExpiredRefAction,
) -> GCResult<ExpireResult> {
    #[allow(deprecated)]
    let storage = asset_manager.storage().as_ref();
    #[allow(deprecated)]
    let storage_settings = asset_manager.storage_settings();
    if !storage.can_write().await? {
        return Err(GCError::Repository(
            RepositoryErrorKind::ReadonlyStorage("Cannot expire snapshots".to_string())
                .into(),
        ));
    }

    let all_refs = stream::iter(list_refs(storage, storage_settings).await?);
    let asset_manager = Arc::clone(&asset_manager.clone());

    all_refs
        .then(move |r| {
            let asset_manager = asset_manager.clone();
            async move {
                let ref_result = expire_ref(asset_manager, &r, older_than).await?;
                Ok::<(Ref, ExpireRefResult), GCError>((r, ref_result))
            }
        })
        .try_fold(ExpireResult::default(), |mut result, (r, ref_result)| async move {
            let ref_is_expired = match ref_result {
                ExpireRefResult::Done {
                    released_snapshots,
                    edited_snapshot,
                    ref_is_expired,
                } => {
                    result.released_snapshots.extend(released_snapshots.into_iter());
                    result.edited_snapshots.insert(edited_snapshot);
                    ref_is_expired
                }
                ExpireRefResult::NothingToDo { ref_is_expired } => ref_is_expired,
            };
            if ref_is_expired {
                match &r {
                    Ref::Tag(name) => {
                        if expired_tags == ExpiredRefAction::Delete {
                            tracing::info!(name, "Deleting expired tag");
                            delete_tag(storage, storage_settings, name.as_str())
                                .await
                                .map_err(GCError::Ref)?;
                            result.deleted_refs.insert(r);
                        }
                    }
                    Ref::Branch(name) => {
                        if expired_branches == ExpiredRefAction::Delete
                            && name != Ref::DEFAULT_BRANCH
                        {
                            tracing::info!(name, "Deleting expired branch");
                            delete_branch(storage, storage_settings, name.as_str())
                                .await
                                .map_err(GCError::Ref)?;
                            result.deleted_refs.insert(r);
                        }
                    }
                }
            }
            Ok(result)
        })
        .await
}
