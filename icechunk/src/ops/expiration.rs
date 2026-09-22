//! Snapshot expiration: collapse history older than a threshold.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU16,
    sync::Arc,
    time::Duration,
};

use backon::{BackoffBuilder as _, ExponentialBuilder, Retryable as _};
use chrono::{DateTime, Utc};
use itertools::Itertools as _;
use tracing::{debug, info, instrument};

use crate::{
    asset_manager::AssetManager,
    config::RepoUpdateRetryConfig,
    format::{
        SnapshotId,
        format_constants::SpecVersionBin,
        repo_info::{RepoAvailability, RepoInfo, UpdateInfo, UpdateType},
        snapshot::{Snapshot, SnapshotInfo},
    },
    ops::{GCError, GCResult, reparent_and_prune},
    refs::Ref,
    repository::{RepositoryError, RepositoryErrorKind},
    storage,
};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};

mod v1;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ExpiredRefAction {
    Delete,
    Ignore,
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct ExpireResult {
    pub released_snapshots: HashSet<SnapshotId>,
    pub edited_snapshots: HashSet<SnapshotId>,
    pub deleted_refs: HashSet<Ref>,
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
/// may exist, for example, in [`crate::Repository`] instances.
///
/// Notice that the snapshot returned as released, are not necessarily
/// available for garbage collection, they could still be pointed by
/// ether refs.
///
/// See: <https://github.com/earth-mover/icechunk/blob/main/design-docs/007-basic-expiration.md>
#[instrument(skip(asset_manager))]
pub async fn expire(
    asset_manager: Arc<AssetManager>,
    older_than: DateTime<Utc>,
    expired_branches: ExpiredRefAction,
    expired_tags: ExpiredRefAction,
    repo_update_retries: Option<&RepoUpdateRetryConfig>,
    num_updates_per_repo_info_file: u16,
) -> GCResult<ExpireResult> {
    if !asset_manager.can_write_to_storage().await? {
        return Err(RepositoryErrorKind::ReadonlyStorage("Cannot expire".to_string()))
            .capture()
            .map_err(GCError::Repository)?;
    }

    // Check repo status (only available on IC2+)
    if asset_manager.spec_version() >= SpecVersionBin::V2 {
        let (repo_info, _) = asset_manager.fetch_repo_info().await?;
        if repo_info.status()?.availability != RepoAvailability::Online {
            return Err(RepositoryErrorKind::ReadonlyRepository(
                "Cannot garbage collect".to_string(),
            ))
            .capture()
            .map_err(GCError::Repository)?;
        }
    }

    match asset_manager.spec_version() {
        SpecVersionBin::V1 => {
            v1::expire(asset_manager, older_than, expired_branches, expired_tags).await
        }
        SpecVersionBin::V2 => {
            expire_v2(
                asset_manager,
                older_than,
                expired_branches,
                expired_tags,
                repo_update_retries,
                num_updates_per_repo_info_file,
            )
            .await
        }
    }
}

/// Since `expire_v2` is a relatively fast operation (repo object only) we retry it if the repo info
/// object was modified since it started
#[instrument(skip(asset_manager))]
pub async fn expire_v2(
    asset_manager: Arc<AssetManager>,
    older_than: DateTime<Utc>,
    expired_branches: ExpiredRefAction,
    expired_tags: ExpiredRefAction,
    repo_update_retries: Option<&RepoUpdateRetryConfig>,
    num_updates_per_repo_info_file: u16,
) -> GCResult<ExpireResult> {
    let default_retry_config = RepoUpdateRetryConfig::default();
    let retry_config = repo_update_retries.unwrap_or(&default_retry_config).retries();

    let backoff = ExponentialBuilder::new()
        .with_min_delay(Duration::from_millis(retry_config.initial_backoff_ms() as u64))
        .with_max_delay(Duration::from_millis(retry_config.max_backoff_ms() as u64))
        .with_max_times(retry_config.max_tries().get() as usize)
        .with_jitter()
        .build();

    let expire = async || {
        expire_v2_one_attempt(
            Arc::clone(&asset_manager),
            older_than,
            expired_branches,
            expired_tags,
            num_updates_per_repo_info_file,
        )
        .await
    };

    expire.retry(backoff)
        .sleep(tokio::time::sleep)
        .when(|e| {
            matches!(
                e,
                GCError::Repository(RepositoryError {
                    kind: RepositoryErrorKind::RepoInfoUpdated,
                    ..
                })
            )
        })
            .notify(|_, _|  {

                    info!(
                        "Repo info object was updated while expire was running, retrying with backoff..."
                    );}
        )
        .await
}

#[instrument(skip(asset_manager))]
async fn expire_v2_one_attempt(
    asset_manager: Arc<AssetManager>,
    older_than: DateTime<Utc>,
    expired_branches: ExpiredRefAction,
    expired_tags: ExpiredRefAction,
    num_updates_per_repo_info_file: u16,
) -> GCResult<ExpireResult> {
    info!("Expiration started");
    let (repo_info, repo_info_version_at_start) = asset_manager.fetch_repo_info().await?;
    let tags: Vec<(Ref, SnapshotId)> = repo_info
        .tags()?
        .map(|(name, snap)| Ok::<_, GCError>((Ref::Tag(name.to_string()), snap)))
        .try_collect()?;
    let branches: Vec<(Ref, SnapshotId)> = repo_info
        .branches()?
        .map(|(name, snap)| Ok::<_, GCError>((Ref::Branch(name.to_string()), snap)))
        .try_collect()?;

    fn split_root<E>(
        mut iter: impl Iterator<Item = Result<SnapshotInfo, E>>,
    ) -> Result<(HashSet<SnapshotId>, Option<SnapshotId>), E> {
        iter.try_fold((HashSet::new(), None), |(mut all, root), snap| match snap {
            Ok(snap) if snap.parent_id.is_some() => {
                all.insert(snap.id);
                Ok((all, root))
            }
            Ok(snap) => Ok((all, Some(snap.id))),
            Err(err) => Err(err),
        })
    }

    debug!("Finding roots");
    let mut all_tips = tags.iter().chain(branches.iter());
    let root_to_snaps = all_tips.try_fold(
        HashMap::new(),
        |mut res: HashMap<SnapshotId, HashSet<SnapshotId>>, (_, tip_snap)| {
            let ancestry = repo_info.ancestry(tip_snap)?;
            let (branch_snaps, root) = split_root(ancestry)?;
            let root = root.unwrap_or(Snapshot::INITIAL_SNAPSHOT_ID);
            match res.get_mut(&root) {
                Some(s) => {
                    s.extend(branch_snaps);
                }
                None => {
                    res.insert(root, branch_snaps);
                }
            };

            Ok::<_, GCError>(res)
        },
    )?;

    let new_parent = move |id: &SnapshotId| {
        for (new_parent, all) in root_to_snaps.iter() {
            if all.contains(id) {
                return Some(new_parent.clone());
            }
        }
        None
    };

    debug!("Finding ref tips");
    let tag_tip_ids: HashSet<SnapshotId> = repo_info.tags()?.map(|(_, id)| id).collect();
    let branch_tip_ids: HashSet<SnapshotId> =
        repo_info.branches()?.map(|(_, id)| id).collect();
    let main_pointee = repo_info.resolve_branch(Ref::DEFAULT_BRANCH)?;

    // All non-root snapshots old enough to be considered expired, regardless of
    // ref protection. Used to determine which branch/tag refs should be deleted.
    // Unlike released_snapshots, this does not exclude snapshots protected by
    // branch/tag tips (e.g. main), so a feature branch sharing main's tip can
    // still be deleted (#1520). Root snapshots (no parent) are always excluded
    // so tags/branches pointing to the initial commit are never deleted (#1534).
    let expired_snapshot_infos: Vec<SnapshotInfo> = repo_info
        .all_snapshots()?
        .filter_map(|si| match si {
            Ok(si) if si.flushed_at < older_than && si.parent_id.is_some() => {
                Some(Ok(si))
            }
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })
        .try_collect()?;

    debug!("Calculating released snapshots");
    let released_snapshots: HashSet<SnapshotId> = expired_snapshot_infos
        .iter()
        .filter_map(|si| {
            // we retain all roots
            if si.flushed_at < older_than && si.parent_id.is_some() {
                use ExpiredRefAction::*;
                if expired_tags == Ignore && tag_tip_ids.contains(&si.id)
                    || (expired_branches == Ignore || si.id == main_pointee)
                        && branch_tip_ids.contains(&si.id)
                {
                    None
                } else {
                    Some(si.id.clone())
                }
            } else {
                None
            }
        })
        .collect();

    let expired_snapshots: HashSet<SnapshotId> =
        expired_snapshot_infos.into_iter().map(|x| x.id).collect();

    let num_released_snapshots = released_snapshots.len();

    debug!("Calculating retained snapshots");
    let mut edited_snapshots = HashSet::new();
    let retained: Vec<_> = repo_info
        .all_snapshots()?
        .filter_map(|si| match si {
            // remove expired snapshots
            Ok(si) if released_snapshots.contains(&si.id) => None,

            // non expired snapshots could need editing to change their parent
            Ok(si) => match si.parent_id.as_ref() {
                Some(parent_id) => {
                    if released_snapshots.contains(parent_id) {
                        // parent is expired, so we change it to the root in that branch/tag
                        edited_snapshots.insert(si.id.clone());
                        // Re-parenting to the root drops every ancestor below
                        // si from its path. Those ancestors carry the tx logs
                        // describing the deltas si now spans (root..si), so
                        // harvest their ids into si.
                        match reparent_and_prune(&repo_info, &si, |a| {
                            // go all the way to the branch root
                            a.parent_id.is_none()
                        }) {
                            Ok((_, pruned_ancestor_tx_logs)) => Some(Ok(SnapshotInfo {
                                parent_id: Some(
                                    new_parent(&si.id)
                                        .unwrap_or(Snapshot::INITIAL_SNAPSHOT_ID),
                                ),
                                pruned_ancestor_tx_logs,
                                ..si
                            })),
                            Err(e) => Some(Err(e)),
                        }
                    } else {
                        // parent is retained, so we retain the snapshot as is
                        Some(Ok(si))
                    }
                }
                // we retain all roots
                None => Some(Ok(si)),
            },
            Err(e) => Some(Err(e)),
        })
        .try_collect()?;

    debug!("Calculating deleted refs");
    let mut deleted_tags: HashSet<_> = tags
        .into_iter()
        .filter_map(|(r, snap_id)| {
            if expired_tags == ExpiredRefAction::Delete
                && expired_snapshots.contains(&snap_id)
            {
                Some(r)
            } else {
                None
            }
        })
        .collect();

    let deleted_branches: HashSet<_> = branches
        .into_iter()
        .filter_map(|(r, snap_id)| {
            if expired_branches == ExpiredRefAction::Delete
                && r.name() != Ref::DEFAULT_BRANCH
                && expired_snapshots.contains(&snap_id)
            {
                Some(r)
            } else {
                None
            }
        })
        .collect();

    info!(
        snapshots = num_released_snapshots,
        branches = deleted_branches.iter().map(|r| r.name()).join("/"),
        tags = deleted_tags.iter().map(|r| r.name()).join("/"),
        "Releasing objects"
    );

    let do_update = |repo_info: Arc<RepoInfo>, backup_path: &str, version| {
        // we retry if the repo info object was modified since we started
        if version != repo_info_version_at_start {
            return Err(RepositoryError::capture(RepositoryErrorKind::RepoInfoUpdated));
        }

        let tags = repo_info
            .tags()
            .inject()?
            .filter(|(name, _)| !deleted_tags.contains(&Ref::Tag(name.to_string())));

        let branches = repo_info.branches().inject()?.filter(|(name, _)| {
            !deleted_branches.contains(&Ref::Branch(name.to_string()))
        });

        let deleted_tag_names = repo_info.deleted_tags().inject()?.chain(
            deleted_tags.iter().filter_map(|r| match r {
                Ref::Tag(name) => Some(name.as_str()),
                Ref::Branch(_) => None,
            }),
        );
        let config_bytes = repo_info.config_bytes_raw().inject()?;
        let new_repo_info = RepoInfo::new(
            asset_manager.spec_version(),
            tags,
            branches,
            deleted_tag_names,
            retained.clone(),
            &repo_info.metadata().inject()?,
            UpdateInfo {
                update_type: UpdateType::ExpirationRanUpdate,
                update_time: Utc::now(),
                previous_updates: repo_info.latest_updates().inject()?,
            },
            Some(backup_path),
            num_updates_per_repo_info_file,
            repo_info.repo_before_updates().inject()?,
            config_bytes.as_deref(),
            repo_info.enabled_feature_flags().inject()?,
            repo_info.disabled_feature_flags().inject()?,
            &repo_info.status().inject()?,
        )
        .inject()?;

        Ok(Arc::new(new_repo_info))
    };

    let retry_settings = storage::RetriesSettings {
        max_tries: Some(NonZeroU16::MIN),
        ..Default::default()
    };
    let _ = asset_manager.update_repo_info(&retry_settings, do_update).await?;

    deleted_tags.extend(deleted_branches);

    debug!("Expiration done");
    Ok(ExpireResult { released_snapshots, edited_snapshots, deleted_refs: deleted_tags })
}
