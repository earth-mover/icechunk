//! Upgrade repositories between Icechunk format versions.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use async_stream::try_stream;
use chrono::{DateTime, TimeDelta, Utc};
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};
use thiserror::Error;
use tokio::io::AsyncReadExt as _;
use tracing::{debug, error, info, warn};

/// Offset added to snapshot timestamps for synthetic branch/tag events.
const SYNTHETIC_EVENT_OFFSET: TimeDelta = TimeDelta::milliseconds(1);

use crate::{
    Repository,
    error::ICError,
    format::{
        CONFIG_FILE_PATH, IcechunkFormatErrorKind, REPO_INFO_FILE_PATH, SnapshotId,
        V1_REFS_FILE_PATH,
        format_constants::SpecVersionBin,
        repo_info::{RepoAvailability, RepoInfo, RepoStatus, UpdateInfo, UpdateType},
        snapshot::SnapshotInfo,
    },
    refs::{Ref, RefData, RefErrorKind, RefResult, list_deleted_tags, list_refs},
    repository::{RepositoryErrorKind, VersionInfo},
    storage::StorageErrorKind,
};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MigrationErrorKind {
    #[error(transparent)]
    RepositoryError(#[from] RepositoryErrorKind),

    #[error(transparent)]
    RefError(#[from] RefErrorKind),

    #[error(transparent)]
    FormatError(#[from] IcechunkFormatErrorKind),

    #[error(transparent)]
    StorageError(#[from] StorageErrorKind),

    #[error(
        "invalid repository version ({actual}), this function can only migrate repositories that use version {expected} of the specification, to repositories with spec version {target}"
    )]
    InvalidRepositoryMigration {
        expected: SpecVersionBin,
        target: SpecVersionBin,
        actual: SpecVersionBin,
    },

    #[error(
        "the storage instance used to initialize this repo is read-only, the migration process needs to make changes in object store"
    )]
    ReadonlyRepo,

    #[error(
        "An unknown error prevented this repo from migrating, please contact the Icechunk maintainers by writing an issue report: https://github.com/earth-mover/icechunk/issues"
    )]
    Unknown,
}

pub type MigrationError = ICError<MigrationErrorKind>;
pub type MigrationResult<A> = Result<A, MigrationError>;

async fn validate_start(repo: &Repository) -> MigrationResult<()> {
    if repo.spec_version() != SpecVersionBin::V1 {
        error!("Target repository must be a 1.X Icechunk repository");
        return Err(MigrationErrorKind::InvalidRepositoryMigration {
            expected: SpecVersionBin::V1,
            target: SpecVersionBin::V2,
            actual: repo.spec_version(),
        })
        .capture();
    }
    if !repo.storage().can_write().await.inject()? {
        error!("Storage instance must be writable");
        return Err(MigrationError::capture(MigrationErrorKind::ReadonlyRepo));
    }
    Ok(())
}

/// Read a deleted tag's snapshot ID directly from V1 storage,
/// bypassing the delete marker check in `fetch_tag`.
async fn fetch_deleted_tag_snapshot_id(
    repo: &Repository,
    tag_name: &str,
) -> Option<SnapshotId> {
    let ref_path = format!("{V1_REFS_FILE_PATH}/tag.{tag_name}/ref.json");
    match repo.storage().get_object(repo.storage_settings(), &ref_path, None).await {
        Ok((mut reader, ..)) => {
            let mut data = Vec::with_capacity(40);
            if reader.read_to_end(&mut data).await.is_err() {
                return None;
            }
            let ref_data: RefData = serde_json::from_slice(&data).ok()?;
            Some(ref_data.snapshot)
        }
        Err(_) => None,
    }
}

/// Generate a synthetic ops log from V1 repository data.
///
/// Reconstructs a plausible history of operations from the snapshot graph,
/// branch/tag state. Uses snapshot `flushed_at` timestamps for commit events.
/// Tag/branch creation events use the `flushed_at` of the snapshot they point to.
fn generate_migration_ops_log(
    main_ancestry: &[SnapshotInfo],
    branches: &[(&str, Vec<SnapshotInfo>)],
    tags: &[(&str, SnapshotId)],
    deleted_tags: &[(&str, SnapshotId)],
    all_snapshots: &[SnapshotInfo],
) -> Vec<(UpdateType, DateTime<Utc>)> {
    if main_ancestry.is_empty() {
        return Vec::new();
    }

    // Build a lookup from snapshot ID to its flushed_at timestamp.
    // We use all_snapshots because tags can point to orphaned snapshots
    // not reachable from any branch ancestry.
    let snap_times: HashMap<&SnapshotId, DateTime<Utc>> =
        all_snapshots.iter().map(|s| (&s.id, s.flushed_at)).collect();

    // main_ancestry is tip-to-root, so last element is the root
    let root = &main_ancestry[main_ancestry.len() - 1];
    let root_time = root.flushed_at;

    // Track which snapshots already have a NewCommitUpdate to avoid duplicates
    // when branches share non-main ancestry (e.g. branch-b forked from branch-a).
    let mut emitted_snap_ids: HashSet<_> =
        main_ancestry.iter().map(|s| s.id.clone()).collect();

    // Upper bound: 1 RepoInitializedUpdate + at most 1 NewCommitUpdate per snapshot
    // + 1 BranchCreatedUpdate per branch + 1 TagCreatedUpdate per tag
    // + 2 per deleted tag (TagCreatedUpdate + TagDeletedUpdate)
    let capacity =
        1 + all_snapshots.len() + branches.len() + tags.len() + 2 * deleted_tags.len();
    let mut entries: Vec<(UpdateType, DateTime<Utc>)> = Vec::with_capacity(capacity);

    // Start with the RepoInitializedUpdate at root snapshot's flushed_at
    entries.push((UpdateType::RepoInitializedUpdate, root_time));

    // Then we add a NewCommitUpdate for each non-root main snapshot (root-to-tip order)
    for snap in main_ancestry.iter().rev().skip(1) {
        entries.push((
            UpdateType::NewCommitUpdate {
                branch: Ref::DEFAULT_BRANCH.to_string(),
                new_snap_id: snap.id.clone(),
            },
            snap.flushed_at,
        ));
    }

    // BranchCreatedUpdate + NewCommitUpdate for each unique snapshot in the ancestry
    // of each branch. Snapshots shared across branches are attributed to the first
    // branch processed.
    for (name, ancestry) in branches {
        // BranchCreatedUpdate is 1ms after the branch tip snapshot
        let branch_tip_time = ancestry.first().map(|s| s.flushed_at).unwrap_or(root_time);
        entries.push((
            UpdateType::BranchCreatedUpdate { name: name.to_string() },
            branch_tip_time + SYNTHETIC_EVENT_OFFSET,
        ));

        for snap in ancestry.iter().rev() {
            if emitted_snap_ids.insert(snap.id.clone()) {
                entries.push((
                    UpdateType::NewCommitUpdate {
                        branch: name.to_string(),
                        new_snap_id: snap.id.clone(),
                    },
                    snap.flushed_at,
                ));
            }
        }
    }

    // TagCreatedUpdate is 1ms after the snapshot it points to
    for (name, snap_id) in tags {
        let time = snap_times.get(snap_id).copied().unwrap_or(root_time);
        entries.push((
            UpdateType::TagCreatedUpdate { name: name.to_string() },
            time + SYNTHETIC_EVENT_OFFSET,
        ));
    }

    // Deleted tags: TagCreatedUpdate 1ms after snapshot, TagDeletedUpdate 2ms after
    for (name, snap_id) in deleted_tags {
        let time = snap_times.get(snap_id).copied().unwrap_or(root_time);
        entries.push((
            UpdateType::TagCreatedUpdate { name: name.to_string() },
            time + SYNTHETIC_EVENT_OFFSET,
        ));
        entries.push((
            UpdateType::TagDeletedUpdate {
                name: name.to_string(),
                previous_snap_id: snap_id.clone(),
            },
            time + SYNTHETIC_EVENT_OFFSET + SYNTHETIC_EVENT_OFFSET,
        ));
    }

    // Sort chronologically (oldest first)
    entries.sort_by_key(|(_, time)| *time);

    // Ensure strictly increasing timestamps: if an entry collides with or is
    // before the previous one, bump it by 1μs.  This mirrors the real ops log
    // where every operation gets a unique timestamp.
    for i in 1..entries.len() {
        let prev_time = entries[i - 1].1;
        if entries[i].1 <= prev_time {
            entries[i].1 = prev_time + TimeDelta::microseconds(1);
        }
    }

    // Reverse for newest-first order
    entries.reverse();
    entries
}

async fn do_migrate(
    repo: &Repository,
    repo_info: Arc<RepoInfo>,
    start_time: Instant,
    delete_unused_v1_files: bool,
) -> MigrationResult<()> {
    info!("Writing new repository info file");
    let new_asset_manager =
        repo.asset_manager().clone_for_spec_version(SpecVersionBin::V2);
    let new_version_info =
        new_asset_manager.create_repo_info(repo_info).await.inject()?;

    info!(version=?new_version_info, "Written repository info file");

    info!("Opening migrated repo");
    let Ok(migrated) = Repository::open(
        Some(repo.config().clone()),
        Arc::clone(repo.storage()),
        Default::default(),
    )
    .await
    else {
        error!("Unknown error during migration. Repository doesn't open");
        delete_repo_info(repo).await?;
        error!("Migration failed");
        return Err(MigrationError::capture(MigrationErrorKind::Unknown));
    };

    let new_spec_version = migrated.spec_version();
    if new_spec_version != SpecVersionBin::V2 {
        error!("Unknown error during migration. Repository doesn't open as 2.0");
        delete_repo_info(repo).await?;
        error!("Migration failed");
        return Err(MigrationError::capture(MigrationErrorKind::Unknown));
    }
    if delete_unused_v1_files {
        if let Err(err) = delete_v1_refs(repo).await {
            delete_repo_info(repo).await?;
            error!("Migration failed");
            return Err(err);
        }
        info!("Opening migrated repo");
        let Ok(migrated) = Repository::open(
            Some(repo.config().clone()),
            Arc::clone(repo.storage()),
            Default::default(),
        )
        .await
        else {
            error!("Unknown error during migration. Repository doesn't open");
            delete_repo_info(repo).await?;
            error!("Migration failed");
            return Err(MigrationError::capture(MigrationErrorKind::Unknown));
        };

        let new_spec_version = migrated.spec_version();
        if new_spec_version != SpecVersionBin::V2 {
            error!("Unknown error during migration. Repository doesn't open as 2.0");
            delete_repo_info(repo).await?;
            error!("Migration failed");
            return Err(MigrationError::capture(MigrationErrorKind::Unknown));
        }

        // Config is now embedded in the repo info, so the V1 config.yaml is no longer needed.
        // This is best-effort — a failure here doesn't invalidate the migration.
        if let Err(err) = delete_config_yaml(repo).await {
            warn!("Failed to delete V1 config.yaml: {err}, continuing anyway");
        }
    }

    info!(
        "Migration completed in {} seconds, you can use the repository now",
        start_time.elapsed().as_secs()
    );
    Ok(())
}

pub async fn migrate_1_to_2(
    repo: Repository,
    dry_run: bool,
    delete_unused_v1_files: bool,
    prefetch_concurrency: Option<usize>,
) -> MigrationResult<()> {
    let prefetch_concurrency = prefetch_concurrency.unwrap_or(64);
    let start_time = Instant::now();
    validate_start(&repo).await?;

    info!("Starting migration");
    info!("Collecting refs");
    let refs = all_roots(&repo).await.inject()?.try_collect::<Vec<_>>().await.inject()?;
    let tags = Vec::from_iter(refs.iter().filter_map(|(r, id)| {
        if r.is_tag() { Some((r.name(), id.clone())) } else { None }
    }));
    let branches = Vec::from_iter(refs.iter().filter_map(|(r, id)| {
        if r.is_branch() { Some((r.name(), id.clone())) } else { None }
    }));

    let deleted_tags =
        list_deleted_tags(repo.storage().as_ref(), repo.storage_settings())
            .await
            .inject()?;

    info!(
        "Found {} refs: {} tags, {} branches, {} deleted tags",
        refs.len(),
        tags.len(),
        branches.len(),
        deleted_tags.len()
    );
    let deleted_tag_names: Vec<&str> = deleted_tags
        .iter()
        .filter_map(|s| {
            s.as_str()
                .strip_prefix("tag.")
                .and_then(|s| s.strip_suffix("/ref.json.deleted"))
        })
        .collect();

    info!("Collecting non-dangling snapshots, this may take a few minutes");

    // Prefetch snapshots concurrently to warm the asset manager cache.
    // We list all snapshot objects, sort by creation time (latest first),
    // and fetch them N-at-a-time in the background while the ancestry
    // walk proceeds. This turns what would be sequential network
    // round-trips into mostly cache hits.
    let asset_manager = Arc::clone(repo.asset_manager());
    let prefetch_handle = {
        let am = Arc::clone(&asset_manager);
        tokio::spawn(async move {
            let concurrency = prefetch_concurrency;
            match am.list_snapshots().await {
                Ok(snapshot_list) => {
                    let mut snap_infos: Vec<_> = match snapshot_list
                        .try_collect::<Vec<_>>()
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(
                                "Snapshot prefetch: failed to collect snapshot list: {e}"
                            );
                            return;
                        }
                    };
                    snap_infos.sort_by(|a, b| b.created_at.cmp(&a.created_at));
                    info!(
                        "Snapshot prefetch: warming cache for {} snapshots with concurrency {}",
                        snap_infos.len(),
                        concurrency,
                    );
                    let fetches = stream::iter(snap_infos.into_iter().map(|info| {
                        let am = Arc::clone(&am);
                        async move {
                            if let Err(e) = am.fetch_snapshot(&info.id).await {
                                debug!(
                                    "Snapshot prefetch: failed to fetch {}: {e}",
                                    info.id
                                );
                            }
                        }
                    }))
                    .buffer_unordered(concurrency)
                    .count()
                    .await;
                    info!("Snapshot prefetch: completed {fetches} fetches");
                }
                Err(e) => {
                    warn!("Snapshot prefetch: failed to list snapshots: {e}");
                }
            }
        })
    };

    let snap_ids = refs.iter().map(|(_, id)| id);
    let all_snapshots =
        pointed_snapshots(&repo, snap_ids).await?.try_collect::<Vec<_>>().await?;

    // Cancel the prefetch if it's still running — we have all the snapshots we need.
    prefetch_handle.abort();

    info!("Found {} non-dangling snapshots", all_snapshots.len());

    info!("Generating migration ops log");
    // Build a lookup so we can reconstruct per-branch ancestry in memory
    // instead of re-fetching from storage.
    let snap_by_id: HashMap<&SnapshotId, &SnapshotInfo> =
        all_snapshots.iter().map(|s| (&s.id, s)).collect();

    // Walk parent pointers from `tip` to root, returning tip-to-root order.
    let ancestry_from = |tip: &SnapshotId| -> Vec<SnapshotInfo> {
        let mut result = Vec::new();
        let mut current = snap_by_id.get(tip);
        while let Some(snap) = current {
            result.push((*snap).clone());
            current = snap.parent_id.as_ref().and_then(|pid| snap_by_id.get(pid));
        }
        result
    };

    // Collect main branch ancestry (tip-to-root)
    let main_snap_id =
        branches.iter().find(|(name, _)| *name == Ref::DEFAULT_BRANCH).map(|(_, id)| id);
    let main_ancestry = main_snap_id.map(&ancestry_from).unwrap_or_default();

    // Collect non-main branch ancestries
    let branch_ancestries: Vec<(&str, Vec<SnapshotInfo>)> = branches
        .iter()
        .filter(|(name, _)| *name != Ref::DEFAULT_BRANCH)
        .map(|(name, snap_id)| (*name, ancestry_from(snap_id)))
        .collect();
    // Fetch snapshot IDs for deleted tags
    let mut deleted_tags_with_snap: Vec<(&str, SnapshotId)> = Vec::new();
    for name in &deleted_tag_names {
        if let Some(snap_id) = fetch_deleted_tag_snapshot_id(&repo, name).await {
            deleted_tags_with_snap.push((name, snap_id));
        } else {
            warn!(
                "Could not fetch snapshot ID for deleted tag '{name}', skipping ops log entry"
            );
        }
    }
    let ops_log = generate_migration_ops_log(
        &main_ancestry,
        &branch_ancestries,
        &tags,
        &deleted_tags_with_snap,
        &all_snapshots,
    );
    info!("Generated {} ops log entries", ops_log.len());

    let previous_updates: Vec<crate::format::IcechunkResult<_>> =
        ops_log.into_iter().map(|(ut, dt)| Ok((ut, dt, None))).collect();

    // Use a page size large enough to hold all synthetic entries (+1 for RepoMigratedUpdate)
    // so nothing is truncated. There are no backup files to chain to, so overflow would
    // silently drop entries. Subsequent operations will use the configured page size.
    let migration_page_size = (previous_updates.len() as u16 + 1)
        .max(repo.config().num_updates_per_repo_info_file());

    info!("Creating repository info file");
    // Read the original V1 config.yaml instead of using repo.config(), which
    // is the fully-merged runtime config. Persisting the runtime config would
    // bake instance-local settings (caching, storage) from the migration
    // caller into the V2 repo, overriding every future client's defaults.
    let persisted_config = repo.asset_manager().fetch_config().await.inject()?;
    let config_bytes: Option<Vec<u8>> = match persisted_config {
        Some((config, _)) => Some(
            flexbuffers::to_vec(&config)
                .map_err(|e| {
                    IcechunkFormatErrorKind::SerializationErrorFlexBuffers(Box::new(e))
                })
                .capture()?,
        ),
        None => None,
    };

    // main_ancestry is tip-to-root, so last element is the root
    let root = &main_ancestry[main_ancestry.len() - 1];
    let root_time = root.flushed_at;

    let repo_info = Arc::new(
        RepoInfo::new(
            SpecVersionBin::V2,
            tags,
            branches,
            deleted_tag_names.iter().copied(),
            all_snapshots,
            &Default::default(),
            UpdateInfo {
                update_type: UpdateType::RepoMigratedUpdate {
                    from_version: SpecVersionBin::V1,
                    to_version: SpecVersionBin::V2,
                },
                update_time: Utc::now(),
                previous_updates,
            },
            None,
            migration_page_size,
            None,
            config_bytes.as_deref(),
            None::<std::iter::Empty<u16>>,
            None::<std::iter::Empty<u16>>,
            &RepoStatus {
                availability: RepoAvailability::Online,
                set_at: root_time,
                limited_availability_reason: None,
            },
        )
        .inject()?,
    );

    if dry_run {
        info!(
            "Migration dry-run completed in {} seconds, your repository wasn't modified, run with `dry_run=False` to actually migrate",
            start_time.elapsed().as_secs()
        );
        Ok(())
    } else {
        do_migrate(&repo, repo_info, start_time, delete_unused_v1_files).await
    }
}

async fn delete_repo_info(repo: &Repository) -> MigrationResult<()> {
    warn!("Deleting generated repo info file");
    repo.storage()
        .delete_objects(
            repo.storage_settings(),
            "",
            stream::iter([(REPO_INFO_FILE_PATH.to_string(), 0)]).boxed(),
        )
        .await
        .inject()?;
    Ok(())
}

const V1_DEFAULT_BRANCH_KEY: &str = "branch.main/ref.json";

/// Delete V1 references during migration.
///
/// We delete the `main` branch first to ensure old IC1 clients fail fast
/// with a "branch not found" error instead of potentially making writes
/// that would be lost when the rest of the V1 refs are cleaned up.
/// This minimizes the window where the repo appears as both valid V1 and V2.
async fn delete_v1_refs(repo: &Repository) -> MigrationResult<()> {
    info!("Deleting V1 references");

    // Delete the main branch first to break IC1 clients immediately
    repo.storage()
        .delete_objects(
            repo.storage_settings(),
            V1_REFS_FILE_PATH,
            stream::iter([(V1_DEFAULT_BRANCH_KEY.to_string(), 0)]).boxed(),
        )
        .await
        .inject()?;
    info!("V1 main branch reference deleted");

    // Then delete remaining V1 refs, as long as main is gone the repo is broken for v1 usage
    let all = repo
        .storage()
        .list_objects(repo.storage_settings(), V1_REFS_FILE_PATH)
        .await
        .inject()?;
    let delete_keys =
        all.map_ok(|li| (li.id, 0)).boxed().try_collect::<Vec<_>>().await.inject()?;

    if !delete_keys.is_empty() {
        repo.storage()
            .delete_objects(
                repo.storage_settings(),
                V1_REFS_FILE_PATH,
                stream::iter(delete_keys).boxed(),
            )
            .await
            .inject()?;
    }

    info!("All V1 references deleted, verifying");
    let remaining = repo
        .storage()
        .list_objects(repo.storage_settings(), V1_REFS_FILE_PATH)
        .await
        .inject()?
        .try_collect::<Vec<_>>()
        .await
        .inject()?;
    if remaining.is_empty() {
        info!("All V1 references have been deleted");
        Ok(())
    } else {
        error!(
            "Found {} remaining V1 references that couldn't be deleted",
            remaining.len()
        );
        Err(MigrationError::capture(MigrationErrorKind::Unknown))
    }
}

async fn delete_config_yaml(repo: &Repository) -> MigrationResult<()> {
    info!("Deleting V1 config.yaml");
    repo.storage()
        .delete_objects(
            repo.storage_settings(),
            "",
            stream::iter([(CONFIG_FILE_PATH.to_string(), 0)]).boxed(),
        )
        .await
        .inject()?;
    info!("V1 config.yaml deleted");
    Ok(())
}

/// Function copied from IC 1.0 with some changes
async fn all_roots<'a>(
    repo: &'a Repository,
) -> RefResult<impl Stream<Item = RefResult<(Ref, SnapshotId)>> + 'a> {
    let all_refs = list_refs(repo.storage().as_ref(), repo.storage_settings()).await?;
    let roots = stream::iter(all_refs).then(move |r| async move {
        r.fetch(repo.storage().as_ref(), repo.storage_settings())
            .await
            .map(|ref_data| (r, ref_data.snapshot))
    });
    Ok(roots)
}

/// Function copied from IC 1.0 with some changes
async fn pointed_snapshots<'a>(
    repo: &'a Repository,
    leaves: impl Iterator<Item = &SnapshotId> + 'a,
) -> MigrationResult<impl Stream<Item = MigrationResult<SnapshotInfo>> + 'a> {
    let mut seen: HashSet<SnapshotId> = HashSet::new();
    let res = try_stream! {

        for pointed_snap_id in leaves {
            if ! seen.contains(pointed_snap_id) {
                let parents = repo.ancestry(&VersionInfo::SnapshotId(pointed_snap_id.clone())).await.inject()?;
                //let parents = Arc::clone(&asset_manager).snapshot_ancestry(&pointed_snap_id).await?;
                for await parent in parents {
                    let parent = parent.inject()?;
                    if seen.insert(parent.id.clone()) {
                        debug!("Found snapshot {}", parent.id);
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

#[cfg(all(test, feature = "object-store-fs"))]
#[expect(clippy::expect_used)]
mod tests {
    use std::{collections::HashMap, path::Path};

    use icechunk_macros::tokio_test;
    use tempfile::{TempDir, tempdir};

    use futures::TryStreamExt as _;

    use crate::{RepositoryConfig, new_local_filesystem_storage, refs};

    use super::*;

    async fn prepare_v1_repo() -> Result<(Repository, TempDir), Box<dyn std::error::Error>>
    {
        let dir = tempdir().expect("cannot create temp dir");
        let source_path = Path::new("../icechunk-python/tests/data/test-repo-v1");
        fs_extra::copy_items(&[source_path], &dir, &Default::default())?;
        let storage =
            new_local_filesystem_storage(dir.path().join("test-repo-v1").as_path())
                .await?;
        let repo =
            Repository::open(None, Arc::clone(&storage), Default::default()).await?;
        Ok((repo, dir))
    }

    #[tokio_test]
    /// Copy the source tree 1.0 repository to a temp dir, then migrate it
    async fn test_1_to_2_migration() -> Result<(), Box<dyn std::error::Error>> {
        use crate::config::CachingConfig;

        let (repo, _tmp) = prepare_v1_repo().await?;
        let storage = Arc::clone(repo.storage());

        // Persist a custom caching config to the V1 repo's config.yaml
        let repo = Repository::open(
            Some(RepositoryConfig {
                caching: Some(CachingConfig {
                    num_chunk_refs: Some(10_000),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            Arc::clone(&storage),
            Default::default(),
        )
        .await?;
        repo.save_config().await?;

        // Re-open with a *different* caching value to simulate a migration
        // client that tuned cache sizes for the migration workload.
        let repo = Repository::open(
            Some(RepositoryConfig {
                caching: Some(CachingConfig {
                    num_chunk_refs: Some(99),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            Arc::clone(&storage),
            Default::default(),
        )
        .await?;

        let mut tag_ancestries_before = HashMap::new();
        for tag in repo.list_tags().await? {
            let anc = repo
                .ancestry(&VersionInfo::TagRef(tag.clone()))
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            tag_ancestries_before.insert(tag, anc);
        }

        let mut branch_ancestries_before = HashMap::new();
        for branch in repo.list_branches().await? {
            let anc = repo
                .ancestry(&VersionInfo::BranchTipRef(branch.clone()))
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            branch_ancestries_before.insert(branch, anc);
        }

        migrate_1_to_2(repo, false, true, None).await.unwrap();
        let repo = Repository::open(None, storage, Default::default()).await?;

        let mut tag_ancestries_after = HashMap::new();
        for tag in repo.list_tags().await? {
            let anc = repo
                .ancestry(&VersionInfo::TagRef(tag.clone()))
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            tag_ancestries_after.insert(tag, anc);
        }

        let mut branch_ancestries_after = HashMap::new();
        for branch in repo.list_branches().await? {
            let anc = repo
                .ancestry(&VersionInfo::BranchTipRef(branch.clone()))
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            branch_ancestries_after.insert(branch, anc);
        }

        assert_eq!(tag_ancestries_before, tag_ancestries_after);
        assert_eq!(branch_ancestries_before, branch_ancestries_after);

        // Verify deleted tag name is preserved (not the full V1 path)
        let (info, _) = repo.asset_manager().fetch_repo_info().await?;
        let deleted_tags: Vec<_> = info.deleted_tags()?.collect();
        assert_eq!(deleted_tags, vec!["deleted"]);

        // Verify the generated ops log
        let (ops_log_stream, _, _) = repo.ops_log().await?;
        let ops_log: Vec<_> = ops_log_stream.try_collect().await?;

        // Newest entry should be RepoMigratedUpdate
        let (_, ref first_update, _) = ops_log[0];
        assert_eq!(
            *first_update,
            UpdateType::RepoMigratedUpdate {
                from_version: SpecVersionBin::V1,
                to_version: SpecVersionBin::V2,
            }
        );

        // Should contain RepoInitializedUpdate
        assert!(
            ops_log.iter().any(|(_, ut, _)| *ut == UpdateType::RepoInitializedUpdate),
            "ops log should contain RepoInitializedUpdate"
        );

        // Count NewCommitUpdate entries (one per non-root snapshot = 5)
        let commit_count = ops_log
            .iter()
            .filter(|(_, ut, _)| matches!(ut, UpdateType::NewCommitUpdate { .. }))
            .count();
        assert_eq!(
            commit_count, 5,
            "should have 5 NewCommitUpdate entries (6 snapshots - 1 root)"
        );

        // Should contain BranchCreatedUpdate for "my-branch"
        assert!(
            ops_log.iter().any(|(_, ut, _)| *ut
                == UpdateType::BranchCreatedUpdate { name: "my-branch".to_string() }),
            "ops log should contain BranchCreatedUpdate for my-branch"
        );

        // Should contain TagCreatedUpdate for each tag (including the one that was deleted)
        for tag_name in ["it works!", "it also works!", "deleted"] {
            assert!(
                ops_log.iter().any(|(_, ut, _)| *ut
                    == UpdateType::TagCreatedUpdate { name: tag_name.to_string() }),
                "ops log should contain TagCreatedUpdate for '{tag_name}'"
            );
        }

        // Verify ordering for the "deleted" tag:
        // TagDeletedUpdate must come after (newer = lower index) TagCreatedUpdate,
        // and the NewCommitUpdate for the pointed snapshot must come before (older = higher index) both.
        // ops_log is newest-first, so lower index = more recent.
        let deleted_tag_created_idx = ops_log
            .iter()
            .position(|(_, ut, _)| {
                *ut == UpdateType::TagCreatedUpdate { name: "deleted".to_string() }
            })
            .expect("TagCreatedUpdate for 'deleted' not found");
        let deleted_tag_deleted_idx = ops_log
            .iter()
            .position(|(_, ut, _)| {
                matches!(ut, UpdateType::TagDeletedUpdate { name, .. } if name == "deleted")
            })
            .expect("TagDeletedUpdate for 'deleted' not found");
        // Find the snapshot ID from the TagDeletedUpdate
        let deleted_tag_snap_id = ops_log
            .iter()
            .find_map(|(_, ut, _)| {
                if let UpdateType::TagDeletedUpdate { name, previous_snap_id } = ut
                    && name == "deleted"
                {
                    return Some(previous_snap_id.clone());
                }
                None
            })
            .expect("TagDeletedUpdate for 'deleted' not found");
        let commit_idx = ops_log
            .iter()
            .position(|(_, ut, _)| {
                matches!(ut, UpdateType::NewCommitUpdate { new_snap_id, .. } if *new_snap_id == deleted_tag_snap_id)
            })
            .expect("NewCommitUpdate for the deleted tag's snapshot not found");

        assert!(
            deleted_tag_deleted_idx < deleted_tag_created_idx,
            "TagDeletedUpdate (idx {deleted_tag_deleted_idx}) should be newer than TagCreatedUpdate (idx {deleted_tag_created_idx})"
        );
        assert!(
            deleted_tag_created_idx < commit_idx,
            "TagCreatedUpdate (idx {deleted_tag_created_idx}) should be newer than NewCommitUpdate (idx {commit_idx})"
        );

        // Verify timestamps are strictly decreasing (newest first)
        for window in ops_log.windows(2) {
            let (time_a, _, _) = &window[0];
            let (time_b, _, _) = &window[1];
            assert!(
                time_a > time_b,
                "ops log timestamps must be strictly decreasing: {time_a} should be > {time_b}"
            );
        }

        // Verify the persisted V1 caching config (num_chunk_refs=10_000) survived
        // migration, and the migration caller's runtime value (99) did NOT leak.
        assert_eq!(
            repo.config().caching().num_chunk_refs(),
            10_000,
            "persisted caching config should survive migration unchanged"
        );

        Ok(())
    }

    #[tokio_test]
    /// Migrating an already-V2 repo should return `InvalidRepositoryMigration` (issue #1524)
    async fn test_1_to_2_migration_already_v2() -> Result<(), Box<dyn std::error::Error>>
    {
        let (repo, _tmp) = prepare_v1_repo().await?;
        let storage = Arc::clone(repo.storage());

        migrate_1_to_2(repo, false, true, None).await.unwrap();

        // Reopen the now-V2 repo and try to migrate again
        let repo = Repository::open(None, storage, Default::default()).await?;
        let result = migrate_1_to_2(repo, false, true, None).await;
        assert!(result.is_err(), "migrating an already-V2 repo should return an error");

        Ok(())
    }

    #[tokio_test]
    /// Copy the source tree 1.0 repository to a temp dir, then migrate it in dry-run mode
    async fn test_1_to_2_migration_dry_run() -> Result<(), Box<dyn std::error::Error>> {
        let (repo, _tmp) = prepare_v1_repo().await?;
        let storage = Arc::clone(repo.storage());

        migrate_1_to_2(repo, true, true, None).await.unwrap();
        let repo = Repository::open(None, storage, Default::default()).await?;

        assert_eq!(repo.spec_version(), SpecVersionBin::V1);
        Ok(())
    }

    #[tokio_test]
    /// Copy the source tree 1.0 repository to a temp dir, then migrate it without deleting v1 files
    async fn test_1_to_2_migration_without_delete()
    -> Result<(), Box<dyn std::error::Error>> {
        let (repo, _tmp) = prepare_v1_repo().await?;
        let storage = Arc::clone(repo.storage());

        migrate_1_to_2(repo, false, false, None).await.unwrap();
        let repo = Repository::open(None, storage, Default::default()).await?;

        assert_eq!(repo.spec_version(), SpecVersionBin::V2);

        assert_eq!(
            refs::list_branches(repo.storage().as_ref(), repo.storage_settings()).await?,
            ["main".to_string(), "my-branch".to_string()].into()
        );
        Ok(())
    }

    #[tokio_test]
    /// Verify the ops log chain isn't broken when post-migration writes overflow
    /// synthetic (`backup_path=None`) entries.
    async fn test_ops_log_chain_after_migration() -> Result<(), Box<dyn std::error::Error>>
    {
        let (repo, _tmp) = prepare_v1_repo().await?;
        let storage = Arc::clone(repo.storage());

        migrate_1_to_2(repo, false, true, None).await.unwrap();

        // Reopen with a very small num_updates_per_file to force overflow
        // of synthetic migration entries immediately.
        let config = RepositoryConfig {
            num_updates_per_repo_info_file: Some(3),
            ..Default::default()
        };
        let repo =
            Repository::open(Some(config), Arc::clone(&storage), Default::default())
                .await?;

        // Record initial ops log length after migration
        let (stream, _, _) = repo.ops_log().await?;
        let ops: Vec<_> = stream.try_collect().await?;
        let mut expected_len = ops.len();

        // The last entry must be RepoInitializedUpdate
        assert!(
            matches!(ops.last().unwrap().1, UpdateType::RepoInitializedUpdate),
            "ops log chain broken after migration: last entry is not RepoInitializedUpdate"
        );

        let snap_id = repo.lookup_branch("main").await?;

        for i in 0..5 {
            repo.create_tag(&format!("post-migration-tag-{i}"), &snap_id).await?;
            expected_len += 1;

            let (stream, _, _) = repo.ops_log().await?;
            let ops: Vec<_> = stream.try_collect().await?;

            assert_eq!(
                ops.len(),
                expected_len,
                "ops log length mismatch on iteration {i}"
            );

            assert!(
                matches!(ops.last().unwrap().1, UpdateType::RepoInitializedUpdate),
                "ops log chain broken on iteration {i}: last entry is not RepoInitializedUpdate"
            );

            // Verify timestamps are strictly decreasing
            for window in ops.windows(2) {
                let (time_a, _, _) = &window[0];
                let (time_b, _, _) = &window[1];
                assert!(
                    time_a > time_b,
                    "ops log timestamps must be strictly decreasing: {time_a} should be > {time_b}"
                );
            }
        }

        Ok(())
    }
}
