//! Upgrade repositories between Icechunk format versions.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use async_stream::try_stream;
use chrono::{DateTime, TimeDelta, Utc};
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use thiserror::Error;
use tokio::io::AsyncReadExt as _;
use tracing::{debug, error, info, warn};

use crate::{
    Repository, StorageError,
    error::ICError,
    format::{
        CONFIG_FILE_PATH, IcechunkFormatError, IcechunkFormatErrorKind,
        REPO_INFO_FILE_PATH, SnapshotId, V1_REFS_FILE_PATH,
        format_constants::SpecVersionBin,
        repo_info::{RepoInfo, UpdateInfo, UpdateType},
        snapshot::SnapshotInfo,
    },
    refs::{
        Ref, RefData, RefError, RefErrorKind, RefResult, list_deleted_tags, list_refs,
    },
    repository::{RepositoryError, RepositoryErrorKind, VersionInfo},
    storage::StorageErrorKind,
};

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MigrationErrorKind {
    #[error(transparent)]
    RepositoryError(RepositoryErrorKind),

    #[error(transparent)]
    RefError(RefErrorKind),

    #[error(transparent)]
    FormatError(IcechunkFormatErrorKind),

    #[error(transparent)]
    StorageError(StorageErrorKind),

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

// it would be great to define this impl in error.rs, but it conflicts with the blanket
// `impl From<T> for T`
impl<E> From<E> for MigrationError
where
    E: Into<MigrationErrorKind>,
{
    fn from(value: E) -> Self {
        Self::new(value.into())
    }
}

impl From<RepositoryError> for MigrationError {
    fn from(value: RepositoryError) -> Self {
        Self::with_context(MigrationErrorKind::RepositoryError(value.kind), value.context)
    }
}

impl From<RefError> for MigrationError {
    fn from(value: RefError) -> Self {
        Self::with_context(MigrationErrorKind::RefError(value.kind), value.context)
    }
}

impl From<IcechunkFormatError> for MigrationError {
    fn from(value: IcechunkFormatError) -> Self {
        Self::with_context(MigrationErrorKind::FormatError(value.kind), value.context)
    }
}

impl From<StorageError> for MigrationError {
    fn from(value: StorageError) -> Self {
        Self::with_context(MigrationErrorKind::StorageError(value.kind), value.context)
    }
}

async fn validate_start(repo: &Repository) -> MigrationResult<()> {
    if repo.spec_version() != SpecVersionBin::V1dot0 {
        error!("Target repository must be a 1.X Icechunk repository");
        return Err(MigrationErrorKind::InvalidRepositoryMigration {
            expected: SpecVersionBin::V1dot0,
            target: SpecVersionBin::V2dot0,
            actual: repo.spec_version(),
        }
        .into());
    }
    if !repo.storage().can_write().await? {
        error!("Storage instance must be writable");
        return Err(MigrationErrorKind::ReadonlyRepo.into());
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
            let mut data = Vec::with_capacity(1_024);
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

    let mut entries: Vec<(UpdateType, DateTime<Utc>)> = Vec::new();

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

    // Followed by a BranchCreatedUpdate + NewCommitUpdate for each unique snapshot
    // in the ancestry of each branch. BranchCreatedUpdate is timestamped to the
    // root snapshot in the branch's ancestry (the first snapshot).
    // Snapshots shared across branches are attributed to the first branch processed.
    for (name, ancestry) in branches {
        let branch_root_time = ancestry.last().map(|s| s.flushed_at).unwrap_or(root_time);
        entries.push((
            UpdateType::BranchCreatedUpdate { name: name.to_string() },
            branch_root_time,
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

    // Each tag gets a TagCreatedUpdate at the flushed_at of the snapshot it points to
    for (name, snap_id) in tags {
        let time = snap_times.get(snap_id).copied().unwrap_or(root_time);
        entries.push((UpdateType::TagCreatedUpdate { name: name.to_string() }, time));
    }

    // Deleted tags: TagCreatedUpdate + TagDeletedUpdate, both at the pointed snapshot's time
    for (name, snap_id) in deleted_tags {
        let time = snap_times.get(snap_id).copied().unwrap_or(root_time);
        entries.push((UpdateType::TagCreatedUpdate { name: name.to_string() }, time));
        entries.push((
            UpdateType::TagDeletedUpdate {
                name: name.to_string(),
                previous_snap_id: snap_id.clone(),
            },
            time,
        ));
    }

    // Sort each update chronologically by timestamp, preserving insertion order for ties
    entries.sort_by_key(|(_, time)| *time);

    // Deduplicate timestamps: ensure strictly increasing, then reverse order as required by UpdateInfo.previous_updates
    let micros = TimeDelta::microseconds(1);
    for i in 1..entries.len() {
        if entries[i].1 <= entries[i - 1].1 {
            entries[i].1 = entries[i - 1].1 + micros;
        }
    }

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
        repo.asset_manager().clone_for_spec_version(SpecVersionBin::V2dot0);
    let new_version_info = new_asset_manager.create_repo_info(repo_info).await?;

    info!(version=?new_version_info, "Written repository info file");

    info!("Opening migrated repo");
    let migrated = match Repository::open(
        Some(repo.config().clone()),
        repo.storage().clone(),
        Default::default(),
    )
    .await
    {
        Ok(repo) => repo,
        Err(_) => {
            error!("Unknown error during migration. Repository doesn't open");
            delete_repo_info(repo).await?;
            error!("Migration failed");
            return Err(MigrationErrorKind::Unknown.into());
        }
    };

    let new_spec_version = migrated.spec_version();
    if new_spec_version != SpecVersionBin::V2dot0 {
        error!("Unknown error during migration. Repository doesn't open as 2.0");
        delete_repo_info(repo).await?;
        error!("Migration failed");
        return Err(MigrationErrorKind::Unknown.into());
    }
    if delete_unused_v1_files {
        if let Err(err) = delete_v1_refs(repo).await {
            delete_repo_info(repo).await?;
            error!("Migration failed");
            return Err(err);
        }
        info!("Opening migrated repo");
        let migrated = match Repository::open(
            Some(repo.config().clone()),
            repo.storage().clone(),
            Default::default(),
        )
        .await
        {
            Ok(repo) => repo,
            Err(_) => {
                error!("Unknown error during migration. Repository doesn't open");
                delete_repo_info(repo).await?;
                error!("Migration failed");
                return Err(MigrationErrorKind::Unknown.into());
            }
        };

        let new_spec_version = migrated.spec_version();
        if new_spec_version != SpecVersionBin::V2dot0 {
            error!("Unknown error during migration. Repository doesn't open as 2.0");
            delete_repo_info(repo).await?;
            error!("Migration failed");
            return Err(MigrationErrorKind::Unknown.into());
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
    repo: &mut Repository,
    dry_run: bool,
    delete_unused_v1_files: bool,
) -> MigrationResult<()> {
    let start_time = Instant::now();
    validate_start(repo).await?;

    info!("Starting migration");
    info!("Collecting refs");
    let refs = all_roots(repo).await?.try_collect::<Vec<_>>().await?;
    let tags = Vec::from_iter(refs.iter().filter_map(|(r, id)| {
        if r.is_tag() { Some((r.name(), id.clone())) } else { None }
    }));
    let branches = Vec::from_iter(refs.iter().filter_map(|(r, id)| {
        if r.is_branch() { Some((r.name(), id.clone())) } else { None }
    }));

    let deleted_tags =
        list_deleted_tags(repo.storage().as_ref(), repo.storage_settings()).await?;

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

    info!("Collecting non-dangling snapshots, this make take a few minutes");
    let snap_ids = refs.iter().map(|(_, id)| id);
    let all_snapshots =
        pointed_snapshots(repo, snap_ids).await?.try_collect::<Vec<_>>().await?;
    info!("Found {} non-dangling snapshots", all_snapshots.len());

    info!("Generating migration ops log");
    // Collect main branch ancestry (tip-to-root)
    let main_snap_id = branches
        .iter()
        .find(|(name, _)| *name == Ref::DEFAULT_BRANCH)
        .map(|(_, id)| id.clone());
    let main_ancestry = if let Some(ref snap_id) = main_snap_id {
        repo.ancestry(&VersionInfo::SnapshotId(snap_id.clone()))
            .await?
            .try_collect::<Vec<_>>()
            .await?
    } else {
        Vec::new()
    };

    // Collect non-main branch ancestries
    let mut branch_ancestries: Vec<(&str, Vec<SnapshotInfo>)> = Vec::new();
    for (name, snap_id) in &branches {
        if *name != Ref::DEFAULT_BRANCH {
            let ancestry = repo
                .ancestry(&VersionInfo::SnapshotId(snap_id.clone()))
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            branch_ancestries.push((*name, ancestry));
        }
    }

    // Fetch snapshot IDs for deleted tags
    let mut deleted_tags_with_snap: Vec<(&str, SnapshotId)> = Vec::new();
    for name in &deleted_tag_names {
        if let Some(snap_id) = fetch_deleted_tag_snapshot_id(repo, name).await {
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
    let config = repo.config().clone();
    let config_bytes: Vec<u8> = flexbuffers::to_vec(&config).map_err(|e| {
        IcechunkFormatError::from(IcechunkFormatErrorKind::SerializationErrorFlexBuffers(
            Box::new(e),
        ))
    })?;
    let repo_info = Arc::new(RepoInfo::new(
        SpecVersionBin::V2dot0,
        tags,
        branches,
        deleted_tag_names.iter().copied(),
        all_snapshots,
        &Default::default(),
        UpdateInfo {
            update_type: UpdateType::RepoMigratedUpdate {
                from_version: SpecVersionBin::V1dot0,
                to_version: SpecVersionBin::V2dot0,
            },
            update_time: Utc::now(),
            previous_updates,
        },
        None,
        migration_page_size,
        None,
        Some(config_bytes.as_slice()),
    )?);

    if dry_run {
        info!(
            "Migration dry-run completed in {} seconds, your repository wasn't modified, run with `dry_run=False` to actually migrate",
            start_time.elapsed().as_secs()
        );
        Ok(())
    } else {
        do_migrate(repo, repo_info, start_time, delete_unused_v1_files).await
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
        .await?;
    Ok(())
}

async fn delete_v1_refs(repo: &Repository) -> MigrationResult<()> {
    info!("Deleting V1 references");
    let all =
        repo.storage().list_objects(repo.storage_settings(), V1_REFS_FILE_PATH).await?;
    let delete_keys = all.map_ok(|li| (li.id, 0)).boxed().try_collect::<Vec<_>>().await?;

    repo.storage()
        .delete_objects(
            repo.storage_settings(),
            V1_REFS_FILE_PATH,
            stream::iter(delete_keys).boxed(),
        )
        .await?;
    info!("V1 references deleted, verifying");
    let remaining = repo
        .storage()
        .list_objects(repo.storage_settings(), V1_REFS_FILE_PATH)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    if remaining.is_empty() {
        info!("All V1 references have been deleted");
        Ok(())
    } else {
        error!(
            "Found {} remaining V1 references that couldn't be deleted",
            remaining.len()
        );
        Err(MigrationErrorKind::Unknown.into())
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
        .await?;
    info!("V1 config.yaml deleted");
    Ok(())
}

/// Function copied from IC 1.0 with some changes
async fn all_roots<'a>(
    repo: &'a Repository,
) -> RefResult<impl Stream<Item = RefResult<(Ref, SnapshotId)>> + 'a> {
    let all_refs = list_refs(repo.storage().as_ref(), repo.storage_settings()).await?;
    let roots = stream::iter(all_refs)
        .then(move |r| async move {
            r.fetch(repo.storage().as_ref(), repo.storage_settings())
                .await
                .map(|ref_data| (r, ref_data.snapshot))
        })
        .err_into();
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
                let parents = repo.ancestry(&VersionInfo::SnapshotId(pointed_snap_id.clone())).await?;
                //let parents = Arc::clone(&asset_manager).snapshot_ancestry(&pointed_snap_id).await?;
                for await parent in parents {
                    let parent = parent?;
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{collections::HashMap, path::Path};

    use icechunk_macros::tokio_test;
    use tempfile::{TempDir, tempdir};

    use crate::{new_local_filesystem_storage, refs};

    use super::*;

    async fn prepare_v1_repo() -> Result<(Repository, TempDir), Box<dyn std::error::Error>>
    {
        let dir = tempdir().expect("cannot create temp dir");
        let source_path = Path::new("../icechunk-python/tests/data/test-repo-v1");
        fs_extra::copy_items(&[source_path], &dir, &Default::default())?;
        let storage =
            new_local_filesystem_storage(dir.path().join("test-repo-v1").as_path())
                .await?;
        let repo = Repository::open(None, storage.clone(), Default::default()).await?;
        Ok((repo, dir))
    }

    #[tokio_test]
    /// Copy the source tree 1.0 repository to a temp dir, then migrate it
    async fn test_1_to_2_migration() -> Result<(), Box<dyn std::error::Error>> {
        let (mut repo, _tmp) = prepare_v1_repo().await?;

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

        migrate_1_to_2(&mut repo, false, true).await.unwrap();
        let repo =
            Repository::open(None, repo.storage().clone(), Default::default()).await?;

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
                from_version: SpecVersionBin::V1dot0,
                to_version: SpecVersionBin::V2dot0,
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
                if let UpdateType::TagDeletedUpdate { name, previous_snap_id } = ut {
                    if name == "deleted" {
                        return Some(previous_snap_id.clone());
                    }
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

        // Verify timestamps are strictly decreasing
        for window in ops_log.windows(2) {
            let (time_a, _, _) = &window[0];
            let (time_b, _, _) = &window[1];
            assert!(
                time_a > time_b,
                "ops log timestamps must be strictly decreasing: {time_a} should be > {time_b}"
            );
        }

        Ok(())
    }

    #[tokio_test]
    /// Copy the source tree 1.0 repository to a temp dir, then migrate it in dry-run mode
    async fn test_1_to_2_migration_dry_run() -> Result<(), Box<dyn std::error::Error>> {
        let (mut repo, _tmp) = prepare_v1_repo().await?;

        migrate_1_to_2(&mut repo, true, true).await.unwrap();
        let repo =
            Repository::open(None, repo.storage().clone(), Default::default()).await?;

        assert_eq!(repo.spec_version(), SpecVersionBin::V1dot0);
        Ok(())
    }

    #[tokio_test]
    /// Copy the source tree 1.0 repository to a temp dir, then migrate it in dry-run mode
    async fn test_1_to_2_migration_without_delete()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut repo, _tmp) = prepare_v1_repo().await?;

        migrate_1_to_2(&mut repo, false, false).await.unwrap();
        let repo =
            Repository::open(None, repo.storage().clone(), Default::default()).await?;

        assert_eq!(repo.spec_version(), SpecVersionBin::V2dot0);

        assert_eq!(
            refs::list_branches(repo.storage().as_ref(), repo.storage_settings()).await?,
            ["main".to_string(), "my-branch".to_string()].into()
        );
        Ok(())
    }
}
