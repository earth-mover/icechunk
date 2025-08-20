use std::{collections::HashSet, sync::Arc};

use async_stream::try_stream;
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use thiserror::Error;
use tracing::{error, info, warn};

use crate::{
    Repository, StorageError,
    error::ICError,
    format::{
        IcechunkFormatError, IcechunkFormatErrorKind, REPO_INFO_FILE_PATH, SnapshotId,
        V1_REFS_FILE_PATH, format_constants::SpecVersionBin, repo_info::RepoInfo,
        snapshot::SnapshotInfo,
    },
    refs::{Ref, RefError, RefErrorKind, RefResult, list_deleted_tags, list_refs},
    repository::{RepositoryError, RepositoryErrorKind, VersionInfo},
    storage::{self, StorageErrorKind},
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

pub async fn migrate_1_to_2(repo: &mut Repository) -> MigrationResult<()> {
    if repo.spec_version() != SpecVersionBin::V1dot0 {
        error!("Target repository must be a 1.X Icechunk repository");
        return Err(MigrationErrorKind::InvalidRepositoryMigration {
            expected: SpecVersionBin::V1dot0,
            target: SpecVersionBin::V2dot0,
            actual: repo.spec_version(),
        }
        .into());
    }
    if !repo.storage().can_write() {
        error!("Storage instance must be writable");
        return Err(MigrationErrorKind::ReadonlyRepo.into());
    }

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

    info!("Collecting pointed snapshots");
    let snap_ids = refs.iter().map(|(_, id)| id);
    let all_snapshots =
        pointed_snapshots(repo, snap_ids).await?.try_collect::<Vec<_>>().await?;
    info!("Found {} pointed snapshots", all_snapshots.len());

    info!("Creating repository info file");
    let repo_info = Arc::new(RepoInfo::new(
        tags,
        branches,
        deleted_tags.iter().map(|s| s.as_str()),
        all_snapshots,
        None,
    )?);

    info!("Writing new repository info file");
    let new_version_info = repo
        .asset_manager()
        .update_repo_info(repo_info, &storage::VersionInfo::for_creation(), None)
        .await?;
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

    info!("Migration completed, you can use the repository now");
    Ok(())
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{collections::HashMap, path::Path};

    use icechunk_macros::tokio_test;
    use tempfile::tempdir;

    use crate::new_local_filesystem_storage;

    use super::*;

    #[tokio_test]
    /// Copy the source tree 1.0 repository to a temp dir, then migrate it
    async fn test_1_to_2_migration() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir().expect("cannot create temp dir");
        let source_path = Path::new("../icechunk-python/tests/data/test-repo");
        fs_extra::copy_items(&[source_path], &dir, &Default::default())?;
        let storage =
            new_local_filesystem_storage(dir.path().join("test-repo").as_path()).await?;
        let mut repo =
            Repository::open(None, storage.clone(), Default::default()).await?;

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

        migrate_1_to_2(&mut repo).await.unwrap();
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
        Ok(())
    }
}
