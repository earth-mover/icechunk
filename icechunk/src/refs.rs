//! Branch and tag management.
//!
//! This module handles named references (branches and tags) that point to snapshots.

use std::{collections::BTreeSet, future::ready, pin::Pin};

use bytes::Bytes;
use futures::{
    FutureExt as _, StreamExt as _, TryStreamExt as _,
    stream::{FuturesOrdered, FuturesUnordered},
};
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use serde_with::{TryFromInto, serde_as};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt as _};
use tracing::instrument;

use crate::{
    Storage, StorageError,
    error::ICError,
    format::{SnapshotId, V1_REFS_FILE_PATH},
    storage::{StorageContext, StorageErrorKind, VersionInfo, VersionedUpdateResult},
};
use icechunk_types::{ICResultExt as _, error::ICResultCtxExt as _};

#[derive(Debug, Error)]
pub enum RefErrorKind {
    #[error(transparent)]
    Storage(#[from] StorageErrorKind),

    #[error("ref not found `{0}`")]
    RefNotFound(String),

    #[error("invalid ref type `{0}`")]
    InvalidRefType(String),

    #[error("invalid ref name `{0}`")]
    InvalidRefName(String),

    #[error("tag already exists, tags are immutable: `{0}`")]
    TagAlreadyExists(String),

    #[error("I/O error")]
    IOError(#[from] std::io::Error),

    #[error("cannot serialize ref json")]
    Serialization(#[from] serde_json::Error),

    #[error("branch update conflict: `({expected_parent:?}) != ({actual_parent:?})`")]
    Conflict { expected_parent: Option<SnapshotId>, actual_parent: Option<SnapshotId> },

    #[error(
        "could not update branch `{branch}` after {attempts} attempts: storage kept rejecting the conditional write while the branch tip was unchanged"
    )]
    UpdateBranchRetriesExhausted { branch: String, attempts: usize },
}

pub type RefError = ICError<RefErrorKind>;

pub type RefResult<A> = Result<A, RefError>;

/// A named reference to a snapshot: either a branch or a tag.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum Ref {
    /// Immutable reference to a specific snapshot.
    Tag(String),
    /// Mutable reference that advances with new commits.
    Branch(String),
}

impl Ref {
    pub const DEFAULT_BRANCH: &'static str = icechunk_types::DEFAULT_BRANCH;

    pub fn name(&self) -> &str {
        match self {
            Ref::Tag(n) => n,
            Ref::Branch(n) => n,
        }
    }

    fn from_path(path: &str) -> RefResult<Self> {
        match path.strip_prefix("tag.") {
            Some(name) => Ok(Ref::Tag(name.to_string())),
            None => match path.strip_prefix("branch.") {
                Some(name) => Ok(Ref::Branch(name.to_string())),
                None => {
                    Err(RefError::capture(RefErrorKind::InvalidRefType(path.to_string())))
                }
            },
        }
    }

    pub fn is_tag(&self) -> bool {
        match &self {
            Ref::Branch(_) => false,
            Ref::Tag(_) => true,
        }
    }

    pub fn is_branch(&self) -> bool {
        match &self {
            Ref::Branch(_) => true,
            Ref::Tag(_) => false,
        }
    }

    pub async fn fetch(
        &self,
        storage: &(dyn Storage + Send + Sync),
        ctx: &StorageContext<'_>,
    ) -> RefResult<RefData> {
        match self {
            Ref::Tag(name) => fetch_tag(storage, ctx, name).await,
            Ref::Branch(name) => fetch_branch_tip_v1(storage, ctx, name).await,
        }
    }
}

/// Data stored for a reference: the snapshot ID it points to.
#[serde_as]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RefData {
    #[serde_as(as = "TryFromInto<String>")]
    pub snapshot: SnapshotId,
}

const REF_KEY_NAME: &str = "ref.json";
const TAG_DELETE_MARKER_KEY_NAME: &str = "ref.json.deleted";

fn tag_key(tag_name: &str) -> RefResult<String> {
    if tag_name.contains('/') {
        return Err(RefError::capture(RefErrorKind::InvalidRefName(
            tag_name.to_string(),
        )));
    }

    Ok(format!("tag.{tag_name}/{REF_KEY_NAME}"))
}

fn tag_delete_marker_key(tag_name: &str) -> RefResult<String> {
    if tag_name.contains('/') {
        return Err(RefError::capture(RefErrorKind::InvalidRefName(
            tag_name.to_string(),
        )));
    }

    Ok(format!("tag.{tag_name}/{TAG_DELETE_MARKER_KEY_NAME}"))
}

fn branch_key(branch_name: &str) -> RefResult<String> {
    if branch_name.contains('/') {
        return Err(RefError::capture(RefErrorKind::InvalidRefName(
            branch_name.to_string(),
        )));
    }
    Ok(format!("branch.{branch_name}/{REF_KEY_NAME}"))
}

#[instrument(skip(storage, ctx))]
pub async fn create_tag(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    name: &str,
    snapshot: SnapshotId,
) -> RefResult<()> {
    let key = tag_key(name)?;
    let path = format!("{V1_REFS_FILE_PATH}/{key}");
    let data = RefData { snapshot };
    let content = serde_json::to_vec(&data).capture()?;
    match storage
        .put_object(
            ctx,
            path.as_str(),
            Bytes::from_owner(content),
            Some("application/json"),
            Default::default(),
            Some(&VersionInfo::for_creation()),
        )
        .await
    {
        Ok(VersionedUpdateResult::Updated { .. }) => Ok(()),
        Ok(VersionedUpdateResult::NotOnLatestVersion) => {
            Err(RefError::capture(RefErrorKind::TagAlreadyExists(name.to_string())))
        }
        Err(err) => Err(err.inject()),
    }
}

/// How many times [`update_branch`] tries the conditional write before giving up.
///
/// When the write is rejected because the ref changed, the next attempt re-reads the
/// branch: a real concurrent update moves the tip and is reported as a
/// [`RefErrorKind::Conflict`] right away. Running out of attempts therefore means the
/// storage keeps rejecting the write while the tip stays the same, for example because
/// it doesn't handle the conditional request correctly (#1091).
const UPDATE_BRANCH_MAX_ATTEMPTS: usize = 10;

#[instrument(skip(storage, ctx))]
pub async fn update_branch(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    name: &str,
    new_snapshot: SnapshotId,
    current_snapshot: Option<&SnapshotId>,
) -> RefResult<VersionInfo> {
    let key = branch_key(name)?;
    let path = format!("{V1_REFS_FILE_PATH}/{key}");
    let data = RefData { snapshot: new_snapshot };
    let content = Bytes::from(serde_json::to_vec(&data).capture()?);

    for _ in 0..UPDATE_BRANCH_MAX_ATTEMPTS {
        let (ref_data, version) = match fetch_branch(storage, ctx, name).await {
            Ok((ref_data, version)) => (Some(ref_data), version),
            Err(RefError { kind: RefErrorKind::RefNotFound(..), .. }) => {
                (None, VersionInfo::for_creation())
            }
            Err(err) => {
                return Err(err);
            }
        };

        if ref_data.as_ref().map(|rd| &rd.snapshot) != current_snapshot {
            return Err(RefErrorKind::Conflict {
                expected_parent: current_snapshot.cloned(),
                actual_parent: ref_data.map(|rd| rd.snapshot),
            })
            .capture();
        }

        match storage
            .put_object(
                ctx,
                path.as_str(),
                content.clone(),
                Some("application/json"),
                Default::default(),
                Some(&version),
            )
            .await
        {
            Ok(VersionedUpdateResult::Updated { new_version }) => return Ok(new_version),
            // The ref changed since we read it: read it again, and a real update
            // will be reported as a conflict on the next attempt
            Ok(VersionedUpdateResult::NotOnLatestVersion) => {}
            Err(err) => return Err(err.inject()),
        }
    }

    Err(RefErrorKind::UpdateBranchRetriesExhausted {
        branch: name.to_string(),
        attempts: UPDATE_BRANCH_MAX_ATTEMPTS,
    })
    .capture()
}

fn ref_name_from_object_name(key: &str) -> Option<String> {
    let ref_name = key.split('/').next()?;
    Some(ref_name.to_string())
}

#[instrument(skip(storage, ctx))]
pub async fn list_refs(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
) -> RefResult<BTreeSet<Ref>> {
    let all = storage
        .list_objects(ctx, format!("{V1_REFS_FILE_PATH}/").as_str())
        .await
        .inject()?
        .map_ok(|li| ref_name_from_object_name(&li.id));
    let candidate_refs: BTreeSet<_> = all
        .map(|r| r.inject())
        .try_filter_map(|path: Option<String>| match path {
            Some(path) => ready(Ref::from_path(path.as_str()).map(Some)),
            None => ready(Ok(None)),
        })
        .try_collect()
        .await?;
    // we have all the candidate refs, but we need to filter out deleted tags
    // we try to resolve all tags in parallel, and filter out the ones that don't resolve
    // TODO: this can probably be optimized by smarter `ref_names`
    let futs: FuturesUnordered<_> = candidate_refs
        .iter()
        .filter_map(|r| match r {
            Ref::Tag(name) => Some(async {
                (name.clone(), fetch_tag(storage, ctx, name.as_str()).await)
            }),
            Ref::Branch(_) => None,
        })
        .collect();
    let deleted_tags: BTreeSet<_> = futs
        .filter_map(|(tag_name, result)| {
            ready(if result.is_err() { Some(Ref::Tag(tag_name)) } else { None })
        })
        .collect()
        .await;

    Ok(candidate_refs.difference(&deleted_tags).cloned().collect())
}

#[instrument(skip(storage, ctx))]
pub async fn list_deleted_tags(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
) -> RefResult<BTreeSet<String>> {
    storage
        .list_objects(ctx, V1_REFS_FILE_PATH)
        .await
        .inject()?
        .try_filter_map(|li| {
            ready(if li.id.ends_with("ref.json.deleted") {
                Ok(Some(li.id))
            } else {
                Ok(None)
            })
        })
        .try_collect()
        .await
        .inject()
}

pub async fn list_tags(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
) -> RefResult<BTreeSet<String>> {
    let tags = list_refs(storage, ctx)
        .await?
        .into_iter()
        .filter_map(|r| match r {
            Ref::Tag(name) => Some(name),
            _ => None,
        })
        .collect();

    Ok(tags)
}

pub async fn list_branches(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
) -> RefResult<BTreeSet<String>> {
    let branches = list_refs(storage, ctx)
        .await?
        .into_iter()
        .filter_map(|r| match r {
            Ref::Branch(name) => Some(name),
            _ => None,
        })
        .collect();

    Ok(branches)
}

#[instrument(skip(storage, ctx))]
pub async fn delete_branch(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    branch: &str,
) -> RefResult<()> {
    // we make sure the branch exists
    _ = fetch_branch_tip_v1(storage, ctx, branch).await?;

    let key = branch_key(branch)?;
    storage
        .delete_objects(ctx, V1_REFS_FILE_PATH, futures::stream::iter([(key, 0)]).boxed())
        .await
        .inject()?;
    Ok(())
}

#[instrument(skip(storage, ctx))]
pub async fn delete_tag(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    tag: &str,
) -> RefResult<()> {
    // we make sure the tag exists
    _ = fetch_tag(storage, ctx, tag).await?;

    // no race condition: delete_tag ^ 2 = delete_tag
    let key = tag_delete_marker_key(tag)?;
    let path = format!("{V1_REFS_FILE_PATH}/{key}");
    match storage
        .put_object(
            ctx,
            path.as_str(),
            Bytes::new(),
            None,
            Default::default(),
            Some(&VersionInfo::for_creation()),
        )
        .await
    {
        Ok(VersionedUpdateResult::Updated { .. }) => Ok(()),
        Ok(VersionedUpdateResult::NotOnLatestVersion) => {
            Err(RefError::capture(RefErrorKind::RefNotFound(tag.to_string())))
        }

        Err(err) => Err(err.inject()),
    }
}

async fn async_read_to_bytes(
    mut read: Pin<Box<dyn AsyncRead + Send>>,
) -> RefResult<Bytes> {
    let mut data = Vec::with_capacity(1_024);
    read.read_to_end(&mut data).await.capture()?;
    Ok(Bytes::from_owner(data))
}

#[instrument(skip(storage, ctx))]
pub async fn fetch_tag(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    name: &str,
) -> RefResult<RefData> {
    let ref_path = tag_key(name)?;
    let delete_marker_path = tag_delete_marker_key(name)?;

    let fut1 = async move {
        let path = format!("{V1_REFS_FILE_PATH}/{ref_path}");
        match storage.get_object(ctx, path.as_str(), None).await {
            Ok((result, ..)) => Ok(async_read_to_bytes(result).await?),
            Err(StorageError { kind: StorageErrorKind::ObjectNotFound, .. }) => {
                Err(RefError::capture(RefErrorKind::RefNotFound(name.to_string())))
            }
            Err(err) => Err(err.inject()),
        }
    }
    .boxed();
    let fut2 = async move {
        let path = format!("{V1_REFS_FILE_PATH}/{delete_marker_path}");
        match storage.get_object(ctx, path.as_str(), None).await {
            Ok(_) => Ok(Bytes::new()),
            Err(StorageError { kind: StorageErrorKind::ObjectNotFound, .. }) => {
                Err(RefError::capture(RefErrorKind::RefNotFound(name.to_string())))
            }
            Err(err) => Err(err.inject()),
        }
    }
    .boxed();

    if let Some((content, is_deleted)) = FuturesOrdered::from_iter([fut1, fut2])
        .collect::<Vec<RefResult<Bytes>>>()
        .await
        .into_iter()
        .next_tuple()
    {
        match is_deleted {
            Ok(_) => Err(RefError::capture(RefErrorKind::RefNotFound(name.to_string()))),
            Err(RefError { kind: RefErrorKind::RefNotFound(_), .. }) => {
                let data = serde_json::from_slice(content?.as_ref()).capture()?;
                Ok(data)
            }
            Err(err) => Err(err),
        }
    } else {
        Err(RefError::capture(RefErrorKind::RefNotFound(name.to_string())))
    }
}

#[instrument(skip(storage, ctx))]
async fn fetch_branch(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    name: &str,
) -> RefResult<(RefData, VersionInfo)> {
    let ref_key = branch_key(name)?;
    let path = format!("{V1_REFS_FILE_PATH}/{ref_key}");
    match storage.get_object(ctx, path.as_str(), None).await {
        Ok((result, version)) => {
            let bytes = async_read_to_bytes(result).await?;
            let data = serde_json::from_slice(bytes.as_ref()).capture()?;
            Ok((data, version))
        }
        Err(StorageError { kind: StorageErrorKind::ObjectNotFound, .. }) => {
            Err(RefError::capture(RefErrorKind::RefNotFound(name.to_string())))
        }
        Err(err) => Err(err.inject()),
    }
}

#[instrument(skip(storage, ctx))]
pub async fn fetch_branch_tip_v1(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    name: &str,
) -> RefResult<RefData> {
    Ok(fetch_branch(storage, ctx, name).await?.0)
}

#[cfg(all(test, feature = "object-store-fs"))]
#[expect(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use futures::Future;
    use icechunk_macros::tokio_test;
    use pretty_assertions::assert_eq;
    use tempfile::{TempDir, tempdir};

    use std::{
        ops::Range,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use chrono::{DateTime, Utc};
    use futures::{Stream, stream::BoxStream};
    use icechunk_storage::sealed::Sealed;

    use super::*;
    use crate::storage::{
        DeleteObjectsResult, GetModifiedResult, ListInfo, RepositoryCreation, Settings,
        StorageInfo, StorageResult, new_in_memory_storage, new_local_filesystem_storage,
    };
    use crate::strategies::ref_data;
    use icechunk_format::roundtrip_serialization_tests;
    use proptest::prelude::*;

    roundtrip_serialization_tests!(serialize_and_deserialize_ref_data - ref_data);

    /// Drift guard: `icechunk-s3` cannot depend on `icechunk-format`, so its
    /// layout-detection anchors are hardcoded literals. Assert they still match
    /// the real format path constants and ref-key helper, so a future rename of
    /// those constants cannot silently break S3 legacy-layout detection.
    #[cfg(feature = "s3")]
    #[test]
    fn s3_layout_anchors_match_format_constants() {
        use icechunk_format::REPO_INFO_FILE_PATH;
        let anchors = icechunk_s3::DEFAULT_LAYOUT_ANCHORS;
        // V2 anchor: the repo-info file.
        assert!(
            anchors.contains(&REPO_INFO_FILE_PATH),
            "{anchors:?} missing REPO_INFO_FILE_PATH {REPO_INFO_FILE_PATH:?}"
        );
        // V1 anchor: the default-branch ref, built from the real (private) helper.
        let v1_default_branch_ref =
            format!("{V1_REFS_FILE_PATH}/{}", branch_key(Ref::DEFAULT_BRANCH).unwrap());
        assert!(
            anchors.contains(&v1_default_branch_ref.as_str()),
            "{anchors:?} missing V1 default-branch ref {v1_default_branch_ref:?}"
        );
    }

    /// Execute the passed block with all test implementations of Storage.
    ///
    /// Currently this function executes against the in-memory and local filesystem `object_store`
    /// implementations.
    async fn with_test_storages<
        R,
        Fut: Future<Output = R>,
        F: FnMut(Arc<dyn Storage + Send + Sync>) -> Fut,
    >(
        mut f: F,
    ) -> ((Arc<dyn Storage>, R), (Arc<dyn Storage>, R, TempDir)) {
        let mem_storage = new_in_memory_storage().await.unwrap();
        println!("Using mem storage");
        let res1 = f(Arc::clone(&mem_storage) as Arc<dyn Storage + Send + Sync>).await;

        let dir = tempdir().expect("cannot create temp dir");
        let local_storage = new_local_filesystem_storage(dir.path())
            .await
            .expect("Cannot create local Storage");

        println!("Using local file system storage");
        let res2 = f(Arc::clone(&local_storage) as Arc<dyn Storage + Send + Sync>).await;
        ((mem_storage, res1), (local_storage, res2, dir))
    }

    #[tokio_test]
    async fn test_refs() -> Result<(), Box<dyn std::error::Error>> {
        let ((_,res1),(_,res2,_)) = with_test_storages::<Result<(), Box<dyn std::error::Error>>, _, _>(|storage|  async move {
            let storage_settings =storage.default_settings().await?;
            let ctx = StorageContext::unattributed(&storage_settings);
            let s1 = SnapshotId::random();
            let s2 = SnapshotId::random();

            let res = fetch_tag(storage.as_ref(), &ctx, "tag1").await;
            assert!(matches!(res, Err(RefError{kind: RefErrorKind::RefNotFound(name),..}) if name == "tag1"));
            assert_eq!(list_refs(storage.as_ref(), &ctx).await?, BTreeSet::new());

            create_tag(storage.as_ref(), &ctx, "tag1", s1.clone()).await?;
            create_tag(storage.as_ref(), &ctx, "tag2", s2.clone()).await?;

            let res = fetch_tag(storage.as_ref(), &ctx, "tag1").await?;
            assert_eq!(res.snapshot, s1);

            let res = fetch_tag(storage.as_ref(), &ctx, "tag2").await?;
            assert_eq!(res.snapshot, s2);

            assert_eq!(
                list_refs(storage.as_ref(), &ctx).await?,
                BTreeSet::from([Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())])
            );

            // attempts to recreate a tag fail
            assert!(matches!(
                create_tag(storage.as_ref(), &ctx, "tag1", s1.clone()).await,
                    Err(RefError{kind: RefErrorKind::TagAlreadyExists(name), ..}) if name == "tag1"
            ));
            assert_eq!(
                list_refs(storage.as_ref(), &ctx).await?,
                BTreeSet::from([Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())])
            );

            // attempting to create a branch that doesn't exist, with a fake parent
            let res =
                update_branch(storage.as_ref(), &ctx, "branch0", s1.clone(), Some(&s2))
                    .await;
            assert!(res.is_err());
            assert_eq!(
                list_refs(storage.as_ref(), &ctx).await?,
                BTreeSet::from([Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())])
            );

            // create a branch successfully
            update_branch(storage.as_ref(), &ctx, "branch1", s1.clone(), None).await?;


            assert_eq!(
                fetch_branch_tip_v1(storage.as_ref(), &ctx, "branch1").await?,
                RefData { snapshot: s1.clone() }
            );


            assert_eq!(
                list_refs(storage.as_ref(), &ctx).await?,
                BTreeSet::from([
                    Ref::Branch("branch1".to_string()),
            Ref::Tag("tag1".to_string()),
                   Ref::Tag("tag2".to_string())
                ])
            );

            // update a branch successfully
            update_branch(
                storage.as_ref(), &ctx,
                "branch1",
                s2.clone(),
                Some(&s1.clone()),
            )
            .await?;

            assert_eq!(
                fetch_branch_tip_v1(storage.as_ref(), &ctx, "branch1").await?,
                RefData { snapshot: s2.clone() }
            );

            let sid = SnapshotId::random();
            // update a branch with the wrong parent
            let res =
                update_branch(storage.as_ref(), &ctx, "branch1", sid.clone(), Some(&s1))
                    .await;
            assert!(matches!(res,
                    Err(RefError{kind: RefErrorKind::Conflict { expected_parent, actual_parent }, ..})
                if expected_parent == Some(s1.clone()) && actual_parent == Some(s2.clone())
            ));

            // update the branch again but now with the right parent
            update_branch(storage.as_ref(), &ctx, "branch1", sid.clone(), Some(&s2))
                .await?;

            assert_eq!(
                fetch_branch_tip_v1(storage.as_ref(), &ctx, "branch1").await?,
                RefData { snapshot: sid.clone() }
            );


            // delete a branch
            delete_branch(storage.as_ref(), &ctx, "branch1").await?;
            assert!(matches!(
                fetch_branch_tip_v1(storage.as_ref(), &ctx, "branch1").await,
                Err(RefError{kind: RefErrorKind::RefNotFound(name),..}) if name == "branch1"
            ));

            Ok(())
        }).await;
        res1?;
        res2?;
        Ok(())
    }

    #[tokio_test]
    async fn test_tag_delete() -> Result<(), Box<dyn std::error::Error>> {
        let ((_, res1), (_, res2, _)) = with_test_storages::<
            Result<(), Box<dyn std::error::Error>>,
            _,
            _,
        >(|storage| async move {
            let storage_settings = storage.default_settings().await?;
            let ctx = StorageContext::unattributed(&storage_settings);
            let s1 = SnapshotId::random();
            let s2 = SnapshotId::random();
            create_tag(storage.as_ref(), &ctx, "tag1", s1).await?;

            // we can delete tags
            delete_tag(storage.as_ref(), &ctx, "tag1").await?;

            // cannot delete twice
            assert!(delete_tag(storage.as_ref(), &ctx, "tag1").await.is_err());

            // we cannot delete non-existent tag
            assert!(
                delete_tag(storage.as_ref(), &ctx, "doesnt_exist",).await.is_err()
            );

            // cannot recreate same tag
            matches!(create_tag(
                storage.as_ref(),
                &ctx,
                "tag1",
                s2.clone(),
            )
            .await, Err(RefError{kind: RefErrorKind::TagAlreadyExists(name),..}) if name == "tag1");

            assert!(list_tags(storage.as_ref(), &ctx).await?.is_empty());

            // can create different tag
            create_tag(storage.as_ref(), &ctx, "tag2", s2).await?;

            // listing doesn't include deleted tags
            assert_eq!(
                list_tags(storage.as_ref(), &ctx).await?,
                ["tag2".to_string()].into(),
            );

            Ok(())
        })
        .await;
        res1?;
        res2?;
        Ok(())
    }

    /// Delegates everything, but reports the first `reject_ref_writes` writes under
    /// the refs prefix as `NotOnLatestVersion` without writing anything: what a
    /// storage that mishandles conditional writes looks like to `update_branch`.
    #[derive(Debug, Serialize, Deserialize)]
    struct RejectingRefWrites {
        backend: Arc<dyn Storage + Send + Sync>,
        reject_ref_writes: usize,
        #[serde(skip)]
        ref_writes: AtomicUsize,
    }

    impl RejectingRefWrites {
        fn new(
            backend: Arc<dyn Storage + Send + Sync>,
            reject_ref_writes: usize,
        ) -> Self {
            Self { backend, reject_ref_writes, ref_writes: AtomicUsize::new(0) }
        }

        fn ref_writes(&self) -> usize {
            self.ref_writes.load(Ordering::SeqCst)
        }
    }

    impl std::fmt::Display for RejectingRefWrites {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RejectingRefWrites({})", self.backend)
        }
    }

    impl Sealed for RejectingRefWrites {}

    #[async_trait::async_trait]
    #[typetag::serde]
    impl Storage for RejectingRefWrites {
        fn storage_info(&self) -> StorageInfo {
            self.backend.storage_info()
        }

        async fn default_settings(&self) -> StorageResult<Settings> {
            self.backend.default_settings().await
        }

        async fn can_write(&self) -> StorageResult<bool> {
            self.backend.can_write().await
        }

        async fn can_create_repository(&self) -> StorageResult<RepositoryCreation> {
            self.backend.can_create_repository().await
        }

        async fn put_object(
            &self,
            ctx: &StorageContext<'_>,
            path: &str,
            bytes: Bytes,
            content_type: Option<&str>,
            metadata: Vec<(String, String)>,
            previous_version: Option<&VersionInfo>,
        ) -> StorageResult<VersionedUpdateResult> {
            if path.starts_with(V1_REFS_FILE_PATH)
                && self.ref_writes.fetch_add(1, Ordering::SeqCst) < self.reject_ref_writes
            {
                return Ok(VersionedUpdateResult::NotOnLatestVersion);
            }
            self.backend
                .put_object(ctx, path, bytes, content_type, metadata, previous_version)
                .await
        }

        async fn copy_object(
            &self,
            ctx: &StorageContext<'_>,
            from: &str,
            to: &str,
            content_type: Option<&str>,
            version: &VersionInfo,
        ) -> StorageResult<VersionedUpdateResult> {
            self.backend.copy_object(ctx, from, to, content_type, version).await
        }

        async fn list_objects<'a>(
            &'a self,
            ctx: &StorageContext<'_>,
            prefix: &str,
        ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
            self.backend.list_objects(ctx, prefix).await
        }

        async fn list_objects_with_id_prefixes<'a>(
            &'a self,
            ctx: &StorageContext<'_>,
            prefix: &str,
            id_prefixes: &[String],
        ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
            self.backend.list_objects_with_id_prefixes(ctx, prefix, id_prefixes).await
        }

        fn lists_id_prefixes_natively(&self) -> bool {
            self.backend.lists_id_prefixes_natively()
        }

        async fn delete_batch(
            &self,
            ctx: &StorageContext<'_>,
            prefix: &str,
            batch: Vec<(String, u64)>,
        ) -> StorageResult<DeleteObjectsResult> {
            self.backend.delete_batch(ctx, prefix, batch).await
        }

        async fn get_object_last_modified(
            &self,
            ctx: &StorageContext<'_>,
            path: &str,
        ) -> StorageResult<DateTime<Utc>> {
            self.backend.get_object_last_modified(ctx, path).await
        }

        async fn get_object_conditional(
            &self,
            ctx: &StorageContext<'_>,
            path: &str,
            previous_version: Option<&VersionInfo>,
        ) -> StorageResult<GetModifiedResult> {
            self.backend.get_object_conditional(ctx, path, previous_version).await
        }

        async fn get_object_range(
            &self,
            ctx: &StorageContext<'_>,
            path: &str,
            range: Option<&Range<u64>>,
        ) -> StorageResult<(
            Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
            VersionInfo,
        )> {
            self.backend.get_object_range(ctx, path, range).await
        }
    }

    /// Before #1091 was fixed, a storage that kept rejecting the conditional write
    /// made `update_branch` recurse until the stack overflowed.
    #[tokio_test]
    async fn test_update_branch_gives_up_when_storage_keeps_rejecting_writes()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage = RejectingRefWrites::new(new_in_memory_storage().await?, usize::MAX);
        let storage_settings = storage.default_settings().await?;
        let ctx = StorageContext::unattributed(&storage_settings);

        let res = update_branch(&storage, &ctx, "main", SnapshotId::random(), None).await;

        assert!(matches!(
            res,
            Err(RefError {
                kind: RefErrorKind::UpdateBranchRetriesExhausted { branch, attempts },
                ..
            }) if branch == "main" && attempts == UPDATE_BRANCH_MAX_ATTEMPTS
        ));
        assert_eq!(storage.ref_writes(), UPDATE_BRANCH_MAX_ATTEMPTS);
        assert!(matches!(
            fetch_branch_tip_v1(&storage, &ctx, "main").await,
            Err(RefError { kind: RefErrorKind::RefNotFound(_), .. })
        ));
        Ok(())
    }

    #[tokio_test]
    async fn test_update_branch_retries_rejected_writes()
    -> Result<(), Box<dyn std::error::Error>> {
        let rejected = UPDATE_BRANCH_MAX_ATTEMPTS - 1;
        let storage = RejectingRefWrites::new(new_in_memory_storage().await?, rejected);
        let storage_settings = storage.default_settings().await?;
        let ctx = StorageContext::unattributed(&storage_settings);
        let snapshot = SnapshotId::random();

        update_branch(&storage, &ctx, "main", snapshot.clone(), None).await?;

        assert_eq!(storage.ref_writes(), rejected + 1);
        assert_eq!(
            fetch_branch_tip_v1(&storage, &ctx, "main").await?,
            RefData { snapshot }
        );
        Ok(())
    }
}
