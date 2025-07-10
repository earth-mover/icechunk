use std::{
    collections::BTreeSet,
    future::{Future, ready},
    pin::Pin,
};

use async_recursion::async_recursion;
use bytes::Bytes;
use futures::{
    FutureExt, StreamExt,
    stream::{FuturesOrdered, FuturesUnordered},
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_with::{TryFromInto, serde_as};
use thiserror::Error;
use tracing::instrument;

use crate::{
    Storage, StorageError,
    error::ICError,
    format::SnapshotId,
    storage::{self, GetRefResult, StorageErrorKind, VersionInfo, WriteRefResult},
};

#[derive(Debug, Error)]
pub enum RefErrorKind {
    #[error(transparent)]
    Storage(StorageErrorKind),

    #[error("ref not found `{0}`")]
    RefNotFound(String),

    #[error("invalid ref type `{0}`")]
    InvalidRefType(String),

    #[error("invalid ref name `{0}`")]
    InvalidRefName(String),

    #[error("tag already exists, tags are immutable: `{0}`")]
    TagAlreadyExists(String),

    #[error("cannot serialize ref json")]
    Serialization(#[from] serde_json::Error),

    #[error("branch update conflict: `({expected_parent:?}) != ({actual_parent:?})`")]
    Conflict { expected_parent: Option<SnapshotId>, actual_parent: Option<SnapshotId> },
}

pub type RefError = ICError<RefErrorKind>;

// it would be great to define this impl in error.rs, but it conflicts with the blanket
// `impl From<T> for T`
impl<E> From<E> for RefError
where
    E: Into<RefErrorKind>,
{
    fn from(value: E) -> Self {
        Self::new(value.into())
    }
}

impl From<StorageError> for RefError {
    fn from(value: StorageError) -> Self {
        Self::with_context(RefErrorKind::Storage(value.kind), value.context)
    }
}

pub type RefResult<A> = Result<A, RefError>;

#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum Ref {
    Tag(String),
    Branch(String),
}

impl Ref {
    pub const DEFAULT_BRANCH: &'static str = "main";

    fn from_path(path: &str) -> RefResult<Self> {
        match path.strip_prefix("tag.") {
            Some(name) => Ok(Ref::Tag(name.to_string())),
            None => match path.strip_prefix("branch.") {
                Some(name) => Ok(Ref::Branch(name.to_string())),
                None => Err(RefErrorKind::InvalidRefType(path.to_string()).into()),
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
        storage_settings: &storage::Settings,
    ) -> RefResult<RefData> {
        match self {
            Ref::Tag(name) => fetch_tag(storage, storage_settings, name).await,
            Ref::Branch(name) => fetch_branch_tip(storage, storage_settings, name).await,
        }
    }
}

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
        return Err(RefErrorKind::InvalidRefName(tag_name.to_string()).into());
    }

    Ok(format!("tag.{tag_name}/{REF_KEY_NAME}"))
}

fn tag_delete_marker_key(tag_name: &str) -> RefResult<String> {
    if tag_name.contains('/') {
        return Err(RefErrorKind::InvalidRefName(tag_name.to_string()).into());
    }

    Ok(format!("tag.{tag_name}/{TAG_DELETE_MARKER_KEY_NAME}"))
}

fn branch_key(branch_name: &str) -> RefResult<String> {
    if branch_name.contains('/') {
        return Err(RefErrorKind::InvalidRefName(branch_name.to_string()).into());
    }
    Ok(format!("branch.{branch_name}/{REF_KEY_NAME}"))
}

#[instrument(skip(storage, storage_settings))]
pub async fn create_tag(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    name: &str,
    snapshot: SnapshotId,
) -> RefResult<()> {
    let key = tag_key(name)?;
    let data = RefData { snapshot };
    let content = serde_json::to_vec(&data)?;
    match storage
        .write_ref(
            storage_settings,
            key.as_str(),
            Bytes::copy_from_slice(&content),
            &VersionInfo::for_creation(),
        )
        .await
    {
        Ok(WriteRefResult::Written) => Ok(()),
        Ok(WriteRefResult::WontOverwrite) => {
            Err(RefErrorKind::TagAlreadyExists(name.to_string()).into())
        }
        Err(err) => Err(err.into()),
    }
}

#[async_recursion]
#[instrument(skip(storage, storage_settings))]
pub async fn update_branch(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    name: &str,
    new_snapshot: SnapshotId,
    current_snapshot: Option<&SnapshotId>,
) -> RefResult<()> {
    let (ref_data, version) = match fetch_branch(storage, storage_settings, name).await {
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
        }
        .into());
    }

    let key = branch_key(name)?;
    let data = RefData { snapshot: new_snapshot };
    let content = serde_json::to_vec(&data)?;
    match storage
        .write_ref(
            storage_settings,
            key.as_str(),
            Bytes::copy_from_slice(&content),
            &version,
        )
        .await
    {
        Ok(WriteRefResult::Written) => Ok(()),
        Ok(WriteRefResult::WontOverwrite) => {
            // If the already exists, an update happened since we checked
            // we can just try again and the conflict will be reported
            update_branch(
                storage,
                storage_settings,
                name,
                data.snapshot,
                current_snapshot,
            )
            .await
        }
        Err(err) => Err(err.into()),
    }
}

#[instrument(skip(storage, storage_settings))]
pub async fn list_refs(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RefResult<BTreeSet<Ref>> {
    let all = storage.ref_names(storage_settings).await?;
    let candidate_refs: BTreeSet<_> =
        all.iter().map(|path| Ref::from_path(path.as_str())).try_collect()?;
    // we have all the candidate refs, but we need to filter out deleted tags
    // we try to resolve all tags in parallel, and filter out the ones that don't resolve
    // TODO: this can probably be optimized by smarter `ref_names`
    let futs: FuturesUnordered<_> = candidate_refs
        .iter()
        .filter_map(|r| match r {
            Ref::Tag(name) => Some(async {
                (name.clone(), fetch_tag(storage, storage_settings, name.as_str()).await)
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

pub async fn list_tags(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RefResult<BTreeSet<String>> {
    let tags = list_refs(storage, storage_settings)
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
    storage_settings: &storage::Settings,
) -> RefResult<BTreeSet<String>> {
    let branches = list_refs(storage, storage_settings)
        .await?
        .into_iter()
        .filter_map(|r| match r {
            Ref::Branch(name) => Some(name),
            _ => None,
        })
        .collect();

    Ok(branches)
}

#[instrument(skip(storage, storage_settings))]
pub async fn delete_branch(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    branch: &str,
) -> RefResult<()> {
    // we make sure the branch exists
    _ = fetch_branch_tip(storage, storage_settings, branch).await?;

    let key = branch_key(branch)?;
    storage.delete_refs(storage_settings, futures::stream::iter([key]).boxed()).await?;
    Ok(())
}

#[instrument(skip(storage, storage_settings))]
pub async fn delete_tag(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    tag: &str,
) -> RefResult<()> {
    // we make sure the tag exists
    _ = fetch_tag(storage, storage_settings, tag).await?;

    // no race condition: delete_tag ^ 2 = delete_tag
    let key = tag_delete_marker_key(tag)?;
    match storage
        .write_ref(
            storage_settings,
            key.as_str(),
            Bytes::from_static(&[]),
            &VersionInfo::for_creation(),
        )
        .await
    {
        Ok(WriteRefResult::Written) => Ok(()),
        Ok(WriteRefResult::WontOverwrite) => {
            Err(RefErrorKind::RefNotFound(tag.to_string()).into())
        }
        Err(err) => Err(err.into()),
    }
}

#[instrument(skip(storage, storage_settings))]
pub async fn fetch_tag(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    name: &str,
) -> RefResult<RefData> {
    let ref_path = tag_key(name)?;
    let delete_marker_path = tag_delete_marker_key(name)?;

    let fut1: Pin<Box<dyn Future<Output = RefResult<Bytes>>>> = async move {
        match storage.get_ref(storage_settings, ref_path.as_str()).await {
            Ok(GetRefResult::Found { bytes, .. }) => Ok(bytes),
            Ok(GetRefResult::NotFound) => {
                Err(RefErrorKind::RefNotFound(name.to_string()).into())
            }
            Err(err) => Err(err.into()),
        }
    }
    .boxed();
    let fut2 = async move {
        match storage.get_ref(storage_settings, delete_marker_path.as_str()).await {
            Ok(GetRefResult::Found { .. }) => Ok(Bytes::new()),
            Ok(GetRefResult::NotFound) => {
                Err(RefErrorKind::RefNotFound(name.to_string()).into())
            }
            Err(err) => Err(err.into()),
        }
    }
    .boxed();

    if let Some((content, is_deleted)) = FuturesOrdered::from_iter([fut1, fut2])
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .next_tuple()
    {
        match is_deleted {
            Ok(_) => Err(RefErrorKind::RefNotFound(name.to_string()).into()),
            Err(RefError { kind: RefErrorKind::RefNotFound(_), .. }) => {
                let data = serde_json::from_slice(content?.as_ref())?;
                Ok(data)
            }
            Err(err) => Err(err),
        }
    } else {
        Err(RefErrorKind::RefNotFound(name.to_string()).into())
    }
}

#[instrument(skip(storage, storage_settings))]
async fn fetch_branch(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    name: &str,
) -> RefResult<(RefData, VersionInfo)> {
    let ref_key = branch_key(name)?;
    match storage.get_ref(storage_settings, ref_key.as_str()).await {
        Ok(GetRefResult::Found { bytes, version }) => {
            let data = serde_json::from_slice(bytes.as_ref())?;
            Ok((data, version))
        }
        Ok(GetRefResult::NotFound) => {
            Err(RefErrorKind::RefNotFound(name.to_string()).into())
        }
        Err(err) => Err(err.into()),
    }
}

#[instrument(skip(storage, storage_settings))]
pub async fn fetch_branch_tip(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    name: &str,
) -> RefResult<RefData> {
    Ok(fetch_branch(storage, storage_settings, name).await?.0)
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use futures::Future;
    use icechunk_macros::tokio_test;
    use pretty_assertions::assert_eq;
    use tempfile::{TempDir, tempdir};

    use crate::storage::{new_in_memory_storage, new_local_filesystem_storage};

    use super::*;

    /// Execute the passed block with all test implementations of Storage.
    ///
    /// Currently this function executes against the in-memory and local filesystem object_store
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
            let storage_settings =storage.default_settings();
            let s1 = SnapshotId::random();
            let s2 = SnapshotId::random();

            let res = fetch_tag(storage.as_ref(), &storage_settings, "tag1").await;
            assert!(matches!(res, Err(RefError{kind: RefErrorKind::RefNotFound(name),..}) if name == "tag1"));
            assert_eq!(list_refs(storage.as_ref(), &storage_settings).await?, BTreeSet::new());

            create_tag(storage.as_ref(), &storage_settings, "tag1", s1.clone()).await?;
            create_tag(storage.as_ref(), &storage_settings, "tag2", s2.clone()).await?;

            let res = fetch_tag(storage.as_ref(), &storage_settings, "tag1").await?;
            assert_eq!(res.snapshot, s1);

            let res = fetch_tag(storage.as_ref(), &storage_settings, "tag2").await?;
            assert_eq!(res.snapshot, s2);

            assert_eq!(
                list_refs(storage.as_ref(), &storage_settings).await?,
                BTreeSet::from([Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())])
            );

            // attempts to recreate a tag fail
            assert!(matches!(
                create_tag(storage.as_ref(), &storage_settings, "tag1", s1.clone()).await,
                    Err(RefError{kind: RefErrorKind::TagAlreadyExists(name), ..}) if name == "tag1"
            ));
            assert_eq!(
                list_refs(storage.as_ref(), &storage_settings).await?,
                BTreeSet::from([Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())])
            );

            // attempting to create a branch that doesn't exist, with a fake parent
            let res =
                update_branch(storage.as_ref(), &storage_settings, "branch0", s1.clone(), Some(&s2))
                    .await;
            assert!(res.is_err());
            assert_eq!(
                list_refs(storage.as_ref(), &storage_settings).await?,
                BTreeSet::from([Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())])
            );

            // create a branch successfully
            update_branch(storage.as_ref(), &storage_settings, "branch1", s1.clone(), None).await?;


            assert_eq!(
                fetch_branch_tip(storage.as_ref(), &storage_settings, "branch1").await?,
                RefData { snapshot: s1.clone() }
            );


            assert_eq!(
                list_refs(storage.as_ref(), &storage_settings).await?,
                BTreeSet::from([
                    Ref::Branch("branch1".to_string()),
            Ref::Tag("tag1".to_string()),
                   Ref::Tag("tag2".to_string())
                ])
            );

            // update a branch successfully
            update_branch(
                storage.as_ref(), &storage_settings,
                "branch1",
                s2.clone(),
                Some(&s1.clone()),
            )
            .await?;

            assert_eq!(
                fetch_branch_tip(storage.as_ref(), &storage_settings, "branch1").await?,
                RefData { snapshot: s2.clone() }
            );

            let sid = SnapshotId::random();
            // update a branch with the wrong parent
            let res =
                update_branch(storage.as_ref(), &storage_settings, "branch1", sid.clone(), Some(&s1))
                    .await;
            assert!(matches!(res,
                    Err(RefError{kind: RefErrorKind::Conflict { expected_parent, actual_parent }, ..})
                if expected_parent == Some(s1.clone()) && actual_parent == Some(s2.clone())
            ));

            // update the branch again but now with the right parent
            update_branch(storage.as_ref(), &storage_settings, "branch1", sid.clone(), Some(&s2))
                .await?;

            assert_eq!(
                fetch_branch_tip(storage.as_ref(), &storage_settings, "branch1").await?,
                RefData { snapshot: sid.clone() }
            );


            // delete a branch
            delete_branch(storage.as_ref(), &storage_settings, "branch1").await?;
            assert!(matches!(
                fetch_branch_tip(storage.as_ref(), &storage_settings, "branch1").await,
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
            let storage_settings = storage.default_settings();
            let s1 = SnapshotId::random();
            let s2 = SnapshotId::random();
            create_tag(storage.as_ref(), &storage_settings, "tag1", s1).await?;

            // we can delete tags
            delete_tag(storage.as_ref(), &storage_settings, "tag1").await?;

            // cannot delete twice
            assert!(delete_tag(storage.as_ref(), &storage_settings, "tag1")
                .await
                .is_err());

            // we cannot delete non-existent tag
            assert!(delete_tag(
                storage.as_ref(),
                &storage_settings,
                "doesnt_exist",
            )
            .await
            .is_err());

            // cannot recreate same tag
            matches!(create_tag(
                storage.as_ref(),
                &storage_settings,
                "tag1",
                s2.clone(),
            )
            .await, Err(RefError{kind: RefErrorKind::TagAlreadyExists(name),..}) if name == "tag1");

            assert!(list_tags(storage.as_ref(), &storage_settings).await?.is_empty());

            // can create different tag
            create_tag(storage.as_ref(), &storage_settings, "tag2", s2).await?;

            // listing doesn't include deleted tags
            assert_eq!(
                list_tags(storage.as_ref(), &storage_settings).await?,
                ["tag2".to_string()].into(),
            );

            Ok(())
        })
        .await;
        res1?;
        res2?;
        Ok(())
    }
}
