use async_recursion::async_recursion;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use proptest::bits::u64;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{zarr::ObjectId, Storage, StorageError};

fn crock_encode_int(n: u64) -> String {
    base32::encode(base32::Alphabet::Crockford, &n.to_be_bytes())
}

fn crock_decode_int(data: &str) -> Option<u64> {
    let bytes = base32::decode(base32::Alphabet::Crockford, data)?;
    let bytes = bytes.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

#[derive(Debug, Error)]
pub enum RefError {
    #[error("storage error `{0:?}`")]
    Storage(#[from] StorageError),

    #[error("ref not found `{0}`")]
    RefNotFound(String),

    #[error("invalid ref type `{0}`")]
    InvalidRefType(String),

    #[error("invalid ref name `{0}`")]
    InvalidRefName(String),

    #[error("invalid branch version `{0}`")]
    InvalidBranchVersion(String),

    #[error("tag already exists, tags are immutable: `{0}`")]
    TagAlreadyExists(String),

    #[error("cannot serialize ref json: `{0}`")]
    Serialization(#[from] serde_json::Error),

    #[error("branch update conflict: `({expected_parent:?}) != ({actual_parent:?})`")]
    Conflict { expected_parent: Option<ObjectId>, actual_parent: Option<ObjectId> },
}

pub type RefResult<A> = Result<A, RefError>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Ref {
    Tag(String),
    Branch(String),
}

impl Ref {
    pub const DEFAULT_BRANCH: &'static str = "main";

    fn from_path(path: &str) -> RefResult<Self> {
        match path.strip_prefix("tag:") {
            Some(name) => Ok(Ref::Tag(name.to_string())),
            None => match path.strip_prefix("branch:") {
                Some(name) => Ok(Ref::Branch(name.to_string())),
                None => Err(RefError::InvalidRefType(path.to_string())),
            },
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BranchVersion(pub u64);

impl BranchVersion {
    fn decode(version: &str) -> RefResult<Self> {
        let n = crock_decode_int(version)
            .ok_or(RefError::InvalidBranchVersion(version.to_string()))?;
        Ok(BranchVersion(u64::MAX - n))
    }

    fn encode(&self) -> String {
        crock_encode_int(u64::MAX - self.0)
    }

    fn to_path(&self, branch_name: &str) -> RefResult<String> {
        branch_key(branch_name, self.encode().as_str())
    }

    fn initial() -> Self {
        Self(0)
    }

    fn inc(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RefData {
    pub snapshot: ObjectId,
}

const TAG_KEY_NAME: &str = "ref.json";

fn tag_key(tag_name: &str) -> RefResult<String> {
    if tag_name.contains('/') {
        return Err(RefError::InvalidRefName(tag_name.to_string()));
    }

    Ok(format!("tag:{}/{}", tag_name, TAG_KEY_NAME))
}

fn branch_root(branch_name: &str) -> RefResult<String> {
    if branch_name.contains('/') {
        return Err(RefError::InvalidRefName(branch_name.to_string()));
    }
    Ok(format!("branch:{}", branch_name))
}

fn branch_key(branch_name: &str, version_id: &str) -> RefResult<String> {
    branch_root(branch_name).map(|root| format!("{}/{}.json", root, version_id))
}

pub async fn create_tag(
    storage: &(dyn Storage + Send + Sync),
    name: &str,
    snapshot: ObjectId,
    overwrite_refs: bool,
) -> RefResult<()> {
    let key = tag_key(name)?;
    let data = RefData { snapshot };
    let content = serde_json::to_vec(&data)?;
    storage
        .write_ref(key.as_str(), overwrite_refs, Bytes::copy_from_slice(&content))
        .await
        .map_err(|e| match e {
            StorageError::RefAlreadyExists(_) => {
                RefError::TagAlreadyExists(name.to_string())
            }
            err => err.into(),
        })?;
    Ok(())
}

#[async_recursion]
pub async fn update_branch(
    storage: &(dyn Storage + Send + Sync),
    name: &str,
    new_snapshot: ObjectId,
    current_snapshot: Option<&ObjectId>,
    overwrite_refs: bool,
) -> RefResult<BranchVersion> {
    let last_version = last_branch_version(storage, name).await;
    let last_ref_data = match last_version {
        Ok(version) => {
            fetch_branch(storage, name, &version).await.map(|d| Some((version, d)))
        }
        Err(RefError::RefNotFound(_)) => Ok(None),
        Err(err) => Err(err),
    }?;
    let last_snapshot = last_ref_data.as_ref().map(|d| &d.1.snapshot);
    if last_snapshot != current_snapshot {
        return Err(RefError::Conflict {
            expected_parent: current_snapshot.cloned(),
            actual_parent: last_snapshot.cloned(),
        });
    }
    let new_version = match last_ref_data {
        Some((version, _)) => version.inc(),
        None => BranchVersion::initial(),
    };

    let key = new_version.to_path(name)?;
    let data = RefData { snapshot: new_snapshot };
    let content = serde_json::to_vec(&data)?;
    match storage
        .write_ref(key.as_str(), overwrite_refs, Bytes::copy_from_slice(&content))
        .await
    {
        Ok(_) => Ok(new_version),
        Err(StorageError::RefAlreadyExists(_)) => {
            // If the branch version already exists, an update happened since we checked
            // we can just try again and the conflict will be reported
            update_branch(storage, name, data.snapshot, current_snapshot, overwrite_refs)
                .await
        }
        Err(err) => Err(RefError::Storage(err)),
    }
}

pub async fn list_refs(storage: &(dyn Storage + Send + Sync)) -> RefResult<Vec<Ref>> {
    let all = storage.ref_names().await?;
    all.iter().map(|path| Ref::from_path(path.as_str())).try_collect()
}

async fn branch_history<'a, 'b>(
    storage: &'a (dyn Storage + Send + Sync),
    branch: &'b str,
) -> RefResult<impl Stream<Item = RefResult<BranchVersion>> + 'a> {
    let key = branch_root(branch)?;
    let all = storage.ref_versions(key.as_str()).await;
    Ok(all.map_err(|e| e.into()).and_then(move |version_id| async move {
        let version = version_id
            .strip_suffix(".json")
            .ok_or(RefError::InvalidRefName(version_id.clone()))?;
        BranchVersion::decode(version)
    }))
}

async fn last_branch_version(
    storage: &(dyn Storage + Send + Sync),
    branch: &str,
) -> RefResult<BranchVersion> {
    // TODO! optimize
    let mut all = Box::pin(branch_history(storage, branch).await?);
    all.try_next().await?.ok_or(RefError::RefNotFound(branch.to_string()))
}

pub async fn fetch_tag(
    storage: &(dyn Storage + Send + Sync),
    name: &str,
) -> RefResult<RefData> {
    let path = tag_key(name)?;
    match storage.get_ref(path.as_str()).await {
        Ok(data) => Ok(serde_json::from_slice(data.as_ref())?),
        Err(StorageError::RefNotFound(..)) => {
            Err(RefError::RefNotFound(name.to_string()))
        }
        Err(err) => Err(err.into()),
    }
}

async fn fetch_branch(
    storage: &(dyn Storage + Send + Sync),
    name: &str,
    version: &BranchVersion,
) -> RefResult<RefData> {
    let path = version.to_path(name)?;
    match storage.get_ref(path.as_str()).await {
        Ok(data) => Ok(serde_json::from_slice(data.as_ref())?),
        Err(StorageError::RefNotFound(..)) => {
            Err(RefError::RefNotFound(name.to_string()))
        }
        Err(err) => Err(err.into()),
    }
}

pub async fn fetch_branch_tip(
    storage: &(dyn Storage + Send + Sync),
    name: &str,
) -> RefResult<RefData> {
    let version = last_branch_version(storage, name).await?;
    fetch_branch(storage, name, &version).await
}

pub async fn fetch_ref(
    storage: &(dyn Storage + Send + Sync),
    ref_name: &str,
) -> RefResult<(Ref, RefData)> {
    match fetch_tag(storage, ref_name).await {
        Ok(from_ref) => Ok((Ref::Tag(ref_name.to_string()), from_ref)),
        Err(RefError::RefNotFound(_)) => {
            let data = fetch_branch_tip(storage, ref_name).await?;
            Ok((Ref::Branch(ref_name.to_string()), data))
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::{iter::once, sync::Arc};

    use futures::Future;
    use pretty_assertions::assert_eq;
    use rand::distributions::{Alphanumeric, DistString};
    use tempfile::{tempdir, TempDir};

    use crate::ObjectStorage;

    use super::*;

    #[tokio::test]
    async fn test_branch_version_encoding() -> Result<(), Box<dyn std::error::Error>> {
        let targets = (0..10u64).chain(once(u64::MAX));
        for n in targets {
            let round = BranchVersion::decode(BranchVersion(n).encode().as_str())?;
            assert_eq!(round, BranchVersion(n));
        }
        Ok(())
    }

    /// Execute the passed block with all test implementations of Storage.
    ///
    /// Currently this function executes agains the in-memory and local filesystem object_store
    /// implementations.
    async fn with_test_storages<
        R,
        Fut: Future<Output = R>,
        F: FnMut(Arc<dyn Storage + Send + Sync>) -> Fut,
    >(
        mut f: F,
    ) -> ((Arc<ObjectStorage>, R), (Arc<ObjectStorage>, R, TempDir)) {
        let prefix: String = Alphanumeric.sample_string(&mut rand::thread_rng(), 10);
        let mem_storage = Arc::new(ObjectStorage::new_in_memory_store(Some(prefix)));
        let res1 = f(Arc::clone(&mem_storage) as Arc<dyn Storage + Send + Sync>).await;

        let dir = tempdir().expect("cannot create temp dir");
        let local_storage = Arc::new(
            ObjectStorage::new_local_store(dir.path())
                .expect("Cannot create local Storage"),
        );

        let res2 = f(Arc::clone(&local_storage) as Arc<dyn Storage + Send + Sync>).await;
        ((mem_storage, res1), (local_storage, res2, dir))
    }

    #[tokio::test]
    async fn test_refs() -> Result<(), Box<dyn std::error::Error>> {
        let ((_,res1),(_,res2,_)) = with_test_storages::<Result<(), Box<dyn std::error::Error>>, _, _>(|storage|  async move {
            let s1 = ObjectId::random();
            let s2 = ObjectId::random();

            let res = fetch_tag(storage.as_ref(), "tag1").await;
            assert!(matches!(res, Err(RefError::RefNotFound(name)) if name == *"tag1"));
            assert_eq!(list_refs(storage.as_ref()).await?, vec![]);

            create_tag(storage.as_ref(), "tag1", s1.clone(), false).await?;
            create_tag(storage.as_ref(), "tag2", s2.clone(), false).await?;

            let res = fetch_tag(storage.as_ref(), "tag1").await?;
            assert_eq!(res.snapshot, s1);

            assert_eq!(
                fetch_tag(storage.as_ref(), "tag1").await?,
                fetch_ref(storage.as_ref(), "tag1").await?.1
            );

            let res = fetch_tag(storage.as_ref(), "tag2").await?;
            assert_eq!(res.snapshot, s2);

            assert_eq!(
                fetch_tag(storage.as_ref(), "tag2").await?,
                fetch_ref(storage.as_ref(), "tag2").await?.1
            );

            assert_eq!(
                list_refs(storage.as_ref()).await?,
                vec![Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())]
            );

            // attempts to recreate a tag fail
            assert!(matches!(
                create_tag(storage.as_ref(), "tag1", s1.clone(), false).await,
                    Err(RefError::TagAlreadyExists(name)) if name == *"tag1"
            ));
            assert_eq!(
                list_refs(storage.as_ref()).await?,
                vec![Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())]
            );

            // attempting to create a branch that doesn't exist, with a fake parent
            let res =
                update_branch(storage.as_ref(), "branch0", s1.clone(), Some(&s2), false)
                    .await;
            assert!(res.is_err());
            assert_eq!(
                list_refs(storage.as_ref()).await?,
                vec![Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())]
            );

            // create a branch successfully
            update_branch(storage.as_ref(), "branch1", s1.clone(), None, false).await?;

            assert_eq!(
                branch_history(storage.as_ref(), "branch1")
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?,
                vec![BranchVersion(0)]
            );
            assert_eq!(
                last_branch_version(storage.as_ref(), "branch1").await?,
                BranchVersion(0)
            );
            assert_eq!(
                fetch_branch(storage.as_ref(), "branch1", &BranchVersion(0)).await?,
                RefData { snapshot: s1.clone() }
            );
            assert_eq!(
                fetch_branch(storage.as_ref(), "branch1", &BranchVersion(0)).await?,
                fetch_ref(storage.as_ref(), "branch1").await?.1
            );

            assert_eq!(
                list_refs(storage.as_ref()).await?,
                vec![
                    Ref::Branch("branch1".to_string()),
                    Ref::Tag("tag1".to_string()),
                    Ref::Tag("tag2".to_string())
                ]
            );

            // update a branch successfully
            update_branch(
                storage.as_ref(),
                "branch1",
                s2.clone(),
                Some(&s1.clone()),
                false,
            )
            .await?;

            assert_eq!(
                branch_history(storage.as_ref(), "branch1")
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?,
                vec![BranchVersion(1), BranchVersion(0)]
            );
            assert_eq!(
                last_branch_version(storage.as_ref(), "branch1").await?,
                BranchVersion(1)
            );

            assert_eq!(
                fetch_branch(storage.as_ref(), "branch1", &BranchVersion(1)).await?,
                RefData { snapshot: s2.clone() }
            );

            assert_eq!(
                fetch_branch(storage.as_ref(), "branch1", &BranchVersion(1)).await?,
                fetch_ref(storage.as_ref(), "branch1").await?.1
            );

            let sid = ObjectId::random();
            // update a branch with the wrong parent
            let res =
                update_branch(storage.as_ref(), "branch1", sid.clone(), Some(&s1), false)
                    .await;
            assert!(matches!(res,
                    Err(RefError::Conflict { expected_parent, actual_parent })
                if expected_parent == Some(s1.clone()) && actual_parent == Some(s2.clone())
            ));

            // update the branch again but now with the right parent
            update_branch(storage.as_ref(), "branch1", sid.clone(), Some(&s2), false)
                .await?;

            assert_eq!(
                branch_history(storage.as_ref(), "branch1")
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?,
                vec![BranchVersion(2), BranchVersion(1), BranchVersion(0)]
            );
            assert_eq!(
                last_branch_version(storage.as_ref(), "branch1").await?,
                BranchVersion(2)
            );

            assert_eq!(
                fetch_branch(storage.as_ref(), "branch1", &BranchVersion(2)).await?,
                fetch_ref(storage.as_ref(), "branch1").await?.1
            );

            assert_eq!(
                fetch_ref(storage.as_ref(), "branch1").await?,
                (Ref::Branch("branch1".to_string()), RefData { snapshot: sid.clone() })
            );

            Ok(())
        }).await;
        res1?;
        res2?;
        Ok(())
    }
}
