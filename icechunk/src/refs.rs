use std::collections::HashSet;

use async_recursion::async_recursion;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, TryFromInto};
use thiserror::Error;

use crate::{format::SnapshotId, storage, Storage, StorageError};

fn crock_encode_int(n: u64) -> String {
    // skip the first 3 bytes (zeroes)
    base32::encode(base32::Alphabet::Crockford, &n.to_be_bytes()[3..=7])
}

fn crock_decode_int(data: &str) -> Option<u64> {
    // re insert the first 3 bytes removed during encoding
    let mut bytes = vec![0, 0, 0];
    bytes.extend(base32::decode(base32::Alphabet::Crockford, data)?);
    Some(u64::from_be_bytes(bytes.as_slice().try_into().ok()?))
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
    Conflict { expected_parent: Option<SnapshotId>, actual_parent: Option<SnapshotId> },
}

pub type RefResult<A> = Result<A, RefError>;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
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
                None => Err(RefError::InvalidRefType(path.to_string())),
            },
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BranchVersion(pub u64);

impl BranchVersion {
    const MAX_VERSION_NUMBER: u64 = 1099511627775;

    fn decode(version: &str) -> RefResult<Self> {
        let n = crock_decode_int(version)
            .ok_or(RefError::InvalidBranchVersion(version.to_string()))?;
        Ok(BranchVersion(BranchVersion::MAX_VERSION_NUMBER - n))
    }

    fn encode(&self) -> String {
        crock_encode_int(BranchVersion::MAX_VERSION_NUMBER - self.0)
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

#[serde_as]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RefData {
    #[serde_as(as = "TryFromInto<String>")]
    pub snapshot: SnapshotId,
}

const TAG_KEY_NAME: &str = "ref.json";

fn tag_key(tag_name: &str) -> RefResult<String> {
    if tag_name.contains('/') {
        return Err(RefError::InvalidRefName(tag_name.to_string()));
    }

    Ok(format!("tag.{}/{}", tag_name, TAG_KEY_NAME))
}

fn branch_root(branch_name: &str) -> RefResult<String> {
    if branch_name.contains('/') {
        return Err(RefError::InvalidRefName(branch_name.to_string()));
    }
    Ok(format!("branch.{}", branch_name))
}

fn branch_key(branch_name: &str, version_id: &str) -> RefResult<String> {
    branch_root(branch_name).map(|root| format!("{}/{}.json", root, version_id))
}

pub async fn create_tag(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    name: &str,
    snapshot: SnapshotId,
    overwrite_refs: bool,
) -> RefResult<()> {
    let key = tag_key(name)?;
    let data = RefData { snapshot };
    let content = serde_json::to_vec(&data)?;
    storage
        .write_ref(
            storage_settings,
            key.as_str(),
            overwrite_refs,
            Bytes::copy_from_slice(&content),
        )
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
    storage_settings: &storage::Settings,
    name: &str,
    new_snapshot: SnapshotId,
    current_snapshot: Option<&SnapshotId>,
    overwrite_refs: bool,
) -> RefResult<BranchVersion> {
    let last_version = last_branch_version(storage, storage_settings, name).await;
    let last_ref_data = match last_version {
        Ok(version) => fetch_branch(storage, storage_settings, name, &version)
            .await
            .map(|d| Some((version, d))),
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
        .write_ref(
            storage_settings,
            key.as_str(),
            overwrite_refs,
            Bytes::copy_from_slice(&content),
        )
        .await
    {
        Ok(_) => Ok(new_version),
        Err(StorageError::RefAlreadyExists(_)) => {
            // If the branch version already exists, an update happened since we checked
            // we can just try again and the conflict will be reported
            update_branch(
                storage,
                storage_settings,
                name,
                data.snapshot,
                current_snapshot,
                overwrite_refs,
            )
            .await
        }
        Err(err) => Err(RefError::Storage(err)),
    }
}

pub async fn list_refs(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RefResult<HashSet<Ref>> {
    let all = storage.ref_names(storage_settings).await?;
    all.iter().map(|path| Ref::from_path(path.as_str())).try_collect()
}

pub async fn list_tags(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RefResult<HashSet<String>> {
    let tags = list_refs(storage, storage_settings)
        .await?
        .into_iter()
        .filter_map(|r| match r {
            Ref::Tag(name) => Some(name),
            _ => None,
        })
        .collect::<HashSet<_>>();

    Ok(tags)
}

pub async fn list_branches(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RefResult<HashSet<String>> {
    let branches = list_refs(storage, storage_settings)
        .await?
        .into_iter()
        .filter_map(|r| match r {
            Ref::Branch(name) => Some(name),
            _ => None,
        })
        .collect::<HashSet<_>>();

    Ok(branches)
}

async fn branch_history<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    branch: &str,
) -> RefResult<impl Stream<Item = RefResult<BranchVersion>> + 'a> {
    let key = branch_root(branch)?;
    let all = storage.ref_versions(storage_settings, key.as_str()).await?;
    Ok(all.map_err(|e| e.into()).and_then(move |version_id| async move {
        let version = version_id
            .strip_suffix(".json")
            .ok_or(RefError::InvalidRefName(version_id.clone()))?;
        BranchVersion::decode(version)
    }))
}

async fn last_branch_version(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    branch: &str,
) -> RefResult<BranchVersion> {
    // TODO! optimize
    let mut all = Box::pin(branch_history(storage, storage_settings, branch).await?);
    all.try_next().await?.ok_or(RefError::RefNotFound(branch.to_string()))
}

pub async fn delete_branch(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    branch: &str,
) -> RefResult<()> {
    let key = branch_root(branch)?;
    let key_ref = key.as_str();
    let refs = storage
        .ref_versions(storage_settings, key_ref)
        .await?
        .filter_map(|v| async move {
            v.ok().map(|v| format!("{}/{}", key_ref, v).as_str().to_string())
        })
        .boxed();
    storage.delete_refs(storage_settings, refs).await?;
    Ok(())
}

pub async fn fetch_tag(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    name: &str,
) -> RefResult<RefData> {
    let path = tag_key(name)?;
    match storage.get_ref(storage_settings, path.as_str()).await {
        Ok(data) => Ok(serde_json::from_slice(data.as_ref())?),
        Err(StorageError::RefNotFound(..)) => {
            Err(RefError::RefNotFound(name.to_string()))
        }
        Err(err) => Err(err.into()),
    }
}

async fn fetch_branch(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    name: &str,
    version: &BranchVersion,
) -> RefResult<RefData> {
    let path = version.to_path(name)?;
    match storage.get_ref(storage_settings, path.as_str()).await {
        Ok(data) => Ok(serde_json::from_slice(data.as_ref())?),
        Err(StorageError::RefNotFound(..)) => {
            Err(RefError::RefNotFound(name.to_string()))
        }
        Err(err) => Err(err.into()),
    }
}

pub async fn fetch_branch_tip(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    name: &str,
) -> RefResult<RefData> {
    let version = last_branch_version(storage, storage_settings, name).await?;
    fetch_branch(storage, storage_settings, name, &version).await
}

pub async fn fetch_ref(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    ref_name: &str,
) -> RefResult<(Ref, RefData)> {
    match fetch_tag(storage, storage_settings, ref_name).await {
        Ok(from_ref) => Ok((Ref::Tag(ref_name.to_string()), from_ref)),
        Err(RefError::RefNotFound(_)) => {
            let data = fetch_branch_tip(storage, storage_settings, ref_name).await?;
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
    use tempfile::{tempdir, TempDir};

    use crate::storage::{new_in_memory_storage, new_local_filesystem_storage};

    use super::*;

    #[tokio::test]
    async fn test_branch_version_encoding() -> Result<(), Box<dyn std::error::Error>> {
        let targets = (0..10u64).chain(once(BranchVersion::MAX_VERSION_NUMBER));
        let encodings = [
            "ZZZZZZZZ", "ZZZZZZZY", "ZZZZZZZX", "ZZZZZZZW", "ZZZZZZZV",
            // no U
            "ZZZZZZZT", "ZZZZZZZS", "ZZZZZZZR", "ZZZZZZZQ", "ZZZZZZZP",
        ];

        for n in targets {
            let encoded = BranchVersion(n).encode();

            if n < 100 {
                assert_eq!(encoded, encodings[n as usize]);
            }
            if n == BranchVersion::MAX_VERSION_NUMBER {
                assert_eq!(encoded, "00000000");
            }

            let round = BranchVersion::decode(encoded.as_str())?;
            assert_eq!(round, BranchVersion(n));
        }
        Ok(())
    }

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
        let mem_storage = new_in_memory_storage().unwrap();
        let res1 = f(Arc::clone(&mem_storage) as Arc<dyn Storage + Send + Sync>).await;

        let dir = tempdir().expect("cannot create temp dir");
        let local_storage = new_local_filesystem_storage(dir.path())
            .expect("Cannot create local Storage");

        let res2 = f(Arc::clone(&local_storage) as Arc<dyn Storage + Send + Sync>).await;
        ((mem_storage, res1), (local_storage, res2, dir))
    }

    #[tokio::test]
    async fn test_refs() -> Result<(), Box<dyn std::error::Error>> {
        let ((_,res1),(_,res2,_)) = with_test_storages::<Result<(), Box<dyn std::error::Error>>, _, _>(|storage|  async move {
            let storage_settings =storage.default_settings();
            let s1 = SnapshotId::random();
            let s2 = SnapshotId::random();

            let res = fetch_tag(storage.as_ref(), &storage_settings, "tag1").await;
            assert!(matches!(res, Err(RefError::RefNotFound(name)) if name == *"tag1"));
            assert_eq!(list_refs(storage.as_ref(), &storage_settings).await?, HashSet::new());

            create_tag(storage.as_ref(), &storage_settings, "tag1", s1.clone(), false).await?;
            create_tag(storage.as_ref(), &storage_settings, "tag2", s2.clone(), false).await?;

            let res = fetch_tag(storage.as_ref(), &storage_settings, "tag1").await?;
            assert_eq!(res.snapshot, s1);

            assert_eq!(
                fetch_tag(storage.as_ref(), &storage_settings, "tag1").await?,
                fetch_ref(storage.as_ref(), &storage_settings, "tag1").await?.1
            );

            let res = fetch_tag(storage.as_ref(), &storage_settings, "tag2").await?;
            assert_eq!(res.snapshot, s2);

            assert_eq!(
                fetch_tag(storage.as_ref(), &storage_settings, "tag2").await?,
                fetch_ref(storage.as_ref(), &storage_settings, "tag2").await?.1
            );

            assert_eq!(
                list_refs(storage.as_ref(), &storage_settings).await?,
                HashSet::from([Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())])
            );

            // attempts to recreate a tag fail
            assert!(matches!(
                create_tag(storage.as_ref(), &storage_settings, "tag1", s1.clone(), false).await,
                    Err(RefError::TagAlreadyExists(name)) if name == *"tag1"
            ));
            assert_eq!(
                list_refs(storage.as_ref(), &storage_settings).await?,
                HashSet::from([Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())])
            );

            // attempting to create a branch that doesn't exist, with a fake parent
            let res =
                update_branch(storage.as_ref(), &storage_settings, "branch0", s1.clone(), Some(&s2), false)
                    .await;
            assert!(res.is_err());
            assert_eq!(
                list_refs(storage.as_ref(), &storage_settings).await?,
                HashSet::from([Ref::Tag("tag1".to_string()), Ref::Tag("tag2".to_string())])
            );

            // create a branch successfully
            update_branch(storage.as_ref(), &storage_settings, "branch1", s1.clone(), None, false).await?;

            assert_eq!(
                branch_history(storage.as_ref(), &storage_settings, "branch1")
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?,
                vec![BranchVersion(0)]
            );
            assert_eq!(
                last_branch_version(storage.as_ref(), &storage_settings, "branch1").await?,
                BranchVersion(0)
            );
            assert_eq!(
                fetch_branch(storage.as_ref(), &storage_settings, "branch1", &BranchVersion(0)).await?,
                RefData { snapshot: s1.clone() }
            );
            assert_eq!(
                fetch_branch(storage.as_ref(), &storage_settings, "branch1", &BranchVersion(0)).await?,
                fetch_ref(storage.as_ref(), &storage_settings, "branch1").await?.1
            );

            assert_eq!(
                list_refs(storage.as_ref(), &storage_settings).await?,
                HashSet::from([
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
                false,
            )
            .await?;

            assert_eq!(
                branch_history(storage.as_ref(), &storage_settings, "branch1")
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?,
                vec![BranchVersion(1), BranchVersion(0)]
            );
            assert_eq!(
                last_branch_version(storage.as_ref(), &storage_settings, "branch1").await?,
                BranchVersion(1)
            );

            assert_eq!(
                fetch_branch(storage.as_ref(), &storage_settings, "branch1", &BranchVersion(1)).await?,
                RefData { snapshot: s2.clone() }
            );

            assert_eq!(
                fetch_branch(storage.as_ref(), &storage_settings, "branch1", &BranchVersion(1)).await?,
                fetch_ref(storage.as_ref(), &storage_settings, "branch1").await?.1
            );

            let sid = SnapshotId::random();
            // update a branch with the wrong parent
            let res =
                update_branch(storage.as_ref(), &storage_settings, "branch1", sid.clone(), Some(&s1), false)
                    .await;
            assert!(matches!(res,
                    Err(RefError::Conflict { expected_parent, actual_parent })
                if expected_parent == Some(s1.clone()) && actual_parent == Some(s2.clone())
            ));

            // update the branch again but now with the right parent
            update_branch(storage.as_ref(), &storage_settings, "branch1", sid.clone(), Some(&s2), false)
                .await?;

            assert_eq!(
                branch_history(storage.as_ref(), &storage_settings, "branch1")
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?,
                vec![BranchVersion(2), BranchVersion(1), BranchVersion(0)]
            );
            assert_eq!(
                last_branch_version(storage.as_ref(), &storage_settings, "branch1").await?,
                BranchVersion(2)
            );

            assert_eq!(
                fetch_branch(storage.as_ref(), &storage_settings, "branch1", &BranchVersion(2)).await?,
                fetch_ref(storage.as_ref(), &storage_settings, "branch1").await?.1
            );

            assert_eq!(
                fetch_ref(storage.as_ref(), &storage_settings, "branch1").await?,
                (Ref::Branch("branch1".to_string()), RefData { snapshot: sid.clone() })
            );

            // delete a branch
            delete_branch(storage.as_ref(), &storage_settings, "branch1").await?;
            assert!(matches!(
                fetch_ref(storage.as_ref(), &storage_settings, "branch1").await,
                Err(RefError::RefNotFound(name)) if name == "branch1"
            ));

            Ok(())
        }).await;
        res1?;
        res2?;
        Ok(())
    }
}
