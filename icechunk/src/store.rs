use std::{
    collections::HashSet,
    fmt::Display,
    iter,
    num::NonZeroU64,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{de, Deserialize, Serialize};
use serde_with::{serde_as, TryFromInto};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{
    format::{
        manifest::{ChunkPayload, VirtualChunkRef},
        snapshot::{NodeData, NodeSnapshot, UserAttributesSnapshot, ZarrArrayMetadata},
        ByteRange, ChunkIndices, ChunkOffset, IcechunkFormatError, Path, PathError,
    },
    metadata::{
        ArrayShape, ChunkKeyEncoding, ChunkShape, Codec, DataType, DimensionNames,
        FillValue, StorageTransformer, UserAttributes,
    },
    refs::RefError,
    repository::RepositoryError,
    session::{get_chunk, is_prefix_match, Session, SessionError},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ListDirItem {
    Key(String),
    Prefix(String),
}

pub type StoreResult<A> = Result<A, StoreError>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum KeyNotFoundError {
    #[error("chunk cannot be find for key `{key}`")]
    ChunkNotFound { key: String, path: Path, coords: ChunkIndices },
    #[error("node not found at `{path}`")]
    NodeNotFound { path: Path },
    #[error("v2 key not found at `{key}`")]
    ZarrV2KeyNotFound { key: String },
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    #[error("invalid zarr key format `{key}`")]
    InvalidKey { key: String },
    #[error("this operation is not allowed: {0}")]
    NotAllowed(String),
    #[error("object not found: `{0}`")]
    NotFound(#[from] KeyNotFoundError),
    #[error("unsuccessful session operation: `{0}`")]
    SessionError(#[from] SessionError),
    #[error("unsuccessful repository operation: `{0}`")]
    RepositoryError(#[from] RepositoryError),
    #[error("error merging stores: `{0}`")]
    MergeError(String),
    #[error("unsuccessful ref operation: `{0}`")]
    RefError(#[from] RefError),
    #[error("cannot commit when no snapshot is present")]
    NoSnapshot,
    #[error("could not create path from prefix")]
    PathError(#[from] PathError),
    #[error("all commits must be made on a branch")]
    NotOnBranch,
    #[error("bad metadata: `{0}`")]
    BadMetadata(#[from] serde_json::Error),
    #[error("deserialization error: `{0}`")]
    DeserializationError(#[from] rmp_serde::decode::Error),
    #[error("serialization error: `{0}`")]
    SerializationError(#[from] rmp_serde::encode::Error),
    #[error("store method `{0}` is not implemented by Icechunk")]
    Unimplemented(&'static str),
    #[error("bad key prefix: `{0}`")]
    BadKeyPrefix(String),
    #[error("error during parallel execution of get_partial_values")]
    PartialValuesPanic,
    #[error("cannot write to read-only store")]
    ReadOnly,
    #[error(
        "uncommitted changes in repository, commit changes or reset repository and try again."
    )]
    UncommittedChanges,
    #[error(
        "invalid chunk location, no matching virtual chunk container: `{chunk_location}`"
    )]
    InvalidVirtualChunkContainer { chunk_location: String },
    #[error("unknown store error: `{0}`")]
    Unknown(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Clone)]
pub struct Store {
    session: Arc<RwLock<Session>>,
    get_partial_values_concurrency: u16,
}

impl Store {
    pub async fn from_session(session: Arc<RwLock<Session>>) -> Self {
        let conc = session.read().await.config().get_partial_values_concurrency;
        Self::from_session_and_config(session, conc)
    }

    pub fn from_session_and_config(
        session: Arc<RwLock<Session>>,
        get_partial_values_concurrency: u16,
    ) -> Self {
        Self { session, get_partial_values_concurrency }
    }

    pub fn from_bytes(bytes: Bytes) -> StoreResult<Self> {
        let session: Session = rmp_serde::from_slice(&bytes).map_err(StoreError::from)?;
        let conc = session.config().get_partial_values_concurrency;
        Ok(Self::from_session_and_config(Arc::new(RwLock::new(session)), conc))
    }

    pub async fn as_bytes(&self) -> StoreResult<Bytes> {
        let session = self.session.write().await;
        let bytes = rmp_serde::to_vec(session.deref()).map_err(StoreError::from)?;
        Ok(Bytes::from(bytes))
    }

    pub fn session(&self) -> Arc<RwLock<Session>> {
        Arc::clone(&self.session)
    }

    pub async fn read_only(&self) -> bool {
        self.session.read().await.read_only()
    }

    pub async fn is_empty(&self, prefix: &str) -> StoreResult<bool> {
        Ok(self.list_dir(prefix).await?.next().await.is_none())
    }

    pub async fn clear(&self) -> StoreResult<()> {
        let mut repo = self.session.write().await;
        Ok(repo.clear().await?)
    }

    pub async fn get(&self, key: &str, byte_range: &ByteRange) -> StoreResult<Bytes> {
        let repo = self.session.read().await;
        get_key(key, byte_range, &repo).await
    }

    /// Get all the requested keys concurrently.
    ///
    /// Returns a vector of the results, in the same order as the keys passed. Errors retrieving
    /// individual keys will be flagged in the inner [`StoreResult`].
    ///
    /// The outer [`StoreResult`] is used to flag a global failure and it could be [`StoreError::PartialValuesPanic`].
    ///
    /// Currently this function is using concurrency but not parallelism. To limit the number of
    /// concurrent tasks use the Store config value `get_partial_values_concurrency`.
    pub async fn get_partial_values(
        &self,
        key_ranges: impl IntoIterator<Item = (String, ByteRange)>,
    ) -> StoreResult<Vec<StoreResult<Bytes>>> {
        // TODO: prototype argument
        //
        // There is a challenges implementing this function: async rust is not well prepared to
        // do scoped tasks. We want to spawn parallel tasks for each key_range, but spawn requires
        // a `'static` `Future`. Since the `Future` needs `&self`, it cannot be `'static`. One
        // solution would be to wrap `self` in an Arc, but that makes client code much more
        // complicated.
        //
        // This [excellent post](https://without.boats/blog/the-scoped-task-trilemma/) explains why something like this is not currently achievable:
        // [Here](https://github.com/tokio-rs/tokio/issues/3162) is a a tokio thread explaining this cannot be done with current Rust.
        //
        // The compromise we found is using [`Stream::for_each_concurrent`]. This achieves the
        // borrowing and the concurrency but not the parallelism. So all the concurrent tasks will
        // execute on the same thread. This is not as bad as it sounds, since most of this will be
        // IO bound.

        let stream = futures::stream::iter(key_ranges);
        let results = Arc::new(Mutex::new(Vec::new()));
        let num_keys = AtomicUsize::new(0);
        stream
            .for_each_concurrent(
                self.get_partial_values_concurrency as usize,
                |(key, range)| {
                    let index = num_keys.fetch_add(1, Ordering::Release);
                    let results = Arc::clone(&results);
                    async move {
                        let value = self.get(&key, &range).await;
                        if let Ok(mut results) = results.lock() {
                            if index >= results.len() {
                                results.resize_with(index + 1, || None);
                            }
                            results[index] = Some(value);
                        }
                    }
                },
            )
            .await;

        let results = Arc::into_inner(results)
            .ok_or(StoreError::PartialValuesPanic)?
            .into_inner()
            .map_err(|_| StoreError::PartialValuesPanic)?;

        debug_assert!(results.len() == num_keys.into_inner());
        let res: Option<Vec<_>> = results.into_iter().collect();
        res.ok_or(StoreError::PartialValuesPanic)
    }

    pub async fn exists(&self, key: &str) -> StoreResult<bool> {
        let guard = self.session.read().await;
        exists(key, guard.deref()).await
    }

    pub fn supports_writes(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub fn supports_deletes(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub async fn set(&self, key: &str, value: Bytes) -> StoreResult<()> {
        if self.read_only().await {
            return Err(StoreError::ReadOnly);
        }

        self.set_with_optional_locking(key, value, None).await
    }

    async fn set_with_optional_locking(
        &self,
        key: &str,
        value: Bytes,
        locked_session: Option<&mut Session>,
    ) -> StoreResult<()> {
        if let Some(session) = locked_session.as_ref() {
            if session.read_only() {
                return Err(StoreError::ReadOnly);
            }
        } else if self.read_only().await {
            return Err(StoreError::ReadOnly);
        }

        match Key::parse(key)? {
            Key::Metadata { node_path } => {
                if let Ok(array_meta) = serde_json::from_slice(value.as_ref()) {
                    self.set_array_meta(node_path, array_meta, locked_session).await
                } else {
                    match serde_json::from_slice(value.as_ref()) {
                        Ok(group_meta) => {
                            self.set_group_meta(node_path, group_meta, locked_session)
                                .await
                        }
                        Err(err) => Err(StoreError::BadMetadata(err)),
                    }
                }
            }
            Key::Chunk { node_path, coords } => {
                match locked_session {
                    Some(session) => {
                        let writer = session.get_chunk_writer();
                        let payload = writer(value).await?;
                        session.set_chunk_ref(node_path, coords, Some(payload)).await?
                    }
                    None => {
                        // we only lock the repository to get the writer
                        let writer = self.session.read().await.get_chunk_writer();
                        // then we can write the bytes without holding the lock
                        let payload = writer(value).await?;
                        // and finally we lock for write and update the reference
                        self.session
                            .write()
                            .await
                            .set_chunk_ref(node_path, coords, Some(payload))
                            .await?
                    }
                }
                Ok(())
            }
            Key::ZarrV2(_) => Err(StoreError::Unimplemented(
                "Icechunk cannot set Zarr V2 metadata keys",
            )),
        }
    }

    pub async fn set_if_not_exists(&self, key: &str, value: Bytes) -> StoreResult<()> {
        let mut guard = self.session.write().await;
        if exists(key, guard.deref()).await? {
            Ok(())
        } else {
            self.set_with_optional_locking(key, value, Some(guard.deref_mut())).await
        }
    }

    // alternate API would take array path, and a mapping from string coord to ChunkPayload
    pub async fn set_virtual_ref(
        &self,
        key: &str,
        reference: VirtualChunkRef,
        validate_container: bool,
    ) -> StoreResult<()> {
        if self.read_only().await {
            return Err(StoreError::ReadOnly);
        }

        match Key::parse(key)? {
            Key::Chunk { node_path, coords } => {
                let mut session = self.session.write().await;
                if validate_container
                    && session.matching_container(&reference.location).is_none()
                {
                    return Err(StoreError::InvalidVirtualChunkContainer {
                        chunk_location: reference.location.0,
                    });
                }
                session
                    .set_chunk_ref(
                        node_path,
                        coords,
                        Some(ChunkPayload::Virtual(reference)),
                    )
                    .await?;
                Ok(())
            }
            Key::Metadata { .. } | Key::ZarrV2(_) => Err(StoreError::NotAllowed(
                format!("use .set to modify metadata for key {}", key),
            )),
        }
    }

    pub async fn delete_dir(&self, prefix: &str) -> StoreResult<()> {
        if self.read_only().await {
            return Err(StoreError::ReadOnly);
        }
        let prefix = prefix.trim_start_matches("/").trim_end_matches("/");
        // TODO: Handling preceding "/" is ugly!
        let path = format!("/{}", prefix)
            .try_into()
            .map_err(|_| StoreError::BadKeyPrefix(prefix.to_owned()))?;

        let mut guard = self.session.write().await;
        let node = guard.get_node(&path).await;
        match node {
            Ok(node) => Ok(guard.deref_mut().delete_node(node).await?),
            Err(SessionError::NodeNotFound { .. }) => {
                // other cases are
                // 1. delete("/path/to/array/c")
                // 2. delete("/path/to/array/c/0/0")
                // 3. delete("/path/to/arr") or "/not-an-array-yet"
                // 4. delete("non-existent-prefix")
                // for cases 1, 2 we can find an ancestor array node and filter its chunks
                // for cases 3, 4 this call is a no-op
                let node = guard.get_closest_ancestor_node(&path).await;
                if let Ok(NodeSnapshot {
                    path: node_path,
                    node_data: NodeData::Array(..),
                    ..
                }) = node
                {
                    let node_path = node_path.clone();
                    let to_delete = guard
                        .array_chunk_iterator(&node_path)
                        .await
                        .try_filter_map(|chunk| async {
                            let coords = chunk.coord.clone();
                            let chunk_key = Key::Chunk {
                                node_path: node_path.clone(),
                                coords: chunk.coord,
                            };
                            let res = if is_prefix_match(&chunk_key.to_string(), prefix) {
                                Some(coords)
                            } else {
                                None
                            };
                            Ok(res)
                        })
                        .try_collect::<Vec<_>>()
                        .await?;
                    Ok(guard
                        .deref_mut()
                        .delete_chunks(&node_path, to_delete.into_iter())
                        .await?)
                } else {
                    // for cases 3, 4 this is a no-op
                    Ok(())
                }
            }
            Err(err) => Err(err)?,
        }
    }

    pub async fn delete(&self, key: &str) -> StoreResult<()> {
        // we need to hold the lock while we do the node search and the write
        // to avoid race conditions with other writers
        // (remember this method takes &self and not &mut self)
        let mut session = self.session.write().await;
        match Key::parse(key)? {
            Key::Metadata { node_path } => {
                let node = session.get_node(&node_path).await;

                // When there is no node at the given key, we don't consider it an error, instead we just do nothing
                if let Err(SessionError::NodeNotFound { path: _, message: _ }) = node {
                    return Ok(());
                };
                Ok(session.delete_node(node.map_err(StoreError::SessionError)?).await?)
            }
            Key::Chunk { node_path, coords } => {
                Ok(session.delete_chunks(&node_path, vec![coords].into_iter()).await?)
            }
            Key::ZarrV2(_) => Ok(()),
        }
    }

    pub fn supports_partial_writes(&self) -> StoreResult<bool> {
        Ok(false)
    }

    pub async fn set_partial_values(
        &self,
        _key_start_values: impl IntoIterator<Item = (&str, ChunkOffset, Bytes)>,
    ) -> StoreResult<()> {
        if self.read_only().await {
            return Err(StoreError::ReadOnly);
        }

        Err(StoreError::Unimplemented("set_partial_values"))
    }

    pub fn supports_listing(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub async fn list(
        &self,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + Send> {
        self.list_prefix("/").await
    }

    pub async fn list_prefix(
        &self,
        prefix: &str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + Send> {
        // TODO: this is inefficient because it filters based on the prefix, instead of only
        // generating items that could potentially match
        let meta = self.list_metadata_prefix(prefix, false).await?;
        let chunks = self.list_chunks_prefix(prefix).await?;
        // FIXME: this is wrong, we are realizing all keys in memory
        // it should be lazy instead
        Ok(futures::stream::iter(meta.chain(chunks).collect::<Vec<_>>().await))
    }

    pub async fn list_dir(
        &self,
        prefix: &str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + Send> {
        let res = self.list_dir_items(prefix).await?.map_ok(|item| match item {
            ListDirItem::Key(k) => k,
            ListDirItem::Prefix(p) => p,
        });
        Ok(res)
    }

    pub async fn list_dir_items(
        &self,
        prefix: &str,
    ) -> StoreResult<impl Stream<Item = StoreResult<ListDirItem>> + Send> {
        let prefix = prefix.trim_end_matches("/");
        let absolute_prefix =
            if !prefix.starts_with("/") { &format!("/{}", prefix) } else { prefix };

        let path = Path::try_from(absolute_prefix)?;
        let session = Arc::clone(&self.session).read_owned().await;
        let results = match session.get_node(&path).await {
            Ok(NodeSnapshot { node_data: NodeData::Array(..), .. }) => {
                // if this is an array we know what to yield
                vec![
                    ListDirItem::Key("zarr.json".to_string()),
                    // The `c` prefix will exist when an array was created and a chunk was written
                    // either (1) in the changeset or (2) as part of a previous snapshot.
                    // It will not exist if (3) the array is recorded as deleted in the change_set.
                    // We ignore these details and simply return "c/" always
                    ListDirItem::Prefix("c".to_string()),
                ]
            }
            Ok(NodeSnapshot { node_data: NodeData::Group, .. }) => {
                // if the prefix is the path to a group we need to discover any nodes with the prefix as node path
                // listing chunks is unnecessary
                self.list_metadata_prefix(prefix, true)
                    .await?
                    .try_filter_map(|x| async move {
                        let x = x.trim_end_matches("/zarr.json").to_string();
                        let res = if x == "zarr.json" {
                            Some(ListDirItem::Key("zarr.json".to_string()))
                        } else if x.matches("/").count() == 0 {
                            Some(ListDirItem::Prefix(x))
                        } else {
                            None
                        };
                        Ok(res)
                    })
                    .try_collect::<Vec<_>>()
                    .await?
            }
            Err(_) => {
                // Finally if there is no node at the prefix we look for chunks belonging to an ancestor node
                // FIXME: This iterates over every chunk of the array and is wasteful
                let node = session.get_closest_ancestor_node(&path).await;
                if let Ok(node) = node {
                    let node_path = node.path.clone();
                    session
                        .array_chunk_iterator(&node.path)
                        .await
                        .try_filter_map(|chunk| async {
                            let chunk_key = Key::Chunk {
                                node_path: node_path.clone(),
                                coords: chunk.coord,
                            }
                            .to_string();
                            let res = if is_prefix_match(&chunk_key, prefix) {
                                {
                                    let trimmed = chunk_key
                                        .trim_start_matches(prefix)
                                        .trim_start_matches("/");
                                    if trimmed.is_empty() {
                                        // we were provided with a prefix that is a path to a chunk key
                                        None
                                    } else if let Some((chunk_prefix, _)) =
                                        // if we can split it, this is a valid prefix to return
                                        trimmed.split_once("/")
                                    {
                                        Some(ListDirItem::Prefix(
                                            chunk_prefix.to_string(),
                                        ))
                                    } else {
                                        // if the prefix matches, and we can't split it
                                        // this is a chunk key result that must be returned
                                        Some(ListDirItem::Key(trimmed.to_string()))
                                    }
                                }
                            } else {
                                None
                            };
                            Ok(res)
                        })
                        .try_collect::<HashSet<_>>()
                        .await?
                        .into_iter()
                        .collect::<Vec<_>>()
                } else {
                    // no ancestor node found, this is a bad prefix, return nothing
                    vec![]
                }
            }
        };
        Ok(futures::stream::iter(results.into_iter().map(Ok)))
    }

    async fn set_array_meta(
        &self,
        path: Path,
        array_meta: ArrayMetadata,
        locked_repo: Option<&mut Session>,
    ) -> Result<(), StoreError> {
        match locked_repo {
            Some(repo) => set_array_meta(path, array_meta, repo).await,
            None => self.set_array_meta_locking(path, array_meta).await,
        }
    }

    async fn set_array_meta_locking(
        &self,
        path: Path,
        array_meta: ArrayMetadata,
    ) -> Result<(), StoreError> {
        // we need to hold the lock while we search the array and do the update to avoid race
        // conditions with other writers (notice we don't take &mut self)
        let mut guard = self.session.write().await;
        set_array_meta(path, array_meta, guard.deref_mut()).await
    }

    async fn set_group_meta(
        &self,
        path: Path,
        group_meta: GroupMetadata,
        locked_repo: Option<&mut Session>,
    ) -> Result<(), StoreError> {
        match locked_repo {
            Some(repo) => set_group_meta(path, group_meta, repo).await,
            None => self.set_group_meta_locking(path, group_meta).await,
        }
    }

    async fn set_group_meta_locking(
        &self,
        path: Path,
        group_meta: GroupMetadata,
    ) -> Result<(), StoreError> {
        // we need to hold the lock while we search the array and do the update to avoid race
        // conditions with other writers (notice we don't take &mut self)
        let mut guard = self.session.write().await;
        set_group_meta(path, group_meta, guard.deref_mut()).await
    }

    async fn list_metadata_prefix<'a, 'b: 'a>(
        &'a self,
        prefix: &'b str,
        strip_prefix: bool,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + 'a> {
        let prefix = prefix.trim_end_matches('/');
        let res = try_stream! {
            let repository = Arc::clone(&self.session).read_owned().await;
            for node in repository.list_nodes().await? {
                // TODO: handle non-utf8?
                let meta_key = Key::Metadata { node_path: node.path }.to_string();
                if is_prefix_match(&meta_key, prefix) {
                    if strip_prefix {
                        yield meta_key.trim_start_matches(prefix).trim_start_matches("/").to_string();
                    } else {
                        yield meta_key;
                    }
                }
            }
        };
        Ok(res)
    }

    async fn list_chunks_prefix<'a, 'b: 'a>(
        &'a self,
        prefix: &'b str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + 'a> {
        let prefix = prefix.trim_end_matches('/');
        let res = try_stream! {
            let repository = Arc::clone(&self.session).read_owned().await;
            // TODO: this is inefficient because it filters based on the prefix, instead of only
            // generating items that could potentially match
            for await maybe_path_chunk in repository.all_chunks().await.map_err(StoreError::SessionError)? {
                // FIXME: utf8 handling
                match maybe_path_chunk {
                    Ok((path, chunk)) => {
                        let chunk_key = Key::Chunk { node_path: path, coords: chunk.coord }.to_string();
                        if is_prefix_match(&chunk_key, prefix) {
                            yield chunk_key;
                        }
                    }
                    Err(err) => Err(err)?
                }
            }
        };
        Ok(res)
    }
}

async fn set_array_meta(
    path: Path,
    array_meta: ArrayMetadata,
    repo: &mut Session,
) -> StoreResult<()> {
    // TODO: Consider deleting all this logic?
    // We try hard to not overwrite existing metadata here.
    // I don't think this is actually useful because Zarr's
    // Array/Group API will require that the user set `overwrite=True`
    // which will delete any existing array metadata. This path is only
    // applicable when using the explicit `store.set` interface.
    if let Ok(node) = repo.get_array(&path).await {
        // Check if the user attributes are different, if they are update them
        let existing_attrs = match node.user_attributes {
            None => None,
            Some(UserAttributesSnapshot::Inline(atts)) => Some(atts),
            // FIXME: implement
            Some(UserAttributesSnapshot::Ref(_)) => None,
        };

        if existing_attrs != array_meta.attributes {
            repo.set_user_attributes(path.clone(), array_meta.attributes).await?;
        }

        // Check if the zarr metadata is different, if it is update it
        if let NodeData::Array(existing_array_metadata, _) = node.node_data {
            if existing_array_metadata != array_meta.zarr_metadata {
                repo.update_array(path, array_meta.zarr_metadata).await?;
            }
        } else {
            // This should be unreachable, but just in case...
            repo.update_array(path, array_meta.zarr_metadata).await?;
        }

        Ok(())
    } else {
        repo.add_array(path.clone(), array_meta.zarr_metadata).await?;
        repo.set_user_attributes(path, array_meta.attributes).await?;
        Ok(())
    }
}

async fn set_group_meta(
    path: Path,
    group_meta: GroupMetadata,
    repo: &mut Session,
) -> StoreResult<()> {
    // we need to hold the lock while we search the group and do the update to avoid race
    // conditions with other writers (notice we don't take &mut self)
    //
    if repo.get_group(&path).await.is_ok() {
        repo.set_user_attributes(path, group_meta.attributes).await?;
        Ok(())
    } else {
        repo.add_group(path.clone()).await?;
        repo.set_user_attributes(path, group_meta.attributes).await?;
        Ok(())
    }
}

async fn get_metadata(
    _key: &str,
    path: &Path,
    range: &ByteRange,
    repo: &Session,
) -> StoreResult<Bytes> {
    let node = repo.get_node(path).await.map_err(|_| {
        StoreError::NotFound(KeyNotFoundError::NodeNotFound { path: path.clone() })
    })?;
    let user_attributes = match node.user_attributes {
        None => None,
        Some(UserAttributesSnapshot::Inline(atts)) => Some(atts),
        // FIXME: implement
        #[allow(clippy::unimplemented)]
        Some(UserAttributesSnapshot::Ref(_)) => unimplemented!(),
    };
    let full_metadata = match node.node_data {
        NodeData::Group => {
            Ok::<Bytes, StoreError>(GroupMetadata::new(user_attributes).to_bytes())
        }
        NodeData::Array(zarr_metadata, _) => {
            Ok(ArrayMetadata::new(user_attributes, zarr_metadata).to_bytes())
        }
    }?;

    Ok(range.slice(full_metadata))
}

async fn get_chunk_bytes(
    key: &str,
    path: Path,
    coords: ChunkIndices,
    byte_range: &ByteRange,
    repo: &Session,
) -> StoreResult<Bytes> {
    let reader = repo.get_chunk_reader(&path, &coords, byte_range).await?;

    // then we can fetch the bytes without holding the lock
    let chunk = get_chunk(reader).await?;
    chunk.ok_or(StoreError::NotFound(KeyNotFoundError::ChunkNotFound {
        key: key.to_string(),
        path,
        coords,
    }))
}

async fn get_key(
    key: &str,
    byte_range: &ByteRange,
    repo: &Session,
) -> StoreResult<Bytes> {
    let bytes = match Key::parse(key)? {
        Key::Metadata { node_path } => {
            get_metadata(key, &node_path, byte_range, repo).await
        }
        Key::Chunk { node_path, coords } => {
            get_chunk_bytes(key, node_path, coords, byte_range, repo).await
        }
        Key::ZarrV2(key) => {
            Err(StoreError::NotFound(KeyNotFoundError::ZarrV2KeyNotFound { key }))
        }
    }?;

    Ok(bytes)
}

async fn exists(key: &str, repo: &Session) -> StoreResult<bool> {
    match get_key(key, &ByteRange::ALL, repo).await {
        Ok(_) => Ok(true),
        Err(StoreError::NotFound(_)) => Ok(false),
        Err(StoreError::SessionError(SessionError::NodeNotFound {
            path: _,
            message: _,
        })) => Ok(false),
        Err(other_error) => Err(other_error),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Key {
    Metadata { node_path: Path },
    Chunk { node_path: Path, coords: ChunkIndices },
    ZarrV2(String),
}

impl Key {
    const ROOT_KEY: &'static str = "zarr.json";
    const METADATA_SUFFIX: &'static str = "/zarr.json";
    const CHUNK_COORD_PREFIX: &'static str = "c";

    fn parse(key: &str) -> Result<Self, StoreError> {
        fn parse_chunk(key: &str) -> Result<Key, StoreError> {
            if key == ".zgroup"
                || key == ".zarray"
                || key == ".zattrs"
                || key == ".zmetadata"
                || key.ends_with("/.zgroup")
                || key.ends_with("/.zarray")
                || key.ends_with("/.zattrs")
                || key.ends_with("/.zmetadata")
            {
                return Ok(Key::ZarrV2(key.to_string()));
            }

            if key == "c" {
                return Ok(Key::Chunk {
                    node_path: Path::root(),
                    coords: ChunkIndices(vec![]),
                });
            }
            if let Some((path, coords)) = key.rsplit_once(Key::CHUNK_COORD_PREFIX) {
                let path = path.strip_suffix('/').unwrap_or(path);
                if coords.is_empty() {
                    Ok(Key::Chunk {
                        node_path: format!("/{path}").try_into().map_err(|_| {
                            StoreError::InvalidKey { key: key.to_string() }
                        })?,
                        coords: ChunkIndices(vec![]),
                    })
                } else {
                    let absolute = format!("/{path}")
                        .try_into()
                        .map_err(|_| StoreError::InvalidKey { key: key.to_string() })?;
                    coords
                        .strip_prefix('/')
                        .ok_or(StoreError::InvalidKey { key: key.to_string() })?
                        .split('/')
                        .map(|s| s.parse::<u32>())
                        .collect::<Result<Vec<_>, _>>()
                        .map(|coords| Key::Chunk {
                            node_path: absolute,
                            coords: ChunkIndices(coords),
                        })
                        .map_err(|_| StoreError::InvalidKey { key: key.to_string() })
                }
            } else {
                Err(StoreError::InvalidKey { key: key.to_string() })
            }
        }

        if key == Key::ROOT_KEY {
            Ok(Key::Metadata { node_path: Path::root() })
        } else if let Some(path) = key.strip_suffix(Key::METADATA_SUFFIX) {
            // we need to be careful indexing into utf8 strings
            Ok(Key::Metadata {
                node_path: format!("/{path}")
                    .try_into()
                    .map_err(|_| StoreError::InvalidKey { key: key.to_string() })?,
            })
        } else {
            parse_chunk(key)
        }
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::Metadata { node_path } => {
                let s =
                    format!("{}{}", &node_path.to_string()[1..], Key::METADATA_SUFFIX)
                        .trim_start_matches('/')
                        .to_string();
                f.write_str(s.as_str())
            }
            Key::Chunk { node_path, coords } => {
                let coords = coords.0.iter().map(|c| c.to_string()).join("/");
                let s = [node_path.to_string()[1..].to_string(), "c".to_string(), coords]
                    .iter()
                    .filter(|s| !s.is_empty())
                    .join("/");
                f.write_str(s.as_str())
            }
            Key::ZarrV2(key) => f.write_str(key.as_str()),
        }
    }
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct ArrayMetadata {
    zarr_format: u8,
    #[serde(deserialize_with = "validate_array_node_type")]
    node_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    attributes: Option<UserAttributes>,
    #[serde(flatten)]
    #[serde_as(as = "TryFromInto<ZarrArrayMetadataSerialzer>")]
    zarr_metadata: ZarrArrayMetadata,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct ZarrArrayMetadataSerialzer {
    pub shape: ArrayShape,
    pub data_type: DataType,

    #[serde_as(as = "TryFromInto<NameConfigSerializer>")]
    #[serde(rename = "chunk_grid")]
    pub chunk_shape: ChunkShape,

    #[serde_as(as = "TryFromInto<NameConfigSerializer>")]
    pub chunk_key_encoding: ChunkKeyEncoding,
    pub fill_value: serde_json::Value,
    pub codecs: Vec<Codec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_transformers: Option<Vec<StorageTransformer>>,
    // each dimension name can be null in Zarr
    pub dimension_names: Option<DimensionNames>,
}

impl TryFrom<ZarrArrayMetadataSerialzer> for ZarrArrayMetadata {
    type Error = IcechunkFormatError;

    fn try_from(value: ZarrArrayMetadataSerialzer) -> Result<Self, Self::Error> {
        let ZarrArrayMetadataSerialzer {
            shape,
            data_type,
            chunk_shape,
            chunk_key_encoding,
            fill_value,
            codecs,
            storage_transformers,
            dimension_names,
        } = value;
        {
            let fill_value = FillValue::from_data_type_and_json(&data_type, &fill_value)?;
            Ok(ZarrArrayMetadata {
                fill_value,
                shape,
                data_type,
                chunk_shape,
                chunk_key_encoding,
                codecs,
                storage_transformers,
                dimension_names,
            })
        }
    }
}

impl From<ZarrArrayMetadata> for ZarrArrayMetadataSerialzer {
    fn from(value: ZarrArrayMetadata) -> Self {
        let ZarrArrayMetadata {
            shape,
            data_type,
            chunk_shape,
            chunk_key_encoding,
            fill_value,
            codecs,
            storage_transformers,
            dimension_names,
        } = value;
        {
            fn fill_value_to_json(f: FillValue) -> serde_json::Value {
                match f {
                    FillValue::Bool(b) => b.into(),
                    FillValue::Int8(n) => n.into(),
                    FillValue::Int16(n) => n.into(),
                    FillValue::Int32(n) => n.into(),
                    FillValue::Int64(n) => n.into(),
                    FillValue::UInt8(n) => n.into(),
                    FillValue::UInt16(n) => n.into(),
                    FillValue::UInt32(n) => n.into(),
                    FillValue::UInt64(n) => n.into(),
                    FillValue::Float16(f) => {
                        if f.is_nan() {
                            FillValue::NAN_STR.into()
                        } else if f == f32::INFINITY {
                            FillValue::INF_STR.into()
                        } else if f == f32::NEG_INFINITY {
                            FillValue::NEG_INF_STR.into()
                        } else {
                            f.into()
                        }
                    }
                    FillValue::Float32(f) => {
                        if f.is_nan() {
                            FillValue::NAN_STR.into()
                        } else if f == f32::INFINITY {
                            FillValue::INF_STR.into()
                        } else if f == f32::NEG_INFINITY {
                            FillValue::NEG_INF_STR.into()
                        } else {
                            f.into()
                        }
                    }
                    FillValue::Float64(f) => {
                        if f.is_nan() {
                            FillValue::NAN_STR.into()
                        } else if f == f64::INFINITY {
                            FillValue::INF_STR.into()
                        } else if f == f64::NEG_INFINITY {
                            FillValue::NEG_INF_STR.into()
                        } else {
                            f.into()
                        }
                    }
                    FillValue::Complex64(r, i) => ([r, i].as_ref()).into(),
                    FillValue::Complex128(r, i) => ([r, i].as_ref()).into(),
                    FillValue::String(s) => s.into(),
                    FillValue::Bytes(b) => b.into(),
                }
            }

            let fill_value = fill_value_to_json(fill_value);
            ZarrArrayMetadataSerialzer {
                shape,
                data_type,
                chunk_shape,
                chunk_key_encoding,
                codecs,
                storage_transformers,
                dimension_names,
                fill_value,
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct GroupMetadata {
    zarr_format: u8,
    #[serde(deserialize_with = "validate_group_node_type")]
    node_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    attributes: Option<UserAttributes>,
}

fn validate_group_node_type<'de, D>(d: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let value = String::deserialize(d)?;

    if value != "group" {
        return Err(de::Error::invalid_value(
            de::Unexpected::Str(value.as_str()),
            &"the word 'group'",
        ));
    }

    Ok(value)
}

fn validate_array_node_type<'de, D>(d: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let value = String::deserialize(d)?;

    if value != "array" {
        return Err(de::Error::invalid_value(
            de::Unexpected::Str(value.as_str()),
            &"the word 'array'",
        ));
    }

    Ok(value)
}

impl ArrayMetadata {
    fn new(attributes: Option<UserAttributes>, zarr_metadata: ZarrArrayMetadata) -> Self {
        Self { zarr_format: 3, node_type: "array".to_string(), attributes, zarr_metadata }
    }

    fn to_bytes(&self) -> Bytes {
        Bytes::from_iter(
            // We can unpack because it comes from controlled datastructures that can be serialized
            #[allow(clippy::expect_used)]
            serde_json::to_vec(self).expect("bug in ArrayMetadata serialization"),
        )
    }
}

impl GroupMetadata {
    fn new(attributes: Option<UserAttributes>) -> Self {
        Self { zarr_format: 3, node_type: "group".to_string(), attributes }
    }

    fn to_bytes(&self) -> Bytes {
        Bytes::from_iter(
            // We can unpack because it comes from controlled datastructures that can be serialized
            #[allow(clippy::expect_used)]
            serde_json::to_vec(self).expect("bug in GroupMetadata serialization"),
        )
    }
}

#[derive(Serialize, Deserialize)]
struct NameConfigSerializer {
    name: String,
    configuration: serde_json::Value,
}

impl From<ChunkShape> for NameConfigSerializer {
    fn from(value: ChunkShape) -> Self {
        let arr = serde_json::Value::Array(
            value
                .0
                .iter()
                .map(|v| {
                    serde_json::Value::Number(serde_json::value::Number::from(v.get()))
                })
                .collect(),
        );
        let kvs = serde_json::value::Map::from_iter(iter::once((
            "chunk_shape".to_string(),
            arr,
        )));
        Self {
            name: "regular".to_string(),
            configuration: serde_json::Value::Object(kvs),
        }
    }
}

impl TryFrom<NameConfigSerializer> for ChunkShape {
    type Error = &'static str;

    fn try_from(value: NameConfigSerializer) -> Result<Self, Self::Error> {
        match value {
            NameConfigSerializer {
                name,
                configuration: serde_json::Value::Object(kvs),
            } if name == "regular" => {
                let values = kvs
                    .get("chunk_shape")
                    .and_then(|v| v.as_array())
                    .ok_or("cannot parse ChunkShape")?;
                let shape = values
                    .iter()
                    .map(|v| v.as_u64().and_then(|u64| NonZeroU64::try_from(u64).ok()))
                    .collect::<Option<Vec<_>>>()
                    .ok_or("cannot parse ChunkShape")?;
                Ok(ChunkShape(shape))
            }
            _ => Err("cannot parse ChunkShape"),
        }
    }
}

impl From<ChunkKeyEncoding> for NameConfigSerializer {
    fn from(_value: ChunkKeyEncoding) -> Self {
        let kvs = serde_json::value::Map::from_iter(iter::once((
            "separator".to_string(),
            serde_json::Value::String("/".to_string()),
        )));
        Self {
            name: "default".to_string(),
            configuration: serde_json::Value::Object(kvs),
        }
    }
}

impl TryFrom<NameConfigSerializer> for ChunkKeyEncoding {
    type Error = &'static str;

    fn try_from(value: NameConfigSerializer) -> Result<Self, Self::Error> {
        //FIXME: we are hardcoding / as the separator
        match value {
            NameConfigSerializer {
                name,
                configuration: serde_json::Value::Object(kvs),
            } if name == "default" => {
                if let Some("/") =
                    kvs.get("separator").ok_or("cannot parse ChunkKeyEncoding")?.as_str()
                {
                    Ok(ChunkKeyEncoding::Slash)
                } else {
                    Err("cannot parse ChunkKeyEncoding")
                }
            }
            _ => Err("cannot parse ChunkKeyEncoding"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {

    use std::collections::HashMap;

    use crate::{
        repository::VersionInfo, storage::new_in_memory_storage, ObjectStorage,
        Repository,
    };

    use super::*;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    async fn add_group(store: &Store, path: &str) -> StoreResult<()> {
        let bytes = Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#);
        store.set(&format!("{}/zarr.json", path), bytes).await?;
        Ok(())
    }

    async fn add_array_and_chunk(store: &Store, path: &str) -> StoreResult<()> {
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set(&format!("{}/zarr.json", path), zarr_meta.clone()).await?;

        let data = Bytes::copy_from_slice(b"hello");
        store.set(&format!("{}/c/0/1/0", path), data).await?;

        let data = Bytes::copy_from_slice(b"hello");
        store.set(&format!("{}/c/1/1/0", path), data).await?;

        Ok(())
    }

    async fn create_memory_store_repository() -> Repository {
        let storage = new_in_memory_storage().expect("failed to create in-memory store");
        Repository::create(None, storage, HashMap::new()).await.unwrap()
    }

    async fn all_keys(store: &Store) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let version1 = keys(store, "/").await?;
        let mut version2 = store.list().await?.try_collect::<Vec<_>>().await?;
        version2.sort();
        assert_eq!(version1, version2);
        Ok(version1)
    }

    async fn keys(
        store: &Store,
        prefix: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut res = store.list_prefix(prefix).await?.try_collect::<Vec<_>>().await?;
        res.sort();
        Ok(res)
    }

    #[test]
    fn test_parse_key() {
        assert!(matches!(
            Key::parse("zarr.json"),
            Ok(Key::Metadata { node_path}) if node_path.to_string() == "/"
        ));
        assert!(matches!(
            Key::parse("a/zarr.json"),
            Ok(Key::Metadata { node_path }) if node_path.to_string() == "/a"
        ));
        assert!(matches!(
            Key::parse("a/b/c/zarr.json"),
            Ok(Key::Metadata { node_path }) if node_path.to_string() == "/a/b/c"
        ));
        assert!(matches!(
            Key::parse("foo/c"),
            Ok(Key::Chunk { node_path, coords }) if node_path.to_string() == "/foo" && coords == ChunkIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("foo/bar/c"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_string() == "/foo/bar" && coords == ChunkIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("foo/c/1/2/3"),
            Ok(Key::Chunk {
                node_path,
                coords,
            }) if node_path.to_string() == "/foo" && coords == ChunkIndices(vec![1,2,3])
        ));
        assert!(matches!(
            Key::parse("foo/bar/baz/c/1/2/3"),
            Ok(Key::Chunk {
                node_path,
                coords,
            }) if node_path.to_string() == "/foo/bar/baz" && coords == ChunkIndices(vec![1,2,3])
        ));
        assert!(matches!(
            Key::parse("c"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_string() == "/" && coords == ChunkIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("c/0/0"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_string() == "/" && coords == ChunkIndices(vec![0,0])
        ));
        assert!(matches!(
            Key::parse(".zarray"),
            Ok(Key::ZarrV2(s) ) if s == ".zarray"
        ));
        assert!(matches!(
            Key::parse(".zgroup"),
            Ok(Key::ZarrV2(s) ) if s == ".zgroup"
        ));
        assert!(matches!(
            Key::parse(".zattrs"),
            Ok(Key::ZarrV2(s) ) if s == ".zattrs"
        ));
        assert!(matches!(
            Key::parse(".zmetadata"),
            Ok(Key::ZarrV2(s) ) if s == ".zmetadata"
        ));
        assert!(matches!(
            Key::parse("foo/.zgroup"),
            Ok(Key::ZarrV2(s) ) if s == "foo/.zgroup"
        ));
        assert!(matches!(
            Key::parse("foo/bar/.zarray"),
            Ok(Key::ZarrV2(s) ) if s == "foo/bar/.zarray"
        ));
        assert!(matches!(
            Key::parse("foo/.zmetadata"),
            Ok(Key::ZarrV2(s) ) if s == "foo/.zmetadata"
        ));
        assert!(matches!(
            Key::parse("foo/.zattrs"),
            Ok(Key::ZarrV2(s) ) if s == "foo/.zattrs"
        ));
    }

    #[test]
    fn test_format_key() {
        assert_eq!(
            Key::Metadata { node_path: Path::root() }.to_string(),
            "zarr.json".to_string()
        );
        assert_eq!(
            Key::Metadata { node_path: "/a".try_into().unwrap() }.to_string(),
            "a/zarr.json".to_string()
        );
        assert_eq!(
            Key::Metadata { node_path: "/a/b/c".try_into().unwrap() }.to_string(),
            "a/b/c/zarr.json".to_string()
        );
        assert_eq!(
            Key::Chunk { node_path: Path::root(), coords: ChunkIndices(vec![]) }
                .to_string(),
            "c".to_string()
        );
        assert_eq!(
            Key::Chunk { node_path: Path::root(), coords: ChunkIndices(vec![0]) }
                .to_string(),
            "c/0".to_string()
        );
        assert_eq!(
            Key::Chunk { node_path: Path::root(), coords: ChunkIndices(vec![1, 2]) }
                .to_string(),
            "c/1/2".to_string()
        );
        assert_eq!(
            Key::Chunk {
                node_path: "/a".try_into().unwrap(),
                coords: ChunkIndices(vec![])
            }
            .to_string(),
            "a/c".to_string()
        );
        assert_eq!(
            Key::Chunk {
                node_path: "/a".try_into().unwrap(),
                coords: ChunkIndices(vec![1])
            }
            .to_string(),
            "a/c/1".to_string()
        );
        assert_eq!(
            Key::Chunk {
                node_path: "/a".try_into().unwrap(),
                coords: ChunkIndices(vec![1, 2])
            }
            .to_string(),
            "a/c/1/2".to_string()
        );
    }

    #[test]
    fn test_metadata_serialization() {
        assert!(serde_json::from_str::<GroupMetadata>(
            r#"{"zarr_format":3, "node_type":"group"}"#
        )
        .is_ok());
        assert!(serde_json::from_str::<GroupMetadata>(
            r#"{"zarr_format":3, "node_type":"array"}"#
        )
        .is_err());

        assert!(serde_json::from_str::<ArrayMetadata>(
            r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
        )
        .is_ok());
        assert!(serde_json::from_str::<ArrayMetadata>(
            r#"{"zarr_format":3,"node_type":"group","shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
        )
        .is_err());

        // deserialize with nan
        assert!(matches!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"NaN","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float16(n) if n.is_nan()
        ));
        assert!(matches!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"NaN","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float32(n) if n.is_nan()
        ));
        assert!(matches!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float64","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"NaN","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float64(n) if n.is_nan()
        ));

        // deserialize with infinity
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float16(f32::INFINITY)
        );
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float32(f32::INFINITY)
        );
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float64","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float64(f64::INFINITY)
        );

        // deserialize with -infinity
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float16","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"-Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float16(f32::NEG_INFINITY)
        );
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"-Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float32(f32::NEG_INFINITY)
        );
        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                r#"{"zarr_format":3,"node_type":"array","shape":[2,2,2],"data_type":"float64","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":"-Infinity","codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#
            ).unwrap().zarr_metadata.fill_value,
            FillValue::Float64(f64::NEG_INFINITY)
        );

        // infinity roundtrip
        let zarr_meta = ZarrArrayMetadata {
            shape: vec![1, 1, 2],
            data_type: DataType::Float16,
            chunk_shape: ChunkShape(vec![NonZeroU64::new(2).unwrap()]),
            chunk_key_encoding: ChunkKeyEncoding::Slash,
            fill_value: FillValue::Float16(f32::NEG_INFINITY),
            codecs: vec![Codec { name: "mycodec".to_string(), configuration: None }],
            storage_transformers: Some(vec![StorageTransformer {
                name: "mytransformer".to_string(),
                configuration: None,
            }]),
            dimension_names: Some(vec![Some("t".to_string())]),
        };
        let zarr_meta = ArrayMetadata::new(None, zarr_meta);

        assert_eq!(
            serde_json::from_str::<ArrayMetadata>(
                serde_json::to_string(&zarr_meta).unwrap().as_str()
            )
            .unwrap(),
            zarr_meta,
        )
    }

    #[tokio::test]
    async fn test_metadata_set_and_get() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = repo.writable_session("main").await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

        assert!(matches!(
            store.get("zarr.json", &ByteRange::ALL).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound {path})) if path.to_string() == "/"
        ));

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        assert_eq!(
            store.get("zarr.json", &ByteRange::ALL).await.unwrap(),
            Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"group"}"#)
        );

        store.set("a/b/zarr.json", Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group", "attributes": {"spam":"ham", "eggs":42}}"#)).await?;
        assert_eq!(
            store.get("a/b/zarr.json", &ByteRange::ALL).await.unwrap(),
            Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group","attributes":{"eggs":42,"spam":"ham"}}"#
            )
        );

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("a/b/array/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(
            store.get("a/b/array/zarr.json", &ByteRange::ALL).await.unwrap(),
            zarr_meta.clone()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_delete() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = repo.writable_session("main").await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;
        let group_data = br#"{"zarr_format":3, "node_type":"group", "attributes": {"spam":"ham", "eggs":42}}"#;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();

        // delete metadata tests
        store.delete("array/zarr.json").await.unwrap();
        assert!(matches!(
            store.get("array/zarr.json", &ByteRange::ALL).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound { path }))
                if path.to_string() == "/array",
        ));
        // Deleting a non-existent key should not fail
        store.delete("array/zarr.json").await.unwrap();

        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();
        store.delete("array/zarr.json").await.unwrap();
        assert!(matches!(
            store.get("array/zarr.json", &ByteRange::ALL).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound { path } ))
                if path.to_string() == "/array",
        ));
        store.set("array/zarr.json", Bytes::copy_from_slice(group_data)).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_set_and_get() -> Result<(), Box<dyn std::error::Error>> {
        // TODO: turn this test into pure Store operations once we support writes through Zarr
        let repo = create_memory_store_repository().await;
        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(
            store.get("array/zarr.json", &ByteRange::ALL).await.unwrap(),
            zarr_meta
        );
        assert_eq!(
            store.get("array/zarr.json", &ByteRange::to_offset(5)).await.unwrap(),
            zarr_meta[..5]
        );
        assert_eq!(
            store.get("array/zarr.json", &ByteRange::from_offset(5)).await.unwrap(),
            zarr_meta[5..]
        );
        assert_eq!(
            store.get("array/zarr.json", &ByteRange::bounded(1, 24)).await.unwrap(),
            zarr_meta[1..24]
        );

        // a small inline chunk
        let small_data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", small_data.clone()).await?;
        assert_eq!(
            store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(),
            small_data
        );
        assert_eq!(
            store.get("array/c/0/1/0", &ByteRange::to_offset(2)).await.unwrap(),
            small_data[0..2]
        );
        assert_eq!(
            store.get("array/c/0/1/0", &ByteRange::from_offset(3)).await.unwrap(),
            small_data[3..]
        );
        assert_eq!(
            store.get("array/c/0/1/0", &ByteRange::bounded(1, 4)).await.unwrap(),
            small_data[1..4]
        );
        // no new chunks written because it was inline
        // FiXME: add this test
        //assert!(in_mem_storage.chunk_ids().is_empty());

        // a big chunk
        let big_data = Bytes::copy_from_slice(b"hello".repeat(512).as_slice());
        store.set("array/c/0/1/1", big_data.clone()).await?;
        assert_eq!(store.get("array/c/0/1/1", &ByteRange::ALL).await.unwrap(), big_data);
        assert_eq!(
            store.get("array/c/0/1/1", &ByteRange::from_offset(512 - 3)).await.unwrap(),
            big_data[(512 - 3)..]
        );
        assert_eq!(
            store.get("array/c/0/1/1", &ByteRange::to_offset(5)).await.unwrap(),
            big_data[..5]
        );
        assert_eq!(
            store.get("array/c/0/1/1", &ByteRange::bounded(20, 90)).await.unwrap(),
            big_data[20..90]
        );
        // FiXME: add this test
        //let chunk_id = in_mem_storage.chunk_ids().iter().next().cloned().unwrap();
        //assert_eq!(in_mem_storage.fetch_chunk(&chunk_id, &None).await?, big_data);

        let _oid = { ds.write().await.commit("commit", None).await? };

        let ds =
            repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;
        assert_eq!(
            store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(),
            small_data
        );
        assert_eq!(store.get("array/c/0/1/1", &ByteRange::ALL).await.unwrap(), big_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_delete() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = repo.writable_session("main").await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();

        let data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", data.clone()).await.unwrap();

        // delete chunk
        store.delete("array/c/0/1/0").await.unwrap();
        // deleting a deleted chunk is allowed
        store.delete("array/c/0/1/0").await.unwrap();
        // deleting non-existent chunk is allowed
        store.delete("array/c/1/1/1").await.unwrap();
        assert!(matches!(
            store.get("array/c/0/1/0", &ByteRange::ALL).await,
            Err(StoreError::NotFound(KeyNotFoundError::ChunkNotFound { key, path, coords }))
                if key == "array/c/0/1/0" && path.to_string() == "/array" && coords == ChunkIndices([0, 1, 0].to_vec())
        ));
        assert!(matches!(
            store.delete("array/foo").await,
            Err(StoreError::InvalidKey { key }) if key == "array/foo",
        ));

        assert!(matches!(
            store.delete("array/c/10/1/1").await,
            Err(StoreError::SessionError(SessionError::InvalidIndex { coords, path }))
                if path.to_string() == "/array" && coords == ChunkIndices([10, 1, 1].to_vec())
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_dir() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = repo.writable_session("main").await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

        add_group(&store, "").await.unwrap();
        add_group(&store, "group").await.unwrap();
        add_array_and_chunk(&store, "group/array").await.unwrap();

        store.delete_dir("group/array").await.unwrap();
        assert!(matches!(
            store.get("group/array/zarr.json", &ByteRange::ALL).await,
            Err(StoreError::NotFound(..))
        ));

        add_array_and_chunk(&store, "group/array").await.unwrap();
        store.delete_dir("group").await.unwrap();
        assert!(matches!(
            store.get("group/zarr.json", &ByteRange::ALL).await,
            Err(StoreError::NotFound(..))
        ));
        assert!(matches!(
            store.get("group/array/zarr.json", &ByteRange::ALL).await,
            Err(StoreError::NotFound(..))
        ));

        add_group(&store, "group").await.unwrap();
        add_array_and_chunk(&store, "group/array").await.unwrap();

        // intentionally adding prefix '/' here.
        store.delete_dir("/group/array/c").await.unwrap();
        let list = store
            .list_prefix("group/array")
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(list, vec!["group/array/zarr.json"]);

        add_array_and_chunk(&store, "group/array").await.unwrap();
        store.delete_dir("group/array/c/0").await.unwrap();
        let mut list = store
            .list_prefix("group/array")
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await?;
        list.sort();
        assert_eq!(list, vec!["group/array/c/1/1/0", "group/array/zarr.json"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_list() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = repo.writable_session("main").await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

        assert!(store.is_empty("").await.unwrap());
        assert!(!store.exists("zarr.json").await.unwrap());

        assert_eq!(all_keys(&store).await.unwrap(), Vec::<String>::new());
        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        assert!(!store.is_empty("").await.unwrap());
        assert!(store.exists("zarr.json").await.unwrap());
        assert_eq!(all_keys(&store).await.unwrap(), vec!["zarr.json".to_string()]);
        store
            .set(
                "group/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        assert_eq!(
            all_keys(&store).await.unwrap(),
            vec!["group/zarr.json".to_string(), "zarr.json".to_string()]
        );
        assert_eq!(
            keys(&store, "group/").await.unwrap(),
            vec!["group/zarr.json".to_string()]
        );

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("group/array/zarr.json", zarr_meta).await?;
        assert!(!store.is_empty("").await.unwrap());
        assert!(store.exists("zarr.json").await.unwrap());
        assert!(store.exists("group/array/zarr.json").await.unwrap());
        assert!(store.exists("group/zarr.json").await.unwrap());
        assert_eq!(
            all_keys(&store).await.unwrap(),
            vec![
                "group/array/zarr.json".to_string(),
                "group/zarr.json".to_string(),
                "zarr.json".to_string()
            ]
        );
        assert_eq!(
            keys(&store, "group/").await.unwrap(),
            vec!["group/array/zarr.json".to_string(), "group/zarr.json".to_string()]
        );
        assert_eq!(
            keys(&store, "group/array/").await.unwrap(),
            vec!["group/array/zarr.json".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_set_array_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = repo.writable_session("main").await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("/array/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(
            store.get("/array/zarr.json", &ByteRange::ALL).await?,
            zarr_meta.clone()
        );

        store.set("0/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(store.get("0/zarr.json", &ByteRange::ALL).await?, zarr_meta.clone());

        store.set("/0/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(store.get("/0/zarr.json", &ByteRange::ALL).await?, zarr_meta);

        // store.set("c/0", zarr_meta.clone()).await?;
        // assert_eq!(store.get("c/0", &ByteRange::ALL).await?, zarr_meta);

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_list() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let session = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&session)).await;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await?;

        let data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", data.clone()).await?;
        store.set("array/c/1/1/1", data.clone()).await?;

        assert_eq!(
            all_keys(&store).await.unwrap(),
            vec![
                "array/c/0/1/0".to_string(),
                "array/c/1/1/1".to_string(),
                "array/zarr.json".to_string(),
                "zarr.json".to_string()
            ]
        );

        session.write().await.commit("foo", None).await?;

        let session = repo.writable_session("main").await?;
        let store = Store::from_session(Arc::new(RwLock::new(session))).await;
        store.clear().await?;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        store.set("array/zarr.json", zarr_meta).await?;

        let data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", data.clone()).await?;
        store.set("array/c/1/1/1", data.clone()).await?;

        assert_eq!(
            all_keys(&store).await.unwrap(),
            vec![
                "array/c/0/1/0".to_string(),
                "array/c/1/1/1".to_string(),
                "array/zarr.json".to_string(),
                "zarr.json".to_string()
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_list_dir() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = repo.writable_session("main").await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta).await?;

        let data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", data.clone()).await?;
        store.set("array/c/1/1/1", data.clone()).await?;
        store.set("array/c/0/0/1", data.clone()).await?;

        assert_eq!(
            all_keys(&store).await.unwrap(),
            vec![
                "array/c/0/0/1".to_string(),
                "array/c/0/1/0".to_string(),
                "array/c/1/1/1".to_string(),
                "array/zarr.json".to_string(),
                "zarr.json".to_string()
            ]
        );

        for prefix in ["/", ""] {
            let mut dir = store.list_dir(prefix).await?.try_collect::<Vec<_>>().await?;
            dir.sort();
            assert_eq!(dir, vec!["array".to_string(), "zarr.json".to_string()]);

            let mut dir =
                store.list_dir_items(prefix).await?.try_collect::<Vec<_>>().await?;
            dir.sort();
            assert_eq!(
                dir,
                vec![
                    ListDirItem::Key("zarr.json".to_string()),
                    ListDirItem::Prefix("array".to_string())
                ]
            );
        }

        let mut dir = store.list_dir("array").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["c".to_string(), "zarr.json".to_string()]);

        let mut dir =
            store.list_dir_items("array").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(
            dir,
            vec![
                ListDirItem::Key("zarr.json".to_string()),
                ListDirItem::Prefix("c".to_string())
            ]
        );

        let mut dir = store.list_dir("array/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["c".to_string(), "zarr.json".to_string()]);

        let mut dir =
            store.list_dir_items("array/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(
            dir,
            vec![
                ListDirItem::Key("zarr.json".to_string()),
                ListDirItem::Prefix("c".to_string())
            ]
        );

        let mut dir = store.list_dir("array/c/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["0".to_string(), "1".to_string()]);

        let mut dir =
            store.list_dir_items("array/c/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(
            dir,
            vec![
                ListDirItem::Prefix("0".to_string()),
                ListDirItem::Prefix("1".to_string()),
            ]
        );

        let mut dir = store.list_dir("array/c/1/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["1".to_string()]);

        let mut dir =
            store.list_dir_items("array/c/1/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec![ListDirItem::Prefix("1".to_string()),]);

        let mut dir =
            store.list_dir_items("array/c/1/1").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec![ListDirItem::Key("1".to_string()),]);

        // When a path to a chunk is passed, return nothing
        let mut dir =
            store.list_dir_items("array/c/1/1/1").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec![]);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_dir_with_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = repo.writable_session("main").await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        store
            .set(
                "group/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("group-suffix/zarr.json", zarr_meta).await.unwrap();
        let data = Bytes::copy_from_slice(b"hello");
        store.set_if_not_exists("group-suffix/c/0/1/0", data.clone()).await.unwrap();

        assert_eq!(
            store.list_dir("group/").await?.try_collect::<Vec<_>>().await?,
            vec!["zarr.json"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_partial_values() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = repo.writable_session("main").await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[20],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x"]}"#);
        store.set("array/zarr.json", zarr_meta).await?;

        let key_vals: Vec<_> = (0i32..20)
            .map(|idx| {
                (
                    format!("array/c/{idx}"),
                    Bytes::copy_from_slice(idx.to_be_bytes().to_owned().as_slice()),
                )
            })
            .collect();

        for (key, value) in key_vals.iter() {
            store.set(key.as_str(), value.clone()).await?;
        }

        let key_ranges = key_vals.iter().map(|(k, _)| (k.clone(), ByteRange::ALL));

        assert_eq!(
            key_vals.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
            store
                .get_partial_values(key_ranges)
                .await?
                .into_iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        );

        // let's try in reverse order
        let key_ranges = key_vals.iter().rev().map(|(k, _)| (k.clone(), ByteRange::ALL));

        assert_eq!(
            key_vals.iter().rev().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
            store
                .get_partial_values(key_ranges)
                .await?
                .into_iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_and_checkout() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();

        let data = Bytes::copy_from_slice(b"hello");
        store.set_if_not_exists("array/c/0/1/0", data.clone()).await.unwrap();
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), data);

        let snapshot_id =
            { ds.write().await.commit("initial commit", None).await.unwrap() };

        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;

        let new_data = Bytes::copy_from_slice(b"world");
        store.set_if_not_exists("array/c/0/1/0", new_data.clone()).await.unwrap();
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), data);

        store.set("array/c/0/1/0", new_data.clone()).await.unwrap();
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), new_data);

        let new_snapshot_id = { ds.write().await.commit("update", None).await.unwrap() };

        let ds = repo.readonly_session(&VersionInfo::SnapshotId(snapshot_id)).await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), data);

        let ds = repo
            .readonly_session(&VersionInfo::SnapshotId(new_snapshot_id.clone()))
            .await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), new_data);

        repo.create_tag("tag_0", &new_snapshot_id).await.unwrap();
        let _ds = repo
            .readonly_session(&VersionInfo::TagRef("tag_0".to_string()))
            .await
            .unwrap();

        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;
        let _newest_data = Bytes::copy_from_slice(b"earth");
        store.set("array/c/0/1/0", data.clone()).await.unwrap();
        assert_eq!(ds.read().await.has_uncommitted_changes(), true);

        ds.write().await.discard_changes();
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), new_data);

        // Create a new branch and do stuff with it
        repo.create_branch("dev", ds.read().await.snapshot_id()).await.unwrap();

        let ds = Arc::new(RwLock::new(repo.writable_session("dev").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;
        store.set("array/c/0/1/0", new_data.clone()).await?;
        let dev_snapshot =
            { ds.write().await.commit("update dev branch", None).await.unwrap() };

        let ds = repo.readonly_session(&VersionInfo::SnapshotId(dev_snapshot)).await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;
        assert_eq!(store.get("array/c/0/1/0", &ByteRange::ALL).await.unwrap(), new_data);
        Ok(())
    }

    #[tokio::test]
    async fn test_clear() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();

        let empty: Vec<String> = Vec::new();
        store.clear().await?;
        assert_eq!(
            store.list_prefix("").await?.try_collect::<Vec<String>>().await?,
            empty
        );

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        store
            .set(
                "group/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        let new_data = Bytes::copy_from_slice(b"world");
        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();
        store.set("group/array/zarr.json", zarr_meta.clone()).await.unwrap();
        store.set("array/c/1/0/0", new_data.clone()).await.unwrap();
        store.set("group/array/c/1/0/0", new_data.clone()).await.unwrap();

        ds.write().await.commit("initial commit", None).await.unwrap();

        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;

        store
            .set(
                "group/group2/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();
        store.set("group/group2/array/zarr.json", zarr_meta.clone()).await.unwrap();
        store.set("group/group2/array/c/1/0/0", new_data.clone()).await.unwrap();

        store.clear().await?;

        assert_eq!(
            store.list_prefix("").await?.try_collect::<Vec<String>>().await?,
            empty
        );

        let empty_snap =
            ds.write().await.commit("no content commit", None).await.unwrap();

        assert_eq!(
            store.list_prefix("").await?.try_collect::<Vec<String>>().await?,
            empty
        );

        let ds = repo.readonly_session(&VersionInfo::SnapshotId(empty_snap)).await?;
        let store = Store::from_session(Arc::new(RwLock::new(ds))).await;
        assert_eq!(
            store.list_prefix("").await?.try_collect::<Vec<String>>().await?,
            empty
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_overwrite() -> Result<(), Box<dyn std::error::Error>> {
        // GH347
        let repo = create_memory_store_repository().await;
        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;

        let meta1 = Bytes::copy_from_slice(
            br#"{"zarr_format":3,"node_type":"group","attributes":{"foo":42}}"#,
        );
        let meta2 = Bytes::copy_from_slice(
            br#"{"zarr_format":3,"node_type":"group","attributes":{"foo":84}}"#,
        );
        let zarr_meta1 = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        let zarr_meta2 = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":84},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);

        // with no commit in the middle, this tests the changeset
        store.set("zarr.json", meta1.clone()).await.unwrap();
        store.set("array/zarr.json", zarr_meta1.clone()).await.unwrap();
        store.delete("zarr.json").await.unwrap();
        store.delete("array/zarr.json").await.unwrap();
        store.set("zarr.json", meta2.clone()).await.unwrap();
        store.set("array/zarr.json", zarr_meta2.clone()).await.unwrap();
        assert_eq!(&store.get("zarr.json", &ByteRange::ALL).await.unwrap(), &meta2);
        assert_eq!(
            &store.get("array/zarr.json", &ByteRange::ALL).await.unwrap(),
            &zarr_meta2
        );

        // with a commit in the middle, this tests the changeset interaction with snapshot
        store.set("zarr.json", meta1).await.unwrap();
        store.set("array/zarr.json", zarr_meta1.clone()).await.unwrap();

        ds.write().await.commit("initial commit", None).await.unwrap();

        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;
        store.delete("zarr.json").await.unwrap();
        store.delete("array/zarr.json").await.unwrap();
        store.set("zarr.json", meta2.clone()).await.unwrap();
        store.set("array/zarr.json", zarr_meta2.clone()).await.unwrap();
        assert_eq!(&store.get("zarr.json", &ByteRange::ALL).await.unwrap(), &meta2);
        ds.write().await.commit("commit 2", None).await.unwrap();
        assert_eq!(&store.get("zarr.json", &ByteRange::ALL).await.unwrap(), &meta2);
        assert_eq!(
            &store.get("array/zarr.json", &ByteRange::ALL).await.unwrap(),
            &zarr_meta2
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_branch_reset() -> Result<(), Box<dyn std::error::Error>> {
        let repo = create_memory_store_repository().await;
        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();

        ds.write().await.commit("root group", None).await.unwrap();

        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;

        store
            .set(
                "a/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();

        let prev_snap = ds.write().await.commit("group a", None).await?;

        let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));
        let store = Store::from_session(Arc::clone(&ds)).await;

        store
            .set(
                "b/zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();

        ds.write().await.commit("group b", None).await.unwrap();
        assert!(store.exists("a/zarr.json").await?);
        assert!(store.exists("b/zarr.json").await?);

        repo.reset_branch("main", &prev_snap).await?;
        let ds = Arc::new(RwLock::new(
            repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?,
        ));
        let store = Store::from_session(Arc::clone(&ds)).await;

        assert!(!store.exists("b/zarr.json").await?);
        assert!(store.exists("a/zarr.json").await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_access_mode() {
        let repo = create_memory_store_repository().await;
        let ds = repo.writable_session("main").await.unwrap();
        let writable_store = Store::from_session(Arc::new(RwLock::new(ds))).await;

        writable_store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();

        let readable_store = Store::from_session(Arc::new(RwLock::new(
            repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
                .await
                .unwrap(),
        )))
        .await;
        assert_eq!(readable_store.read_only().await, true);

        let result = readable_store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await;
        let correct_error = matches!(result, Err(StoreError::ReadOnly));
        assert!(correct_error);
    }

    #[tokio::test]
    async fn test_serialize() {
        let repo_dir = TempDir::new().expect("could not create temp dir");
        let storage = Arc::new(
            ObjectStorage::new_local_filesystem(repo_dir.path())
                .expect("could not create storage"),
        );

        let repo = Repository::create(None, storage, HashMap::new()).await.unwrap();
        let ds = Arc::new(RwLock::new(repo.writable_session("main").await.unwrap()));
        let store = Store::from_session(Arc::clone(&ds)).await;
        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await
            .unwrap();

        ds.write().await.commit("first", None).await.unwrap();

        let store_bytes = store.as_bytes().await.unwrap();
        let store2: Store = Store::from_bytes(store_bytes).unwrap();

        let zarr_json = store2.get("zarr.json", &ByteRange::ALL).await.unwrap();
        assert_eq!(
            zarr_json,
            Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"group"}"#)
        );
    }
}
