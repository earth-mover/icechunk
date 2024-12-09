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
use serde_with::{serde_as, skip_serializing_none, TryFromInto};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{
    format::{
        manifest::VirtualChunkRef,
        snapshot::{NodeData, UserAttributesSnapshot},
        ByteRange, ChunkIndices, ChunkOffset, IcechunkFormatError, Path,
    }, metadata::{
        ArrayShape, ChunkKeyEncoding, ChunkShape, Codec, DataType, DimensionNames,
        FillValue, StorageTransformer, UserAttributes,
    }, refs::RefError, repository::{get_chunk, ChunkPayload, RepositoryError, ZarrArrayMetadata}, session::{Session, SessionError}
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
    #[error("all commits must be made on a branch")]
    NotOnBranch,
    #[error("bad metadata: `{0}`")]
    BadMetadata(#[from] serde_json::Error),
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
    #[error("unknown store error: `{0}`")]
    Unknown(Box<dyn std::error::Error + Send + Sync>),
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoreOptions {
    pub get_partial_values_concurrency: u16,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self { get_partial_values_concurrency: 10 }
    }
}


pub struct Store {
    session: Arc<RwLock<Session>>,
    config: StoreOptions,
    read_only: bool,
}

impl Store {
    pub fn from_session(
        session: Arc<RwLock<Session>>,
        config: StoreOptions,
        read_only: bool,
    ) -> Self {
        Self { session, config, read_only }
    }

    pub async fn is_empty<'a>(self, prefix: &'a str) -> StoreResult<bool> {
        let res = self.list_dir(prefix).await?.next().await;
        Ok(res.is_none())
    }

    pub async fn clear(&self) -> StoreResult<()> {
        let mut guard = self.session.write().await.clear().await?;
        Ok(())
    }

    pub async fn get(&self, key: &str, byte_range: &ByteRange) -> StoreResult<Bytes> {
        let guard = self.session.read().await;
        get_key(key, byte_range, &guard).await
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
                self.config.get_partial_values_concurrency as usize,
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
        exists(key, &guard).await
    }

    pub fn supports_writes(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub fn supports_deletes(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub async fn set(&self, key: &str, value: Bytes) -> StoreResult<()> {
        self.set_with_optional_locking(key, value, None).await
    }

    async fn set_with_optional_locking(
        &self,
        key: &str,
        value: Bytes,
        locked_session: Option<&mut Session>,
    ) -> StoreResult<()> {
        if self.read_only {
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
                        let writer = session.get_chunk_writer()?;
                        let payload = writer(value).await?;
                        session.set_chunk_ref(node_path, coords, Some(payload)).await?
                    }
                    None => {
                        let mut session = self.session.write().await;
                        // we only lock the session to get the writer
                        let writer = session.get_chunk_writer()?;
                        // then we can write the bytes without holding the lock
                        let payload = writer(value).await?;
                        // and finally we lock for write and update the reference
                        session.set_chunk_ref(node_path, coords, Some(payload)).await?
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
        // we use a write lock to check if the key exists and then to set it, so we dont have race conditions
        // between the check and the set
        let mut guard = self.session.write().await;
        if exists(key, &guard).await? {
            Ok(())
        } else {
            self.set_with_optional_locking(key, value, Some(&mut guard)).await
        }
    }

    // alternate API would take array path, and a mapping from string coord to ChunkPayload
    pub async fn set_virtual_ref(
        &self,
        key: &str,
        reference: VirtualChunkRef,
    ) -> StoreResult<()> {
        if self.read_only {
            return Err(StoreError::ReadOnly);
        }

        match Key::parse(key)? {
            Key::Chunk { node_path, coords } => {
                self.session
                    .write()
                    .await
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

    pub async fn delete(&self, key: &str) -> StoreResult<()> {
        if self.read_only {
            return Err(StoreError::ReadOnly);
        }

        match Key::parse(key)? {
            Key::Metadata { node_path } => {
                // we need to hold the lock while we do the node search and the write
                // to avoid race conditions with other writers
                // (remember this method takes &self and not &mut self)
                let mut guard = self.session.write().await;
                let node = guard.get_node(&node_path).await;

                // When there is no node at the given key, we don't consider it an error, instead we just do nothing
                if let Err(RepositoryError::NodeNotFound { path: _, message: _ }) = node {
                    return Ok(());
                };

                let node = node.map_err(StoreError::RepositoryError)?;
                match node.node_data {
                    NodeData::Array(_, _) => Ok(guard.delete_array(node_path).await?),
                    NodeData::Group => Ok(guard.delete_group(node_path).await?),
                }
            }
            Key::Chunk { node_path, coords } => {
                let mut guard = self.session.write().await;
                match guard.set_chunk_ref(node_path, coords, None).await {
                    Ok(_) => Ok(()),
                    Err(SessionError::RepositoryError(
                        RepositoryError::NodeNotFound { path: _, message: _ },
                    )) => {
                        // When there is no chunk at the given key, we don't consider it an error, instead we just do nothing
                        Ok(())
                    }
                    Err(err) => Err(StoreError::SessionError(err)),
                }
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
        if self.read_only {
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

    pub async fn list_prefix<'a>(
        &self,
        prefix: &'a str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + Send> {
        // TODO: this is inefficient because it filters based on the prefix, instead of only
        // generating items that could potentially match
        let meta = self.list_metadata_prefix(prefix).await?;
        let chunks = self.list_chunks_prefix(prefix).await?;
        // FIXME: this is wrong, we are realizing all keys in memory
        // it should be lazy instead
        Ok(futures::stream::iter(meta.chain(chunks).collect::<Vec<_>>().await))
    }

    pub async fn list_dir<'a>(
        &self,
        prefix: &'a str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + Send> {
        // TODO: this is inefficient because it filters based on the prefix, instead of only
        // generating items that could potentially match
        // FIXME: this is not lazy, it goes through every chunk. This should be implemented using
        // metadata only, and ignore the chunks, but we should decide on that based on Zarr3 spec
        // evolution
        let res = self.list_dir_items(prefix).await?.map_ok(|item| match item {
            ListDirItem::Key(k) => k,
            ListDirItem::Prefix(p) => p,
        });
        Ok(res)
    }

    pub async fn list_dir_items<'a>(
        &self,
        prefix: &'a str,
    ) -> StoreResult<impl Stream<Item = StoreResult<ListDirItem>> + Send> {
        // TODO: this is inefficient because it filters based on the prefix, instead of only
        // generating items that could potentially match
        // FIXME: this is not lazy, it goes through every chunk. This should be implemented using
        // metadata only, and ignore the chunks, but we should decide on that based on Zarr3 spec
        // evolution

        let idx: usize = if prefix == "/" { 0 } else { prefix.len() };

        let parents: HashSet<_> = self
            .list_prefix(prefix)
            .await?
            .map_ok(move |s| {
                // If the prefix is "/", get rid of it. This can happen when prefix is missing
                // the trailing slash (as it does in zarr-python impl)
                let rem = &s[idx..].trim_start_matches('/');
                match rem.split_once('/') {
                    Some((prefix, _)) => ListDirItem::Prefix(prefix.to_string()),
                    None => ListDirItem::Key(rem.to_string()),
                }
            })
            .try_collect()
            .await?;
        // We tould return a Stream<Item = String> with this implementation, but the present
        // signature is better if we change the impl
        Ok(futures::stream::iter(parents.into_iter().map(Ok)))
    }

    async fn set_array_meta(
        &self,
        path: Path,
        array_meta: ArrayMetadata,
        locked_session: Option<&mut Session>,
    ) -> Result<(), StoreError> {
        match locked_session {
            Some(session) => set_array_meta(path, array_meta, session).await,
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
        set_array_meta(path, array_meta, &mut guard).await
    }

    async fn set_group_meta(
        &self,
        path: Path,
        group_meta: GroupMetadata,
        locked_session: Option<&mut Session>,
    ) -> Result<(), StoreError> {
        match locked_session {
            Some(session) => set_group_meta(path, group_meta, session).await,
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
        set_group_meta(path, group_meta, &mut guard).await
    }

    async fn list_metadata_prefix<'a, 'b: 'a>(
        &'a self,
        prefix: &'b str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + 'a> {
        let prefix = prefix.trim_end_matches('/');
        let res = try_stream! {
            let guard = self.session.read().await;
            for node in guard.list_nodes().await? {
                // TODO: handle non-utf8?
                let meta_key = Key::Metadata { node_path: node.path }.to_string();
                    match meta_key.strip_prefix(prefix) {
                        None => {}
                        Some(rest) => {
                            // we have a few cases
                            if prefix.is_empty()   // if prefix was empty anything matches
                               || rest.is_empty()  // if stripping prefix left empty we have a match
                               || rest.starts_with('/') // next component so we match
                               // what we don't include is other matches,
                               // we want to catch prefix/foo but not prefix-foo
                            {
                                yield meta_key;
                            }

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
            // TODO: this is inefficient because it filters based on the prefix, instead of only
            // generating items that could potentially match
            let guard = self.session.read().await;
            for await maybe_path_chunk in guard.all_chunks().await.map_err(StoreError::RepositoryError)? {
                // FIXME: utf8 handling
                match maybe_path_chunk {
                    Ok((path,chunk)) => {
                        let chunk_key = Key::Chunk { node_path: path, coords: chunk.coord }.to_string();
                        if chunk_key.starts_with(prefix) {
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
    session: &mut Session,
) -> Result<(), StoreError> {
    if session.get_array(&path).await.is_ok() {
        // TODO: we don't necessarily need to update both
        session.set_user_attributes(path.clone(), array_meta.attributes).await?;
        session.update_array(path, array_meta.zarr_metadata).await?;
        Ok(())
    } else {
        session.add_array(path.clone(), array_meta.zarr_metadata).await?;
        session.set_user_attributes(path, array_meta.attributes).await?;
        Ok(())
    }
}

async fn set_group_meta(
    path: Path,
    group_meta: GroupMetadata,
    session: &mut Session,
) -> Result<(), StoreError> {
    // we need to hold the lock while we search the group and do the update to avoid race
    // conditions with other writers (notice we don't take &mut self)
    //
    if session.get_group(&path).await.is_ok() {
        session.set_user_attributes(path, group_meta.attributes).await?;
        Ok(())
    } else {
        session.add_group(path.clone()).await?;
        session.set_user_attributes(path, group_meta.attributes).await?;
        Ok(())
    }
}

async fn get_metadata(
    _key: &str,
    path: &Path,
    range: &ByteRange,
    session: &Session,
) -> StoreResult<Bytes> {
    let node = session.get_node(path).await.map_err(|_| {
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
    session: &Session,
) -> StoreResult<Bytes> {
    let reader = session.get_chunk_reader(&path, &coords, byte_range).await?;

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
    session: &Session,
) -> StoreResult<Bytes> {
    let bytes = match Key::parse(key)? {
        Key::Metadata { node_path } => {
            get_metadata(key, &node_path, byte_range, session).await
        }
        Key::Chunk { node_path, coords } => {
            get_chunk_bytes(key, node_path, coords, byte_range, session).await
        }
        Key::ZarrV2(key) => {
            Err(StoreError::NotFound(KeyNotFoundError::ZarrV2KeyNotFound { key }))
        }
    }?;

    Ok(bytes)
}

async fn exists(key: &str, session: &Session) -> StoreResult<bool> {
    match get_key(key, &ByteRange::ALL, session).await {
        Ok(_) => Ok(true),
        Err(StoreError::NotFound(_)) => Ok(false),
        Err(StoreError::RepositoryError(RepositoryError::NodeNotFound {
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
