use std::{
    collections::HashSet,
    iter,
    num::NonZeroU64,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none, TryFromInto};
use thiserror::Error;

use crate::{
    dataset::{
        ArrayShape, ChunkIndices, ChunkKeyEncoding, ChunkShape, Codec, DataType,
        DatasetError, DimensionNames, FillValue, Path, StorageTransformer,
        UserAttributes, ZarrArrayMetadata,
    },
    format::{
        snapshot::{NodeData, UserAttributesSnapshot}, // TODO: we shouldn't need these imports, too low level
        ChunkOffset,
        IcechunkFormatError,
    },
    refs::BranchVersion,
    Dataset, MemCachingStorage, ObjectStorage, Storage,
};

pub use crate::format::ObjectId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum StorageConfig {
    #[serde(rename = "in_memory")]
    InMemory,

    #[serde(rename = "local_filesystem")]
    LocalFileSystem { root: PathBuf },

    #[serde(rename = "s3")]
    S3ObjectStore {
        bucket: String,
        prefix: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        endpoint: Option<String>,
    },

    #[serde(rename = "cached")]
    Cached { approx_max_memory_bytes: u64, backend: Box<StorageConfig> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum VersionInfo {
    #[serde(rename = "snapshot_id")]
    SnapshotId(ObjectId),
    #[serde(rename = "tag_ref")]
    TagRef(String),
    #[serde(rename = "branch_tip_ref")]
    BranchTipRef(String),
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DatasetConfig {
    pub previous_version: Option<VersionInfo>,
    pub inline_chunk_threshold_bytes: Option<u16>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoreConfig {
    storage: StorageConfig,
    dataset: DatasetConfig,
    get_partial_values_concurrency: Option<u16>,
}

pub type ByteRange = (Option<ChunkOffset>, Option<ChunkOffset>);
pub type StoreResult<A> = Result<A, StoreError>;

#[derive(Debug, Clone)]
pub struct Store {
    dataset: Dataset,
    get_partial_values_concurrency: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KeyNotFoundError {
    #[error("chunk cannot be find for key `{key}`")]
    ChunkNotFound { key: String, path: Path, coords: ChunkIndices },
    #[error("node not found at `{path}`")]
    NodeNotFound { path: Path },
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("invalid zarr key format `{key}`")]
    InvalidKey { key: String },
    #[error("object not found: `{0}`")]
    NotFound(#[from] KeyNotFoundError),
    #[error("unsuccessful dataset operation: `{0}`")]
    CannotUpdate(#[from] DatasetError),
    #[error("bad metadata: `{0}`")]
    BadMetadata(#[from] serde_json::Error),
    #[error("store method `{0}` is not implemented by Icechunk")]
    Unimplemented(&'static str),
    #[error("bad key prefix: `{0}`")]
    BadKeyPrefix(String),
    #[error("error during parallel execution of get_partial_values")]
    PartialValuesPanic,
    #[error("unknown store error: `{0}`")]
    Unknown(Box<dyn std::error::Error + Send + Sync>),
}

impl Store {
    pub async fn from_config(config: &StoreConfig) -> Result<Self, String> {
        let storage = mk_storage(&config.storage)?;
        let dataset = mk_dataset(&config.dataset, storage).await?;
        Ok(Self::new(dataset, config.get_partial_values_concurrency))
    }

    pub async fn from_json_config(json: &[u8]) -> Result<Self, String> {
        let config: StoreConfig =
            serde_json::from_slice(json).map_err(|e| e.to_string())?;
        Self::from_config(&config).await
    }

    pub fn new(dataset: Dataset, get_partial_values_concurrency: Option<u16>) -> Self {
        Store {
            dataset,
            get_partial_values_concurrency: get_partial_values_concurrency.unwrap_or(10),
        }
    }

    pub async fn checkout(&mut self, version: VersionInfo) -> Result<(), DatasetError> {
        let storage = self.dataset.storage().clone();
        let dataset = match version {
            VersionInfo::SnapshotId(sid) => Dataset::update(storage, sid),
            VersionInfo::TagRef(tag) => Dataset::from_tag(storage, &tag).await?,
            VersionInfo::BranchTipRef(branch) => {
                Dataset::from_branch_tip(storage, &branch).await?
            }
        }
        .build();

        self.dataset = dataset;
        Ok(())
    }

    pub async fn commit(
        &mut self,
        update_branch_name: &str,
        message: &str,
    ) -> Result<(ObjectId, BranchVersion), DatasetError> {
        self.dataset.commit(update_branch_name, message).await
    }

    pub fn dataset(self) -> Dataset {
        self.dataset
    }

    pub async fn empty(&self) -> StoreResult<bool> {
        let res = self.dataset.list_nodes().await?.next().is_none();
        Ok(res)
    }

    pub async fn clear(&mut self) -> StoreResult<()> {
        todo!()
    }

    // TODO: prototype argument
    pub async fn get(&self, key: &str, _byte_range: &ByteRange) -> StoreResult<Bytes> {
        match Key::parse(key)? {
            Key::Metadata { node_path } => self.get_metadata(key, &node_path).await,
            Key::Chunk { node_path, coords } => {
                self.get_chunk(key, node_path, coords).await
            }
        }
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

    // TODO: prototype argument
    pub async fn exists(&self, key: &str) -> StoreResult<bool> {
        match self.get(key, &(None, None)).await {
            Ok(_) => Ok(true),
            Err(StoreError::NotFound(_)) => Ok(false),
            Err(other_error) => Err(other_error),
        }
    }

    pub fn supports_writes(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub async fn set(&mut self, key: &str, value: Bytes) -> StoreResult<()> {
        match Key::parse(key)? {
            Key::Metadata { node_path } => {
                if let Ok(array_meta) = serde_json::from_slice(value.as_ref()) {
                    self.set_array_meta(node_path, array_meta).await
                } else {
                    match serde_json::from_slice(value.as_ref()) {
                        Ok(group_meta) => {
                            self.set_group_meta(node_path, group_meta).await
                        }
                        Err(err) => Err(StoreError::BadMetadata(err)),
                    }
                }
            }
            Key::Chunk { ref node_path, ref coords } => {
                self.dataset.set_chunk(node_path, coords, value).await?;
                Ok(())
            }
        }
    }

    pub async fn delete(&mut self, key: &str) -> StoreResult<()> {
        let ds = &mut self.dataset;
        match Key::parse(key)? {
            Key::Metadata { node_path } => {
                let node = ds.get_node(&node_path).await.map_err(|_| {
                    KeyNotFoundError::NodeNotFound { path: node_path.clone() }
                })?;
                match node.node_data {
                    NodeData::Array(_, _) => Ok(ds.delete_array(node_path).await?),
                    NodeData::Group => Ok(ds.delete_group(node_path).await?),
                }
            }
            Key::Chunk { node_path, coords } => {
                Ok(ds.set_chunk_ref(node_path, coords, None).await?)
            }
        }
    }

    pub fn supports_partial_writes(&self) -> StoreResult<bool> {
        Ok(false)
    }

    pub async fn set_partial_values(
        &mut self,
        _key_start_values: impl IntoIterator<Item = (&str, ChunkOffset, Bytes)>,
    ) -> StoreResult<()> {
        Err(StoreError::Unimplemented("set_partial_values"))
    }

    pub fn supports_listing(&self) -> StoreResult<bool> {
        Ok(true)
    }

    pub async fn list(
        &self,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + '_ + Send> {
        self.list_prefix("/").await
    }

    pub async fn list_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + 'a + Send> {
        // TODO: this is inefficient because it filters based on the prefix, instead of only
        // generating items that could potentially match
        let meta = self.list_metadata_prefix(prefix).await?;
        let chunks = self.list_chunks_prefix(prefix).await?;
        Ok(meta.chain(chunks))
    }

    pub async fn list_dir<'a>(
        &'a self,
        prefix: &'a str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + 'a + Send> {
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
                // If the prefix is "/", get rid of it. This can happend when prefix is missing 
                // the trailing slash (as it does in zarr-python impl)
                let rem = &s[idx..].trim_start_matches("/");
                let parent = rem.split_once('/').map_or(*rem, |(parent, _)| parent);
                parent.to_string()
            })
            .try_collect()
            .await?;
        // We tould return a Stream<Item = String> with this implementation, but the present
        // signature is better if we change the impl
        Ok(futures::stream::iter(parents.into_iter().map(Ok)))
    }

    async fn get_chunk(
        &self,
        key: &str,
        path: Path,
        coords: ChunkIndices,
    ) -> StoreResult<Bytes> {
        let chunk = self.dataset.get_chunk(&path, &coords).await?;
        chunk.ok_or(StoreError::NotFound(KeyNotFoundError::ChunkNotFound {
            key: key.to_string(),
            path,
            coords,
        }))
    }

    async fn get_metadata(&self, _key: &str, path: &Path) -> StoreResult<Bytes> {
        let node = self.dataset.get_node(path).await.map_err(|_| {
            StoreError::NotFound(KeyNotFoundError::NodeNotFound { path: path.clone() })
        })?;
        let user_attributes = match node.user_attributes {
            None => None,
            Some(UserAttributesSnapshot::Inline(atts)) => Some(atts),
            // FIXME: implement
            Some(UserAttributesSnapshot::Ref(_)) => todo!(),
        };
        match node.node_data {
            NodeData::Group => Ok(GroupMetadata::new(user_attributes).to_bytes()),
            NodeData::Array(zarr_metadata, _) => {
                Ok(ArrayMetadata::new(user_attributes, zarr_metadata).to_bytes())
            }
        }
    }

    async fn set_array_meta(
        &mut self,
        path: Path,
        array_meta: ArrayMetadata,
    ) -> Result<(), StoreError> {
        if self.dataset.get_array(&path).await.is_ok() {
            // TODO: we don't necessarily need to update both
            self.dataset.set_user_attributes(path.clone(), array_meta.attributes).await?;
            self.dataset.update_array(path, array_meta.zarr_metadata).await?;
            Ok(())
        } else {
            self.dataset.add_array(path.clone(), array_meta.zarr_metadata).await?;
            self.dataset.set_user_attributes(path, array_meta.attributes).await?;
            Ok(())
        }
    }

    async fn set_group_meta(
        &mut self,
        path: Path,
        group_meta: GroupMetadata,
    ) -> Result<(), StoreError> {
        if self.dataset.get_group(&path).await.is_ok() {
            self.dataset.set_user_attributes(path, group_meta.attributes).await?;
            Ok(())
        } else {
            self.dataset.add_group(path.clone()).await?;
            self.dataset.set_user_attributes(path, group_meta.attributes).await?;
            Ok(())
        }
    }

    async fn list_metadata_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + 'a> {
        let prefix = prefix.trim_end_matches("/");

        let nodes = futures::stream::iter(self.dataset.list_nodes().await?);
        // TODO: handle non-utf8?
        Ok(nodes.map_err(|e| e.into()).try_filter_map(move |node| async move {
            Ok(Key::Metadata { node_path: node.path }.to_string().and_then(|key| {
                if key.starts_with(prefix) {
                    Some(key)
                } else {
                    None
                }
            }))
        }))
    }

    async fn list_chunks_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> StoreResult<impl Stream<Item = StoreResult<String>> + 'a> {
        let prefix = prefix.trim_end_matches("/");

        // TODO: this is inefficient because it filters based on the prefix, instead of only
        // generating items that could potentially match
        let chunks = self.dataset.all_chunks().await?;
        Ok(chunks.map_err(|e| e.into()).try_filter_map(move |(path, chunk)| async move {
            //FIXME: utf handling
            Ok(Key::Chunk { node_path: path, coords: chunk.coord }
                .to_string()
                .and_then(|key| if key.starts_with(prefix) { Some(key) } else { None }))
        }))
    }
}

async fn mk_dataset(
    dataset: &DatasetConfig,
    storage: Arc<dyn Storage + Send + Sync>,
) -> Result<Dataset, String> {
    let mut builder = match &dataset.previous_version {
        None => Dataset::create(storage),
        Some(VersionInfo::SnapshotId(sid)) => Dataset::update(storage, sid.clone()),
        Some(VersionInfo::TagRef(tag)) => Dataset::from_tag(storage, tag)
            .await
            .map_err(|err| format!("Error fetching tag: {err}"))?,
        Some(VersionInfo::BranchTipRef(branch)) => {
            Dataset::from_branch_tip(storage, branch)
                .await
                .map_err(|err| format!("Error fetching branch: {err}"))?
        }
    };
    if let Some(thr) = dataset.inline_chunk_threshold_bytes {
        builder.with_inline_threshold_bytes(thr);
    }
    // TODO: add error checking, does the previous version exist?
    Ok(builder.build())
}

fn mk_storage(config: &StorageConfig) -> Result<Arc<dyn Storage + Send + Sync>, String> {
    match config {
        StorageConfig::InMemory => Ok(Arc::new(ObjectStorage::new_in_memory_store())),
        StorageConfig::LocalFileSystem { root } => {
            let storage = ObjectStorage::new_local_store(root)
                .map_err(|e| format!("Error creating storage: {e}"))?;
            Ok(Arc::new(storage))
        }
        StorageConfig::S3ObjectStore {
            bucket,
            prefix,
            access_key_id,
            secret_access_key,
            endpoint,
        } => {
            let storage = if let (Some(access_key_id), Some(secret_access_key)) =
                (access_key_id, secret_access_key)
            {
                ObjectStorage::new_s3_store_with_config(
                    bucket,
                    prefix,
                    access_key_id,
                    secret_access_key,
                    endpoint.clone(),
                )
            } else {
                ObjectStorage::new_s3_store_from_env(bucket, prefix)
            }
            .map_err(|e| format!("Error creating storage: {e}"))?;
            Ok(Arc::new(storage))
        }
        StorageConfig::Cached { approx_max_memory_bytes, backend } => {
            let backend = mk_storage(backend)?;
            let storage = MemCachingStorage::new(backend, *approx_max_memory_bytes);
            Ok(Arc::new(storage))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Key {
    Metadata { node_path: Path },
    Chunk { node_path: Path, coords: ChunkIndices },
}

impl Key {
    const ROOT_KEY: &'static str = "zarr.json";
    const METADATA_SUFFIX: &'static str = "/zarr.json";
    const CHUNK_COORD_PREFIX: &'static str = "c";

    fn parse(key: &str) -> Result<Self, StoreError> {
        fn parse_chunk(key: &str) -> Result<Key, StoreError> {
            if key == "c" {
                return Ok(Key::Chunk {
                    node_path: "/".into(),
                    coords: ChunkIndices(vec![]),
                });
            }
            if let Some((path, coords)) = key.rsplit_once(Key::CHUNK_COORD_PREFIX) {
                let path = path.strip_suffix('/').unwrap_or(path);
                if coords.is_empty() {
                    Ok(Key::Chunk {
                        node_path: ["/", path].iter().collect(),
                        coords: ChunkIndices(vec![]),
                    })
                } else {
                    coords
                        .strip_prefix('/')
                        .ok_or(StoreError::InvalidKey { key: key.to_string() })?
                        .split('/')
                        .map(|s| s.parse::<u64>())
                        .collect::<Result<Vec<_>, _>>()
                        .map(|coords| Key::Chunk {
                            node_path: ["/", path].iter().collect(),
                            coords: ChunkIndices(coords),
                        })
                        .map_err(|_| StoreError::InvalidKey { key: key.to_string() })
                }
            } else {
                Err(StoreError::InvalidKey { key: key.to_string() })
            }
        }

        if key == Key::ROOT_KEY {
            Ok(Key::Metadata { node_path: "/".into() })
        } else if key.starts_with('/') {
            Err(StoreError::InvalidKey { key: key.to_string() })
        } else if let Some(path) = key.strip_suffix(Key::METADATA_SUFFIX) {
            // we need to be careful indexing into utf8 strings
            Ok(Key::Metadata { node_path: ["/", path].iter().collect() })
        } else {
            parse_chunk(key)
        }
    }

    fn to_string(&self) -> Option<String> {
        match self {
            Key::Metadata { node_path } => node_path.as_path().to_str().map(|s| {
                format!("{}{}", &s[1..], Key::METADATA_SUFFIX)
                    .trim_start_matches('/')
                    .to_string()
            }),
            Key::Chunk { node_path, coords } => {
                node_path.as_path().to_str().map(|path| {
                    let coords = coords.0.iter().map(|c| c.to_string()).join("/");
                    [path[1..].to_string(), "c".to_string(), coords]
                        .iter()
                        .filter(|s| !s.is_empty())
                        .join("/")
                })
            }
        }
    }
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct ArrayMetadata {
    zarr_format: u8,
    node_type: String,
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
            #[allow(clippy::expect_used)]
            let fill_value = serde_json::to_value(fill_value)
                .expect("Fill values are always serializable");
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
    node_type: String,
    attributes: Option<UserAttributes>,
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

    use std::borrow::BorrowMut;

    use super::*;
    use pretty_assertions::assert_eq;

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
            Ok(Key::Metadata { node_path}) if node_path.to_str() == Some("/")
        ));
        assert!(matches!(
            Key::parse("a/zarr.json"),
            Ok(Key::Metadata { node_path }) if node_path.to_str() == Some("/a")
        ));
        assert!(matches!(
            Key::parse("a/b/c/zarr.json"),
            Ok(Key::Metadata { node_path }) if node_path.to_str() == Some("/a/b/c")
        ));
        assert!(matches!(
            Key::parse("foo/c"),
            Ok(Key::Chunk { node_path, coords }) if node_path.to_str() == Some("/foo") && coords == ChunkIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("foo/bar/c"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_str() == Some("/foo/bar") && coords == ChunkIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("foo/c/1/2/3"),
            Ok(Key::Chunk {
                node_path,
                coords,
            }) if node_path.to_str() == Some("/foo") && coords == ChunkIndices(vec![1,2,3])
        ));
        assert!(matches!(
            Key::parse("foo/bar/baz/c/1/2/3"),
            Ok(Key::Chunk {
                node_path,
                coords,
            }) if node_path.to_str() == Some("/foo/bar/baz") && coords == ChunkIndices(vec![1,2,3])
        ));
        assert!(matches!(
            Key::parse("c"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_str() == Some("/") && coords == ChunkIndices(vec![])
        ));
        assert!(matches!(
            Key::parse("c/0/0"),
            Ok(Key::Chunk { node_path, coords}) if node_path.to_str() == Some("/") && coords == ChunkIndices(vec![0,0])
        ));
    }

    #[test]
    fn test_format_key() {
        assert_eq!(
            Key::Metadata { node_path: "/".into() }.to_string(),
            Some("zarr.json".to_string())
        );
        assert_eq!(
            Key::Metadata { node_path: "/a".into() }.to_string(),
            Some("a/zarr.json".to_string())
        );
        assert_eq!(
            Key::Metadata { node_path: "/a/b/c".into() }.to_string(),
            Some("a/b/c/zarr.json".to_string())
        );
        assert_eq!(
            Key::Chunk { node_path: "/".into(), coords: ChunkIndices(vec![]) }
                .to_string(),
            Some("c".to_string())
        );
        assert_eq!(
            Key::Chunk { node_path: "/".into(), coords: ChunkIndices(vec![0]) }
                .to_string(),
            Some("c/0".to_string())
        );
        assert_eq!(
            Key::Chunk { node_path: "/".into(), coords: ChunkIndices(vec![1, 2]) }
                .to_string(),
            Some("c/1/2".to_string())
        );
        assert_eq!(
            Key::Chunk { node_path: "/a".into(), coords: ChunkIndices(vec![]) }
                .to_string(),
            Some("a/c".to_string())
        );
        assert_eq!(
            Key::Chunk { node_path: "/a".into(), coords: ChunkIndices(vec![1]) }
                .to_string(),
            Some("a/c/1".to_string())
        );
        assert_eq!(
            Key::Chunk { node_path: "/a".into(), coords: ChunkIndices(vec![1, 2]) }
                .to_string(),
            Some("a/c/1/2".to_string())
        );
    }

    #[tokio::test]
    async fn test_metadata_set_and_get() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let ds = Dataset::create(Arc::clone(&storage)).build();
        let mut store = Store::new(ds, None);

        assert!(matches!(
            store.get("zarr.json", &(None, None)).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound {path})) if path.to_str() == Some("/")
        ));

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        assert_eq!(
            store.get("zarr.json", &(None, None)).await.unwrap(),
            Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group","attributes":null}"#
            )
        );

        store.set("a/b/zarr.json", Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group", "attributes": {"spam":"ham", "eggs":42}}"#)).await?;
        assert_eq!(
            store.get("a/b/zarr.json", &(None, None)).await.unwrap(),
            Bytes::copy_from_slice(
                br#"{"zarr_format":3,"node_type":"group","attributes":{"eggs":42,"spam":"ham"}}"#
            )
        );

        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("a/b/array/zarr.json", zarr_meta.clone()).await?;
        assert_eq!(
            store.get("a/b/array/zarr.json", &(None, None)).await.unwrap(),
            zarr_meta.clone()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_delete() {
        let in_mem_storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let storage =
            Arc::clone(&(in_mem_storage.clone() as Arc<dyn Storage + Send + Sync>));
        let ds = Dataset::create(Arc::clone(&storage)).build();
        let mut store = Store::new(ds, None);
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
            store.get("array/zarr.json", &(None, None)).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound { path }))
                if path.to_str() == Some("/array"),
        ));
        store.set("array/zarr.json", zarr_meta.clone()).await.unwrap();
        store.delete("array/zarr.json").await.unwrap();
        assert!(matches!(
            store.get("array/zarr.json", &(None, None)).await,
            Err(StoreError::NotFound(KeyNotFoundError::NodeNotFound { path } ))
                if path.to_str() == Some("/array"),
        ));
        store.set("array/zarr.json", Bytes::copy_from_slice(group_data)).await.unwrap();
    }

    #[tokio::test]
    async fn test_chunk_set_and_get() -> Result<(), Box<dyn std::error::Error>> {
        // TODO: turn this test into pure Store operations once we support writes through Zarr
        let in_mem_storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let storage =
            Arc::clone(&(in_mem_storage.clone() as Arc<dyn Storage + Send + Sync>));
        let ds = Dataset::create(Arc::clone(&storage)).build();
        let mut store = Store::new(ds, None);

        store
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;
        let zarr_meta = Bytes::copy_from_slice(br#"{"zarr_format":3,"node_type":"array","attributes":{"foo":42},"shape":[2,2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[1,1,1]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"mycodec","configuration":{"foo":42}}],"storage_transformers":[{"name":"mytransformer","configuration":{"bar":43}}],"dimension_names":["x","y","t"]}"#);
        store.set("array/zarr.json", zarr_meta.clone()).await?;

        // a small inline chunk
        let small_data = Bytes::copy_from_slice(b"hello");
        store.set("array/c/0/1/0", small_data.clone()).await?;
        assert_eq!(store.get("array/c/0/1/0", &(None, None)).await.unwrap(), small_data);
        // no new chunks written because it was inline
        // FiXME: add this test
        //assert!(in_mem_storage.chunk_ids().is_empty());

        // a big chunk
        let big_data = Bytes::copy_from_slice(b"hello".repeat(512).as_slice());
        store.set("array/c/0/1/1", big_data.clone()).await?;
        assert_eq!(store.get("array/c/0/1/1", &(None, None)).await.unwrap(), big_data);
        // FiXME: add this test
        //let chunk_id = in_mem_storage.chunk_ids().iter().next().cloned().unwrap();
        //assert_eq!(in_mem_storage.fetch_chunk(&chunk_id, &None).await?, big_data);

        let mut ds = store.dataset();
        let oid = ds.flush().await?;

        let ds = Dataset::update(storage, oid).build();
        let store = Store::new(ds, None);
        assert_eq!(store.get("array/c/0/1/0", &(None, None)).await.unwrap(), small_data);
        assert_eq!(store.get("array/c/0/1/1", &(None, None)).await.unwrap(), big_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_chunk_delete() {
        let in_mem_storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let storage =
            Arc::clone(&(in_mem_storage.clone() as Arc<dyn Storage + Send + Sync>));
        let ds = Dataset::create(Arc::clone(&storage)).build();
        let mut store = Store::new(ds, None);

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
            store.get("array/c/0/1/0", &(None, None)).await,
            Err(StoreError::NotFound(KeyNotFoundError::ChunkNotFound { key, path, coords }))
                if key == "array/c/0/1/0" && path.to_str() == Some("/array") && coords == ChunkIndices([0, 1, 0].to_vec())
        ));
        assert!(matches!(
            store.delete("array/foo").await,
            Err(StoreError::InvalidKey { key }) if key == "array/foo",
        ));
        // FIXME: deleting an invalid chunk should not be allowed.
        store.delete("array/c/10/1/1").await.unwrap();
    }

    #[tokio::test]
    async fn test_metadata_list() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let ds = Dataset::create(Arc::clone(&storage)).build();
        let mut store = Store::new(ds, None);

        assert!(store.empty().await.unwrap());
        assert!(!store.exists("zarr.json").await.unwrap());

        assert_eq!(all_keys(&store).await.unwrap(), Vec::<String>::new());
        store
            .borrow_mut()
            .set(
                "zarr.json",
                Bytes::copy_from_slice(br#"{"zarr_format":3, "node_type":"group"}"#),
            )
            .await?;

        assert!(!store.empty().await.unwrap());
        assert!(store.exists("zarr.json").await.unwrap());
        assert_eq!(all_keys(&store).await.unwrap(), vec!["zarr.json".to_string()]);
        store
            .borrow_mut()
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
        assert!(!store.empty().await.unwrap());
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
    async fn test_chunk_list() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let ds = Dataset::create(Arc::clone(&storage)).build();
        let mut store = Store::new(ds, None);

        store
            .borrow_mut()
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
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let ds = Dataset::create(Arc::clone(&storage)).build();
        let mut store = Store::new(ds, None);

        store
            .borrow_mut()
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

        assert_eq!(
            all_keys(&store).await.unwrap(),
            vec![
                "array/c/0/1/0".to_string(),
                "array/c/1/1/1".to_string(),
                "array/zarr.json".to_string(),
                "zarr.json".to_string()
            ]
        );

        let mut dir = store.list_dir("/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["array".to_string(), "zarr.json".to_string()]);

        let mut dir = store.list_dir("array").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["c".to_string(), "zarr.json".to_string()]);

        let mut dir = store.list_dir("array/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["c".to_string(), "zarr.json".to_string()]);

        let mut dir = store.list_dir("array/c/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["0".to_string(), "1".to_string()]);

        let mut dir = store.list_dir("array/c/1/").await?.try_collect::<Vec<_>>().await?;
        dir.sort();
        assert_eq!(dir, vec!["1".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_partial_values() -> Result<(), Box<dyn std::error::Error>> {
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let ds = Dataset::create(Arc::clone(&storage)).build();
        let mut store = Store::new(ds, None);

        store
            .borrow_mut()
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

        let key_ranges =
            key_vals.iter().map(|(k, _)| (k.clone(), (None::<u64>, None::<u64>)));

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
        let key_ranges =
            key_vals.iter().rev().map(|(k, _)| (k.clone(), (None::<u64>, None::<u64>)));

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
        let storage: Arc<dyn Storage + Send + Sync> =
            Arc::new(ObjectStorage::new_in_memory_store());
        let ds = Dataset::create(Arc::clone(&storage)).build();
        let mut store = Store::new(ds, None);

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

        let (snapshot_id, _version) =
            store.commit("main", "initial commit").await.unwrap();

        let new_data = Bytes::copy_from_slice(b"world");
        store.set("array/c/0/1/0", new_data.clone()).await.unwrap();
        let (new_snapshot_id, _version) = store.commit("main", "update").await.unwrap();

        store.checkout(VersionInfo::SnapshotId(snapshot_id.clone())).await.unwrap();
        assert_eq!(store.get("array/c/0/1/0", &(None, None)).await.unwrap(), data);

        store.checkout(VersionInfo::SnapshotId(new_snapshot_id)).await.unwrap();
        assert_eq!(store.get("array/c/0/1/0", &(None, None)).await.unwrap(), new_data);

        let new_store_from_snapshot =
            Store::new(Dataset::update(Arc::clone(&storage), snapshot_id).build(), None);
        assert_eq!(
            new_store_from_snapshot.get("array/c/0/1/0", &(None, None)).await.unwrap(),
            data
        );

        Ok(())
    }

    #[test]
    fn test_store_config_deserialization() -> Result<(), Box<dyn std::error::Error>> {
        let expected = StoreConfig {
            storage: StorageConfig::Cached {
                approx_max_memory_bytes: 1_000_000,
                backend: Box::new(StorageConfig::LocalFileSystem {
                    root: "/tmp/test".into(),
                }),
            },
            dataset: DatasetConfig {
                inline_chunk_threshold_bytes: Some(128),
                previous_version: Some(VersionInfo::SnapshotId(ObjectId([
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                ]))),
            },
            get_partial_values_concurrency: Some(100),
        };

        let json = r#"
            {"storage": {
                "type": "cached",
                "approx_max_memory_bytes":1000000,
                "backend":{"type": "local_filesystem", "root":"/tmp/test"}
                },
             "dataset": {
                "previous_version": {"snapshot_id":"000G40R40M30E209185GR38E1W"},
                "inline_chunk_threshold_bytes":128
             },
             "get_partial_values_concurrency": 100
            }
        "#;
        assert_eq!(expected, serde_json::from_str(json)?);

        let json = r#"
            {"storage":
                {"type": "cached",
                 "approx_max_memory_bytes":1000000,
                 "backend":{"type": "local_filesystem", "root":"/tmp/test"}
                },
             "dataset": {
                "previous_version": null,
                "inline_chunk_threshold_bytes": null
             }}
        "#;
        assert_eq!(
            StoreConfig {
                dataset: DatasetConfig {
                    previous_version: None,
                    inline_chunk_threshold_bytes: None,
                },
                get_partial_values_concurrency: None,
                ..expected.clone()
            },
            serde_json::from_str(json)?
        );

        let json = r#"
            {"storage":
                {"type": "cached",
                 "approx_max_memory_bytes":1000000,
                 "backend":{"type": "local_filesystem", "root":"/tmp/test"}
                },
             "dataset": {}
            }
        "#;
        assert_eq!(
            StoreConfig {
                dataset: DatasetConfig {
                    previous_version: None,
                    inline_chunk_threshold_bytes: None,
                },
                get_partial_values_concurrency: None,
                ..expected.clone()
            },
            serde_json::from_str(json)?
        );

        let json = r#"
            {"storage":{"type": "in_memory"},
             "dataset": {}
            }
        "#;
        assert_eq!(
            StoreConfig {
                dataset: DatasetConfig {
                    previous_version: None,
                    inline_chunk_threshold_bytes: None,
                },
                storage: StorageConfig::InMemory,
                get_partial_values_concurrency: None,
            },
            serde_json::from_str(json)?
        );

        let json = r#"
            {"storage":{"type": "s3", "bucket":"test", "prefix":"root"},
             "dataset": {}
            }
        "#;
        assert_eq!(
            StoreConfig {
                dataset: DatasetConfig {
                    previous_version: None,
                    inline_chunk_threshold_bytes: None,
                },
                storage: StorageConfig::S3ObjectStore {
                    bucket: String::from("test"),
                    prefix: String::from("root"),
                    access_key_id: None,
                    secret_access_key: None,
                    endpoint: None
                },
                get_partial_values_concurrency: None,
            },
            serde_json::from_str(json)?
        );

        let json = r#"
        {"storage":{
             "type": "s3", 
             "bucket":"test", 
             "prefix":"root", 
             "access_key_id":"my-key", 
             "secret_access_key":"my-secret-key", 
             "endpoint": "http://localhost:9000"
         },
         "dataset": {}
        }
    "#;
        assert_eq!(
            StoreConfig {
                dataset: DatasetConfig {
                    previous_version: None,
                    inline_chunk_threshold_bytes: None,
                },
                storage: StorageConfig::S3ObjectStore {
                    bucket: String::from("test"),
                    prefix: String::from("root"),
                    access_key_id: Some(String::from("my-key")),
                    secret_access_key: Some(String::from("my-secret-key")),
                    endpoint: Some(String::from("http://localhost:9000"))
                },
                get_partial_values_concurrency: None,
            },
            serde_json::from_str(json)?
        );

        Ok(())
    }
}
