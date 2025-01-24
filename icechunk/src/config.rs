use core::fmt;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    storage,
    virtual_chunks::{mk_default_containers, ContainerName, VirtualChunkContainer},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3Options {
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub anonymous: bool,
    pub allow_http: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectStoreConfig {
    InMemory,
    LocalFileSystem(PathBuf),
    S3Compatible(S3Options),
    S3(S3Options),
    Gcs(HashMap<String, String>),
    Azure(HashMap<String, String>),
    Tigris(S3Options),
}

#[derive(Debug, PartialEq, Eq, Default, Serialize, Deserialize, Clone, Copy)]
pub enum CompressionAlgorithm {
    #[default]
    Zstd,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct CompressionConfig {
    pub algorithm: Option<CompressionAlgorithm>,
    pub level: Option<u8>,
}

impl CompressionConfig {
    pub fn algorithm(&self) -> CompressionAlgorithm {
        self.algorithm.unwrap_or_default()
    }

    pub fn level(&self) -> u8 {
        self.level.unwrap_or(1)
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            algorithm: other.algorithm.or(self.algorithm),
            level: other.level.or(self.level),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct CachingConfig {
    pub num_snapshot_nodes: Option<u64>,
    pub num_chunk_refs: Option<u64>,
    pub num_transaction_changes: Option<u64>,
    pub num_bytes_attributes: Option<u64>,
    pub num_bytes_chunks: Option<u64>,
}

impl CachingConfig {
    pub fn num_snapshot_nodes(&self) -> u64 {
        self.num_snapshot_nodes.unwrap_or(10_000)
    }
    pub fn num_chunk_refs(&self) -> u64 {
        self.num_chunk_refs.unwrap_or(5_000_000)
    }
    pub fn num_transaction_changes(&self) -> u64 {
        self.num_transaction_changes.unwrap_or(0)
    }
    pub fn num_bytes_attributes(&self) -> u64 {
        self.num_bytes_attributes.unwrap_or(0)
    }
    pub fn num_bytes_chunks(&self) -> u64 {
        self.num_bytes_chunks.unwrap_or(0)
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            num_snapshot_nodes: other.num_snapshot_nodes.or(self.num_snapshot_nodes),
            num_chunk_refs: other.num_chunk_refs.or(self.num_chunk_refs),
            num_transaction_changes: other
                .num_transaction_changes
                .or(self.num_transaction_changes),
            num_bytes_attributes: other
                .num_bytes_attributes
                .or(self.num_bytes_attributes),
            num_bytes_chunks: other.num_bytes_chunks.or(self.num_bytes_chunks),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RepositoryConfig {
    /// Chunks smaller than this will be stored inline in the manifst
    pub inline_chunk_threshold_bytes: Option<u16>,
    /// Unsafely overwrite refs on write. This is not recommended, users should only use it at their
    /// own risk in object stores for which we don't support write-object-if-not-exists. There is
    /// the possibility of race conditions if this variable is set to true and there are concurrent
    /// commit attempts.
    pub unsafe_overwrite_refs: Option<bool>,

    /// Concurrency used by the get_partial_values operation to fetch different keys in parallel
    pub get_partial_values_concurrency: Option<u16>,

    pub compression: Option<CompressionConfig>,
    pub caching: Option<CachingConfig>,

    // If not set it will use the Storage implementation default
    pub storage: Option<storage::Settings>,

    pub virtual_chunk_containers: Option<HashMap<ContainerName, VirtualChunkContainer>>,
}

static DEFAULT_COMPRESSION: OnceLock<CompressionConfig> = OnceLock::new();
static DEFAULT_CACHING: OnceLock<CachingConfig> = OnceLock::new();
static DEFAULT_VIRTUAL_CHUNK_CONTAINERS: OnceLock<
    HashMap<ContainerName, VirtualChunkContainer>,
> = OnceLock::new();

impl RepositoryConfig {
    pub fn inline_chunk_threshold_bytes(&self) -> u16 {
        self.inline_chunk_threshold_bytes.unwrap_or(512)
    }
    pub fn unsafe_overwrite_refs(&self) -> bool {
        self.unsafe_overwrite_refs.unwrap_or(false)
    }
    pub fn get_partial_values_concurrency(&self) -> u16 {
        self.get_partial_values_concurrency.unwrap_or(10)
    }

    pub fn compression(&self) -> &CompressionConfig {
        self.compression.as_ref().unwrap_or_else(|| {
            DEFAULT_COMPRESSION.get_or_init(CompressionConfig::default)
        })
    }
    pub fn caching(&self) -> &CachingConfig {
        self.caching
            .as_ref()
            .unwrap_or_else(|| DEFAULT_CACHING.get_or_init(CachingConfig::default))
    }
    pub fn storage(&self) -> Option<&storage::Settings> {
        self.storage.as_ref()
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            inline_chunk_threshold_bytes: other
                .inline_chunk_threshold_bytes
                .or(self.inline_chunk_threshold_bytes),
            unsafe_overwrite_refs: other
                .unsafe_overwrite_refs
                .or(self.unsafe_overwrite_refs),
            get_partial_values_concurrency: other
                .get_partial_values_concurrency
                .or(self.get_partial_values_concurrency),
            compression: match (&self.compression, other.compression) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            caching: match (&self.caching, other.caching) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            storage: match (&self.storage, other.storage) {
                (None, None) => None,
                (None, Some(s)) => Some(s),
                (Some(s), None) => Some(s.clone()),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            virtual_chunk_containers: match (
                &self.virtual_chunk_containers,
                other.virtual_chunk_containers,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(mine), Some(theirs)) => {
                    let mut merged = mine.clone();
                    merged.extend(theirs);
                    Some(merged)
                }
            },
        }
    }
}

impl RepositoryConfig {
    pub fn set_virtual_chunk_container(&mut self, cont: VirtualChunkContainer) {
        if self.virtual_chunk_containers.is_none() {
            self.virtual_chunk_containers = Some(
                self.virtual_chunk_containers()
                    .map(|cont| (cont.name.clone(), cont.clone()))
                    .collect(),
            )
        }
        self.virtual_chunk_containers
            .as_mut()
            .map(|map| map.insert(cont.name.clone(), cont));
    }

    pub fn get_virtual_chunk_container(
        &self,
        cont_name: &str,
    ) -> Option<&VirtualChunkContainer> {
        self.virtual_chunk_containers
            .as_ref()
            .unwrap_or_else(|| {
                DEFAULT_VIRTUAL_CHUNK_CONTAINERS.get_or_init(mk_default_containers)
            })
            .get(cont_name)
    }

    pub fn virtual_chunk_containers(
        &self,
    ) -> impl Iterator<Item = &VirtualChunkContainer> {
        self.virtual_chunk_containers
            .as_ref()
            .unwrap_or_else(|| {
                DEFAULT_VIRTUAL_CHUNK_CONTAINERS.get_or_init(mk_default_containers)
            })
            .values()
    }

    pub fn clear_virtual_chunk_containers(&mut self) {
        self.virtual_chunk_containers = Some(Default::default())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct S3StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expires_after: Option<DateTime<Utc>>,
}

#[async_trait]
#[typetag::serde(tag = "type")]
pub trait CredentialsFetcher: fmt::Debug + Sync + Send {
    async fn get(&self) -> Result<S3StaticCredentials, String>;
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(tag = "type")]
pub enum S3Credentials {
    #[default]
    #[serde(rename = "from_env")]
    FromEnv,
    #[serde(rename = "none")]
    Anonymous,
    #[serde(rename = "static")]
    Static(S3StaticCredentials),
    #[serde(rename = "refreshable")]
    Refreshable(Arc<dyn CredentialsFetcher>),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum GcsStaticCredentials {
    ServiceAccount(PathBuf),
    ServiceAccountKey(String),
    ApplicationCredentials(PathBuf),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum GcsCredentials {
    #[serde(rename = "from_env")]
    FromEnv,
    #[serde(rename = "static")]
    Static(GcsStaticCredentials),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum AzureStaticCredentials {
    AccessKey(String),
    SASToken(String),
    BearerToken(String),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum AzureCredentials {
    #[serde(rename = "from_env")]
    FromEnv,
    #[serde(rename = "static")]
    Static(AzureStaticCredentials),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Credentials {
    S3(S3Credentials),
    Gcs(GcsCredentials),
    Azure(AzureCredentials),
}
