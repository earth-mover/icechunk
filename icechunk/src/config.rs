use core::fmt;
use std::{
    collections::HashMap,
    ops::Bound,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
pub use object_store::gcp::GcpCredential;
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
    // field was added in v0.2.6
    #[serde(default = "default_force_path_style")]
    pub force_path_style: bool,
}

fn default_force_path_style() -> bool {
    false
}

impl fmt::Display for S3Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "S3Options(region={}, endpoint_url={}, anonymous={}, allow_http={}, force_path_style={})",
            self.region.as_deref().unwrap_or("None"),
            self.endpoint_url.as_deref().unwrap_or("None"),
            self.anonymous,
            self.allow_http,
            self.force_path_style,
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
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
#[serde(rename_all = "snake_case")]
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
        self.level.unwrap_or(3)
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ManifestPreloadCondition {
    Or(Vec<ManifestPreloadCondition>),
    And(Vec<ManifestPreloadCondition>),
    PathMatches { regex: String },
    NameMatches { regex: String },
    NumRefs { from: Bound<u32>, to: Bound<u32> },
    True,
    False,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct ManifestPreloadConfig {
    pub max_total_refs: Option<u32>,
    pub preload_if: Option<ManifestPreloadCondition>,
}

impl ManifestPreloadConfig {
    pub fn merge(&self, other: Self) -> Self {
        Self {
            max_total_refs: other.max_total_refs.or(self.max_total_refs),
            preload_if: other.preload_if.or(self.preload_if.clone()),
        }
    }

    pub fn max_total_refs(&self) -> u32 {
        self.max_total_refs.unwrap_or(10_000)
    }

    pub fn preload_if(&self) -> &ManifestPreloadCondition {
        self.preload_if.as_ref().unwrap_or_else(|| {
            DEFAULT_MANIFEST_PRELOAD_CONDITION.get_or_init(|| {
                ManifestPreloadCondition::And(vec![
                    ManifestPreloadCondition::Or(vec![
                        // regexes taken from https://github.com/xarray-contrib/cf-xarray/blob/1591ff5ea7664a6bdef24055ef75e242cd5bfc8b/cf_xarray/criteria.py#L149-L160
                        ManifestPreloadCondition::NameMatches {
                            // time
                            regex: r#"^\bt\b$|^(time|min|hour|day|week|month|year)[0-9]*$"#.to_string(), // codespell:ignore
                        },
                        ManifestPreloadCondition::NameMatches {
                            // Z
                            regex: r#"^(z|nav_lev|gdep|lv_|[o]*lev|bottom_top|sigma|h(ei)?ght|altitude|depth|isobaric|pres|isotherm)[a-z_]*[0-9]*$"#.to_string(), // codespell:ignore

                        },
                        ManifestPreloadCondition::NameMatches {
                            // Y
                            regex: r#"^(y|j|nlat|rlat|nj)$"#.to_string(), // codespell:ignore
                        },
                        ManifestPreloadCondition::NameMatches {
                            // latitude
                            regex: r#"^y?(nav_lat|lat|gphi)[a-z0-9]*$"#.to_string(), // codespell:ignore
                        },
                        ManifestPreloadCondition::NameMatches {
                            // longitude
                            regex: r#"^x?(nav_lon|lon|glam)[a-z0-9]*$"#.to_string(), // codespell:ignore
                        },
                        ManifestPreloadCondition::NameMatches {
                            // X
                            regex: r#"^(x|i|nlon|rlon|ni)$"#.to_string(), // codespell:ignore
                        },
                    ]),
                    ManifestPreloadCondition::NumRefs {
                        from: Bound::Unbounded,
                        to: Bound::Included(1000),
                    },
                ])
            })
        })
    }
}

static DEFAULT_MANIFEST_PRELOAD_CONDITION: OnceLock<ManifestPreloadCondition> =
    OnceLock::new();

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct ManifestConfig {
    pub preload: Option<ManifestPreloadConfig>,
}

static DEFAULT_MANIFEST_PRELOAD_CONFIG: OnceLock<ManifestPreloadConfig> = OnceLock::new();

impl ManifestConfig {
    pub fn merge(&self, other: Self) -> Self {
        Self { preload: other.preload.or(self.preload.clone()) }
    }

    pub fn preload(&self) -> &ManifestPreloadConfig {
        self.preload.as_ref().unwrap_or_else(|| {
            DEFAULT_MANIFEST_PRELOAD_CONFIG.get_or_init(ManifestPreloadConfig::default)
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RepositoryConfig {
    /// Chunks smaller than this will be stored inline in the manifest
    pub inline_chunk_threshold_bytes: Option<u16>,

    /// Concurrency used by the get_partial_values operation to fetch different keys in parallel
    pub get_partial_values_concurrency: Option<u16>,

    pub compression: Option<CompressionConfig>,
    pub caching: Option<CachingConfig>,

    // If not set it will use the Storage implementation default
    pub storage: Option<storage::Settings>,

    pub virtual_chunk_containers: Option<HashMap<ContainerName, VirtualChunkContainer>>,

    pub manifest: Option<ManifestConfig>,
}

static DEFAULT_COMPRESSION: OnceLock<CompressionConfig> = OnceLock::new();
static DEFAULT_CACHING: OnceLock<CachingConfig> = OnceLock::new();
static DEFAULT_VIRTUAL_CHUNK_CONTAINERS: OnceLock<
    HashMap<ContainerName, VirtualChunkContainer>,
> = OnceLock::new();
static DEFAULT_MANIFEST_CONFIG: OnceLock<ManifestConfig> = OnceLock::new();

impl RepositoryConfig {
    pub fn inline_chunk_threshold_bytes(&self) -> u16 {
        self.inline_chunk_threshold_bytes.unwrap_or(512)
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

    pub fn manifest(&self) -> &ManifestConfig {
        self.manifest.as_ref().unwrap_or_else(|| {
            DEFAULT_MANIFEST_CONFIG.get_or_init(ManifestConfig::default)
        })
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            inline_chunk_threshold_bytes: other
                .inline_chunk_threshold_bytes
                .or(self.inline_chunk_threshold_bytes),
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
            manifest: match (&self.manifest, other.manifest) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
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
#[typetag::serde(tag = "s3_credentials_fetcher_type")]
pub trait S3CredentialsFetcher: fmt::Debug + Sync + Send {
    async fn get(&self) -> Result<S3StaticCredentials, String>;
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(tag = "s3_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum S3Credentials {
    #[default]
    FromEnv,
    Anonymous,
    Static(S3StaticCredentials),
    Refreshable(Arc<dyn S3CredentialsFetcher>),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "gcs_static_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum GcsStaticCredentials {
    ServiceAccount(PathBuf),
    ServiceAccountKey(String),
    ApplicationCredentials(PathBuf),
    BearerToken(GcsBearerCredential),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "gcs_bearer_credential_type")]
#[serde(rename_all = "snake_case")]
pub struct GcsBearerCredential {
    pub bearer: String,
    pub expires_after: Option<DateTime<Utc>>,
}

impl From<&GcsBearerCredential> for GcpCredential {
    fn from(value: &GcsBearerCredential) -> Self {
        GcpCredential { bearer: value.bearer.clone() }
    }
}

#[async_trait]
#[typetag::serde(tag = "gcs_credentials_fetcher_type")]
pub trait GcsCredentialsFetcher: fmt::Debug + Sync + Send {
    async fn get(&self) -> Result<GcsBearerCredential, String>;
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(tag = "gcs_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum GcsCredentials {
    #[default]
    FromEnv,
    Static(GcsStaticCredentials),
    Refreshable(Arc<dyn GcsCredentialsFetcher>),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "az_static_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum AzureStaticCredentials {
    AccessKey(String),
    SASToken(String),
    BearerToken(String),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "az_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum AzureCredentials {
    FromEnv,
    Static(AzureStaticCredentials),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "credential_type")]
#[serde(rename_all = "snake_case")]
pub enum Credentials {
    S3(S3Credentials),
    Gcs(GcsCredentials),
    Azure(AzureCredentials),
}
