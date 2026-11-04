//! Configuration types for repositories and storage.
//!
//! Key types:
//! - [`RepositoryConfig`] - Main configuration for a repository
//! - [`ObjectStoreConfig`] - Storage backend selection (S3, GCS, Azure, local, etc.)
//! - [`S3Credentials`], [`GcsCredentials`], [`AzureCredentials`] - Cloud authentication
//! - [`CachingConfig`] - Cache size limits
//! - [`ManifestSplittingConfig`] - Controls how manifests are partitioned

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Display,
    num::NonZeroU16,
    ops::Bound,
    path::PathBuf,
    sync::OnceLock,
};

use flexbuffers::{FlexBufferType, MapReader, Reader, ReaderError};
use icechunk_types::ICResultExt as _;
use itertools::Either;
use regex::bytes::Regex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    format::{
        IcechunkFormatError, IcechunkFormatErrorKind, IcechunkResult, Path,
        envelope::DEFAULT_COMPRESSION_LEVEL,
    },
    storage::{self, RetriesSettings},
    virtual_chunks::{UndecodableVirtualChunkContainer, VirtualChunkContainer},
};

// Re-export backend-specific types from their canonical modules so that existing
// consumers (`crate::config::S3Options`, etc.) continue to work.
pub use crate::storage::s3_config::{
    S3ChecksumAlgorithm, S3Credentials, S3CredentialsFetcher, S3Options,
    S3StaticCredentials,
};
#[cfg(feature = "object-store-azure")]
pub use crate::storage::{
    AzureCredentials, AzureCredentialsFetcher, AzureRefreshableCredential,
    AzureStaticCredentials,
};
#[cfg(feature = "object-store-gcs")]
pub use crate::storage::{
    GcsBearerCredential, GcsCredentials, GcsCredentialsFetcher, GcsStaticCredentials,
};
#[cfg(feature = "object-store-gcs")]
pub use icechunk_arrow_object_store::object_store::gcp::GcpCredential;

/// Configuration for the HTTP(S) object store backend.
///
/// The `opts` field accepts `ClientConfigKey` names (in `snake_case`) as keys and is
/// flattened in serde so that existing serialized configs that stored a plain
/// `HashMap<String, String>` continue to deserialise correctly.
///
/// The `headers` field carries static HTTP headers that are injected into every
/// request made by the underlying HTTP client (e.g. `"authorization": "Bearer …"`).
/// Header values are **not** included in `Debug`/`Display` output to avoid leaking
/// credentials in logs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct HttpConfig {
    /// Generic transport options (`ClientConfigKey` names → values).
    #[serde(default, flatten)]
    pub opts: HashMap<String, String>,
    /// Static HTTP headers injected into every request.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
}

/// Storage backend configuration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreConfig {
    InMemory,
    #[cfg(feature = "object-store-fs")]
    LocalFileSystem(PathBuf),
    Http(HttpConfig),
    S3Compatible(S3Options),
    S3(S3Options),
    #[cfg(feature = "object-store-gcs")]
    Gcs(HashMap<String, String>),
    /// For compatibility reason we cannot change this structure,
    /// but a key in this map should be "account"
    #[cfg(feature = "object-store-azure")]
    Azure(HashMap<String, String>),
    Tigris(S3Options),
}

#[derive(Debug, PartialEq, Eq, Default, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum CompressionAlgorithm {
    #[default]
    Zstd,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy, Default)]
pub struct CompressionConfig {
    #[serde(default)]
    pub algorithm: Option<CompressionAlgorithm>,
    #[serde(default)]
    pub level: Option<u8>,
}

impl CompressionConfig {
    pub fn algorithm(&self) -> CompressionAlgorithm {
        self.algorithm.unwrap_or_default()
    }

    pub fn level(&self) -> u8 {
        self.level.unwrap_or(DEFAULT_COMPRESSION_LEVEL)
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            algorithm: other.algorithm.or(self.algorithm),
            level: other.level.or(self.level),
        }
    }
}

/// Cache size configuration for in-memory caches.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy, Default)]
pub struct CachingConfig {
    #[serde(default)]
    pub num_snapshot_nodes: Option<u64>,

    #[serde(default)]
    pub num_chunk_refs: Option<u64>,

    #[serde(default)]
    pub num_transaction_changes: Option<u64>,

    #[serde(default)]
    pub num_bytes_attributes: Option<u64>,

    #[serde(default)]
    pub num_bytes_chunks: Option<u64>,
}

impl CachingConfig {
    pub fn num_snapshot_nodes(&self) -> u64 {
        self.num_snapshot_nodes.unwrap_or(500_000)
    }
    pub fn num_chunk_refs(&self) -> u64 {
        self.num_chunk_refs.unwrap_or(15_000_000)
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

#[derive(Debug, PartialEq, Eq, Serialize, Hash, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ManifestSplitCondition {
    Or(Vec<ManifestSplitCondition>),
    And(Vec<ManifestSplitCondition>),
    PathMatches { regex: String },
    NameMatches { regex: String },
    AnyArray,
}

//```yaml
//rules:
//  - path: ./2m_temperature  # regex, 3D variable: (null, latitude, longitude)
//    manifest-split-sizes:
//      - 0: 120
//  - path: ./temperature  # 4D variable: (time, level, latitude, longitude)
//    manifest-split-sizes:
//      - "level": 1  # alternatively 0: 1
//      - "time": 12  #           and 1: 12
//  - path: ./temperature
//    manifest-split-sizes:
//      - "level": 1
//      - "time": 8760  # ~1 year
//      - "latitude": null  # for unspecified, default is null, which means never split.
//  - path: ./*   # the default rules
//    manifest-split-sizes: null  # no splitting, just a single manifest per array
//```

impl ManifestSplitCondition {
    // from_yaml?
    pub fn matches(&self, path: &Path) -> bool {
        use ManifestSplitCondition::*;
        match self {
            AnyArray => true,
            Or(vec) => vec.iter().any(|c| c.matches(path)),
            And(vec) => vec.iter().all(|c| c.matches(path)),
            // TODO: precompile the regex
            PathMatches { regex } => Regex::new(regex)
                .map(|regex| regex.is_match(path.to_string().as_bytes()))
                .unwrap_or(false),
            // TODO: precompile the regex
            NameMatches { regex } => Regex::new(regex)
                .map(|regex| {
                    path.name()
                        .map(|name| regex.is_match(name.as_bytes()))
                        .unwrap_or(false)
                })
                .unwrap_or(false),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum ManifestSplitDimCondition {
    Axis(usize),
    DimensionName(String),
    // TODO: Since dimension name can be null,
    // i don't think we can have DimensionName(r"*") catch the "Any" case
    Any,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct ManifestSplitDim {
    pub condition: ManifestSplitDimCondition,
    pub num_chunks: u32,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct ManifestSplittingConfig {
    // need to preserve insertion order of conditions, so hashmap doesn't work
    pub split_sizes: Option<Vec<(ManifestSplitCondition, Vec<ManifestSplitDim>)>>,
}

impl Default for ManifestSplittingConfig {
    fn default() -> Self {
        let inner = vec![ManifestSplitDim {
            condition: ManifestSplitDimCondition::Any,
            num_chunks: u32::MAX,
        }];
        let new = vec![(
            ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
            inner,
        )];
        Self { split_sizes: Some(new) }
    }
}

impl ManifestSplittingConfig {
    pub fn with_size(split_size: u32) -> Self {
        let split_sizes = vec![(
            ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
            vec![ManifestSplitDim {
                condition: ManifestSplitDimCondition::Any,
                num_chunks: split_size,
            }],
        )];
        ManifestSplittingConfig { split_sizes: Some(split_sizes) }
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
    pub max_arrays_to_scan: Option<u32>,
}

impl ManifestPreloadConfig {
    pub fn merge(&self, other: Self) -> Self {
        Self {
            max_total_refs: other.max_total_refs.or(self.max_total_refs),
            preload_if: other.preload_if.or(self.preload_if.clone()),
            max_arrays_to_scan: other.max_arrays_to_scan.or(self.max_arrays_to_scan),
        }
    }

    pub fn max_total_refs(&self) -> u32 {
        self.max_total_refs.unwrap_or(10_000)
    }

    pub fn max_arrays_to_scan(&self) -> u32 {
        self.max_arrays_to_scan.unwrap_or(50)
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy, Default)]
pub struct ManifestVirtualChunkLocationCompressionConfig {
    #[serde(default)]
    pub min_num_chunks: Option<u16>,
    #[serde(default)]
    pub dictionary_max_training_samples: Option<u16>,
    #[serde(default)]
    pub dictionary_max_size_bytes: Option<u32>,
    #[serde(default)]
    pub compression_level: Option<i32>,
}

impl ManifestVirtualChunkLocationCompressionConfig {
    pub fn min_num_chunks(&self) -> u16 {
        self.min_num_chunks.unwrap_or(1000)
    }

    pub fn dictionary_max_training_samples(&self) -> u16 {
        self.dictionary_max_training_samples.unwrap_or(100)
    }

    pub fn dictionary_max_size_bytes(&self) -> u32 {
        self.dictionary_max_size_bytes.unwrap_or(2 * 1024)
    }

    pub fn compression_level(&self) -> i32 {
        self.compression_level.unwrap_or(3)
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            min_num_chunks: other.min_num_chunks.or(self.min_num_chunks),
            dictionary_max_training_samples: other
                .dictionary_max_training_samples
                .or(self.dictionary_max_training_samples),
            dictionary_max_size_bytes: other
                .dictionary_max_size_bytes
                .or(self.dictionary_max_size_bytes),
            compression_level: other.compression_level.or(self.compression_level),
        }
    }
}

impl From<&ManifestVirtualChunkLocationCompressionConfig>
    for crate::format::manifest::LocationCompressionConfig
{
    fn from(c: &ManifestVirtualChunkLocationCompressionConfig) -> Self {
        Self {
            min_num_chunks: c.min_num_chunks(),
            dictionary_max_training_samples: c.dictionary_max_training_samples(),
            dictionary_max_size_bytes: c.dictionary_max_size_bytes(),
            compression_level: c.compression_level(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct ManifestConfig {
    #[serde(default)]
    pub preload: Option<ManifestPreloadConfig>,
    #[serde(default)]
    pub splitting: Option<ManifestSplittingConfig>,
    #[serde(default)]
    pub virtual_chunk_location_compression:
        Option<ManifestVirtualChunkLocationCompressionConfig>,
    #[serde(default)]
    pub max_concurrent_manifest_fetches_during_commit: Option<u16>,
}

static DEFAULT_MANIFEST_PRELOAD_CONFIG: OnceLock<ManifestPreloadConfig> = OnceLock::new();
static DEFAULT_MANIFEST_SPLITTING_CONFIG: OnceLock<ManifestSplittingConfig> =
    OnceLock::new();
static DEFAULT_MANIFEST_VIRTUAL_CHUNK_LOCATION_COMPRESSION_CONFIG: OnceLock<
    ManifestVirtualChunkLocationCompressionConfig,
> = OnceLock::new();

impl ManifestConfig {
    pub fn merge(&self, other: Self) -> Self {
        Self {
            preload: other.preload.or(self.preload.clone()),
            splitting: other.splitting.or(self.splitting.clone()),
            virtual_chunk_location_compression: match (
                &self.virtual_chunk_location_compression,
                other.virtual_chunk_location_compression,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            max_concurrent_manifest_fetches_during_commit: other
                .max_concurrent_manifest_fetches_during_commit
                .or(self.max_concurrent_manifest_fetches_during_commit),
        }
    }

    pub fn preload(&self) -> &ManifestPreloadConfig {
        self.preload.as_ref().unwrap_or_else(|| {
            DEFAULT_MANIFEST_PRELOAD_CONFIG.get_or_init(ManifestPreloadConfig::default)
        })
    }

    pub fn splitting(&self) -> &ManifestSplittingConfig {
        self.splitting.as_ref().unwrap_or_else(|| {
            DEFAULT_MANIFEST_SPLITTING_CONFIG
                .get_or_init(ManifestSplittingConfig::default)
        })
    }

    pub fn virtual_chunk_location_compression(
        &self,
    ) -> &ManifestVirtualChunkLocationCompressionConfig {
        self.virtual_chunk_location_compression.as_ref().unwrap_or_else(|| {
            DEFAULT_MANIFEST_VIRTUAL_CHUNK_LOCATION_COMPRESSION_CONFIG
                .get_or_init(ManifestVirtualChunkLocationCompressionConfig::default)
        })
    }

    /// The effective manifest update concurrency, always at least 1.
    pub fn max_concurrent_manifest_fetches_during_commit(&self) -> u16 {
        self.max_concurrent_manifest_fetches_during_commit.unwrap_or(1).max(1)
    }

    // for testing only, create a config with no preloading, no splitting, and no max_arrays to scan
    pub fn empty() -> Self {
        ManifestConfig {
            preload: Some(ManifestPreloadConfig {
                max_total_refs: None,
                preload_if: None,
                max_arrays_to_scan: None,
            }),
            splitting: None,
            virtual_chunk_location_compression: None,
            max_concurrent_manifest_fetches_during_commit: None,
        }
    }
}

/// Retry configuration for repo info update operations.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RepoUpdateRetryConfig {
    /// Default retry settings for all repo update operations.
    #[serde(default)]
    pub default: Option<RetriesSettings>,
}

static DEFAULT_REPO_UPDATE_RETRIES: OnceLock<RetriesSettings> = OnceLock::new();

impl RepoUpdateRetryConfig {
    fn default_retries() -> &'static RetriesSettings {
        DEFAULT_REPO_UPDATE_RETRIES.get_or_init(|| RetriesSettings {
            max_tries: Some(NonZeroU16::new(100).unwrap_or(NonZeroU16::MIN)),
            initial_backoff_ms: Some(50),
            max_backoff_ms: Some(30_000),
        })
    }

    /// The configured settings, with each unset field taken from the repo
    /// update defaults rather than the generic storage ones.
    pub fn retries(&self) -> RetriesSettings {
        Self::default_retries().merge(self.default.unwrap_or_default())
    }

    pub fn merge(&self, other: Self) -> Self {
        Self {
            default: match (&self.default, other.default) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
        }
    }
}

/// Configuration options for a repository.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RepositoryConfig {
    /// Chunks smaller than this will be stored inline in the manifest
    #[serde(default)]
    pub inline_chunk_threshold_bytes: Option<u16>,

    /// Concurrency used by the `get_partial_values` operation to fetch different keys in parallel
    #[serde(default)]
    pub get_partial_values_concurrency: Option<u16>,

    #[serde(default)]
    pub compression: Option<CompressionConfig>,

    #[serde(default)]
    pub max_concurrent_requests: Option<u16>,

    #[serde(default)]
    pub max_concurrent_decodes: Option<u16>,

    #[serde(default)]
    pub caching: Option<CachingConfig>,

    // If not set it will use the Storage implementation default
    #[serde(default)]
    pub storage: Option<storage::Settings>,

    // Compatibility note:
    // The key is this hashmap used to be the ContainerName, which
    // we have eliminated. We maintain the types for compatibility
    // with persisted configurations
    #[serde(default)]
    pub virtual_chunk_containers: Option<HashMap<String, VirtualChunkContainer>>,

    #[serde(default)]
    pub manifest: Option<ManifestConfig>,

    #[serde(default)]
    pub previous_file: Option<String>,

    #[serde(default)]
    pub repo_update_retries: Option<RepoUpdateRetryConfig>,

    #[serde(default)]
    pub num_updates_per_repo_info_file: Option<u16>,
}

/// What a library couldn't decode in a stored repository configuration.
/// Saving the configuration keeps those stored values, see
/// [`RepositoryConfig::to_stored`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UndecodableConfig {
    /// Known top-level keys whose stored value didn't decode and that no
    /// later configuration has changed.
    #[serde(default)]
    keys: BTreeSet<String>,
    /// Virtual chunk containers whose store settings didn't decode, by
    /// their key in `virtual_chunk_containers`, for resolving locations.
    #[serde(default)]
    virtual_chunk_containers: HashMap<String, UndecodableVirtualChunkContainer>,
}

impl UndecodableConfig {
    pub(crate) fn virtual_chunk_containers(
        &self,
    ) -> impl Iterator<Item = &UndecodableVirtualChunkContainer> {
        self.virtual_chunk_containers.values()
    }

    /// What stays undecodable once `merged`, the result of merging another
    /// configuration into `stored`, replaces `stored`. A key stays unless the
    /// merge changes its value: echoing the same configuration back (as a
    /// binding round trip does) doesn't count as setting it. A container
    /// stays unless a container of `merged` replaces it.
    pub(crate) fn after_merge(
        mut self,
        stored: &RepositoryConfig,
        merged: &RepositoryConfig,
    ) -> Self {
        let json = |config| serde_json::to_value(config).unwrap_or_default();
        let (before, after) = (json(stored), json(merged));
        self.keys.retain(|key| before.get(key) == after.get(key));
        self.drop_replaced_containers(merged.virtual_chunk_containers.as_ref());
        self
    }

    /// Drops the containers that a decodable container of `containers`
    /// replaces: one with the same key or the same name.
    fn drop_replaced_containers(
        &mut self,
        containers: Option<&HashMap<String, VirtualChunkContainer>>,
    ) {
        let Some(containers) = containers else { return };
        self.virtual_chunk_containers
            .retain(|key, cont| !replaces(containers, key, cont.name()));
    }
}

/// Whether a container of `containers` has the key `key` or the name `name`.
fn replaces(
    containers: &HashMap<String, VirtualChunkContainer>,
    key: &str,
    name: Option<&str>,
) -> bool {
    containers.contains_key(key)
        || name.is_some_and(|name| containers.values().any(|c| c.name() == Some(name)))
}

static DEFAULT_COMPRESSION: OnceLock<CompressionConfig> = OnceLock::new();
static DEFAULT_CACHING: OnceLock<CachingConfig> = OnceLock::new();
static DEFAULT_MANIFEST_CONFIG: OnceLock<ManifestConfig> = OnceLock::new();
static DEFAULT_REPO_UPDATE_RETRY_CONFIG: OnceLock<RepoUpdateRetryConfig> =
    OnceLock::new();
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: u16 = 256;

/// One decode per core, so decompression cannot oversubscribe the machine.
pub(crate) fn default_max_concurrent_decodes() -> u16 {
    let cores = std::thread::available_parallelism().map_or(8, |n| n.get());
    u16::try_from(cores).unwrap_or(u16::MAX)
}
pub use crate::format::repo_info::DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE;

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

    pub fn max_concurrent_requests(&self) -> u16 {
        self.max_concurrent_requests.unwrap_or(DEFAULT_MAX_CONCURRENT_REQUESTS)
    }

    pub fn max_concurrent_decodes(&self) -> u16 {
        self.max_concurrent_decodes.unwrap_or_else(default_max_concurrent_decodes)
    }

    pub fn repo_update_retries(&self) -> &RepoUpdateRetryConfig {
        self.repo_update_retries.as_ref().unwrap_or_else(|| {
            DEFAULT_REPO_UPDATE_RETRY_CONFIG.get_or_init(RepoUpdateRetryConfig::default)
        })
    }

    pub fn num_updates_per_repo_info_file(&self) -> u16 {
        self.num_updates_per_repo_info_file
            .unwrap_or(DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE)
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
                (Some(c), None) => Some(*c),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            max_concurrent_requests: match (
                &self.max_concurrent_requests,
                other.max_concurrent_requests,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(_), Some(theirs)) => Some(theirs),
            },
            max_concurrent_decodes: match (
                &self.max_concurrent_decodes,
                other.max_concurrent_decodes,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(_), Some(theirs)) => Some(theirs),
            },
            caching: match (&self.caching, other.caching) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            storage: match (&self.storage, other.storage) {
                (None, None) => None,
                (None, Some(s)) => Some(s),
                (Some(s), None) => Some(s.clone()),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            // For virtual_chunk_containers, replace rather than extend.
            // This is consistent with other fields: if specified, it replaces.
            virtual_chunk_containers: match (
                &self.virtual_chunk_containers,
                other.virtual_chunk_containers,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(_), Some(theirs)) => Some(theirs),
            },
            manifest: match (&self.manifest, other.manifest) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            previous_file: match (&self.previous_file, other.previous_file) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(_), Some(theirs)) => Some(theirs),
            },
            repo_update_retries: match (
                &self.repo_update_retries,
                other.repo_update_retries,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(mine), Some(theirs)) => Some(mine.merge(theirs)),
            },
            num_updates_per_repo_info_file: match (
                &self.num_updates_per_repo_info_file,
                other.num_updates_per_repo_info_file,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(_), Some(theirs)) => Some(theirs),
            },
        }
    }
}

impl RepositoryConfig {
    /// Add a container, replacing any container with the same name or the
    /// same `url_prefix`.
    pub fn set_virtual_chunk_container(
        &mut self,
        cont: VirtualChunkContainer,
    ) -> Result<(), String> {
        let containers =
            self.virtual_chunk_containers.get_or_insert_with(Default::default);
        if let Some(ref new_name) = cont.name {
            // Named containers are identified by name. Remove any existing
            // container with the same name (which may have a different url_prefix).
            containers.retain(|_, v| v.name.as_deref() != Some(new_name.as_str()));
        }
        containers.insert(cont.url_prefix().to_string(), cont);
        Ok(())
    }

    pub fn get_virtual_chunk_container(
        &self,
        url_prefix: &str,
    ) -> Option<&VirtualChunkContainer> {
        self.virtual_chunk_containers.as_ref().and_then(|h| h.get(url_prefix))
    }

    pub fn virtual_chunk_containers(
        &self,
    ) -> impl Iterator<Item = &VirtualChunkContainer> {
        match self.virtual_chunk_containers.as_ref() {
            Some(h) => Either::Left(h.values()),
            None => Either::Right(std::iter::empty()),
        }
    }

    /// Remove the containers this library can decode. Containers whose store
    /// settings it can't decode stay: a library never removes those, it can
    /// only replace one with a container of the same name or `url_prefix`.
    /// Supplying `virtual_chunk_containers` when opening a repository
    /// likewise replaces only the decodable containers.
    pub fn clear_virtual_chunk_containers(&mut self) {
        self.virtual_chunk_containers = Some(Default::default());
    }

    /// Decode the configuration a repo info stores (`Repo.config`), and what
    /// of it this library can't decode. Never fails: a top-level key whose
    /// value this library can't decode reads as absent, and a virtual chunk
    /// container whose store settings it can't decode is kept as
    /// undecodable. Logs a warning for each.
    pub(crate) fn from_stored(bytes: &[u8]) -> (Self, UndecodableConfig) {
        let decoded = decode_stored(bytes);
        for problem in decoded.problems {
            tracing::warn!("{problem}");
        }
        (decoded.config, decoded.undecodable)
    }

    /// Encode the configuration as a repo info stores it, merged into
    /// `current`, the stored configuration of the repo info it replaces. Of
    /// `current`, it keeps the keys this library doesn't know, the values of
    /// known keys it can't decode (unless this configuration sets the key and
    /// `undecodable`, what reading this configuration couldn't decode, doesn't
    /// list it), and the virtual chunk containers whose store settings it can't
    /// decode (unless a container of this configuration replaces one: same
    /// key or name).
    pub(crate) fn to_stored(
        &self,
        undecodable: &UndecodableConfig,
        current: Option<&[u8]>,
    ) -> IcechunkResult<StoredRepositoryConfig> {
        let ours_bytes = flexbuffers::to_vec(self).map_err(Box::new).capture()?;
        let ours = Reader::get_root(ours_bytes.as_slice())
            .and_then(|root| root.get_map())
            .map_err(reader_error)?;
        let current_map = current
            .and_then(|bytes| Reader::get_root(bytes).and_then(|r| r.get_map()).ok());
        let current_decoded = current.map(decode_stored);
        let current_undecodable_keys =
            current_decoded.as_ref().map(|d| &d.undecodable.keys);
        let kept_containers: Vec<(&str, Reader<&[u8]>)> =
            match (&current_decoded, &current_map) {
                (Some(decoded), Some(current)) => {
                    let entries = current
                        .index(VIRTUAL_CHUNK_CONTAINERS_KEY)
                        .and_then(|value| value.get_map())
                        .ok();
                    decoded
                        .undecodable
                        .virtual_chunk_containers
                        .iter()
                        .filter(|(key, cont)| {
                            !self
                                .virtual_chunk_containers
                                .as_ref()
                                .is_some_and(|ours| replaces(ours, key, cont.name()))
                        })
                        .filter_map(|(key, _)| {
                            Some((
                                key.as_str(),
                                entries.as_ref()?.index(key.as_str()).ok()?,
                            ))
                        })
                        .collect()
                }
                _ => Vec::new(),
            };

        let mut merged = BTreeMap::new();
        for (key, value) in ours.iter_keys().zip(ours.iter_values()) {
            let is_null = value.flexbuffer_type() == FlexBufferType::Null;
            let keeps_current = current_undecodable_keys
                .is_some_and(|keys| keys.contains(key))
                && (is_null || undecodable.keys.contains(key));
            let stored = match current_map.as_ref().and_then(|c| c.index(key).ok()) {
                Some(current) if keeps_current => StoredValue::Value(current),
                _ if key == VIRTUAL_CHUNK_CONTAINERS_KEY
                    && !kept_containers.is_empty() =>
                {
                    let mut containers: BTreeMap<_, _> = kept_containers
                        .iter()
                        .map(|(key, value)| (*key, StoredValue::Value(value.clone())))
                        .collect();
                    if !is_null {
                        let decoded = value.as_map();
                        containers.extend(
                            decoded
                                .iter_keys()
                                .zip(decoded.iter_values().map(StoredValue::Value)),
                        );
                    }
                    StoredValue::Map(containers)
                }
                _ => StoredValue::Value(value),
            };
            merged.insert(key, stored);
        }
        if let Some(current) = &current_map {
            for (key, value) in current.iter_keys().zip(current.iter_values()) {
                merged.entry(key).or_insert(StoredValue::Value(value));
            }
        }
        let bytes = flexbuffers::to_vec(&merged).map_err(Box::new).capture()?;
        Ok(StoredRepositoryConfig(bytes))
    }
}

const VIRTUAL_CHUNK_CONTAINERS_KEY: &str = "virtual_chunk_containers";

/// A repository configuration encoded as a repo info stores it, see
/// [`RepositoryConfig::to_stored`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredRepositoryConfig(Vec<u8>);

impl Serialize for StoredRepositoryConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let root =
            Reader::get_root(self.0.as_slice()).map_err(serde::ser::Error::custom)?; // codespell:ignore ser
        (&root).serialize(serializer)
    }
}

enum StoredValue<'a> {
    Value(Reader<&'a [u8]>),
    Map(BTreeMap<&'a str, StoredValue<'a>>),
}

impl Serialize for StoredValue<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Value(value) => value.serialize(serializer),
            Self::Map(map) => map.serialize(serializer),
        }
    }
}

fn reader_error(err: ReaderError) -> IcechunkFormatError {
    IcechunkFormatError::capture(
        IcechunkFormatErrorKind::DeserializationErrorFlexBuffers(Box::new(err.into())),
    )
}

struct DecodedStoredConfig {
    config: RepositoryConfig,
    undecodable: UndecodableConfig,
    problems: Vec<String>,
}

fn decode_stored(bytes: &[u8]) -> DecodedStoredConfig {
    let map = match Reader::get_root(bytes).and_then(|root| root.get_map()) {
        Ok(map) => map,
        Err(err) => {
            return DecodedStoredConfig {
                config: RepositoryConfig::default(),
                undecodable: UndecodableConfig::default(),
                problems: vec![format!(
                    "ignoring the repository configuration, this Icechunk library can't decode it: {err}"
                )],
            };
        }
    };
    let mut decoder = StoredConfigDecoder {
        map,
        undecodable_keys: BTreeSet::new(),
        problems: Vec::new(),
    };
    let (virtual_chunk_containers, undecodable_virtual_chunk_containers) =
        decoder.virtual_chunk_containers();
    let config = RepositoryConfig {
        inline_chunk_threshold_bytes: decoder.key("inline_chunk_threshold_bytes"),
        get_partial_values_concurrency: decoder.key("get_partial_values_concurrency"),
        compression: decoder.key("compression"),
        max_concurrent_requests: decoder.key("max_concurrent_requests"),
        max_concurrent_decodes: decoder.key("max_concurrent_decodes"),
        caching: decoder.key("caching"),
        storage: decoder.key("storage"),
        virtual_chunk_containers,
        manifest: decoder.key("manifest"),
        previous_file: decoder.key("previous_file"),
        repo_update_retries: decoder.key("repo_update_retries"),
        num_updates_per_repo_info_file: decoder.key("num_updates_per_repo_info_file"),
    };
    DecodedStoredConfig {
        config,
        undecodable: UndecodableConfig {
            keys: decoder.undecodable_keys,
            virtual_chunk_containers: undecodable_virtual_chunk_containers,
        },
        problems: decoder.problems,
    }
}

/// Decodes the top-level keys of a stored configuration one at a time.
struct StoredConfigDecoder<'a> {
    map: MapReader<&'a [u8]>,
    undecodable_keys: BTreeSet<String>,
    problems: Vec<String>,
}

impl<'a> StoredConfigDecoder<'a> {
    fn fail(&mut self, key: &str, reason: impl Display) {
        self.undecodable_keys.insert(key.to_string());
        self.problems.push(format!(
            "ignoring the repository configuration key `{key}`, this Icechunk library can't decode its value: {reason}"
        ));
    }

    fn value(&mut self, key: &str) -> Option<Reader<&'a [u8]>> {
        match self.map.index(key) {
            Ok(value) => Some(value),
            Err(ReaderError::KeyNotFound) => None,
            Err(err) => {
                self.fail(key, err);
                None
            }
        }
    }

    fn key<T: DeserializeOwned>(&mut self, key: &str) -> Option<T> {
        let value = self.value(key)?;
        Option::<T>::deserialize(value).unwrap_or_else(|err| {
            self.fail(key, err);
            None
        })
    }

    /// The containers that decode, and those whose store settings don't.
    /// The first is `Some` whenever the stored value is a map.
    fn virtual_chunk_containers(
        &mut self,
    ) -> (
        Option<HashMap<String, VirtualChunkContainer>>,
        HashMap<String, UndecodableVirtualChunkContainer>,
    ) {
        let key = VIRTUAL_CHUNK_CONTAINERS_KEY;
        let mut undecodable = HashMap::new();
        let Some(value) = self.value(key) else {
            return (None, undecodable);
        };
        if value.flexbuffer_type() == FlexBufferType::Null {
            return (None, undecodable);
        }
        let entries = match value.get_map() {
            Ok(entries) => entries,
            Err(err) => {
                self.fail(key, err);
                return (None, undecodable);
            }
        };
        let mut decoded = HashMap::new();
        for (entry_key, entry) in entries.iter_keys().zip(entries.iter_values()) {
            let err = match VirtualChunkContainer::deserialize(entry.clone()) {
                Ok(cont) => {
                    decoded.insert(entry_key.to_string(), cont);
                    continue;
                }
                Err(err) => err,
            };
            let string_field =
                |name: &str| {
                    entry.as_map().index(name).ok().and_then(|value| {
                        Option::<String>::deserialize(value).ok().flatten()
                    })
                };
            self.problems.push(format!(
                "the virtual chunk container `{entry_key}` has settings this Icechunk library can't decode, its chunks can't be read: {err}"
            ));
            undecodable.insert(
                entry_key.to_string(),
                UndecodableVirtualChunkContainer::new(
                    string_field("name"),
                    string_field("url_prefix"),
                ),
            );
        }
        (Some(decoded), undecodable)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "credential_type")]
#[serde(rename_all = "snake_case")]
pub enum Credentials {
    S3(S3Credentials),
    #[cfg(feature = "object-store-gcs")]
    Gcs(GcsCredentials),
    #[cfg(feature = "object-store-azure")]
    Azure(AzureCredentials),
    /// Explicit sentinel authorizing access to a local-filesystem virtual chunk
    /// container, which requires no credentials. This is the explicit replacement
    /// for passing `None` for a `file://` container.
    LocalFileSystemAccess,
    /// Explicit sentinel authorizing access to an HTTP(S) virtual chunk container,
    /// which requires no credentials. This is the explicit replacement for passing
    /// `None` for an `http://`/`https://` container.
    HttpAccess,
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "object-store-http")]
    use crate::config::HttpConfig;
    use std::collections::BTreeSet;

    use crate::{
        ObjectStoreConfig, RepositoryConfig,
        config::{CachingConfig, S3Options, UndecodableConfig},
        strategies::{repository_config, s3_static_credentials},
        virtual_chunks::VirtualChunkContainer,
    };
    use icechunk_format::roundtrip_serialization_tests;

    use proptest::prelude::*;

    roundtrip_serialization_tests!(
        test_config_roundtrip - repository_config,
        test_s3_static_credentials_roundtrip - s3_static_credentials
    );

    #[cfg(feature = "object-store-gcs")]
    roundtrip_serialization_tests!(
        test_gcs_static_credentials_roundtrip - gcs_static_credentials
    );

    #[cfg(feature = "object-store-gcs")]
    use crate::strategies::gcs_static_credentials;

    #[cfg(feature = "object-store-azure")]
    roundtrip_serialization_tests!(
        test_azure_static_credentials_roundtrip - azure_static_credentials
    );

    #[cfg(feature = "object-store-azure")]
    use crate::strategies::azure_static_credentials;

    #[icechunk_macros::test]
    fn test_max_concurrent_manifest_fetches_during_commit_default_and_merge() {
        use crate::config::ManifestConfig;

        // Unset resolves to the serial default.
        let unset = ManifestConfig::default();
        assert_eq!(unset.max_concurrent_manifest_fetches_during_commit, None);
        assert_eq!(unset.max_concurrent_manifest_fetches_during_commit(), 1);

        // An explicit value is returned by the accessor.
        let set = ManifestConfig {
            max_concurrent_manifest_fetches_during_commit: Some(16),
            ..Default::default()
        };
        assert_eq!(set.max_concurrent_manifest_fetches_during_commit(), 16);

        // merge: the other config's value wins when set, otherwise ours is kept.
        assert_eq!(
            unset.merge(set.clone()).max_concurrent_manifest_fetches_during_commit,
            Some(16)
        );
        assert_eq!(
            set.merge(ManifestConfig::default())
                .max_concurrent_manifest_fetches_during_commit,
            Some(16)
        );
        // When both are set, the other config's value wins.
        let other = ManifestConfig {
            max_concurrent_manifest_fetches_during_commit: Some(4),
            ..Default::default()
        };
        assert_eq!(
            set.merge(other).max_concurrent_manifest_fetches_during_commit,
            Some(4)
        );
    }

    #[icechunk_macros::test]
    fn test_repo_update_retries_unset_fields_use_repo_update_defaults() {
        use crate::{config::RepoUpdateRetryConfig, storage::RetriesSettings};
        use std::num::NonZeroU16;

        let repo_defaults = RetriesSettings {
            max_tries: NonZeroU16::new(100),
            initial_backoff_ms: Some(50),
            max_backoff_ms: Some(30_000),
        };
        assert_eq!(RepoUpdateRetryConfig::default().retries(), repo_defaults);

        // setting one field must not drop the others to the storage defaults
        let partial = RepoUpdateRetryConfig {
            default: Some(RetriesSettings {
                initial_backoff_ms: Some(10),
                ..Default::default()
            }),
        };
        assert_eq!(
            partial.retries(),
            RetriesSettings { initial_backoff_ms: Some(10), ..repo_defaults }
        );

        let full = RetriesSettings {
            max_tries: NonZeroU16::new(3),
            initial_backoff_ms: Some(1),
            max_backoff_ms: Some(2),
        };
        assert_eq!(RepoUpdateRetryConfig { default: Some(full) }.retries(), full);
    }

    #[icechunk_macros::test]
    fn test_merge_replaces_virtual_chunk_containers() {
        // Create a config with a VCC
        let mut config1 = RepositoryConfig::default();
        config1
            .set_virtual_chunk_container(
                VirtualChunkContainer::new(
                    "s3://bucket1/".to_string(),
                    ObjectStoreConfig::S3(S3Options::default().with_region("us-east-1")),
                )
                .unwrap(),
            )
            .unwrap();

        // Create a config with no VCCs (cleared)
        let mut config2 = RepositoryConfig::default();
        config2.clear_virtual_chunk_containers();

        // Merge: config2 (empty VCCs) should replace config1's VCCs
        let merged = config1.merge(config2);

        // VCCs should be empty after merge
        assert_eq!(
            merged.virtual_chunk_containers,
            Some(std::collections::HashMap::new()),
            "Merging with cleared VCCs should result in empty VCCs"
        );
    }

    #[icechunk_macros::test]
    fn test_merge_replaces_virtual_chunk_containers_with_new_ones() {
        // Create a config with VCC1
        let mut config1 = RepositoryConfig::default();
        config1
            .set_virtual_chunk_container(
                VirtualChunkContainer::new(
                    "s3://bucket1/".to_string(),
                    ObjectStoreConfig::S3(S3Options::default().with_region("us-east-1")),
                )
                .unwrap(),
            )
            .unwrap();

        // Create a config with VCC2
        let mut config2 = RepositoryConfig::default();
        config2
            .set_virtual_chunk_container(
                VirtualChunkContainer::new(
                    "s3://bucket2/".to_string(),
                    ObjectStoreConfig::S3(S3Options::default().with_region("us-west-2")),
                )
                .unwrap(),
            )
            .unwrap();

        // Merge: config2's VCCs should replace config1's VCCs
        let merged = config1.merge(config2);

        let vccs = merged.virtual_chunk_containers.unwrap();
        assert_eq!(vccs.len(), 1, "Should have exactly one VCC after merge");
        assert!(
            vccs.contains_key("s3://bucket2/"),
            "Should contain bucket2, not bucket1"
        );
        assert!(!vccs.contains_key("s3://bucket1/"), "Should not contain bucket1");
    }

    #[icechunk_macros::test]
    fn test_yaml_config_deserialization() {
        let mut yaml_parts = vec![
            r#"
inline_chunk_threshold_bytes: null
get_partial_values_concurrency: null
compression: null
max_concurrent_requests: null
max_concurrent_decodes: null
caching: null
storage: null
virtual_chunk_containers:
  s3://my-s3-bucket/:
    name: null
    url_prefix: s3://my-s3-bucket/
    store: !s3
      region: us-east-1
      endpoint_url: null
      anonymous: false
      allow_http: false
      force_path_style: false
      network_stream_timeout_seconds: 60
      requester_pays: false"#
                .to_string(),
        ];

        #[cfg(feature = "object-store-http")]
        yaml_parts.push(
            r#"
  https://example.com/data/:
    name: null
    url_prefix: https://example.com/data/
    store: !http
      allow_http: "true""#
                .to_string(),
        );

        #[cfg(feature = "object-store-gcs")]
        yaml_parts.push(
            r#"
  gcs://my-gcs-bucket/:
    name: null
    url_prefix: gcs://my-gcs-bucket/
    store: !gcs {}"#
                .to_string(),
        );

        yaml_parts.push("\nmanifest: null\n".to_string());
        let yaml = yaml_parts.join("");

        let config: RepositoryConfig = serde_yaml_ng::from_str(&yaml).unwrap();

        let vccs = config.virtual_chunk_containers.as_ref().unwrap();

        let mut expected_len = 1; // S3 is always present

        // S3 container
        let s3 = &vccs["s3://my-s3-bucket/"];
        assert_eq!(s3.url_prefix(), "s3://my-s3-bucket/");
        match &s3.store {
            ObjectStoreConfig::S3(opts) => {
                assert_eq!(opts.region, Some("us-east-1".to_string()));
                assert!(!opts.anonymous);
                assert!(!opts.allow_http);
                assert!(!opts.force_path_style);
                assert_eq!(opts.network_stream_timeout_seconds, Some(60));
                assert!(!opts.requester_pays);
            }
            other => panic!("Expected S3, got {other:?}"),
        }

        // HTTP container
        #[cfg(feature = "object-store-http")]
        {
            expected_len += 1;
            let http = &vccs["https://example.com/data/"];
            assert_eq!(http.url_prefix(), "https://example.com/data/");
            match &http.store {
                ObjectStoreConfig::Http(cfg) => {
                    assert_eq!(
                        cfg.opts.get("allow_http").map(String::as_str),
                        Some("true")
                    );
                    assert!(cfg.headers.is_empty());
                }
                other => panic!("Expected Http, got {other:?}"),
            }
        }

        // GCS container
        #[cfg(feature = "object-store-gcs")]
        {
            expected_len += 1;
            let gcs = &vccs["gcs://my-gcs-bucket/"];
            assert_eq!(gcs.url_prefix(), "gcs://my-gcs-bucket/");
            assert!(matches!(gcs.store, ObjectStoreConfig::Gcs(_)));
        }

        assert_eq!(vccs.len(), expected_len);

        // All other top-level fields should be None
        assert!(config.inline_chunk_threshold_bytes.is_none());
        assert!(config.get_partial_values_concurrency.is_none());
        assert!(config.compression.is_none());
        assert!(config.max_concurrent_requests.is_none());
        assert!(config.max_concurrent_decodes.is_none());
        assert!(config.caching.is_none());
        assert!(config.storage.is_none());
        assert!(config.manifest.is_none());
    }

    #[icechunk_macros::test]
    fn test_set_vcc_name_uniqueness() {
        let mut config = RepositoryConfig::default();

        // First named container succeeds
        config
            .set_virtual_chunk_container(
                VirtualChunkContainer::new_named(
                    "my-data".to_string(),
                    "s3://bucket1/prefix/".to_string(),
                    ObjectStoreConfig::S3(S3Options::default().with_region("us-east-1")),
                )
                .unwrap(),
            )
            .unwrap();

        // Same name, different prefix should replace the old entry
        config
            .set_virtual_chunk_container(
                VirtualChunkContainer::new_named(
                    "my-data".to_string(),
                    "s3://bucket2/other/".to_string(),
                    ObjectStoreConfig::S3(S3Options::default().with_region("us-east-1")),
                )
                .unwrap(),
            )
            .unwrap();
        assert!(config.get_virtual_chunk_container("s3://bucket1/prefix/").is_none());
        assert!(config.get_virtual_chunk_container("s3://bucket2/other/").is_some());

        // Same name, same prefix should succeed (update)
        config
            .set_virtual_chunk_container(
                VirtualChunkContainer::new_named(
                    "my-data".to_string(),
                    "s3://bucket1/prefix/".to_string(),
                    ObjectStoreConfig::S3(S3Options::default().with_region("us-west-2")),
                )
                .unwrap(),
            )
            .unwrap();

        // Unnamed containers always succeed
        config
            .set_virtual_chunk_container(
                VirtualChunkContainer::new(
                    "s3://bucket3/".to_string(),
                    ObjectStoreConfig::S3(S3Options::default().with_region("us-east-1")),
                )
                .unwrap(),
            )
            .unwrap();
    }

    /// Verify that repositories written by icechunk 2.0.5 (which used
    /// `ObjectStoreConfig::Http(HashMap<String,String>)`) can still be read
    /// after this PR changed the type to `Http(HttpConfig)`.
    /// This test pins that behavior so a serde or dependency
    /// change cannot silently break existing repositories.
    #[icechunk_macros::test]
    #[cfg(feature = "object-store-http")]
    fn test_http_vcc_backward_compat_yaml() {
        // Exact YAML that icechunk 2.0.5 would write to disk for a
        // RepositoryConfig with an HTTP VCC that has allow_http=true.
        let old_yaml = r#"
virtual_chunk_containers:
  https://example.com/:
    name: null
    url_prefix: https://example.com/
    store: !http
      allow_http: "true"
manifest: null
"#;
        let config: RepositoryConfig = serde_yaml_ng::from_str(old_yaml)
            .expect("old 2.0.5 YAML must deserialize cleanly with new HttpConfig type");

        let vccs = config.virtual_chunk_containers.as_ref().unwrap();
        match &vccs["https://example.com/"].store {
            ObjectStoreConfig::Http(HttpConfig { opts, headers }) => {
                assert_eq!(
                    opts.get("allow_http").map(String::as_str),
                    Some("true"),
                    "opts must be preserved when reading old on-disk format"
                );
                assert!(
                    headers.is_empty(),
                    "headers must be empty when reading old on-disk format"
                );
            }
            other => panic!("Expected Http, got {other:?}"),
        }
    }

    /// Round-trip: a `RepositoryConfig` with an HTTP VCC that has headers
    /// serializes to YAML and deserializes back with headers intact.
    #[icechunk_macros::test]
    #[cfg(feature = "object-store-http")]
    fn test_http_vcc_yaml_roundtrip_with_headers() {
        use std::collections::HashMap;

        use crate::config::{HttpConfig, ObjectStoreConfig, VirtualChunkContainer};

        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer secret".to_string());
        headers.insert("X-Custom-Header".to_string(), "value".to_string());

        let mut opts = HashMap::new();
        opts.insert("allow_http".to_string(), "true".to_string());

        let container = VirtualChunkContainer::new(
            "https://example.com/".to_string(),
            ObjectStoreConfig::Http(HttpConfig { opts, headers }),
        )
        .expect("valid VirtualChunkContainer");

        let mut vccs = HashMap::new();
        vccs.insert("https://example.com/".to_string(), container);

        let original = RepositoryConfig {
            virtual_chunk_containers: Some(vccs),
            ..Default::default()
        };

        let yaml = serde_yaml_ng::to_string(&original)
            .expect("RepositoryConfig with HTTP headers must serialize to YAML");

        let roundtripped: RepositoryConfig = serde_yaml_ng::from_str(&yaml)
            .expect("serialized YAML must deserialize back cleanly");

        let vccs = roundtripped.virtual_chunk_containers.as_ref().unwrap();
        match &vccs["https://example.com/"].store {
            ObjectStoreConfig::Http(HttpConfig { opts, headers }) => {
                assert_eq!(
                    opts.get("allow_http").map(String::as_str),
                    Some("true"),
                    "opts must survive YAML round-trip"
                );
                assert_eq!(
                    headers.get("Authorization").map(String::as_str),
                    Some("Bearer secret"),
                    "Authorization header must survive YAML round-trip"
                );
                assert_eq!(
                    headers.get("X-Custom-Header").map(String::as_str),
                    Some("value"),
                    "X-Custom-Header must survive YAML round-trip"
                );
                assert_eq!(headers.len(), 2, "no extra headers should appear");
            }
            other => panic!("Expected Http, got {other:?}"),
        }
    }

    proptest! {
        /// What this library stores, it and older libraries (which decode
        /// the whole map at once) read back unchanged.
        #[icechunk_macros::test]
        fn stored_config_round_trips(config in repository_config()) {
            let stored = flexbuffers::to_vec(
                config.to_stored(&UndecodableConfig::default(), None).unwrap(),
            )
            .unwrap();
            prop_assert_eq!(
                RepositoryConfig::from_stored(&stored),
                (config.clone(), UndecodableConfig::default())
            );
            let strict: RepositoryConfig = flexbuffers::from_slice(&stored).unwrap();
            prop_assert_eq!(&strict, &config);
        }
    }

    fn stored(value: &serde_json::Value) -> Vec<u8> {
        flexbuffers::to_vec(value).unwrap()
    }

    fn stored_as_json(
        config: &RepositoryConfig,
        undecodable: &UndecodableConfig,
        current: Option<&[u8]>,
    ) -> serde_json::Value {
        let bytes =
            flexbuffers::to_vec(config.to_stored(undecodable, current).unwrap()).unwrap();
        flexbuffers::from_slice(&bytes).unwrap()
    }

    fn later_config() -> serde_json::Value {
        let container = VirtualChunkContainer::new_named(
            "known".to_string(),
            "s3://bucket/".to_string(),
            ObjectStoreConfig::S3(S3Options::default()),
        )
        .unwrap();
        serde_json::json!({
            "inline_chunk_threshold_bytes": 7,
            "caching": "not a map",
            "key_from_the_future": [1, 2, 3],
            "virtual_chunk_containers": {
                "s3://bucket/": serde_json::to_value(&container).unwrap(),
                "future://bucket/": {
                    "name": "future",
                    "url_prefix": "future://bucket/",
                    "store": {"storage_from_the_future": {"a": 1}},
                },
            },
        })
    }

    #[icechunk_macros::test]
    fn stored_config_decodes_each_key_on_its_own() {
        let (config, undecodable) =
            RepositoryConfig::from_stored(&stored(&later_config()));
        assert_eq!(config.inline_chunk_threshold_bytes, Some(7));
        assert_eq!(config.caching, None);
        let decoded: Vec<_> =
            config.virtual_chunk_containers().map(|c| c.url_prefix()).collect();
        assert_eq!(decoded, vec!["s3://bucket/"]);
        let undecodable_containers: Vec<_> = undecodable
            .virtual_chunk_containers()
            .map(|c| (c.name(), c.url_prefix()))
            .collect();
        assert_eq!(
            undecodable_containers,
            vec![(Some("future"), Some("future://bucket/"))]
        );
        assert_eq!(undecodable.keys, BTreeSet::from(["caching".to_string()]));
    }

    #[icechunk_macros::test]
    fn stored_config_that_is_not_a_map_reads_as_the_default() {
        for bytes in [stored(&serde_json::json!([1, 2])), b"garbage".to_vec(), Vec::new()]
        {
            assert_eq!(
                RepositoryConfig::from_stored(&bytes),
                (RepositoryConfig::default(), UndecodableConfig::default())
            );
        }
    }

    #[icechunk_macros::test]
    fn to_stored_keeps_what_it_cannot_decode_unless_set_by_intent() {
        let later = later_config();
        let current = stored(&later);
        let (read, undecodable) = RepositoryConfig::from_stored(&current);
        // a value injected without merging, as opening injects defaults
        let config = RepositoryConfig {
            caching: Some(CachingConfig::default()),
            inline_chunk_threshold_bytes: Some(8),
            ..read.clone()
        };
        let saved = stored_as_json(&config, &undecodable, Some(&current));
        assert_eq!(saved["inline_chunk_threshold_bytes"], 8);
        for key in ["caching", "key_from_the_future", "virtual_chunk_containers"] {
            assert_eq!(saved[key], later[key], "{key}");
        }

        // the same value echoed back through a merge doesn't count as setting it
        let echoed = config.merge(config.clone());
        let echoed_undecodable = undecodable.clone().after_merge(&config, &echoed);
        assert_eq!(
            stored_as_json(&echoed, &echoed_undecodable, Some(&current))["caching"],
            later["caching"]
        );

        let caching = CachingConfig { num_chunk_refs: Some(3), ..Default::default() };
        let set = config
            .merge(RepositoryConfig { caching: Some(caching), ..Default::default() });
        let set_undecodable = undecodable.after_merge(&config, &set);
        let saved = stored_as_json(&set, &set_undecodable, Some(&current));
        assert_eq!(saved["caching"], serde_json::to_value(caching).unwrap());

        // a known key that a concurrent writer made undecodable, and that this
        // configuration doesn't set, is kept
        let saved = stored_as_json(
            &RepositoryConfig::default(),
            &UndecodableConfig::default(),
            Some(&current),
        );
        assert_eq!(saved["caching"], later["caching"]);
    }

    #[icechunk_macros::test]
    fn to_stored_takes_undecodable_containers_from_the_current_configuration() {
        let later = later_config();
        let current = stored(&later);
        // a configuration that never saw them, as after a binding round trip
        let mut config = RepositoryConfig::default();
        config.clear_virtual_chunk_containers();
        let saved =
            stored_as_json(&config, &UndecodableConfig::default(), Some(&current));
        let containers = saved["virtual_chunk_containers"].as_object().unwrap();
        assert_eq!(containers.keys().collect::<Vec<_>>(), vec!["future://bucket/"]);
        assert_eq!(
            containers["future://bucket/"],
            later["virtual_chunk_containers"]["future://bucket/"]
        );
    }

    #[icechunk_macros::test]
    fn setting_a_container_replaces_an_undecodable_one_of_the_same_name() {
        let later = later_config();
        let current = stored(&later);
        let (read, undecodable) = RepositoryConfig::from_stored(&current);
        let replacement = VirtualChunkContainer::new_named(
            "future".to_string(),
            "s3://other/".to_string(),
            ObjectStoreConfig::S3(S3Options::default()),
        )
        .unwrap();
        let mut config = read.clone();
        config.set_virtual_chunk_container(replacement.clone()).unwrap();
        let undecodable = undecodable.after_merge(&read, &config);
        assert_eq!(undecodable.virtual_chunk_containers().count(), 0);

        let saved = stored_as_json(&config, &undecodable, Some(&current));
        let containers = saved["virtual_chunk_containers"].as_object().unwrap();
        let mut keys: Vec<_> = containers.keys().collect();
        keys.sort();
        assert_eq!(keys, vec!["s3://bucket/", "s3://other/"]);
        assert_eq!(
            containers["s3://other/"],
            serde_json::to_value(&replacement).unwrap()
        );
    }

    #[icechunk_macros::test]
    fn merge_keeps_undecodable_containers_unless_replaced() {
        let (config, undecodable) =
            RepositoryConfig::from_stored(&stored(&later_config()));
        let after = |merged: &RepositoryConfig| {
            undecodable
                .clone()
                .after_merge(&config, merged)
                .virtual_chunk_containers()
                .count()
        };
        let kept = config.merge(RepositoryConfig {
            virtual_chunk_containers: Some(Default::default()),
            ..Default::default()
        });
        assert_eq!(after(&kept), 1);
        let mut cleared = config.clone();
        cleared.clear_virtual_chunk_containers();
        assert_eq!(after(&cleared), 1);

        let mut replacing = RepositoryConfig::default();
        replacing
            .set_virtual_chunk_container(
                VirtualChunkContainer::new_named(
                    "future".to_string(),
                    "s3://other/".to_string(),
                    ObjectStoreConfig::S3(S3Options::default()),
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(after(&config.merge(replacing)), 0);
    }

    /// Repositories, which hold it, are serialized with `rmp_serde`, e.g. to
    /// send them to other processes.
    #[icechunk_macros::test]
    fn undecodable_config_survives_rmp_serialization() {
        let (_, undecodable) = RepositoryConfig::from_stored(&stored(&later_config()));
        let bytes = rmp_serde::to_vec(&undecodable).unwrap();
        let roundtrip: UndecodableConfig = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(roundtrip, undecodable);
        assert_eq!(roundtrip.virtual_chunk_containers().count(), 1);
    }
}
