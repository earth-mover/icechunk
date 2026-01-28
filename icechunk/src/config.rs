use core::fmt;
use std::{
    collections::HashMap,
    ops::Bound,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use itertools::Either;
pub use object_store::gcp::GcpCredential;
use regex::bytes::Regex;
use serde::{Deserialize, Serialize};

use crate::{
    format::Path,
    registry::{
        deserialize_gcs_credentials_fetcher, deserialize_s3_credentials_fetcher,
        serialize_with_tag,
    },
    storage,
    virtual_chunks::VirtualChunkContainer,
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
    pub network_stream_timeout_seconds: Option<u32>,
    #[serde(default)]
    pub requester_pays: bool,
}

fn default_force_path_style() -> bool {
    false
}

impl fmt::Display for S3Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "S3Options(region={}, endpoint_url={}, anonymous={}, allow_http={}, force_path_style={}, network_stream_timeout_seconds={}, requester_pays={})",
            self.region.as_deref().unwrap_or("None"),
            self.endpoint_url.as_deref().unwrap_or("None"),
            self.anonymous,
            self.allow_http,
            self.force_path_style,
            self.network_stream_timeout_seconds
                .map(|n| n.to_string())
                .unwrap_or("None".to_string()),
            self.requester_pays,
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreConfig {
    InMemory,
    LocalFileSystem(PathBuf),
    Http(HashMap<String, String>),
    S3Compatible(S3Options),
    S3(S3Options),
    Gcs(HashMap<String, String>),
    /// For compatibility reason we cannot change this structure,
    /// but a key in this map should be "account"
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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Default)]
pub struct ManifestConfig {
    #[serde(default)]
    pub preload: Option<ManifestPreloadConfig>,
    #[serde(default)]
    pub splitting: Option<ManifestSplittingConfig>,
}

static DEFAULT_MANIFEST_PRELOAD_CONFIG: OnceLock<ManifestPreloadConfig> = OnceLock::new();
static DEFAULT_MANIFEST_SPLITTING_CONFIG: OnceLock<ManifestSplittingConfig> =
    OnceLock::new();

impl ManifestConfig {
    pub fn merge(&self, other: Self) -> Self {
        Self {
            preload: other.preload.or(self.preload.clone()),
            splitting: other.splitting.or(self.splitting.clone()),
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
    // for testing only, create a config with no preloading, no splitting, and no max_arrays to scan
    pub fn empty() -> Self {
        ManifestConfig {
            preload: Some(ManifestPreloadConfig {
                max_total_refs: None,
                preload_if: None,
                max_arrays_to_scan: None,
            }),
            splitting: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RepositoryConfig {
    /// Chunks smaller than this will be stored inline in the manifest
    #[serde(default)]
    pub inline_chunk_threshold_bytes: Option<u16>,

    /// Concurrency used by the get_partial_values operation to fetch different keys in parallel
    #[serde(default)]
    pub get_partial_values_concurrency: Option<u16>,

    #[serde(default)]
    pub compression: Option<CompressionConfig>,

    #[serde(default)]
    pub max_concurrent_requests: Option<u16>,

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
}

static DEFAULT_COMPRESSION: OnceLock<CompressionConfig> = OnceLock::new();
static DEFAULT_CACHING: OnceLock<CachingConfig> = OnceLock::new();
static DEFAULT_MANIFEST_CONFIG: OnceLock<ManifestConfig> = OnceLock::new();
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: u16 = 256;

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
            max_concurrent_requests: match (
                &self.max_concurrent_requests,
                other.max_concurrent_requests,
            ) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(*c),
                (Some(_), Some(theirs)) => Some(theirs),
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
            previous_file: match (&self.previous_file, other.previous_file) {
                (None, None) => None,
                (None, Some(c)) => Some(c),
                (Some(c), None) => Some(c.clone()),
                (Some(_), Some(theirs)) => Some(theirs),
            },
        }
    }
}

impl RepositoryConfig {
    pub fn set_virtual_chunk_container(&mut self, cont: VirtualChunkContainer) {
        let mut before = self.virtual_chunk_containers.clone().unwrap_or_default();
        before.insert(cont.url_prefix().to_string(), cont);
        self.virtual_chunk_containers = Some(before);
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
pub trait S3CredentialsFetcher: fmt::Debug + Sync + Send {
    /// Returns the type tag used for serialization/deserialization.
    fn type_tag(&self) -> &'static str;

    /// Serialize self for use with the registry pattern.
    fn as_serialize(&self) -> &dyn erased_serde::Serialize;

    async fn get(&self) -> Result<S3StaticCredentials, String>;
}

/// Serde helper module for Arc<dyn S3CredentialsFetcher>
pub mod s3_credentials_fetcher_serde {
    use super::*;

    pub fn serialize<S>(
        fetcher: &Arc<dyn S3CredentialsFetcher>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_with_tag(
            serializer,
            "s3_credentials_fetcher_type",
            fetcher.type_tag(),
            fetcher.as_serialize(),
        )
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Arc<dyn S3CredentialsFetcher>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = rmpv::Value::deserialize(deserializer)?;
        deserialize_s3_credentials_fetcher(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(tag = "s3_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum S3Credentials {
    #[default]
    FromEnv,
    Anonymous,
    Static(S3StaticCredentials),
    Refreshable(#[serde(with = "s3_credentials_fetcher_serde")] Arc<dyn S3CredentialsFetcher>),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
// We need to adjacently tag because we more than one variant with matching inner types https://github.com/serde-rs/serde/issues/1307
#[serde(tag = "gcs_static_credential_type", content = "__field0")]
#[serde(rename_all = "snake_case")]
pub enum GcsStaticCredentials {
    ServiceAccount(PathBuf),
    ServiceAccountKey(String),
    ApplicationCredentials(PathBuf),
    BearerToken(GcsBearerCredential),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
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
pub trait GcsCredentialsFetcher: fmt::Debug + Sync + Send {
    /// Returns the type tag used for serialization/deserialization.
    fn type_tag(&self) -> &'static str;

    /// Serialize self for use with the registry pattern.
    fn as_serialize(&self) -> &dyn erased_serde::Serialize;

    async fn get(&self) -> Result<GcsBearerCredential, String>;
}

/// Serde helper module for Arc<dyn GcsCredentialsFetcher>
pub mod gcs_credentials_fetcher_serde {
    use super::*;

    pub fn serialize<S>(
        fetcher: &Arc<dyn GcsCredentialsFetcher>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_with_tag(
            serializer,
            "gcs_credentials_fetcher_type",
            fetcher.type_tag(),
            fetcher.as_serialize(),
        )
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Arc<dyn GcsCredentialsFetcher>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = rmpv::Value::deserialize(deserializer)?;
        deserialize_gcs_credentials_fetcher(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(tag = "gcs_credential_type")]
#[serde(rename_all = "snake_case")]
pub enum GcsCredentials {
    #[default]
    FromEnv,
    Anonymous,
    Static(GcsStaticCredentials),
    Refreshable(#[serde(with = "gcs_credentials_fetcher_serde")] Arc<dyn GcsCredentialsFetcher>),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
// We need to adjacently tag because we more than one variant with matching inner types https://github.com/serde-rs/serde/issues/1307
#[serde(tag = "az_static_credential_type", content = "__field0")]
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

// This macro is used for creating property tests
// which check that serializing and deserializing
// an instance of a type T is equivalent to the
// identity function
// Given pairs of test names and arbitraries to be used
// for the tests, e.g., (n1, a1), (n2, a2),... (nx, ax)
// the tests can be created by doing
// roundtrip_serialization_tests!(n1 - a1, n2 - a2, .... nx - ax)
#[macro_export]
macro_rules! roundtrip_serialization_tests {
        ($($test_name: ident - $generator: ident), +) => {
                            $(
                proptest!{
        #[icechunk_macros::test]
                    fn $test_name(elem in $generator()) {
           let bytes = rmp_serde::to_vec(&elem).unwrap();
            let roundtrip = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(elem, roundtrip);
        }})*
        }
    }

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use crate::strategies::{
        azure_credentials, gcs_static_credentials, repository_config,
        s3_static_credentials,
    };

    use proptest::prelude::*;

    roundtrip_serialization_tests!(
        test_config_roundtrip - repository_config,
        test_s3_static_credentials_roundtrip - s3_static_credentials,
        test_gcs_static_credentials_roundtrip - gcs_static_credentials,
        test_azure_credentials_roundtrip - azure_credentials
    );
}
