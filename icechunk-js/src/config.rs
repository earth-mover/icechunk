use std::collections::HashMap;
use std::num::{NonZeroU16, NonZeroU64};

use napi_derive::napi;

use icechunk::config::{
    CachingConfig, CompressionAlgorithm, CompressionConfig, ManifestConfig,
    RepositoryConfig,
};
use icechunk::storage::{ConcurrencySettings, RetriesSettings, Settings};

#[cfg(not(target_family = "wasm"))]
use icechunk::virtual_chunks::VirtualChunkContainer;

#[cfg(not(target_family = "wasm"))]
use crate::storage::JsObjectStoreConfig;

/// Compression algorithm
#[napi(js_name = "CompressionAlgorithm")]
#[derive(Clone, Debug)]
pub enum JsCompressionAlgorithm {
    Zstd,
}

impl From<JsCompressionAlgorithm> for CompressionAlgorithm {
    fn from(value: JsCompressionAlgorithm) -> Self {
        match value {
            JsCompressionAlgorithm::Zstd => CompressionAlgorithm::Zstd,
        }
    }
}

/// Compression configuration
#[napi(object, js_name = "CompressionConfig")]
#[derive(Clone, Debug)]
pub struct JsCompressionConfig {
    pub algorithm: Option<JsCompressionAlgorithm>,
    pub level: Option<u32>,
}

impl From<JsCompressionConfig> for CompressionConfig {
    fn from(value: JsCompressionConfig) -> Self {
        CompressionConfig {
            algorithm: value.algorithm.map(|a| a.into()),
            level: value.level.map(|l| l as u8),
        }
    }
}

/// Caching configuration
#[napi(object, js_name = "CachingConfig")]
#[derive(Clone, Debug)]
pub struct JsCachingConfig {
    pub num_snapshot_nodes: Option<i64>,
    pub num_chunk_refs: Option<i64>,
    pub num_transaction_changes: Option<i64>,
    pub num_bytes_attributes: Option<i64>,
    pub num_bytes_chunks: Option<i64>,
}

impl From<JsCachingConfig> for CachingConfig {
    fn from(value: JsCachingConfig) -> Self {
        CachingConfig {
            num_snapshot_nodes: value.num_snapshot_nodes.map(|v| v as u64),
            num_chunk_refs: value.num_chunk_refs.map(|v| v as u64),
            num_transaction_changes: value.num_transaction_changes.map(|v| v as u64),
            num_bytes_attributes: value.num_bytes_attributes.map(|v| v as u64),
            num_bytes_chunks: value.num_bytes_chunks.map(|v| v as u64),
        }
    }
}

/// Storage concurrency settings
#[napi(object, js_name = "StorageConcurrencySettings")]
#[derive(Clone, Debug)]
pub struct JsStorageConcurrencySettings {
    pub max_concurrent_requests_for_object: Option<u32>,
    pub ideal_concurrent_request_size: Option<i64>,
}

impl From<JsStorageConcurrencySettings> for ConcurrencySettings {
    fn from(value: JsStorageConcurrencySettings) -> Self {
        ConcurrencySettings {
            max_concurrent_requests_for_object: value
                .max_concurrent_requests_for_object
                .and_then(|v| NonZeroU16::new(v as u16)),
            ideal_concurrent_request_size: value
                .ideal_concurrent_request_size
                .and_then(|v| NonZeroU64::new(v as u64)),
        }
    }
}

/// Storage retries settings
#[napi(object, js_name = "StorageRetriesSettings")]
#[derive(Clone, Debug)]
pub struct JsStorageRetriesSettings {
    pub max_tries: Option<u32>,
    pub initial_backoff_ms: Option<u32>,
    pub max_backoff_ms: Option<u32>,
}

impl From<JsStorageRetriesSettings> for RetriesSettings {
    fn from(value: JsStorageRetriesSettings) -> Self {
        RetriesSettings {
            max_tries: value.max_tries.and_then(|v| NonZeroU16::new(v as u16)),
            initial_backoff_ms: value.initial_backoff_ms,
            max_backoff_ms: value.max_backoff_ms,
        }
    }
}

/// Storage settings
#[napi(object, js_name = "StorageSettings")]
#[derive(Clone, Debug)]
pub struct JsStorageSettings {
    pub concurrency: Option<JsStorageConcurrencySettings>,
    pub retries: Option<JsStorageRetriesSettings>,
    pub unsafe_use_conditional_update: Option<bool>,
    pub unsafe_use_conditional_create: Option<bool>,
    pub unsafe_use_metadata: Option<bool>,
    pub storage_class: Option<String>,
    pub metadata_storage_class: Option<String>,
    pub chunks_storage_class: Option<String>,
    pub minimum_size_for_multipart_upload: Option<i64>,
}

impl From<JsStorageSettings> for Settings {
    fn from(value: JsStorageSettings) -> Self {
        Settings {
            concurrency: value.concurrency.map(|c| c.into()),
            retries: value.retries.map(|r| r.into()),
            unsafe_use_conditional_update: value.unsafe_use_conditional_update,
            unsafe_use_conditional_create: value.unsafe_use_conditional_create,
            unsafe_use_metadata: value.unsafe_use_metadata,
            storage_class: value.storage_class,
            metadata_storage_class: value.metadata_storage_class,
            chunks_storage_class: value.chunks_storage_class,
            minimum_size_for_multipart_upload: value
                .minimum_size_for_multipart_upload
                .map(|v| v as u64),
        }
    }
}

/// Virtual chunk container configuration
#[cfg(not(target_family = "wasm"))]
#[napi(object, js_name = "VirtualChunkContainer")]
#[derive(Clone, Debug)]
pub struct JsVirtualChunkContainer {
    pub name: Option<String>,
    pub url_prefix: String,
    pub store: JsObjectStoreConfig,
}

/// Repository configuration
///
/// The `manifest` field accepts a JSON object matching the serde serialization
/// of `ManifestConfig`. Example:
/// ```js
/// {
///   manifest: {
///     preload: {
///       max_total_refs: 1000,
///       preload_if: { true: null },
///       max_arrays_to_scan: 10
///     },
///     splitting: {
///       split_sizes: [
///         [{ path_matches: { regex: ".*" } }, [{ condition: "any", num_chunks: 100 }]]
///       ]
///     }
///   }
/// }
/// ```
#[cfg(not(target_family = "wasm"))]
#[napi(object, js_name = "RepositoryConfig")]
#[derive(Clone, Debug)]
pub struct JsRepositoryConfig {
    pub inline_chunk_threshold_bytes: Option<u32>,
    pub get_partial_values_concurrency: Option<u32>,
    pub compression: Option<JsCompressionConfig>,
    pub max_concurrent_requests: Option<u32>,
    pub caching: Option<JsCachingConfig>,
    pub storage: Option<JsStorageSettings>,
    /// Manifest configuration, passed as a JSON-compatible object.
    /// The object is deserialized using serde, matching the Rust ManifestConfig structure.
    pub manifest: Option<serde_json::Value>,
    /// Virtual chunk containers configuration
    pub virtual_chunk_containers: Option<HashMap<String, JsVirtualChunkContainer>>,
}

/// Repository configuration (WASM build — no virtual chunk support)
#[cfg(target_family = "wasm")]
#[napi(object, js_name = "RepositoryConfig")]
#[derive(Clone, Debug)]
pub struct JsRepositoryConfig {
    pub inline_chunk_threshold_bytes: Option<u32>,
    pub get_partial_values_concurrency: Option<u32>,
    pub compression: Option<JsCompressionConfig>,
    pub max_concurrent_requests: Option<u32>,
    pub caching: Option<JsCachingConfig>,
    pub storage: Option<JsStorageSettings>,
    /// Manifest configuration, passed as a JSON-compatible object.
    /// The object is deserialized using serde, matching the Rust ManifestConfig structure.
    pub manifest: Option<serde_json::Value>,
}

impl TryFrom<JsRepositoryConfig> for RepositoryConfig {
    type Error = String;

    fn try_from(value: JsRepositoryConfig) -> Result<Self, Self::Error> {
        let manifest: Option<ManifestConfig> = value
            .manifest
            .map(|v| serde_json::from_value(v).map_err(|e| e.to_string()))
            .transpose()?;

        #[cfg(not(target_family = "wasm"))]
        let virtual_chunk_containers = value
            .virtual_chunk_containers
            .map(|containers| {
                containers
                    .into_iter()
                    .map(|(k, v)| {
                        let container =
                            VirtualChunkContainer::new(v.url_prefix, v.store.into())?;
                        Ok((k, container))
                    })
                    .collect::<Result<HashMap<_, _>, String>>()
            })
            .transpose()?;
        #[cfg(target_family = "wasm")]
        let virtual_chunk_containers = None;

        Ok(RepositoryConfig {
            inline_chunk_threshold_bytes: value
                .inline_chunk_threshold_bytes
                .map(|v| v as u16),
            get_partial_values_concurrency: value
                .get_partial_values_concurrency
                .map(|v| v as u16),
            compression: value.compression.map(|c| c.into()),
            max_concurrent_requests: value.max_concurrent_requests.map(|v| v as u16),
            caching: value.caching.map(|c| c.into()),
            storage: value.storage.map(|s| s.into()),
            virtual_chunk_containers,
            manifest,
            previous_file: None,
        })
    }
}
