//! Type registry for dynamic trait object deserialization.
//!
//! This module provides a manual registry pattern to replace `typetag` for WASM compatibility.
//! The registry allows registering deserializers for trait objects using type tags, enabling
//! feature-gated backends and external crate registration (e.g., credential fetchers).

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use crate::config::{GcsCredentialsFetcher, S3CredentialsFetcher};
use crate::storage::object_store::ObjectStoreBackend;
use crate::storage::{Storage, StorageError, StorageErrorKind};
use std::sync::Arc;

/// A function that deserializes a type from an rmpv::Value.
pub type DeserializeFn<T> = fn(rmpv::Value) -> Result<T, StorageError>;

/// A thread-safe registry mapping type tags to deserializer functions.
pub struct TypeRegistry<T> {
    deserializers: RwLock<HashMap<&'static str, DeserializeFn<T>>>,
}

impl<T> TypeRegistry<T> {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            deserializers: RwLock::new(HashMap::new()),
        }
    }

    /// Register a deserializer for a type tag.
    ///
    /// # Panics
    /// Panics if the lock is poisoned.
    pub fn register(&self, type_tag: &'static str, f: DeserializeFn<T>) {
        #[allow(clippy::expect_used)]
        let mut map = self.deserializers.write().expect("Registry lock poisoned");
        map.insert(type_tag, f);
    }

    /// Get the deserializer for a type tag.
    ///
    /// # Panics
    /// Panics if the lock is poisoned.
    pub fn get(&self, type_tag: &str) -> Option<DeserializeFn<T>> {
        #[allow(clippy::expect_used)]
        let map = self.deserializers.read().expect("Registry lock poisoned");
        map.get(type_tag).copied()
    }
}

impl<T> Default for TypeRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Global Registry Instances
// =============================================================================

static STORAGE_REGISTRY: OnceLock<TypeRegistry<Arc<dyn Storage>>> = OnceLock::new();
static OBJECT_STORE_BACKEND_REGISTRY: OnceLock<TypeRegistry<Arc<dyn ObjectStoreBackend>>> =
    OnceLock::new();
static S3_CREDENTIALS_FETCHER_REGISTRY: OnceLock<
    TypeRegistry<Arc<dyn S3CredentialsFetcher>>,
> = OnceLock::new();
static GCS_CREDENTIALS_FETCHER_REGISTRY: OnceLock<
    TypeRegistry<Arc<dyn GcsCredentialsFetcher>>,
> = OnceLock::new();

/// Get the global Storage registry, initializing it if necessary.
pub fn storage_registry() -> &'static TypeRegistry<Arc<dyn Storage>> {
    STORAGE_REGISTRY.get_or_init(|| {
        let registry = TypeRegistry::new();
        init_storage_registry(&registry);
        registry
    })
}

/// Get the global ObjectStoreBackend registry, initializing it if necessary.
pub fn object_store_backend_registry() -> &'static TypeRegistry<Arc<dyn ObjectStoreBackend>>
{
    OBJECT_STORE_BACKEND_REGISTRY.get_or_init(|| {
        let registry = TypeRegistry::new();
        init_object_store_backend_registry(&registry);
        registry
    })
}

/// Get the global S3CredentialsFetcher registry, initializing it if necessary.
pub fn s3_credentials_fetcher_registry(
) -> &'static TypeRegistry<Arc<dyn S3CredentialsFetcher>> {
    S3_CREDENTIALS_FETCHER_REGISTRY.get_or_init(TypeRegistry::new)
}

/// Get the global GcsCredentialsFetcher registry, initializing it if necessary.
pub fn gcs_credentials_fetcher_registry(
) -> &'static TypeRegistry<Arc<dyn GcsCredentialsFetcher>> {
    GCS_CREDENTIALS_FETCHER_REGISTRY.get_or_init(TypeRegistry::new)
}

// =============================================================================
// Public Registration API for External Crates
// =============================================================================

/// Register a custom S3 credentials fetcher deserializer.
///
/// This allows external crates (e.g., icechunk-python) to register their own
/// credential fetcher implementations.
pub fn register_s3_credentials_fetcher(
    tag: &'static str,
    f: DeserializeFn<Arc<dyn S3CredentialsFetcher>>,
) {
    s3_credentials_fetcher_registry().register(tag, f);
}

/// Register a custom GCS credentials fetcher deserializer.
///
/// This allows external crates (e.g., icechunk-python) to register their own
/// credential fetcher implementations.
pub fn register_gcs_credentials_fetcher(
    tag: &'static str,
    f: DeserializeFn<Arc<dyn GcsCredentialsFetcher>>,
) {
    gcs_credentials_fetcher_registry().register(tag, f);
}

// =============================================================================
// Helper Functions for Deserialization
// =============================================================================

/// Extract a type tag from an rmpv::Value map.
pub fn extract_type_tag(value: &rmpv::Value, tag_field: &str) -> Result<String, StorageError> {
    let map = value.as_map().ok_or_else(|| {
        StorageError::from(StorageErrorKind::Other(format!(
            "Expected map for tagged type, got {:?}",
            value
        )))
    })?;

    for (k, v) in map {
        if let Some(key) = k.as_str() {
            if key == tag_field {
                if let Some(tag) = v.as_str() {
                    return Ok(tag.to_string());
                }
            }
        }
    }

    Err(StorageError::from(StorageErrorKind::Other(format!(
        "Missing type tag field '{}' in value",
        tag_field
    ))))
}

/// Deserialize from rmpv::Value using serde.
pub fn from_rmpv<T: serde::de::DeserializeOwned>(value: rmpv::Value) -> Result<T, StorageError> {
    rmpv::ext::from_value(value).map_err(|e| {
        StorageError::from(StorageErrorKind::Other(format!(
            "Failed to deserialize: {}",
            e
        )))
    })
}

// =============================================================================
// Registry Initialization Functions
// =============================================================================

fn init_storage_registry(registry: &TypeRegistry<Arc<dyn Storage>>) {
    use crate::storage::object_store::ObjectStorage;
    use crate::storage::redirect::RedirectStorage;

    // ObjectStorage - always available
    registry.register("ObjectStorage", |value| {
        let storage: ObjectStorage = from_rmpv(value)?;
        Ok(Arc::new(storage))
    });

    // RedirectStorage - always available
    registry.register("RedirectStorage", |value| {
        let storage: RedirectStorage = from_rmpv(value)?;
        Ok(Arc::new(storage))
    });

    // S3Storage - only available with s3 feature
    #[cfg(feature = "s3")]
    {
        use crate::storage::s3::S3Storage;
        registry.register("S3Storage", |value| {
            let storage: S3Storage = from_rmpv(value)?;
            Ok(Arc::new(storage))
        });
    }

    // LoggingStorage - only in tests
    #[cfg(test)]
    {
        use crate::storage::logging::LoggingStorage;
        registry.register("LoggingStorage", |value| {
            let storage: LoggingStorage = from_rmpv(value)?;
            Ok(Arc::new(storage))
        });
    }
}

fn init_object_store_backend_registry(registry: &TypeRegistry<Arc<dyn ObjectStoreBackend>>) {
    use crate::storage::object_store::*;

    // InMemory - always available in object_store
    registry.register("in_memory_object_store_provider", |value| {
        let backend: InMemoryObjectStoreBackend = from_rmpv(value)?;
        Ok(Arc::new(backend))
    });

    // LocalFileSystem - always available
    #[cfg(feature = "object-store-local")]
    registry.register("local_file_system_object_store_provider", |value| {
        let backend: LocalFileSystemObjectStoreBackend = from_rmpv(value)?;
        Ok(Arc::new(backend))
    });

    // HTTP
    #[cfg(feature = "object-store-http")]
    registry.register("http_object_store_provider", |value| {
        let backend: HttpObjectStoreBackend = from_rmpv(value)?;
        Ok(Arc::new(backend))
    });

    // S3
    #[cfg(feature = "object-store-s3")]
    registry.register("s3_object_store_provider", |value| {
        let backend: S3ObjectStoreBackend = from_rmpv(value)?;
        Ok(Arc::new(backend))
    });

    // GCS
    #[cfg(feature = "object-store-gcs")]
    registry.register("gcs_object_store_provider", |value| {
        let backend: GcsObjectStoreBackend = from_rmpv(value)?;
        Ok(Arc::new(backend))
    });

    // Azure
    #[cfg(feature = "object-store-azure")]
    registry.register("azure_object_store_provider", |value| {
        let backend: AzureObjectStoreBackend = from_rmpv(value)?;
        Ok(Arc::new(backend))
    });
}

// =============================================================================
// Serialization Helpers
// =============================================================================

/// Serialize a tagged type to a map with a type tag field.
///
/// This produces output compatible with the old typetag format.
/// The output is a map with the tag field first, followed by all fields from the value.
pub fn serialize_with_tag<S, T>(
    serializer: S,
    tag_field: &'static str,
    tag_value: &'static str,
    value: &T,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: serde::Serialize + ?Sized,
{
    use serde::ser::SerializeMap;

    // First serialize the value to JSON to get its fields as a universal intermediate format
    let value_json = serde_json::to_value(value).map_err(serde::ser::Error::custom)?;

    let map = value_json.as_object().ok_or_else(|| {
        serde::ser::Error::custom(format!(
            "Expected serialized value to be a map/object, got: {}",
            value_json
        ))
    })?;

    // Create a new map with the tag field first, then all original fields
    let mut ser_map = serializer.serialize_map(Some(map.len() + 1))?;
    ser_map.serialize_entry(tag_field, tag_value)?;

    for (k, v) in map {
        ser_map.serialize_entry(k, v)?;
    }

    ser_map.end()
}

// =============================================================================
// Credential Fetcher Deserialization Helpers
// =============================================================================

/// Deserialize an S3CredentialsFetcher from rmpv::Value.
pub fn deserialize_s3_credentials_fetcher(
    value: rmpv::Value,
) -> Result<Arc<dyn S3CredentialsFetcher>, StorageError> {
    let type_tag = extract_type_tag(&value, "s3_credentials_fetcher_type")?;
    let registry = s3_credentials_fetcher_registry();

    let deserialize_fn = registry.get(&type_tag).ok_or_else(|| {
        StorageError::from(StorageErrorKind::Other(format!(
            "Unknown S3CredentialsFetcher type: {}. Make sure the credential fetcher is registered.",
            type_tag
        )))
    })?;

    deserialize_fn(value)
}

/// Deserialize a GcsCredentialsFetcher from rmpv::Value.
pub fn deserialize_gcs_credentials_fetcher(
    value: rmpv::Value,
) -> Result<Arc<dyn GcsCredentialsFetcher>, StorageError> {
    let type_tag = extract_type_tag(&value, "gcs_credentials_fetcher_type")?;
    let registry = gcs_credentials_fetcher_registry();

    let deserialize_fn = registry.get(&type_tag).ok_or_else(|| {
        StorageError::from(StorageErrorKind::Other(format!(
            "Unknown GcsCredentialsFetcher type: {}. Make sure the credential fetcher is registered.",
            type_tag
        )))
    })?;

    deserialize_fn(value)
}
