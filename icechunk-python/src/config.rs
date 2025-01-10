use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    num::{NonZeroU16, NonZeroU64},
    path::PathBuf,
    sync::Arc,
};

use icechunk::{
    config::{
        AzureCredentials, AzureStaticCredentials, CachingConfig, CompressionAlgorithm,
        CompressionConfig, Credentials, CredentialsFetcher, GcsCredentials,
        GcsStaticCredentials, S3Credentials, S3Options, S3StaticCredentials,
    },
    storage::{self, ConcurrencySettings},
    virtual_chunks::VirtualChunkContainer,
    ObjectStoreConfig, RepositoryConfig, Storage,
};
use pyo3::{
    pyclass, pymethods,
    types::{PyAnyMethods, PyModule, PyType},
    Bound, PyErr, PyResult, Python,
};

use crate::errors::PyIcechunkStoreError;

#[pyclass(name = "S3StaticCredentials")]
#[derive(Clone, Debug)]
pub struct PyS3StaticCredentials {
    #[pyo3(get, set)]
    access_key_id: String,
    #[pyo3(get, set)]
    secret_access_key: String,
    #[pyo3(get, set)]
    session_token: Option<String>,
    #[pyo3(get, set)]
    expires_after: Option<DateTime<Utc>>,
}

impl From<&PyS3StaticCredentials> for S3StaticCredentials {
    fn from(credentials: &PyS3StaticCredentials) -> Self {
        S3StaticCredentials {
            access_key_id: credentials.access_key_id.clone(),
            secret_access_key: credentials.secret_access_key.clone(),
            session_token: credentials.session_token.clone(),
            expires_after: credentials.expires_after,
        }
    }
}

impl From<PyS3StaticCredentials> for S3StaticCredentials {
    fn from(credentials: PyS3StaticCredentials) -> Self {
        S3StaticCredentials {
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.secret_access_key,
            session_token: credentials.session_token,
            expires_after: credentials.expires_after,
        }
    }
}

#[pymethods]
impl PyS3StaticCredentials {
    #[new]
    #[pyo3(signature = (
        access_key_id,
        secret_access_key,
        session_token = None,
        expires_after = None,
    ))]
    fn new(
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        expires_after: Option<DateTime<Utc>>,
    ) -> Self {
        Self { access_key_id, secret_access_key, session_token, expires_after }
    }
}

#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PythonCredentialsFetcher {
    pub pickled_function: Vec<u8>,
}

#[pymethods]
impl PythonCredentialsFetcher {
    #[new]
    pub fn new(pickled_function: Vec<u8>) -> Self {
        PythonCredentialsFetcher { pickled_function }
    }
}

#[async_trait]
#[typetag::serde]
impl CredentialsFetcher for PythonCredentialsFetcher {
    async fn get(&self) -> Result<S3StaticCredentials, String> {
        Python::with_gil(|py| {
            let pickle_module = PyModule::import(py, "pickle")?;
            let loads_function = pickle_module.getattr("loads")?;
            let fetcher = loads_function.call1((self.pickled_function.clone(),))?;
            let creds: PyS3StaticCredentials = fetcher.call0()?.extract()?;
            Ok(creds.into())
        })
        .map_err(|e: PyErr| e.to_string())
    }
}

#[pyclass(name = "S3Credentials")]
#[derive(Clone, Debug)]
pub enum PyS3Credentials {
    FromEnv(),
    Anonymous(),
    Static(PyS3StaticCredentials),
    Refreshable(Vec<u8>),
}

impl From<PyS3Credentials> for S3Credentials {
    fn from(credentials: PyS3Credentials) -> Self {
        match credentials {
            PyS3Credentials::FromEnv() => S3Credentials::FromEnv,
            PyS3Credentials::Anonymous() => S3Credentials::Anonymous,
            PyS3Credentials::Static(creds) => S3Credentials::Static(creds.into()),
            PyS3Credentials::Refreshable(pickled_function) => {
                S3Credentials::Refreshable(Arc::new(PythonCredentialsFetcher {
                    pickled_function,
                }))
            }
        }
    }
}

#[pyclass(name = "GcsStaticCredentials")]
#[derive(Clone, Debug)]
pub enum PyGcsStaticCredentials {
    ServiceAccount(String),
    ServiceAccountKey(String),
    ApplicationCredentials(String),
}

impl From<PyGcsStaticCredentials> for GcsStaticCredentials {
    fn from(value: PyGcsStaticCredentials) -> Self {
        match value {
            PyGcsStaticCredentials::ServiceAccount(path) => {
                GcsStaticCredentials::ServiceAccount(path.into())
            }
            PyGcsStaticCredentials::ServiceAccountKey(key) => {
                GcsStaticCredentials::ServiceAccountKey(key)
            }
            PyGcsStaticCredentials::ApplicationCredentials(path) => {
                GcsStaticCredentials::ApplicationCredentials(path.into())
            }
        }
    }
}

#[pyclass(name = "GcsCredentials")]
#[derive(Clone, Debug)]
pub enum PyGcsCredentials {
    FromEnv(),
    Static(PyGcsStaticCredentials),
}

impl From<PyGcsCredentials> for GcsCredentials {
    fn from(value: PyGcsCredentials) -> Self {
        match value {
            PyGcsCredentials::FromEnv() => GcsCredentials::FromEnv,
            PyGcsCredentials::Static(creds) => GcsCredentials::Static(creds.into()),
        }
    }
}

#[pyclass(name = "AzureStaticCredentials")]
#[derive(Clone, Debug)]
pub enum PyAzureStaticCredentials {
    AccessKey(String),
    SasToken(String),
    BearerToken(String),
}

impl From<PyAzureStaticCredentials> for AzureStaticCredentials {
    fn from(value: PyAzureStaticCredentials) -> Self {
        match value {
            PyAzureStaticCredentials::AccessKey(key) => {
                AzureStaticCredentials::AccessKey(key)
            }
            PyAzureStaticCredentials::SasToken(token) => {
                AzureStaticCredentials::SASToken(token)
            }
            PyAzureStaticCredentials::BearerToken(key) => {
                AzureStaticCredentials::BearerToken(key)
            }
        }
    }
}

#[pyclass(name = "AzureCredentials")]
#[derive(Clone, Debug)]
pub enum PyAzureCredentials {
    FromEnv(),
    Static(PyAzureStaticCredentials),
}

#[pyclass(name = "Credentials")]
#[derive(Clone, Debug)]
pub enum PyCredentials {
    S3(PyS3Credentials),
    Gcs(PyGcsCredentials),
    Azure(PyAzureCredentials),
}

impl From<PyAzureCredentials> for AzureCredentials {
    fn from(value: PyAzureCredentials) -> Self {
        match value {
            PyAzureCredentials::FromEnv() => AzureCredentials::FromEnv,
            PyAzureCredentials::Static(creds) => AzureCredentials::Static(creds.into()),
        }
    }
}

impl From<PyCredentials> for Credentials {
    fn from(value: PyCredentials) -> Self {
        match value {
            PyCredentials::S3(cred) => Credentials::S3(cred.into()),
            PyCredentials::Gcs(cred) => Credentials::Gcs(cred.into()),
            PyCredentials::Azure(cred) => Credentials::Azure(cred.into()),
        }
    }
}

#[pyclass(name = "S3Options", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyS3Options {
    #[pyo3(get, set)]
    pub region: Option<String>,
    #[pyo3(get, set)]
    pub endpoint_url: Option<String>,
    #[pyo3(get, set)]
    pub allow_http: bool,
    #[pyo3(get, set)]
    pub anonymous: bool,
}

#[pymethods]
impl PyS3Options {
    #[new]
    #[pyo3(signature = ( region=None, endpoint_url=None, allow_http=false, anonymous=false))]
    pub(crate) fn new(
        region: Option<String>,
        endpoint_url: Option<String>,
        allow_http: bool,
        anonymous: bool,
    ) -> Self {
        Self { region, endpoint_url, allow_http, anonymous }
    }
}

impl From<PyS3Options> for S3Options {
    fn from(options: PyS3Options) -> Self {
        S3Options {
            region: options.region.clone(),
            endpoint_url: options.endpoint_url.clone(),
            allow_http: options.allow_http,
            anonymous: options.anonymous,
        }
    }
}

impl From<S3Options> for PyS3Options {
    fn from(value: S3Options) -> Self {
        Self {
            region: value.region,
            endpoint_url: value.endpoint_url,
            allow_http: value.allow_http,
            anonymous: value.anonymous,
        }
    }
}

#[pyclass(name = "ObjectStoreConfig", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PyObjectStoreConfig {
    InMemory(),
    LocalFileSystem(PathBuf),
    S3Compatible(PyS3Options),
    S3(PyS3Options),
    Gcs(Option<HashMap<String, String>>),
    Azure(HashMap<String, String>),
    Tigris(),
}

impl From<PyObjectStoreConfig> for ObjectStoreConfig {
    fn from(value: PyObjectStoreConfig) -> Self {
        match value {
            PyObjectStoreConfig::InMemory() => ObjectStoreConfig::InMemory,
            PyObjectStoreConfig::LocalFileSystem(path) => {
                ObjectStoreConfig::LocalFileSystem(path)
            }
            PyObjectStoreConfig::S3Compatible(opts) => {
                ObjectStoreConfig::S3Compatible(opts.into())
            }
            PyObjectStoreConfig::S3(opts) => ObjectStoreConfig::S3(opts.into()),
            PyObjectStoreConfig::Gcs(opts) => {
                ObjectStoreConfig::Gcs(opts.unwrap_or_default())
            }
            PyObjectStoreConfig::Azure(opts) => ObjectStoreConfig::Azure(opts),
            PyObjectStoreConfig::Tigris() => ObjectStoreConfig::Tigris {},
        }
    }
}

impl From<ObjectStoreConfig> for PyObjectStoreConfig {
    fn from(value: ObjectStoreConfig) -> Self {
        match value {
            ObjectStoreConfig::InMemory => PyObjectStoreConfig::InMemory(),
            ObjectStoreConfig::LocalFileSystem(path_buf) => {
                PyObjectStoreConfig::LocalFileSystem(path_buf)
            }
            ObjectStoreConfig::S3Compatible(opts) => {
                PyObjectStoreConfig::S3Compatible(opts.into())
            }
            ObjectStoreConfig::S3(opts) => PyObjectStoreConfig::S3(opts.into()),
            ObjectStoreConfig::Gcs(opts) => PyObjectStoreConfig::Gcs(Some(opts)),
            ObjectStoreConfig::Azure(opts) => PyObjectStoreConfig::Azure(opts),
            ObjectStoreConfig::Tigris {} => PyObjectStoreConfig::Tigris(),
        }
    }
}

#[pyclass(name = "VirtualChunkContainer", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyVirtualChunkContainer {
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub url_prefix: String,
    #[pyo3(get, set)]
    pub store: PyObjectStoreConfig,
}

#[pymethods]
impl PyVirtualChunkContainer {
    #[new]
    pub fn new(name: String, url_prefix: String, store: PyObjectStoreConfig) -> Self {
        Self { name, url_prefix, store }
    }
}

impl From<PyVirtualChunkContainer> for VirtualChunkContainer {
    fn from(value: PyVirtualChunkContainer) -> Self {
        Self { name: value.name, url_prefix: value.url_prefix, store: value.store.into() }
    }
}

impl From<VirtualChunkContainer> for PyVirtualChunkContainer {
    fn from(value: VirtualChunkContainer) -> Self {
        Self { name: value.name, url_prefix: value.url_prefix, store: value.store.into() }
    }
}

#[pyclass(name = "CompressionAlgorithm", eq, eq_int)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PyCompressionAlgorithm {
    Zstd,
}

#[pymethods]
impl PyCompressionAlgorithm {
    #[staticmethod]
    /// Create a default `CompressionAlgorithm` instance
    fn default() -> Self {
        CompressionAlgorithm::default().into()
    }
}

impl From<CompressionAlgorithm> for PyCompressionAlgorithm {
    fn from(value: CompressionAlgorithm) -> Self {
        match value {
            CompressionAlgorithm::Zstd => PyCompressionAlgorithm::Zstd,
        }
    }
}

impl From<PyCompressionAlgorithm> for CompressionAlgorithm {
    fn from(value: PyCompressionAlgorithm) -> Self {
        match value {
            PyCompressionAlgorithm::Zstd => CompressionAlgorithm::Zstd,
        }
    }
}

#[pyclass(name = "CompressionConfig", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyCompressionConfig {
    #[pyo3(get, set)]
    pub algorithm: PyCompressionAlgorithm,
    #[pyo3(get, set)]
    pub level: u8,
}

#[pymethods]
impl PyCompressionConfig {
    #[staticmethod]
    /// Create a default `CompressionConfig` instance
    fn default() -> Self {
        CompressionConfig::default().into()
    }
}

impl From<CompressionConfig> for PyCompressionConfig {
    fn from(value: CompressionConfig) -> Self {
        Self { algorithm: value.algorithm.into(), level: value.level }
    }
}

impl From<PyCompressionConfig> for CompressionConfig {
    fn from(value: PyCompressionConfig) -> Self {
        Self { algorithm: value.algorithm.into(), level: value.level }
    }
}

#[pyclass(name = "CachingConfig", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyCachingConfig {
    #[pyo3(get, set)]
    pub snapshots_cache_size: u16,
    #[pyo3(get, set)]
    pub manifests_cache_size: u16,
    #[pyo3(get, set)]
    pub transactions_cache_size: u16,
    #[pyo3(get, set)]
    pub attributes_cache_size: u16,
    #[pyo3(get, set)]
    pub chunks_cache_size: u16,
}

#[pymethods]
impl PyCachingConfig {
    #[staticmethod]
    /// Create a default `CachingConfig` instance
    fn default() -> Self {
        CachingConfig::default().into()
    }
}

impl From<PyCachingConfig> for CachingConfig {
    fn from(value: PyCachingConfig) -> Self {
        Self {
            snapshots_cache_size: value.snapshots_cache_size,
            manifests_cache_size: value.manifests_cache_size,
            transactions_cache_size: value.transactions_cache_size,
            attributes_cache_size: value.attributes_cache_size,
            chunks_cache_size: value.chunks_cache_size,
        }
    }
}

impl From<CachingConfig> for PyCachingConfig {
    fn from(value: CachingConfig) -> Self {
        Self {
            snapshots_cache_size: value.snapshots_cache_size,
            manifests_cache_size: value.manifests_cache_size,
            transactions_cache_size: value.transactions_cache_size,
            attributes_cache_size: value.attributes_cache_size,
            chunks_cache_size: value.chunks_cache_size,
        }
    }
}

#[pyclass(name = "StorageConcurrencySettings", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyStorageConcurrencySettings {
    #[pyo3(get, set)]
    pub max_concurrent_requests_for_object: NonZeroU16,
    #[pyo3(get, set)]
    pub ideal_concurrent_request_size: NonZeroU64,
}

impl From<ConcurrencySettings> for PyStorageConcurrencySettings {
    fn from(value: ConcurrencySettings) -> Self {
        Self {
            max_concurrent_requests_for_object: value.max_concurrent_requests_for_object,
            ideal_concurrent_request_size: value.ideal_concurrent_request_size,
        }
    }
}

impl From<PyStorageConcurrencySettings> for ConcurrencySettings {
    fn from(value: PyStorageConcurrencySettings) -> Self {
        Self {
            max_concurrent_requests_for_object: value.max_concurrent_requests_for_object,
            ideal_concurrent_request_size: value.ideal_concurrent_request_size,
        }
    }
}

#[pyclass(name = "StorageSettings", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyStorageSettings {
    #[pyo3(get, set)]
    pub concurrency: PyStorageConcurrencySettings,
}

impl From<storage::Settings> for PyStorageSettings {
    fn from(value: storage::Settings) -> Self {
        Self { concurrency: value.concurrency.into() }
    }
}

impl From<PyStorageSettings> for storage::Settings {
    fn from(value: PyStorageSettings) -> Self {
        Self { concurrency: value.concurrency.into() }
    }
}

#[pyclass(name = "RepositoryConfig", eq)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PyRepositoryConfig {
    #[pyo3(get, set)]
    pub inline_chunk_threshold_bytes: u16,
    #[pyo3(get, set)]
    pub unsafe_overwrite_refs: bool,
    #[pyo3(get, set)]
    pub get_partial_values_concurrency: u16,
    #[pyo3(get, set)]
    pub compression: PyCompressionConfig,
    #[pyo3(get, set)]
    pub caching: PyCachingConfig,
    #[pyo3(get, set)]
    pub storage: Option<PyStorageSettings>,
    #[pyo3(get, set)]
    pub virtual_chunk_containers: HashMap<String, PyVirtualChunkContainer>,
}

impl From<PyRepositoryConfig> for RepositoryConfig {
    fn from(value: PyRepositoryConfig) -> Self {
        Self {
            inline_chunk_threshold_bytes: value.inline_chunk_threshold_bytes,
            unsafe_overwrite_refs: value.unsafe_overwrite_refs,
            get_partial_values_concurrency: value.get_partial_values_concurrency,
            storage: value.storage.map(|s| s.into()),
            compression: value.compression.into(),
            caching: value.caching.into(),
            virtual_chunk_containers: value
                .virtual_chunk_containers
                .into_iter()
                .map(|(name, cont)| (name, cont.into()))
                .collect(),
        }
    }
}

impl From<RepositoryConfig> for PyRepositoryConfig {
    fn from(value: RepositoryConfig) -> Self {
        Self {
            inline_chunk_threshold_bytes: value.inline_chunk_threshold_bytes,
            unsafe_overwrite_refs: value.unsafe_overwrite_refs,
            get_partial_values_concurrency: value.get_partial_values_concurrency,
            storage: value.storage.map(|s| s.into()),
            compression: value.compression.into(),
            caching: value.caching.into(),
            virtual_chunk_containers: value
                .virtual_chunk_containers
                .into_iter()
                .map(|(name, cont)| (name, cont.into()))
                .collect(),
        }
    }
}

#[pymethods]
impl PyRepositoryConfig {
    #[staticmethod]
    /// Create a default `RepositoryConfig` instance
    fn default() -> Self {
        RepositoryConfig::default().into()
    }

    pub fn set_virtual_chunk_container(&mut self, cont: PyVirtualChunkContainer) {
        self.virtual_chunk_containers.insert(cont.name.clone(), cont);
    }

    pub fn clear_virtual_chunk_containers(&mut self) {
        self.virtual_chunk_containers.clear();
    }
}

#[pyclass(name = "Storage")]
#[derive(Clone, Debug)]
pub struct PyStorage(pub Arc<dyn Storage + Send + Sync>);

#[pymethods]
impl PyStorage {
    #[pyo3(signature = ( config, bucket, prefix, credentials=None))]
    #[classmethod]
    pub fn new_s3(
        _cls: &Bound<'_, PyType>,
        config: PyS3Options,
        bucket: String,
        prefix: Option<String>,
        credentials: Option<PyS3Credentials>,
    ) -> PyResult<Self> {
        let storage = icechunk::storage::new_s3_storage(
            config.into(),
            bucket,
            prefix,
            credentials.map(|cred| cred.into()),
        )
        .map_err(PyIcechunkStoreError::StorageError)?;

        Ok(PyStorage(storage))
    }

    #[classmethod]
    pub fn new_in_memory(_cls: &Bound<'_, PyType>) -> PyResult<Self> {
        let storage = icechunk::storage::new_in_memory_storage()
            .map_err(PyIcechunkStoreError::StorageError)?;

        Ok(PyStorage(storage))
    }

    #[classmethod]
    pub fn new_local_filesystem(
        _cls: &Bound<'_, PyType>,
        path: PathBuf,
    ) -> PyResult<Self> {
        let storage = icechunk::storage::new_local_filesystem_storage(&path)
            .map_err(PyIcechunkStoreError::StorageError)?;

        Ok(PyStorage(storage))
    }

    #[staticmethod]
    #[pyo3(signature = (bucket, prefix, credentials=None, *, config=None))]
    pub fn new_gcs(
        bucket: String,
        prefix: Option<String>,
        credentials: Option<PyGcsCredentials>,
        config: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let storage = icechunk::storage::new_gcs_storage(
            bucket,
            prefix,
            credentials.map(|cred| cred.into()),
            config,
        )
        .map_err(PyIcechunkStoreError::StorageError)?;

        Ok(PyStorage(storage))
    }

    #[staticmethod]
    #[pyo3(signature = (container, prefix, credentials=None, *, config=None))]
    pub fn new_azure_blob(
        container: String,
        prefix: String,
        credentials: Option<PyAzureCredentials>,
        config: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let storage = icechunk::storage::new_azure_blob_storage(
            container,
            prefix,
            credentials.map(|cred| cred.into()),
            config,
        )
        .map_err(PyIcechunkStoreError::StorageError)?;

        Ok(PyStorage(storage))
    }

    pub fn default_settings(&self) -> PyStorageSettings {
        self.0.default_settings().into()
    }
}
