use async_trait::async_trait;
use chrono::{DateTime, Datelike, TimeDelta, Timelike, Utc};
use icechunk::storage::RetriesSettings;
use itertools::Itertools;
use pyo3::exceptions::PyValueError;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::{
    collections::HashMap,
    fmt::Display,
    hash::DefaultHasher,
    num::{NonZeroU16, NonZeroU64},
    path::PathBuf,
    sync::Arc,
};

use icechunk::{
    ObjectStoreConfig, RepositoryConfig, Storage,
    config::{
        AzureCredentials, AzureStaticCredentials, CachingConfig, CompressionAlgorithm,
        CompressionConfig, Credentials, GcsBearerCredential, GcsCredentials,
        GcsCredentialsFetcher, GcsStaticCredentials, ManifestConfig,
        ManifestPreloadCondition, ManifestPreloadConfig, ManifestSplitCondition,
        ManifestSplitDim, ManifestSplitDimCondition, ManifestSplittingConfig,
        S3Credentials, S3CredentialsFetcher, S3Options, S3StaticCredentials,
    },
    storage::{self, ConcurrencySettings},
    virtual_chunks::VirtualChunkContainer,
};
use pyo3::{
    Bound, FromPyObject, Py, PyErr, PyResult, Python, pyclass, pymethods,
    types::{PyAnyMethods, PyModule, PyType},
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
    pub fn new(
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        expires_after: Option<DateTime<Utc>>,
    ) -> Self {
        Self { access_key_id, secret_access_key, session_token, expires_after }
    }

    pub fn __repr__(&self) -> String {
        // TODO: escape
        format!(
            r#"S3StaticCredentials(access_key_id="{ak}", secret_access_key="{sk}", session_token={st}, expires_after={ea})"#,
            ak = self.access_key_id.as_str(),
            sk = self.secret_access_key.as_str(),
            st = format_option(self.session_token.as_ref()),
            ea = format_option(self.expires_after.as_ref().map(datetime_repr))
        )
    }
}

pub(crate) fn format_option_to_string<T: Display>(o: Option<T>) -> String {
    match o.as_ref() {
        None => "None".to_string(),
        Some(s) => s.to_string(),
    }
}

fn format_option<'a, T: AsRef<str> + 'a>(o: Option<T>) -> String {
    match o.as_ref() {
        None => "None".to_string(),
        Some(s) => s.as_ref().to_string(),
    }
}

fn format_bool(b: bool) -> &'static str {
    match b {
        true => "True",
        false => "False",
    }
}

fn format_str(s: &str) -> String {
    format!(r#""{s}""#)
}

pub(crate) fn datetime_repr(d: &DateTime<Utc>) -> String {
    format!(
        "datetime.datetime({y},{month},{d},{h},{min},{sec},{micro}, tzinfo=datetime.timezone.utc)",
        y = d.year(),
        month = d.month(),
        d = d.day(),
        h = d.hour(),
        min = d.minute(),
        sec = d.second(),
        micro = (d.nanosecond() / 1000),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PythonCredentialsFetcher<CredType> {
    pub pickled_function: Vec<u8>,
    pub current: Option<CredType>,
}

impl<CredType> PythonCredentialsFetcher<CredType> {
    fn new(pickled_function: Vec<u8>) -> Self {
        PythonCredentialsFetcher { pickled_function, current: None }
    }

    fn new_with_current<C>(pickled_function: Vec<u8>, current: C) -> Self
    where
        C: Into<CredType>,
    {
        PythonCredentialsFetcher { pickled_function, current: Some(current.into()) }
    }
}

fn call_pickled<PyCred>(
    py: Python<'_>,
    pickled_function: Vec<u8>,
) -> Result<PyCred, PyErr>
where
    PyCred: for<'a> FromPyObject<'a>,
{
    let pickle_module = PyModule::import(py, "pickle")?;
    let loads_function = pickle_module.getattr("loads")?;
    let fetcher = loads_function.call1((pickled_function,))?;
    let creds: PyCred = fetcher.call0()?.extract()?;
    Ok(creds)
}

#[async_trait]
#[typetag::serde]
impl S3CredentialsFetcher for PythonCredentialsFetcher<S3StaticCredentials> {
    async fn get(&self) -> Result<S3StaticCredentials, String> {
        Python::with_gil(|py| {
            if let Some(static_creds) = self.current.as_ref() {
                // avoid herding by adding some randomness
                let delta = TimeDelta::seconds(rand::random_range(5..180));
                let expiration = static_creds
                    .expires_after
                    .map(|exp| exp - delta)
                    .unwrap_or(chrono::DateTime::<Utc>::MAX_UTC);
                if Utc::now() < expiration {
                    return Ok(static_creds.clone());
                }
            }
            call_pickled::<PyS3StaticCredentials>(py, self.pickled_function.clone())
                .map(|c| c.into())
        })
        .map_err(|e: PyErr| e.to_string())
    }
}

#[async_trait]
#[typetag::serde]
impl GcsCredentialsFetcher for PythonCredentialsFetcher<GcsBearerCredential> {
    async fn get(&self) -> Result<GcsBearerCredential, String> {
        Python::with_gil(|py| {
            if let Some(static_creds) = self.current.as_ref() {
                // avoid herding by adding some randomness
                let delta = TimeDelta::seconds(rand::random_range(5..180));
                let expiration = static_creds
                    .expires_after
                    .map(|exp| exp - delta)
                    .unwrap_or(chrono::DateTime::<Utc>::MAX_UTC);
                if Utc::now() < expiration {
                    return Ok(static_creds.clone());
                }
            }
            call_pickled::<PyGcsBearerCredential>(py, self.pickled_function.clone())
                .map(|c| c.into())
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
    Refreshable { pickled_function: Vec<u8>, current: Option<PyS3StaticCredentials> },
}

impl From<PyS3Credentials> for S3Credentials {
    fn from(credentials: PyS3Credentials) -> Self {
        match credentials {
            PyS3Credentials::FromEnv() => S3Credentials::FromEnv,
            PyS3Credentials::Anonymous() => S3Credentials::Anonymous,
            PyS3Credentials::Static(creds) => S3Credentials::Static(creds.into()),
            PyS3Credentials::Refreshable { pickled_function, current } => {
                let fetcher = if let Some(current) = current {
                    PythonCredentialsFetcher::new_with_current(pickled_function, current)
                } else {
                    PythonCredentialsFetcher::new(pickled_function)
                };

                S3Credentials::Refreshable(Arc::new(fetcher))
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
    BearerToken(String),
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
            PyGcsStaticCredentials::BearerToken(token) => {
                GcsStaticCredentials::BearerToken(GcsBearerCredential {
                    bearer: token,
                    expires_after: None,
                })
            }
        }
    }
}

#[pyclass(name = "GcsBearerCredential")]
#[derive(Clone, Debug)]
pub struct PyGcsBearerCredential {
    pub bearer: String,
    pub expires_after: Option<DateTime<Utc>>,
}

#[pymethods]
impl PyGcsBearerCredential {
    #[new]
    #[pyo3(signature = (bearer, *, expires_after = None))]
    pub fn new(bearer: String, expires_after: Option<DateTime<Utc>>) -> Self {
        PyGcsBearerCredential { bearer, expires_after }
    }
}

impl From<PyGcsBearerCredential> for GcsBearerCredential {
    fn from(value: PyGcsBearerCredential) -> Self {
        GcsBearerCredential { bearer: value.bearer, expires_after: value.expires_after }
    }
}

impl From<GcsBearerCredential> for PyGcsBearerCredential {
    fn from(value: GcsBearerCredential) -> Self {
        PyGcsBearerCredential { bearer: value.bearer, expires_after: value.expires_after }
    }
}

#[pyclass(name = "GcsCredentials")]
#[derive(Clone, Debug)]
pub enum PyGcsCredentials {
    FromEnv(),
    Static(PyGcsStaticCredentials),
    Refreshable { pickled_function: Vec<u8>, current: Option<PyGcsBearerCredential> },
}

impl From<PyGcsCredentials> for GcsCredentials {
    fn from(value: PyGcsCredentials) -> Self {
        match value {
            PyGcsCredentials::FromEnv() => GcsCredentials::FromEnv,
            PyGcsCredentials::Static(creds) => GcsCredentials::Static(creds.into()),
            PyGcsCredentials::Refreshable { pickled_function, current } => {
                let fetcher = if let Some(current) = current {
                    PythonCredentialsFetcher::new_with_current(pickled_function, current)
                } else {
                    PythonCredentialsFetcher::new(pickled_function)
                };

                GcsCredentials::Refreshable(Arc::new(fetcher))
            }
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
    #[pyo3(get, set)]
    pub force_path_style: bool,
}

#[pymethods]
impl PyS3Options {
    #[new]
    #[pyo3(signature = ( region=None, endpoint_url=None, allow_http=false, anonymous=false, force_path_style=false))]
    pub(crate) fn new(
        region: Option<String>,
        endpoint_url: Option<String>,
        allow_http: bool,
        anonymous: bool,
        force_path_style: bool,
    ) -> Self {
        Self { region, endpoint_url, allow_http, anonymous, force_path_style }
    }

    pub fn __repr__(&self) -> String {
        // TODO: escape
        format!(
            r#"S3Options(region={region}, endpoint_url={url}, allow_http={http}, anonymous={anon}, force_path_style={force_path_style})"#,
            region = format_option(self.region.as_ref()),
            url = format_option(self.endpoint_url.as_ref()),
            http = format_bool(self.allow_http),
            anon = format_bool(self.anonymous),
            force_path_style = format_bool(self.force_path_style),
        )
    }
}

impl From<&PyS3Options> for S3Options {
    fn from(options: &PyS3Options) -> Self {
        S3Options {
            region: options.region.clone(),
            endpoint_url: options.endpoint_url.clone(),
            allow_http: options.allow_http,
            anonymous: options.anonymous,
            force_path_style: options.force_path_style,
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
            force_path_style: value.force_path_style,
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
    Azure(Option<HashMap<String, String>>),
    Tigris(PyS3Options),
    Http(Option<HashMap<String, String>>),
}

impl From<&PyObjectStoreConfig> for ObjectStoreConfig {
    fn from(value: &PyObjectStoreConfig) -> Self {
        match value {
            PyObjectStoreConfig::InMemory() => ObjectStoreConfig::InMemory,
            PyObjectStoreConfig::LocalFileSystem(path) => {
                ObjectStoreConfig::LocalFileSystem(path.clone())
            }
            PyObjectStoreConfig::S3Compatible(opts) => {
                ObjectStoreConfig::S3Compatible(opts.into())
            }
            PyObjectStoreConfig::S3(opts) => ObjectStoreConfig::S3(opts.into()),
            PyObjectStoreConfig::Gcs(opts) => {
                ObjectStoreConfig::Gcs(opts.clone().unwrap_or_default())
            }
            PyObjectStoreConfig::Azure(opts) => {
                ObjectStoreConfig::Azure(opts.clone().unwrap_or_default())
            }
            PyObjectStoreConfig::Tigris(opts) => ObjectStoreConfig::Tigris(opts.into()),
            PyObjectStoreConfig::Http(opts) => {
                ObjectStoreConfig::Http(opts.clone().unwrap_or_default())
            }
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
            ObjectStoreConfig::Azure(opts) => PyObjectStoreConfig::Azure(Some(opts)),
            ObjectStoreConfig::Tigris(opts) => PyObjectStoreConfig::Tigris(opts.into()),
            ObjectStoreConfig::Http(opts) => PyObjectStoreConfig::Http(Some(opts)),
        }
    }
}

#[pyclass(name = "VirtualChunkContainer", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyVirtualChunkContainer {
    #[pyo3(get, set)]
    pub name: Option<String>,
    #[pyo3(get, set)]
    pub url_prefix: String,
    #[pyo3(get, set)]
    pub store: PyObjectStoreConfig,
}

#[pymethods]
impl PyVirtualChunkContainer {
    #[new]
    pub fn new(url_prefix: String, store: PyObjectStoreConfig) -> Self {
        Self { name: None, url_prefix, store }
    }
}

impl TryFrom<&PyVirtualChunkContainer> for VirtualChunkContainer {
    type Error = String;

    fn try_from(value: &PyVirtualChunkContainer) -> Result<Self, Self::Error> {
        let cont =
            VirtualChunkContainer::new(value.url_prefix.clone(), (&value.store).into())?;
        Ok(cont)
    }
}

impl From<VirtualChunkContainer> for PyVirtualChunkContainer {
    fn from(value: VirtualChunkContainer) -> Self {
        let url = value.url_prefix().to_string();
        Self { name: value.name, url_prefix: url, store: value.store.into() }
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

    #[new]
    fn new() -> Self {
        Self::default()
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
    pub algorithm: Option<PyCompressionAlgorithm>,
    #[pyo3(get, set)]
    pub level: Option<u8>,
}

#[pymethods]
impl PyCompressionConfig {
    #[staticmethod]
    /// Create a default `CompressionConfig` instance
    fn default() -> Self {
        CompressionConfig::default().into()
    }

    #[pyo3(signature = (algorithm=None, level=None))]
    #[new]
    pub fn new(algorithm: Option<PyCompressionAlgorithm>, level: Option<u8>) -> Self {
        Self { algorithm, level }
    }

    pub fn __repr__(&self) -> String {
        format!(
            r#"CompressionConfig(algorithm=None, level={level})"#,
            level = format_option_to_string(self.level.map(|l| l.to_string())),
        )
    }
}

impl From<CompressionConfig> for PyCompressionConfig {
    fn from(value: CompressionConfig) -> Self {
        Self { algorithm: value.algorithm.map(|a| a.into()), level: value.level }
    }
}

impl From<&PyCompressionConfig> for CompressionConfig {
    fn from(value: &PyCompressionConfig) -> Self {
        Self {
            algorithm: value.algorithm.as_ref().map(|a| a.clone().into()),
            level: value.level,
        }
    }
}

#[pyclass(name = "CachingConfig", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyCachingConfig {
    #[pyo3(get, set)]
    pub num_snapshot_nodes: Option<u64>,
    #[pyo3(get, set)]
    pub num_chunk_refs: Option<u64>,
    #[pyo3(get, set)]
    pub num_transaction_changes: Option<u64>,
    #[pyo3(get, set)]
    pub num_bytes_attributes: Option<u64>,
    #[pyo3(get, set)]
    pub num_bytes_chunks: Option<u64>,
}

#[pymethods]
impl PyCachingConfig {
    #[staticmethod]
    /// Create a default `CachingConfig` instance
    fn default() -> Self {
        CachingConfig::default().into()
    }

    #[pyo3(signature = (num_snapshot_nodes=None, num_chunk_refs=None, num_transaction_changes=None, num_bytes_attributes=None, num_bytes_chunks=None))]
    #[new]
    pub fn new(
        num_snapshot_nodes: Option<u64>,
        num_chunk_refs: Option<u64>,
        num_transaction_changes: Option<u64>,
        num_bytes_attributes: Option<u64>,
        num_bytes_chunks: Option<u64>,
    ) -> Self {
        Self {
            num_snapshot_nodes,
            num_chunk_refs,
            num_transaction_changes,
            num_bytes_attributes,
            num_bytes_chunks,
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            r#"CachingConfig(num_snapshot_nodes={snap}, num_chunk_refs={man}, num_transaction_changes={tx}, num_bytes_attributes={att}, num_bytes_chunks={chunks})"#,
            snap = format_option_to_string(self.num_snapshot_nodes),
            man = format_option_to_string(self.num_chunk_refs),
            tx = format_option_to_string(self.num_transaction_changes),
            att = format_option_to_string(self.num_bytes_attributes),
            chunks = format_option_to_string(self.num_bytes_chunks),
        )
    }
}

impl From<&PyCachingConfig> for CachingConfig {
    fn from(value: &PyCachingConfig) -> Self {
        Self {
            num_snapshot_nodes: value.num_snapshot_nodes,
            num_chunk_refs: value.num_chunk_refs,
            num_transaction_changes: value.num_transaction_changes,
            num_bytes_attributes: value.num_bytes_attributes,
            num_bytes_chunks: value.num_bytes_chunks,
        }
    }
}

impl From<CachingConfig> for PyCachingConfig {
    fn from(value: CachingConfig) -> Self {
        Self {
            num_snapshot_nodes: value.num_snapshot_nodes,
            num_chunk_refs: value.num_chunk_refs,
            num_transaction_changes: value.num_transaction_changes,
            num_bytes_attributes: value.num_bytes_attributes,
            num_bytes_chunks: value.num_bytes_chunks,
        }
    }
}

#[pyclass(name = "StorageRetriesSettings", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyStorageRetriesSettings {
    #[pyo3(get, set)]
    pub max_tries: Option<NonZeroU16>,
    #[pyo3(get, set)]
    pub initial_backoff_ms: Option<u32>,
    #[pyo3(get, set)]
    pub max_backoff_ms: Option<u32>,
}

impl From<RetriesSettings> for PyStorageRetriesSettings {
    fn from(value: RetriesSettings) -> Self {
        Self {
            max_tries: value.max_tries,
            initial_backoff_ms: value.initial_backoff_ms,
            max_backoff_ms: value.max_backoff_ms,
        }
    }
}

impl From<&PyStorageRetriesSettings> for RetriesSettings {
    fn from(value: &PyStorageRetriesSettings) -> Self {
        Self {
            max_tries: value.max_tries,
            initial_backoff_ms: value.initial_backoff_ms,
            max_backoff_ms: value.max_backoff_ms,
        }
    }
}

#[pymethods]
impl PyStorageRetriesSettings {
    #[pyo3(signature = (max_tries=None, initial_backoff_ms=None, max_backoff_ms=None))]
    #[new]
    pub fn new(
        max_tries: Option<NonZeroU16>,
        initial_backoff_ms: Option<u32>,
        max_backoff_ms: Option<u32>,
    ) -> Self {
        Self { max_tries, initial_backoff_ms, max_backoff_ms }
    }

    pub fn __repr__(&self) -> String {
        storage_retries_settings_repr(self)
    }
}

fn storage_retries_settings_repr(s: &PyStorageRetriesSettings) -> String {
    format!(
        r#"StorageRetriesSettings(max_tries={max}, initial_backoff_ms={init}, max_backoff_ms={max_back})"#,
        max = format_option_to_string(s.max_tries),
        init = format_option_to_string(s.initial_backoff_ms),
        max_back = format_option_to_string(s.max_backoff_ms),
    )
}

#[pyclass(name = "StorageConcurrencySettings", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyStorageConcurrencySettings {
    #[pyo3(get, set)]
    pub max_concurrent_requests_for_object: Option<NonZeroU16>,
    #[pyo3(get, set)]
    pub ideal_concurrent_request_size: Option<NonZeroU64>,
}

impl From<ConcurrencySettings> for PyStorageConcurrencySettings {
    fn from(value: ConcurrencySettings) -> Self {
        Self {
            max_concurrent_requests_for_object: value.max_concurrent_requests_for_object,
            ideal_concurrent_request_size: value.ideal_concurrent_request_size,
        }
    }
}

impl From<&PyStorageConcurrencySettings> for ConcurrencySettings {
    fn from(value: &PyStorageConcurrencySettings) -> Self {
        Self {
            max_concurrent_requests_for_object: value.max_concurrent_requests_for_object,
            ideal_concurrent_request_size: value.ideal_concurrent_request_size,
        }
    }
}

#[pymethods]
impl PyStorageConcurrencySettings {
    #[pyo3(signature = (max_concurrent_requests_for_object=None, ideal_concurrent_request_size=None))]
    #[new]
    pub fn new(
        max_concurrent_requests_for_object: Option<NonZeroU16>,
        ideal_concurrent_request_size: Option<NonZeroU64>,
    ) -> Self {
        Self { max_concurrent_requests_for_object, ideal_concurrent_request_size }
    }

    pub fn __repr__(&self) -> String {
        storage_concurrency_settings_repr(self)
    }
}

fn storage_concurrency_settings_repr(s: &PyStorageConcurrencySettings) -> String {
    format!(
        r#"StorageConcurrencySettings(max_concurrent_requests_for_object={max}, ideal_concurrent_request_size={ideal})"#,
        max = format_option_to_string(s.max_concurrent_requests_for_object),
        ideal = format_option_to_string(s.ideal_concurrent_request_size),
    )
}

#[pyclass(name = "StorageSettings", eq)]
#[derive(Debug)]
pub struct PyStorageSettings {
    #[pyo3(get, set)]
    pub concurrency: Option<Py<PyStorageConcurrencySettings>>,
    #[pyo3(get, set)]
    pub retries: Option<Py<PyStorageRetriesSettings>>,
    #[pyo3(get, set)]
    pub unsafe_use_conditional_update: Option<bool>,
    #[pyo3(get, set)]
    pub unsafe_use_conditional_create: Option<bool>,
    #[pyo3(get, set)]
    pub unsafe_use_metadata: Option<bool>,
    #[pyo3(get, set)]
    pub storage_class: Option<String>,
    #[pyo3(get, set)]
    pub metadata_storage_class: Option<String>,
    #[pyo3(get, set)]
    pub chunks_storage_class: Option<String>,
    #[pyo3(get, set)]
    pub minimum_size_for_multipart_upload: Option<u64>,
}

impl From<storage::Settings> for PyStorageSettings {
    fn from(value: storage::Settings) -> Self {
        Python::with_gil(|py| Self {
            #[allow(clippy::expect_used)]
            concurrency: value.concurrency.map(|c| {
                Py::new(py, Into::<PyStorageConcurrencySettings>::into(c))
                    .expect("Cannot create instance of StorageConcurrencySettings")
            }),
            #[allow(clippy::expect_used)]
            retries: value.retries.map(|c| {
                Py::new(py, Into::<PyStorageRetriesSettings>::into(c))
                    .expect("Cannot create instance of StorageRetriesSettings")
            }),

            unsafe_use_conditional_create: value.unsafe_use_conditional_create,
            unsafe_use_conditional_update: value.unsafe_use_conditional_update,
            unsafe_use_metadata: value.unsafe_use_metadata,
            storage_class: value.storage_class,
            metadata_storage_class: value.metadata_storage_class,
            chunks_storage_class: value.chunks_storage_class,
            minimum_size_for_multipart_upload: value.minimum_size_for_multipart_upload,
        })
    }
}

impl From<&PyStorageSettings> for storage::Settings {
    fn from(value: &PyStorageSettings) -> Self {
        Python::with_gil(|py| Self {
            concurrency: value.concurrency.as_ref().map(|c| (&*c.borrow(py)).into()),
            retries: value.retries.as_ref().map(|c| (&*c.borrow(py)).into()),
            unsafe_use_conditional_create: value.unsafe_use_conditional_create,
            unsafe_use_conditional_update: value.unsafe_use_conditional_update,
            unsafe_use_metadata: value.unsafe_use_metadata,
            storage_class: value.storage_class.clone(),
            metadata_storage_class: value.metadata_storage_class.clone(),
            chunks_storage_class: value.chunks_storage_class.clone(),
            minimum_size_for_multipart_upload: value.minimum_size_for_multipart_upload,
        })
    }
}

impl PartialEq for PyStorageSettings {
    fn eq(&self, other: &Self) -> bool {
        let x: storage::Settings = self.into();
        let y: storage::Settings = other.into();
        x == y
    }
}

impl Eq for PyStorageSettings {}

#[pymethods]
impl PyStorageSettings {
    #[pyo3(signature = ( concurrency=None, retries=None, unsafe_use_conditional_create=None, unsafe_use_conditional_update=None, unsafe_use_metadata=None, storage_class=None, metadata_storage_class=None, chunks_storage_class=None, minimum_size_for_multipart_upload=None))]
    #[new]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        concurrency: Option<Py<PyStorageConcurrencySettings>>,
        retries: Option<Py<PyStorageRetriesSettings>>,
        unsafe_use_conditional_create: Option<bool>,
        unsafe_use_conditional_update: Option<bool>,
        unsafe_use_metadata: Option<bool>,
        storage_class: Option<String>,
        metadata_storage_class: Option<String>,
        chunks_storage_class: Option<String>,
        minimum_size_for_multipart_upload: Option<u64>,
    ) -> Self {
        Self {
            concurrency,
            retries,
            unsafe_use_conditional_create,
            unsafe_use_metadata,
            unsafe_use_conditional_update,
            storage_class,
            metadata_storage_class,
            chunks_storage_class,
            minimum_size_for_multipart_upload,
        }
    }

    pub fn __repr__(&self) -> String {
        let inner_conc = match &self.concurrency {
            None => "None".to_string(),
            Some(conc) => Python::with_gil(|py| {
                let conc = &*conc.borrow(py);
                storage_concurrency_settings_repr(conc)
            }),
        };

        let inner_retries = match &self.retries {
            None => "None".to_string(),
            Some(retries) => Python::with_gil(|py| {
                let conc = &*retries.borrow(py);
                storage_retries_settings_repr(conc)
            }),
        };
        format!(
            r#"StorageSettings(concurrency={conc}, retries={retr}, unsafe_use_conditional_create={cr}, unsafe_use_conditional_update={up}, unsafe_use_metadata={me}, storage_class={sc}, metadata_storage_class={msc}, chunks_storage_class={csc})"#,
            conc = inner_conc,
            retr = inner_retries,
            cr = format_option(self.unsafe_use_conditional_create.map(format_bool)),
            up = format_option(self.unsafe_use_conditional_update.map(format_bool)),
            me = format_option(self.unsafe_use_metadata.map(format_bool)),
            sc = format_option(
                self.storage_class.as_ref().map(|s| format_str(s.as_str()))
            ),
            msc = format_option(
                self.metadata_storage_class.as_ref().map(|s| format_str(s.as_str()))
            ),
            csc = format_option(
                self.chunks_storage_class.as_ref().map(|s| format_str(s.as_str()))
            ),
        )
    }
}

#[pyclass(name = "ManifestPreloadCondition", eq)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PyManifestPreloadCondition {
    Or(Vec<PyManifestPreloadCondition>),
    And(Vec<PyManifestPreloadCondition>),
    PathMatches { regex: String },
    NameMatches { regex: String },
    NumRefs { from: Option<u32>, to: Option<u32> },
    True(),
    False(),
}

#[pymethods]
impl PyManifestPreloadCondition {
    #[staticmethod]
    pub fn or_conditions(conditions: Vec<PyManifestPreloadCondition>) -> Self {
        Self::Or(conditions)
    }
    #[staticmethod]
    pub fn and_conditions(conditions: Vec<PyManifestPreloadCondition>) -> Self {
        Self::And(conditions)
    }
    #[staticmethod]
    pub fn path_matches(regex: String) -> Self {
        Self::PathMatches { regex }
    }
    #[staticmethod]
    pub fn name_matches(regex: String) -> Self {
        Self::NameMatches { regex }
    }
    #[staticmethod]
    #[pyo3(signature = (from, to))]
    pub fn num_refs(from: Option<u32>, to: Option<u32>) -> Self {
        Self::NumRefs { from, to }
    }
    #[staticmethod]
    pub fn r#true() -> Self {
        Self::True()
    }
    #[staticmethod]
    pub fn r#false() -> Self {
        Self::False()
    }

    pub fn __and__(&self, other: &Self) -> Self {
        Self::And(vec![self.clone(), other.clone()])
    }
    pub fn __or__(&self, other: &Self) -> Self {
        Self::Or(vec![self.clone(), other.clone()])
    }
}

impl From<&PyManifestPreloadCondition> for ManifestPreloadCondition {
    fn from(value: &PyManifestPreloadCondition) -> Self {
        use PyManifestPreloadCondition::*;
        match value {
            Or(vec) => Self::Or(vec.iter().map(|c| c.into()).collect()),
            And(vec) => Self::And(vec.iter().map(|c| c.into()).collect()),
            PathMatches { regex } => Self::PathMatches { regex: regex.clone() },
            NameMatches { regex } => Self::NameMatches { regex: regex.clone() },
            NumRefs { from, to } => Self::NumRefs {
                from: from
                    .map(std::ops::Bound::Included)
                    .unwrap_or(std::ops::Bound::Unbounded),
                to: to
                    .map(std::ops::Bound::Excluded)
                    .unwrap_or(std::ops::Bound::Unbounded),
            },
            True() => Self::True,
            False() => Self::False,
        }
    }
}

impl From<ManifestPreloadCondition> for PyManifestPreloadCondition {
    fn from(value: ManifestPreloadCondition) -> Self {
        fn bound_from(from: std::ops::Bound<u32>) -> Option<u32> {
            match from {
                std::ops::Bound::Included(n) => Some(n),
                std::ops::Bound::Excluded(n) => Some(n + 1),
                std::ops::Bound::Unbounded => None,
            }
        }

        fn bound_to(to: std::ops::Bound<u32>) -> Option<u32> {
            match to {
                std::ops::Bound::Included(n) => Some(n + 1),
                std::ops::Bound::Excluded(n) => Some(n),
                std::ops::Bound::Unbounded => None,
            }
        }

        use ManifestPreloadCondition::*;
        match value {
            Or(vec) => Self::Or(vec.into_iter().map(|c| c.into()).collect()),
            And(vec) => Self::And(vec.into_iter().map(|c| c.into()).collect()),
            PathMatches { regex } => Self::PathMatches { regex },
            NameMatches { regex } => Self::NameMatches { regex },
            NumRefs { from, to } => {
                Self::NumRefs { from: bound_from(from), to: bound_to(to) }
            }
            True => Self::True(),
            False => Self::False(),
        }
    }
}

#[pyclass(name = "ManifestPreloadConfig", eq)]
#[derive(Debug)]
pub struct PyManifestPreloadConfig {
    #[pyo3(get, set)]
    pub max_total_refs: Option<u32>,
    #[pyo3(get, set)]
    pub preload_if: Option<Py<PyManifestPreloadCondition>>,
}

#[pymethods]
impl PyManifestPreloadConfig {
    #[new]
    #[pyo3(signature = (max_total_refs=None, preload_if=None))]
    fn new(
        max_total_refs: Option<u32>,
        preload_if: Option<Py<PyManifestPreloadCondition>>,
    ) -> Self {
        Self { max_total_refs, preload_if }
    }
}

impl PartialEq for PyManifestPreloadConfig {
    fn eq(&self, other: &Self) -> bool {
        let x: ManifestPreloadConfig = self.into();
        let y: ManifestPreloadConfig = other.into();
        x == y
    }
}

impl From<&PyManifestPreloadConfig> for ManifestPreloadConfig {
    fn from(value: &PyManifestPreloadConfig) -> Self {
        Python::with_gil(|py| Self {
            max_total_refs: value.max_total_refs,
            preload_if: value.preload_if.as_ref().map(|c| (&*c.borrow(py)).into()),
        })
    }
}

impl From<ManifestPreloadConfig> for PyManifestPreloadConfig {
    fn from(value: ManifestPreloadConfig) -> Self {
        #[allow(clippy::expect_used)]
        Python::with_gil(|py| Self {
            max_total_refs: value.max_total_refs,
            preload_if: value.preload_if.map(|c| {
                Py::new(py, Into::<PyManifestPreloadCondition>::into(c))
                    .expect("Cannot create instance of ManifestPreloadCondition")
            }),
        })
    }
}

#[pyclass(name = "ManifestSplitCondition", eq)]
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum PyManifestSplitCondition {
    Or(Vec<PyManifestSplitCondition>),
    And(Vec<PyManifestSplitCondition>),
    PathMatches { regex: String },
    NameMatches { regex: String },
    AnyArray(),
}

#[pymethods]
impl PyManifestSplitCondition {
    #[staticmethod]
    pub fn or_conditions(conditions: Vec<PyManifestSplitCondition>) -> Self {
        Self::Or(conditions)
    }
    #[staticmethod]
    pub fn and_conditions(conditions: Vec<PyManifestSplitCondition>) -> Self {
        Self::And(conditions)
    }
    #[staticmethod]
    pub fn path_matches(regex: String) -> Self {
        Self::PathMatches { regex }
    }
    #[staticmethod]
    pub fn name_matches(regex: String) -> Self {
        Self::NameMatches { regex }
    }

    pub fn __repr__(&self) -> String {
        use PyManifestSplitCondition::*;
        match self {
            Or(conditions) => {
                let mut res =
                    conditions.iter().fold("Or(".to_string(), |mut state, condition| {
                        state.push_str(&condition.__repr__());
                        state
                    });
                res.push(')');
                res
            }
            And(conditions) => {
                let mut res =
                    conditions.iter().fold("And(".to_string(), |mut state, condition| {
                        state.push_str(&condition.__repr__());
                        state
                    });
                res.push(')');
                res
            }
            PathMatches { regex } => format!("PathMatches('{regex}')"),
            NameMatches { regex } => format!("NameMatches('{regex}')"),
            AnyArray() => "AnyArray".to_string(),
        }
    }

    fn __hash__(&self) -> usize {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish() as usize
    }

    fn __or__(&self, other: &Self) -> Self {
        Self::Or(vec![self.clone(), other.clone()])
    }

    fn __and__(&self, other: &Self) -> Self {
        Self::And(vec![self.clone(), other.clone()])
    }
}

impl From<&PyManifestSplitCondition> for ManifestSplitCondition {
    fn from(value: &PyManifestSplitCondition) -> Self {
        use PyManifestSplitCondition::*;
        match value {
            Or(vec) => Self::Or(vec.iter().map(|c| c.into()).collect()),
            And(vec) => Self::And(vec.iter().map(|c| c.into()).collect()),
            PathMatches { regex } => Self::PathMatches { regex: regex.clone() },
            NameMatches { regex } => Self::NameMatches { regex: regex.clone() },
            AnyArray() => Self::AnyArray,
        }
    }
}

impl From<ManifestSplitCondition> for PyManifestSplitCondition {
    fn from(value: ManifestSplitCondition) -> Self {
        use ManifestSplitCondition::*;
        match value {
            AnyArray => Self::AnyArray(),
            Or(vec) => Self::Or(vec.into_iter().map(|c| c.into()).collect()),
            And(vec) => Self::And(vec.into_iter().map(|c| c.into()).collect()),
            PathMatches { regex } => Self::PathMatches { regex },
            NameMatches { regex } => Self::NameMatches { regex },
        }
    }
}

#[pyclass(name = "ManifestSplitDimCondition")]
#[derive(Clone, Debug, Hash)]
pub enum PyManifestSplitDimCondition {
    Axis(usize),
    DimensionName(String),
    Any(),
}

#[pymethods]
impl PyManifestSplitDimCondition {
    pub fn __repr__(&self) -> String {
        use PyManifestSplitDimCondition::*;
        match self {
            Axis(axis) => format!("Axis({axis})"),
            DimensionName(name) => format!(r#"DimensionName("{name}")"#),
            Any() => "Any".to_string(),
        }
    }

    fn __hash__(&self) -> usize {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish() as usize
    }
}

impl From<&PyManifestSplitDimCondition> for ManifestSplitDimCondition {
    fn from(value: &PyManifestSplitDimCondition) -> Self {
        use PyManifestSplitDimCondition::*;
        match value {
            Axis(axis) => ManifestSplitDimCondition::Axis(*axis),
            DimensionName(name) => ManifestSplitDimCondition::DimensionName(name.clone()),
            Any() => ManifestSplitDimCondition::Any,
        }
    }
}
impl From<ManifestSplitDimCondition> for PyManifestSplitDimCondition {
    fn from(value: ManifestSplitDimCondition) -> Self {
        use ManifestSplitDimCondition::*;
        match value {
            Axis(a) => PyManifestSplitDimCondition::Axis(a),
            DimensionName(name) => PyManifestSplitDimCondition::DimensionName(name),
            Any => PyManifestSplitDimCondition::Any(),
        }
    }
}

type DimConditions = Vec<(PyManifestSplitDimCondition, u32)>;

#[pyclass(name = "ManifestSplittingConfig", eq)]
#[derive(Debug)]
pub struct PyManifestSplittingConfig {
    #[pyo3(get, set)]
    pub split_sizes: Option<Vec<(PyManifestSplitCondition, DimConditions)>>,
}

#[pymethods]
impl PyManifestSplittingConfig {
    #[new]
    #[pyo3(signature = (split_sizes=None))]
    fn new(split_sizes: Option<Vec<(PyManifestSplitCondition, DimConditions)>>) -> Self {
        Self { split_sizes }
    }

    pub fn __repr__(&self) -> String {
        match &self.split_sizes {
            Some(split_sizes) => {
                let reprs: Vec<String> = split_sizes
                    .iter()
                    .map(|(condition, dims)| {
                        let condition_repr = format!("{condition:?}"); // Using Debug for PyManifestSplitCondition
                        let dims_repr: Vec<String> = dims
                            .iter()
                            .map(|(dim_condition, num)| {
                                format!("({dim_condition:?}, {num})")
                            })
                            .collect();
                        format!("({}, [{}])", condition_repr, dims_repr.join(", "))
                    })
                    .collect();

                format!("ManifestSplittingConfig({})", reprs.join(", "))
            }
            None => "ManifestSplittingConfig(None)".to_string(),
        }
    }
}

impl PartialEq for PyManifestSplittingConfig {
    fn eq(&self, other: &Self) -> bool {
        let x: ManifestSplittingConfig = self.into();
        let y: ManifestSplittingConfig = other.into();
        x == y
    }
}

impl From<ManifestSplittingConfig> for PyManifestSplittingConfig {
    fn from(value: ManifestSplittingConfig) -> Self {
        Self {
            split_sizes: value.split_sizes.map(|c| {
                c.into_iter()
                    .map(|(x, v)| {
                        (
                            x.into(),
                            v.into_iter()
                                .map(|split_dim| {
                                    (split_dim.condition.into(), split_dim.num_chunks)
                                })
                                .collect(),
                        )
                    })
                    .collect()
            }),
        }
    }
}

impl From<&PyManifestSplittingConfig> for ManifestSplittingConfig {
    fn from(value: &PyManifestSplittingConfig) -> Self {
        Self {
            split_sizes: value.split_sizes.as_ref().map(|c| {
                c.iter()
                    .map(|(x, v)| {
                        (
                            x.into(),
                            v.iter()
                                .map(|(cond, size)| ManifestSplitDim {
                                    condition: cond.into(),
                                    num_chunks: *size,
                                })
                                .collect(),
                        )
                    })
                    .collect()
            }),
        }
    }
}

#[pyclass(name = "ManifestConfig", eq)]
#[derive(Debug, Default)]
pub struct PyManifestConfig {
    #[pyo3(get, set)]
    pub preload: Option<Py<PyManifestPreloadConfig>>,
    #[pyo3(get, set)]
    pub splitting: Option<Py<PyManifestSplittingConfig>>,
}

#[pymethods]
impl PyManifestConfig {
    #[new]
    #[pyo3(signature = (preload=None, splitting=None))]
    fn new(
        preload: Option<Py<PyManifestPreloadConfig>>,
        splitting: Option<Py<PyManifestSplittingConfig>>,
    ) -> Self {
        Self { preload, splitting }
    }

    pub fn __repr__(&self) -> String {
        // TODO: improve repr
        format!(
            r#"ManifestConfig(preload={pre}, splitting={sha})"#,
            pre = format_option_to_string(self.preload.as_ref().map(|l| l.to_string())),
            sha = format_option_to_string(self.splitting.as_ref().map(|l| l.to_string())),
        )
    }
}

impl PartialEq for PyManifestConfig {
    fn eq(&self, other: &Self) -> bool {
        let x: ManifestConfig = self.into();
        let y: ManifestConfig = other.into();
        x == y
    }
}

impl From<&PyManifestConfig> for ManifestConfig {
    fn from(value: &PyManifestConfig) -> Self {
        Python::with_gil(|py| Self {
            preload: value.preload.as_ref().map(|c| (&*c.borrow(py)).into()),
            splitting: value.splitting.as_ref().map(|c| (&*c.borrow(py)).into()),
        })
    }
}

impl From<ManifestConfig> for PyManifestConfig {
    fn from(value: ManifestConfig) -> Self {
        #[allow(clippy::expect_used)]
        Python::with_gil(|py| Self {
            preload: value.preload.map(|c| {
                Py::new(py, Into::<PyManifestPreloadConfig>::into(c))
                    .expect("Cannot create instance of ManifestPreloadConfig")
            }),
            splitting: value.splitting.map(|c| {
                Py::new(py, Into::<PyManifestSplittingConfig>::into(c))
                    .expect("Cannot create instance of ManifestSplittingConfig")
            }),
        })
    }
}

#[pyclass(name = "RepositoryConfig", eq)]
#[derive(Debug)]
pub struct PyRepositoryConfig {
    #[pyo3(get, set)]
    pub inline_chunk_threshold_bytes: Option<u16>,
    #[pyo3(get, set)]
    pub get_partial_values_concurrency: Option<u16>,
    #[pyo3(get, set)]
    pub compression: Option<Py<PyCompressionConfig>>,
    #[pyo3(get, set)]
    pub caching: Option<Py<PyCachingConfig>>,
    #[pyo3(get, set)]
    pub storage: Option<Py<PyStorageSettings>>,
    #[pyo3(get)]
    pub virtual_chunk_containers: Option<HashMap<String, PyVirtualChunkContainer>>,
    #[pyo3(get, set)]
    pub manifest: Option<Py<PyManifestConfig>>,
}

impl PartialEq for PyRepositoryConfig {
    fn eq(&self, other: &Self) -> bool {
        let x: Result<RepositoryConfig, _> = self.try_into();
        let y: Result<RepositoryConfig, _> = other.try_into();
        x == y
    }
}

impl TryFrom<&PyRepositoryConfig> for RepositoryConfig {
    type Error = String;

    fn try_from(value: &PyRepositoryConfig) -> Result<Self, Self::Error> {
        let cont = value
            .virtual_chunk_containers
            .as_ref()
            .map(|c| {
                c.iter()
                    .map(|(name, cont)| cont.try_into().map(|cont| (name.clone(), cont)))
                    .try_collect()
            })
            .transpose()?;
        Python::with_gil(|py| {
            Ok(Self {
                inline_chunk_threshold_bytes: value.inline_chunk_threshold_bytes,
                get_partial_values_concurrency: value.get_partial_values_concurrency,
                compression: value.compression.as_ref().map(|c| (&*c.borrow(py)).into()),
                caching: value.caching.as_ref().map(|c| (&*c.borrow(py)).into()),
                storage: value.storage.as_ref().map(|s| (&*s.borrow(py)).into()),
                virtual_chunk_containers: cont,
                manifest: value.manifest.as_ref().map(|c| (&*c.borrow(py)).into()),
            })
        })
    }
}

impl From<RepositoryConfig> for PyRepositoryConfig {
    fn from(value: RepositoryConfig) -> Self {
        #[allow(clippy::expect_used)]
        Python::with_gil(|py| Self {
            inline_chunk_threshold_bytes: value.inline_chunk_threshold_bytes,
            get_partial_values_concurrency: value.get_partial_values_concurrency,
            compression: value.compression.map(|c| {
                Py::new(py, Into::<PyCompressionConfig>::into(c))
                    .expect("Cannot create instance of CompressionConfig")
            }),
            caching: value.caching.map(|c| {
                Py::new(py, Into::<PyCachingConfig>::into(c))
                    .expect("Cannot create instance of CachingConfig")
            }),
            storage: value.storage.map(|storage| {
                Py::new(py, Into::<PyStorageSettings>::into(storage))
                    .expect("Cannot create instance of StorageSettings")
            }),
            virtual_chunk_containers: value
                .virtual_chunk_containers
                .map(|c| c.into_iter().map(|(name, cont)| (name, cont.into())).collect()),

            manifest: value.manifest.map(|c| {
                Py::new(py, Into::<PyManifestConfig>::into(c))
                    .expect("Cannot create instance of ManifestConfig")
            }),
        })
    }
}

#[pymethods]
impl PyRepositoryConfig {
    #[staticmethod]
    /// Create a default `RepositoryConfig` instance
    fn default() -> Self {
        RepositoryConfig::default().into()
    }

    #[new]
    #[pyo3(signature = (inline_chunk_threshold_bytes = None, get_partial_values_concurrency = None, compression = None, caching = None, storage = None, virtual_chunk_containers = None, manifest = None))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inline_chunk_threshold_bytes: Option<u16>,
        get_partial_values_concurrency: Option<u16>,
        compression: Option<Py<PyCompressionConfig>>,
        caching: Option<Py<PyCachingConfig>>,
        storage: Option<Py<PyStorageSettings>>,
        virtual_chunk_containers: Option<HashMap<String, PyVirtualChunkContainer>>,
        manifest: Option<Py<PyManifestConfig>>,
    ) -> Self {
        Self {
            inline_chunk_threshold_bytes,
            get_partial_values_concurrency,
            compression,
            caching,
            storage,
            virtual_chunk_containers,
            manifest,
        }
    }

    pub fn set_virtual_chunk_container(
        &mut self,
        cont: PyVirtualChunkContainer,
    ) -> PyResult<()> {
        // TODO: this is a very ugly way to do it but, it avoids duplicating logic
        let this: &PyRepositoryConfig = &*self;
        let mut c: RepositoryConfig = this.try_into().map_err(PyValueError::new_err)?;
        c.set_virtual_chunk_container((&cont).try_into().map_err(PyValueError::new_err)?);
        self.virtual_chunk_containers = c
            .virtual_chunk_containers
            .map(|c| c.into_iter().map(|(s, c)| (s, c.into())).collect());
        Ok(())
    }

    pub fn clear_virtual_chunk_containers(&mut self) -> PyResult<()> {
        let this: &PyRepositoryConfig = &*self;
        let mut c: RepositoryConfig = this.try_into().map_err(PyValueError::new_err)?;
        c.clear_virtual_chunk_containers();
        self.virtual_chunk_containers = c
            .virtual_chunk_containers
            .map(|c| c.into_iter().map(|(s, c)| (s, c.into())).collect());
        Ok(())
    }

    pub fn get_virtual_chunk_container(
        &self,
        name: &str,
    ) -> PyResult<Option<PyVirtualChunkContainer>> {
        let c: RepositoryConfig = self.try_into().map_err(PyValueError::new_err)?;
        Ok(c.get_virtual_chunk_container(name).map(|c| c.clone().into()))
    }

    pub fn __repr__(&self) -> String {
        #[allow(clippy::expect_used)]
        Python::with_gil(|py| {
            let comp: String = format_option(self.compression.as_ref().map(|c| {
                c.call_method0(py, "__repr__")
                    .expect("Cannot call __repr__")
                    .extract::<String>(py)
                    .expect("Cannot call __repr__")
            }));
            let caching: String = format_option(self.caching.as_ref().map(|c| {
                c.call_method0(py, "__repr__")
                    .expect("Cannot call __repr__")
                    .extract::<String>(py)
                    .expect("Cannot call __repr__")
            }));
            let storage: String = format_option(self.storage.as_ref().map(|st| {
                st.call_method0(py, "__repr__")
                    .expect("Cannot call __repr__")
                    .extract::<String>(py)
                    .expect("Cannot call __repr__")
            }));
            let manifest: String = format_option(self.manifest.as_ref().map(|c| {
                c.call_method0(py, "__repr__")
                    .expect("Cannot call __repr__")
                    .extract::<String>(py)
                    .expect("Cannot call __repr__")
            }));
            // TODO: virtual chunk containers
            format!(
                r#"RepositoryConfig(inline_chunk_threshold_bytes={inl}, get_partial_values_concurrency={partial}, compression={comp}, caching={caching}, storage={storage}, manifest={manifest})"#,
                inl = format_option_to_string(self.inline_chunk_threshold_bytes),
                partial = format_option_to_string(self.get_partial_values_concurrency),
                comp = comp,
                caching = caching,
                storage = storage,
                manifest = manifest,
            )
        })
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
        config: &PyS3Options,
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

    #[pyo3(signature = ( config, bucket, prefix, credentials=None))]
    #[classmethod]
    pub fn new_s3_object_store(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        config: &PyS3Options,
        bucket: String,
        prefix: Option<String>,
        credentials: Option<PyS3Credentials>,
    ) -> PyResult<Self> {
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = icechunk::storage::new_s3_object_store_storage(
                    config.into(),
                    bucket,
                    prefix,
                    credentials.map(|cred| cred.into()),
                )
                .await
                .map_err(PyIcechunkStoreError::StorageError)?;

                Ok(PyStorage(storage))
            })
        })
    }

    #[pyo3(signature = ( config, bucket, prefix, use_weak_consistency, credentials=None))]
    #[classmethod]
    pub fn new_tigris(
        _cls: &Bound<'_, PyType>,
        config: &PyS3Options,
        bucket: String,
        prefix: Option<String>,
        use_weak_consistency: bool,
        credentials: Option<PyS3Credentials>,
    ) -> PyResult<Self> {
        let storage = icechunk::storage::new_tigris_storage(
            config.into(),
            bucket,
            prefix,
            credentials.map(|cred| cred.into()),
            use_weak_consistency,
        )
        .map_err(PyIcechunkStoreError::StorageError)?;

        Ok(PyStorage(storage))
    }

    #[pyo3(signature = ( config, bucket=None, prefix=None, account_id=None, credentials=None))]
    #[classmethod]
    pub fn new_r2(
        _cls: &Bound<'_, PyType>,
        config: &PyS3Options,
        bucket: Option<String>,
        prefix: Option<String>,
        account_id: Option<String>,
        credentials: Option<PyS3Credentials>,
    ) -> PyResult<Self> {
        let storage = icechunk::storage::new_r2_storage(
            config.into(),
            bucket,
            prefix,
            account_id,
            credentials.map(|cred| cred.into()),
        )
        .map_err(PyIcechunkStoreError::StorageError)?;

        Ok(PyStorage(storage))
    }

    #[classmethod]
    pub fn new_in_memory(_cls: &Bound<'_, PyType>, py: Python<'_>) -> PyResult<Self> {
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = icechunk::storage::new_in_memory_storage()
                    .await
                    .map_err(PyIcechunkStoreError::StorageError)?;

                Ok(PyStorage(storage))
            })
        })
    }

    #[classmethod]
    pub fn new_local_filesystem(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        path: PathBuf,
    ) -> PyResult<Self> {
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = icechunk::storage::new_local_filesystem_storage(&path)
                    .await
                    .map_err(PyIcechunkStoreError::StorageError)?;

                Ok(PyStorage(storage))
            })
        })
    }

    #[classmethod]
    #[pyo3(signature = (bucket, prefix, credentials=None, *, config=None))]
    pub fn new_gcs(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        bucket: String,
        prefix: Option<String>,
        credentials: Option<PyGcsCredentials>,
        config: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = icechunk::storage::new_gcs_storage(
                    bucket,
                    prefix,
                    credentials.map(|cred| cred.into()),
                    config,
                )
                .await
                .map_err(PyIcechunkStoreError::StorageError)?;

                Ok(PyStorage(storage))
            })
        })
    }

    #[classmethod]
    #[pyo3(signature = (account, container, prefix, credentials=None, *, config=None))]
    pub fn new_azure_blob(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        account: String,
        container: String,
        prefix: String,
        credentials: Option<PyAzureCredentials>,
        config: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = icechunk::storage::new_azure_blob_storage(
                    account,
                    container,
                    Some(prefix),
                    credentials.map(|cred| cred.into()),
                    config,
                )
                .await
                .map_err(PyIcechunkStoreError::StorageError)?;

                Ok(PyStorage(storage))
            })
        })
    }

    pub fn __repr__(&self) -> String {
        format!("{}", self.0)
    }

    pub fn default_settings(&self) -> PyStorageSettings {
        self.0.default_settings().into()
    }
}
