use async_trait::async_trait;
use chrono::{DateTime, Datelike as _, TimeDelta, Timelike as _, Utc};
use futures::TryStreamExt as _;
use icechunk::storage::RetriesSettings;
use itertools::Itertools as _;
use pyo3::exceptions::PyValueError;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher as _};
use std::{
    collections::HashMap,
    hash::DefaultHasher,
    num::{NonZeroU16, NonZeroU64},
    path::PathBuf,
    sync::Arc,
};

use icechunk::{
    ObjectStoreConfig, RepositoryConfig, Storage,
    config::{
        AzureCredentials, AzureCredentialsFetcher, AzureRefreshableCredential,
        AzureStaticCredentials, CachingConfig, CompressionAlgorithm, CompressionConfig,
        Credentials, GcsBearerCredential, GcsCredentials, GcsCredentialsFetcher,
        GcsStaticCredentials, ManifestConfig, ManifestPreloadCondition,
        ManifestPreloadConfig, ManifestSplitCondition, ManifestSplitDim,
        ManifestSplitDimCondition, ManifestSplittingConfig,
        ManifestVirtualChunkLocationCompressionConfig, RepoUpdateRetryConfig,
        S3Credentials, S3CredentialsFetcher, S3Options, S3StaticCredentials,
    },
    storage::{self, ConcurrencySettings},
    virtual_chunks::VirtualChunkContainer,
};
use pyo3::{
    Bound, FromPyObject, Py, PyErr, PyResult, Python, pyclass, pymethods,
    types::{PyAnyMethods as _, PyModule, PyType},
};

use crate::display::{
    PyRepr, ReprMode, dataclass_html_repr, dataclass_str, py_bool, py_option,
    py_option_bool_or_default, py_option_nested_repr, py_option_nested_repr_or_default,
    py_option_or_default, py_option_str,
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

// Non-executable: contains secrets that should not be printed in full
impl PyRepr for PyS3StaticCredentials {
    const EXECUTABLE: bool = false;
    fn cls_name() -> &'static str {
        "icechunk.credentials.S3StaticCredentials"
    }
    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            (
                "access_key_id",
                format!("{}****", &self.access_key_id[..4.min(self.access_key_id.len())]),
            ),
            ("secret_access_key", "****".to_string()),
            (
                "session_token",
                if self.session_token.is_some() {
                    "****".to_string()
                } else {
                    "None".to_string()
                },
            ),
            (
                "expires_after",
                self.expires_after
                    .as_ref()
                    .map(datetime_repr)
                    .unwrap_or_else(|| "None".to_string()),
            ),
        ]
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
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
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
    pub initial: Option<CredType>,
}

impl<CredType> PythonCredentialsFetcher<CredType> {
    fn new(pickled_function: Vec<u8>) -> Self {
        PythonCredentialsFetcher { pickled_function, initial: None }
    }

    fn new_with_initial<C>(pickled_function: Vec<u8>, current: C) -> Self
    where
        C: Into<CredType>,
    {
        PythonCredentialsFetcher { pickled_function, initial: Some(current.into()) }
    }
}

fn call_pickled<PyCred>(
    py: Python<'_>,
    pickled_function: Vec<u8>,
) -> Result<PyCred, PyErr>
where
    PyCred: for<'a, 'py> FromPyObject<'a, 'py>,
    for<'a, 'py> <PyCred as FromPyObject<'a, 'py>>::Error: Into<PyErr>,
{
    let pickle_module = PyModule::import(py, "pickle")?;
    let loads_function = pickle_module.getattr("loads")?;
    let fetcher = loads_function.call1((pickled_function,))?;
    let creds: PyCred = fetcher.call0()?.extract().map_err(Into::into)?;
    Ok(creds)
}

#[async_trait]
#[typetag::serde]
impl S3CredentialsFetcher for PythonCredentialsFetcher<S3StaticCredentials> {
    async fn get(&self) -> Result<S3StaticCredentials, String> {
        if let Some(static_creds) = self.initial.as_ref() {
            match static_creds.expires_after {
                None => {
                    return Ok(static_creds.clone());
                }
                Some(expiration)
                    if expiration
                        > Utc::now()
                            + TimeDelta::seconds(rand::random_range(120..=180)) =>
                {
                    return Ok(static_creds.clone());
                }
                _ => {}
            }
        }
        Python::attach(|py| {
            call_pickled::<PyS3StaticCredentials>(py, self.pickled_function.clone())
                .map(|c| c.into())
        })
        .map_err(|e: PyErr| e.to_string())
    }
}

#[async_trait]
#[typetag::serde]
impl AzureCredentialsFetcher for PythonCredentialsFetcher<AzureRefreshableCredential> {
    async fn get(&self) -> Result<AzureRefreshableCredential, String> {
        if let Some(creds) = self.initial.as_ref() {
            match creds.expires_after() {
                None => {
                    return Ok(creds.clone());
                }
                Some(expiration)
                    if expiration
                        > Utc::now()
                            + TimeDelta::seconds(rand::random_range(120..=180)) =>
                {
                    return Ok(creds.clone());
                }
                _ => {}
            }
        }
        Python::attach(|py| {
            call_pickled::<PyAzureRefreshableCredential>(
                py,
                self.pickled_function.clone(),
            )
            .map(|c| c.into())
        })
        .map_err(|e: PyErr| e.to_string())
    }
}

#[async_trait]
#[typetag::serde]
impl GcsCredentialsFetcher for PythonCredentialsFetcher<GcsBearerCredential> {
    async fn get(&self) -> Result<GcsBearerCredential, String> {
        if let Some(static_creds) = self.initial.as_ref() {
            match static_creds.expires_after {
                None => {
                    return Ok(static_creds.clone());
                }
                Some(expiration)
                    if expiration
                        > Utc::now()
                            + TimeDelta::seconds(rand::random_range(120..=180)) =>
                {
                    return Ok(static_creds.clone());
                }
                _ => {}
            }
        }
        Python::attach(|py| {
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
                    PythonCredentialsFetcher::new_with_initial(pickled_function, current)
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
    #[pyo3(get)]
    pub bearer: String,
    #[pyo3(get)]
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
    Anonymous(),
    FromEnv(),
    Static(PyGcsStaticCredentials),
    Refreshable { pickled_function: Vec<u8>, current: Option<PyGcsBearerCredential> },
}

impl From<PyGcsCredentials> for GcsCredentials {
    fn from(value: PyGcsCredentials) -> Self {
        match value {
            PyGcsCredentials::Anonymous() => GcsCredentials::Anonymous,
            PyGcsCredentials::FromEnv() => GcsCredentials::FromEnv,
            PyGcsCredentials::Static(creds) => GcsCredentials::Static(creds.into()),
            PyGcsCredentials::Refreshable { pickled_function, current } => {
                let fetcher = if let Some(current) = current {
                    PythonCredentialsFetcher::new_with_initial(pickled_function, current)
                } else {
                    PythonCredentialsFetcher::new(pickled_function)
                };

                GcsCredentials::Refreshable(Arc::new(fetcher))
            }
        }
    }
}

#[pyclass(name = "AzureRefreshableCredential")]
#[derive(Clone, Debug)]
pub enum PyAzureRefreshableCredential {
    AccessKey { key: String, expires_after: Option<DateTime<Utc>> },
    SasToken { token: String, expires_after: Option<DateTime<Utc>> },
    BearerToken { bearer: String, expires_after: Option<DateTime<Utc>> },
}

impl From<PyAzureRefreshableCredential> for AzureRefreshableCredential {
    fn from(value: PyAzureRefreshableCredential) -> Self {
        match value {
            PyAzureRefreshableCredential::AccessKey { key, expires_after } => {
                AzureRefreshableCredential::AccessKey { key, expires_after }
            }
            PyAzureRefreshableCredential::SasToken { token, expires_after } => {
                AzureRefreshableCredential::SASToken { token, expires_after }
            }
            PyAzureRefreshableCredential::BearerToken { bearer, expires_after } => {
                AzureRefreshableCredential::BearerToken { bearer, expires_after }
            }
        }
    }
}

impl From<AzureRefreshableCredential> for PyAzureRefreshableCredential {
    fn from(value: AzureRefreshableCredential) -> Self {
        match value {
            AzureRefreshableCredential::AccessKey { key, expires_after } => {
                PyAzureRefreshableCredential::AccessKey { key, expires_after }
            }
            AzureRefreshableCredential::SASToken { token, expires_after } => {
                PyAzureRefreshableCredential::SasToken { token, expires_after }
            }
            AzureRefreshableCredential::BearerToken { bearer, expires_after } => {
                PyAzureRefreshableCredential::BearerToken { bearer, expires_after }
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
    Refreshable {
        pickled_function: Vec<u8>,
        current: Option<PyAzureRefreshableCredential>,
    },
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
            PyAzureCredentials::Refreshable { pickled_function, current } => {
                let fetcher = if let Some(current) = current {
                    PythonCredentialsFetcher::new_with_initial(pickled_function, current)
                } else {
                    PythonCredentialsFetcher::new(pickled_function)
                };

                AzureCredentials::Refreshable(Arc::new(fetcher))
            }
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
    #[pyo3(get, set)]
    pub network_stream_timeout_seconds: Option<u32>,
    #[pyo3(get, set)]
    pub requester_pays: bool,
}

impl PyRepr for PyS3Options {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.storage.S3Options"
    }
    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("region", py_option_str(&self.region)),
            ("endpoint_url", py_option_str(&self.endpoint_url)),
            ("allow_http", py_bool(self.allow_http)),
            ("anonymous", py_bool(self.anonymous)),
            ("force_path_style", py_bool(self.force_path_style)),
            (
                "network_stream_timeout_seconds",
                py_option(&self.network_stream_timeout_seconds),
            ),
            ("requester_pays", py_bool(self.requester_pays)),
        ]
    }
}

#[pymethods]
impl PyS3Options {
    #[new]
    #[pyo3(signature = ( region=None, endpoint_url=None, allow_http=false, anonymous=false, force_path_style=false, network_stream_timeout_seconds=None, requester_pays=false))]
    pub(crate) fn new(
        region: Option<String>,
        endpoint_url: Option<String>,
        allow_http: bool,
        anonymous: bool,
        force_path_style: bool,
        network_stream_timeout_seconds: Option<u32>,
        requester_pays: bool,
    ) -> Self {
        Self {
            region,
            endpoint_url,
            allow_http,
            anonymous,
            force_path_style,
            network_stream_timeout_seconds,
            requester_pays,
        }
    }

    pub fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
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
            network_stream_timeout_seconds: options.network_stream_timeout_seconds,
            requester_pays: options.requester_pays,
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
            network_stream_timeout_seconds: value.network_stream_timeout_seconds,
            requester_pays: value.requester_pays,
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
            PyObjectStoreConfig::Azure(config) => {
                ObjectStoreConfig::Azure(config.clone().unwrap_or_default())
            }
            PyObjectStoreConfig::Tigris(opts) => ObjectStoreConfig::Tigris(opts.into()),
            PyObjectStoreConfig::Http(opts) => {
                ObjectStoreConfig::Http(opts.clone().unwrap_or_default())
            }
        }
    }
}

impl PyObjectStoreConfig {
    fn cls_name(&self) -> &'static str {
        match self {
            Self::InMemory() => "icechunk.config.ObjectStoreConfig.InMemory",
            Self::LocalFileSystem(_) => {
                "icechunk.config.ObjectStoreConfig.LocalFileSystem"
            }
            Self::S3Compatible(_) => "icechunk.config.ObjectStoreConfig.S3Compatible",
            Self::S3(_) => "icechunk.config.ObjectStoreConfig.S3",
            Self::Gcs(_) => "icechunk.config.ObjectStoreConfig.Gcs",
            Self::Azure(_) => "icechunk.config.ObjectStoreConfig.Azure",
            Self::Tigris(_) => "icechunk.config.ObjectStoreConfig.Tigris",
            Self::Http(_) => "icechunk.config.ObjectStoreConfig.Http",
        }
    }

    /// Fields for str/html modes (named key: value pairs).
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        match self {
            Self::InMemory() => vec![],
            Self::LocalFileSystem(path) => {
                vec![("path", format!("\"{}\"", path.display()))]
            }
            Self::S3Compatible(opts) | Self::S3(opts) | Self::Tigris(opts) => {
                opts.fields(mode)
            }
            Self::Gcs(opts) => vec![("config", format!("{opts:?}"))],
            Self::Azure(config) => vec![("config", format!("{config:?}"))],
            Self::Http(opts) => vec![("config", format!("{opts:?}"))],
        }
    }

    pub(crate) fn render(&self, mode: ReprMode) -> String {
        match mode {
            ReprMode::Repr => {
                // Executable repr: variants take positional args, so we
                // can't use dataclass_repr's key=value format. Instead
                // produce e.g. `icechunk.ObjectStoreConfig.S3(icechunk.S3Options(...))`
                let cls = self.cls_name();
                match self {
                    Self::InMemory() => format!("{cls}()"),
                    Self::LocalFileSystem(path) => {
                        format!("{cls}(\"{}\")", path.display())
                    }
                    Self::S3Compatible(opts) | Self::S3(opts) | Self::Tigris(opts) => {
                        format!("{cls}({})", <PyS3Options as PyRepr>::__repr__(opts))
                    }
                    Self::Gcs(opts) => format!("{cls}({opts:?})"),
                    Self::Azure(config) => format!("{cls}({config:?})"),
                    Self::Http(opts) => format!("{cls}({opts:?})"),
                }
            }
            ReprMode::Str | ReprMode::Html => {
                let fields = self.fields(mode);
                let refs: Vec<(&str, &str)> =
                    fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
                match mode {
                    ReprMode::Str => dataclass_str(self.cls_name(), &refs),
                    ReprMode::Html => dataclass_html_repr(self.cls_name(), &refs),
                    ReprMode::Repr => unreachable!(),
                }
            }
        }
    }
}

#[pymethods]
impl PyObjectStoreConfig {
    fn __repr__(&self) -> String {
        self.render(ReprMode::Repr)
    }

    fn __str__(&self) -> String {
        self.render(ReprMode::Str)
    }

    fn _repr_html_(&self) -> String {
        self.render(ReprMode::Html)
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
            ObjectStoreConfig::Azure(config) => PyObjectStoreConfig::Azure(Some(config)),
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

impl PyRepr for PyVirtualChunkContainer {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.virtual.VirtualChunkContainer"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("name", py_option_str(&self.name)),
            ("url_prefix", format!("\"{}\"", self.url_prefix)),
            ("store", self.store.render(mode)),
        ]
    }
}

#[pymethods]
impl PyVirtualChunkContainer {
    #[new]
    #[pyo3(signature = (url_prefix, store, name = None))]
    pub fn new(
        url_prefix: String,
        store: PyObjectStoreConfig,
        name: Option<String>,
    ) -> Self {
        Self { name, url_prefix, store }
    }

    pub fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl TryFrom<&PyVirtualChunkContainer> for VirtualChunkContainer {
    type Error = String;

    fn try_from(value: &PyVirtualChunkContainer) -> Result<Self, Self::Error> {
        let store = (&value.store).into();
        match value.name.clone() {
            Some(name) => {
                VirtualChunkContainer::new_named(name, value.url_prefix.clone(), store)
            }
            None => VirtualChunkContainer::new(value.url_prefix.clone(), store),
        }
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

impl PyRepr for PyCompressionConfig {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.config.CompressionConfig"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        let defaults = CompressionConfig::default();
        vec![
            (
                "algorithm",
                match (&self.algorithm, mode) {
                    (Some(a), _) => format!("{a:?}"),
                    (None, ReprMode::Repr) => "None".to_string(),
                    (None, _) => {
                        let alg: PyCompressionAlgorithm = defaults.algorithm().into();
                        format!("{alg:?} (default)")
                    }
                },
            ),
            (
                "level",
                py_option_or_default(&self.level, &defaults.level().to_string(), mode),
            ),
        ]
    }
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
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
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

impl PyRepr for PyCachingConfig {
    const EXECUTABLE: bool = true;

    fn cls_name() -> &'static str {
        "icechunk.config.CachingConfig"
    }

    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        let d = CachingConfig::default();
        vec![
            ("num_snapshot_nodes", py_option_or_default(&self.num_snapshot_nodes, &d.num_snapshot_nodes().to_string(), mode)),
            ("num_chunk_refs", py_option_or_default(&self.num_chunk_refs, &d.num_chunk_refs().to_string(), mode)),
            ("num_transaction_changes", py_option_or_default(&self.num_transaction_changes, &d.num_transaction_changes().to_string(), mode)),
            ("num_bytes_attributes", py_option_or_default(&self.num_bytes_attributes, &d.num_bytes_attributes().to_string(), mode)),
            ("num_bytes_chunks", py_option_or_default(&self.num_bytes_chunks, &d.num_bytes_chunks().to_string(), mode)),
        ]
    }
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
        <Self as PyRepr>::__repr__(self)
    }

    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
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

impl PyRepr for PyStorageRetriesSettings {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.storage.StorageRetriesSettings"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        let d = RetriesSettings::default();
        vec![
            ("max_tries", py_option_or_default(&self.max_tries, &d.max_tries().to_string(), mode)),
            ("initial_backoff_ms", py_option_or_default(&self.initial_backoff_ms, &d.initial_backoff_ms().to_string(), mode)),
            ("max_backoff_ms", py_option_or_default(&self.max_backoff_ms, &d.max_backoff_ms().to_string(), mode)),
        ]
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
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

#[pyclass(name = "StorageTimeoutSettings", eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyStorageTimeoutSettings {
    #[pyo3(get, set)]
    pub connect_timeout_ms: Option<u32>,
    #[pyo3(get, set)]
    pub read_timeout_ms: Option<u32>,
    #[pyo3(get, set)]
    pub operation_timeout_ms: Option<u32>,
    #[pyo3(get, set)]
    pub operation_attempt_timeout_ms: Option<u32>,
}

impl From<storage::TimeoutSettings> for PyStorageTimeoutSettings {
    fn from(value: storage::TimeoutSettings) -> Self {
        Self {
            connect_timeout_ms: value.connect_timeout_ms,
            read_timeout_ms: value.read_timeout_ms,
            operation_timeout_ms: value.operation_timeout_ms,
            operation_attempt_timeout_ms: value.operation_attempt_timeout_ms,
        }
    }
}

impl From<&PyStorageTimeoutSettings> for storage::TimeoutSettings {
    fn from(value: &PyStorageTimeoutSettings) -> Self {
        Self {
            connect_timeout_ms: value.connect_timeout_ms,
            read_timeout_ms: value.read_timeout_ms,
            operation_timeout_ms: value.operation_timeout_ms,
            operation_attempt_timeout_ms: value.operation_attempt_timeout_ms,
        }
    }
}

impl PyRepr for PyStorageTimeoutSettings {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.storage.StorageTimeoutSettings"
    }
    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("connect_timeout_ms", py_option(&self.connect_timeout_ms)),
            ("read_timeout_ms", py_option(&self.read_timeout_ms)),
            ("operation_timeout_ms", py_option(&self.operation_timeout_ms)),
            (
                "operation_attempt_timeout_ms",
                py_option(&self.operation_attempt_timeout_ms),
            ),
        ]
    }
}

#[pymethods]
impl PyStorageTimeoutSettings {
    #[pyo3(signature = (connect_timeout_ms=None, read_timeout_ms=None, operation_timeout_ms=None, operation_attempt_timeout_ms=None))]
    #[new]
    pub fn new(
        connect_timeout_ms: Option<u32>,
        read_timeout_ms: Option<u32>,
        operation_timeout_ms: Option<u32>,
        operation_attempt_timeout_ms: Option<u32>,
    ) -> Self {
        Self {
            connect_timeout_ms,
            read_timeout_ms,
            operation_timeout_ms,
            operation_attempt_timeout_ms,
        }
    }

    pub fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

#[pyclass(name = "RepoUpdateRetryConfig", eq)]
#[derive(Debug)]
pub struct PyRepoUpdateRetryConfig {
    #[pyo3(get, set)]
    pub default: Option<Py<PyStorageRetriesSettings>>,
}

impl PartialEq for PyRepoUpdateRetryConfig {
    fn eq(&self, other: &Self) -> bool {
        let x: RepoUpdateRetryConfig = self.into();
        let y: RepoUpdateRetryConfig = other.into();
        x == y
    }
}

impl From<RepoUpdateRetryConfig> for PyRepoUpdateRetryConfig {
    fn from(value: RepoUpdateRetryConfig) -> Self {
        #[expect(clippy::expect_used)]
        Python::attach(|py| Self {
            default: value.default.map(|r| {
                Py::new(py, Into::<PyStorageRetriesSettings>::into(r))
                    .expect("Cannot create instance of StorageRetriesSettings")
            }),
        })
    }
}

impl From<&PyRepoUpdateRetryConfig> for RepoUpdateRetryConfig {
    fn from(value: &PyRepoUpdateRetryConfig) -> Self {
        Python::attach(|py| Self {
            default: value.default.as_ref().map(|r| (&*r.borrow(py)).into()),
        })
    }
}

impl PyRepr for PyRepoUpdateRetryConfig {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.config.RepoUpdateRetryConfig"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        vec![("default", py_option_nested_repr_or_default(
            &self.default,
            mode,
            || RepoUpdateRetryConfig::default().retries().clone().into(),
        ))]
    }
}

#[pymethods]
impl PyRepoUpdateRetryConfig {
    #[pyo3(signature = (default=None))]
    #[new]
    pub fn new(default: Option<Py<PyStorageRetriesSettings>>) -> Self {
        Self { default }
    }

    pub fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
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

impl PyRepr for PyStorageConcurrencySettings {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.storage.StorageConcurrencySettings"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        let d = ConcurrencySettings::default();
        vec![
            (
                "max_concurrent_requests_for_object",
                py_option_or_default(&self.max_concurrent_requests_for_object, &d.max_concurrent_requests_for_object().to_string(), mode),
            ),
            (
                "ideal_concurrent_request_size",
                py_option_or_default(&self.ideal_concurrent_request_size, &d.ideal_concurrent_request_size().to_string(), mode),
            ),
        ]
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
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

#[pyclass(name = "StorageSettings", eq)]
#[derive(Debug)]
pub struct PyStorageSettings {
    #[pyo3(get, set)]
    pub concurrency: Option<Py<PyStorageConcurrencySettings>>,
    #[pyo3(get, set)]
    pub retries: Option<Py<PyStorageRetriesSettings>>,
    #[pyo3(get, set)]
    pub timeouts: Option<Py<PyStorageTimeoutSettings>>,
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
        Python::attach(|py| Self {
            #[expect(clippy::expect_used)]
            concurrency: value.concurrency.map(|c| {
                Py::new(py, Into::<PyStorageConcurrencySettings>::into(c))
                    .expect("Cannot create instance of StorageConcurrencySettings")
            }),
            #[expect(clippy::expect_used)]
            retries: value.retries.map(|c| {
                Py::new(py, Into::<PyStorageRetriesSettings>::into(c))
                    .expect("Cannot create instance of StorageRetriesSettings")
            }),
            #[expect(clippy::expect_used)]
            timeouts: value.timeouts.map(|c| {
                Py::new(py, Into::<PyStorageTimeoutSettings>::into(c))
                    .expect("Cannot create instance of StorageTimeoutSettings")
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
        Python::attach(|py| Self {
            concurrency: value.concurrency.as_ref().map(|c| (&*c.borrow(py)).into()),
            retries: value.retries.as_ref().map(|c| (&*c.borrow(py)).into()),
            timeouts: value.timeouts.as_ref().map(|c| (&*c.borrow(py)).into()),
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

impl PyRepr for PyStorageSettings {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.storage.StorageSettings"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        let d = storage::Settings::default();
        // Scalar fields first, then nested objects
        vec![
            (
                "unsafe_use_conditional_create",
                py_option_bool_or_default(&self.unsafe_use_conditional_create, d.unsafe_use_conditional_create(), mode),
            ),
            (
                "unsafe_use_conditional_update",
                py_option_bool_or_default(&self.unsafe_use_conditional_update, d.unsafe_use_conditional_update(), mode),
            ),
            (
                "unsafe_use_metadata",
                py_option_bool_or_default(&self.unsafe_use_metadata, d.unsafe_use_metadata(), mode),
            ),
            ("storage_class", py_option_str(&self.storage_class)),
            ("metadata_storage_class", py_option_str(&self.metadata_storage_class)),
            ("chunks_storage_class", py_option_str(&self.chunks_storage_class)),
            (
                "minimum_size_for_multipart_upload",
                py_option_or_default(&self.minimum_size_for_multipart_upload, &d.minimum_size_for_multipart_upload().to_string(), mode),
            ),
            ("concurrency", py_option_nested_repr_or_default(&self.concurrency, mode, || ConcurrencySettings::default().into())),
            ("retries", py_option_nested_repr_or_default(&self.retries, mode, || RetriesSettings::default().into())),
            ("timeouts", py_option_nested_repr(&self.timeouts, mode)),
        ]
    }
}

#[pymethods]
impl PyStorageSettings {
    #[pyo3(signature = ( concurrency=None, retries=None, unsafe_use_conditional_create=None, unsafe_use_conditional_update=None, unsafe_use_metadata=None, storage_class=None, metadata_storage_class=None, chunks_storage_class=None, minimum_size_for_multipart_upload=None, timeouts=None))]
    #[new]
    #[expect(clippy::too_many_arguments)]
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
        timeouts: Option<Py<PyStorageTimeoutSettings>>,
    ) -> Self {
        Self {
            concurrency,
            retries,
            timeouts,
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
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
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

    pub fn __repr__(&self) -> String {
        use PyManifestPreloadCondition::*;
        match self {
            Or(conditions) => {
                let inner: Vec<_> = conditions.iter().map(|c| c.__repr__()).collect();
                format!(
                    "icechunk.config.ManifestPreloadCondition.or_conditions([{}])",
                    inner.join(", ")
                )
            }
            And(conditions) => {
                let inner: Vec<_> = conditions.iter().map(|c| c.__repr__()).collect();
                format!(
                    "icechunk.config.ManifestPreloadCondition.and_conditions([{}])",
                    inner.join(", ")
                )
            }
            PathMatches { regex } => {
                format!(
                    "icechunk.config.ManifestPreloadCondition.path_matches(\"{regex}\")"
                )
            }
            NameMatches { regex } => {
                format!(
                    "icechunk.config.ManifestPreloadCondition.name_matches(\"{regex}\")"
                )
            }
            NumRefs { from, to } => {
                format!(
                    "icechunk.config.ManifestPreloadCondition.num_refs({}, {})",
                    py_option(from),
                    py_option(to),
                )
            }
            True() => "icechunk.config.ManifestPreloadCondition.true()".to_string(),
            False() => "icechunk.config.ManifestPreloadCondition.false()".to_string(),
        }
    }

    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __and__(&self, other: &Self) -> Self {
        Self::And(vec![self.clone(), other.clone()])
    }
    pub fn __or__(&self, other: &Self) -> Self {
        Self::Or(vec![self.clone(), other.clone()])
    }
}

impl PyManifestPreloadCondition {
    pub(crate) fn render(&self, mode: ReprMode) -> String {
        match mode {
            ReprMode::Str => self.__str__(),
            ReprMode::Repr | ReprMode::Html => self.__repr__(),
        }
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
    #[pyo3(get, set)]
    pub max_arrays_to_scan: Option<u32>,
}

impl PyRepr for PyManifestPreloadConfig {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.config.ManifestPreloadConfig"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        let d = ManifestPreloadConfig::default();
        let preload_if = match &self.preload_if {
            None => "None".to_string(),
            Some(py_obj) => Python::attach(|py| py_obj.borrow(py).render(mode)),
        };
        vec![
            ("max_total_refs", py_option_or_default(&self.max_total_refs, &d.max_total_refs().to_string(), mode)),
            ("preload_if", preload_if),
            ("max_arrays_to_scan", py_option_or_default(&self.max_arrays_to_scan, &d.max_arrays_to_scan().to_string(), mode)),
        ]
    }
}

#[pymethods]
impl PyManifestPreloadConfig {
    #[new]
    #[pyo3(signature = (max_total_refs=None, preload_if=None, max_arrays_to_scan=None))]
    fn new(
        max_total_refs: Option<u32>,
        preload_if: Option<Py<PyManifestPreloadCondition>>,
        max_arrays_to_scan: Option<u32>,
    ) -> Self {
        Self { max_total_refs, preload_if, max_arrays_to_scan }
    }

    pub fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
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
        Python::attach(|py| Self {
            max_total_refs: value.max_total_refs,
            preload_if: value.preload_if.as_ref().map(|c| (&*c.borrow(py)).into()),
            max_arrays_to_scan: value.max_arrays_to_scan,
        })
    }
}

impl From<ManifestPreloadConfig> for PyManifestPreloadConfig {
    fn from(value: ManifestPreloadConfig) -> Self {
        #[expect(clippy::expect_used)]
        Python::attach(|py| Self {
            max_total_refs: value.max_total_refs,
            preload_if: value.preload_if.map(|c| {
                Py::new(py, Into::<PyManifestPreloadCondition>::into(c))
                    .expect("Cannot create instance of ManifestPreloadCondition")
            }),
            max_arrays_to_scan: value.max_arrays_to_scan,
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
                let inner: Vec<_> = conditions.iter().map(|c| c.__repr__()).collect();
                format!(
                    "icechunk.config.ManifestSplitCondition.or_conditions([{}])",
                    inner.join(", ")
                )
            }
            And(conditions) => {
                let inner: Vec<_> = conditions.iter().map(|c| c.__repr__()).collect();
                format!(
                    "icechunk.config.ManifestSplitCondition.and_conditions([{}])",
                    inner.join(", ")
                )
            }
            PathMatches { regex } => {
                format!(
                    "icechunk.config.ManifestSplitCondition.path_matches(\"{regex}\")"
                )
            }
            NameMatches { regex } => {
                format!(
                    "icechunk.config.ManifestSplitCondition.name_matches(\"{regex}\")"
                )
            }
            AnyArray() => {
                "icechunk.config.ManifestSplitCondition.any_array()".to_string()
            }
        }
    }

    pub fn __str__(&self) -> String {
        self.__repr__()
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

impl PyManifestSplitCondition {
    pub(crate) fn render(&self, mode: ReprMode) -> String {
        match mode {
            ReprMode::Str => self.__str__(),
            ReprMode::Repr | ReprMode::Html => self.__repr__(),
        }
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
            Axis(axis) => {
                format!("icechunk.config.ManifestSplitDimCondition.axis({axis})")
            }
            DimensionName(name) => {
                format!(
                    "icechunk.config.ManifestSplitDimCondition.dimension_name(\"{name}\")"
                )
            }
            Any() => "icechunk.config.ManifestSplitDimCondition.any()".to_string(),
        }
    }

    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __hash__(&self) -> usize {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish() as usize
    }
}

impl PyManifestSplitDimCondition {
    pub(crate) fn render(&self, mode: ReprMode) -> String {
        match mode {
            ReprMode::Str => self.__str__(),
            ReprMode::Repr | ReprMode::Html => self.__repr__(),
        }
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

impl PyRepr for PyManifestSplittingConfig {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.config.ManifestSplittingConfig"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        let split_sizes = match &self.split_sizes {
            None => "None".to_string(),
            Some(sizes) => {
                let reprs: Vec<String> = sizes
                    .iter()
                    .map(|(condition, dims)| {
                        let dims_repr: Vec<String> = dims
                            .iter()
                            .map(|(dim_condition, num)| {
                                format!("({}, {num})", dim_condition.render(mode))
                            })
                            .collect();
                        format!(
                            "({}, [{}])",
                            condition.render(mode),
                            dims_repr.join(", ")
                        )
                    })
                    .collect();
                format!("[{}]", reprs.join(", "))
            }
        };
        vec![("split_sizes", split_sizes)]
    }
}

#[pymethods]
impl PyManifestSplittingConfig {
    #[new]
    #[pyo3(signature = (split_sizes=None))]
    fn new(split_sizes: Option<Vec<(PyManifestSplitCondition, DimConditions)>>) -> Self {
        Self { split_sizes }
    }

    pub fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
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

#[pyclass(name = "ManifestVirtualChunkLocationCompressionConfig", eq)]
#[derive(Debug, Default)]
pub struct PyManifestVirtualChunkLocationCompressionConfig {
    #[pyo3(get, set)]
    pub min_num_chunks: Option<u16>,
    #[pyo3(get, set)]
    pub dictionary_max_training_samples: Option<u16>,
    #[pyo3(get, set)]
    pub dictionary_max_size_bytes: Option<u32>,
    #[pyo3(get, set)]
    pub compression_level: Option<i32>,
}

impl PyRepr for PyManifestVirtualChunkLocationCompressionConfig {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.config.ManifestVirtualChunkLocationCompressionConfig"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        let d = ManifestVirtualChunkLocationCompressionConfig::default();
        vec![
            ("min_num_chunks", py_option_or_default(&self.min_num_chunks, &d.min_num_chunks().to_string(), mode)),
            (
                "dictionary_max_training_samples",
                py_option_or_default(&self.dictionary_max_training_samples, &d.dictionary_max_training_samples().to_string(), mode),
            ),
            ("dictionary_max_size_bytes", py_option_or_default(&self.dictionary_max_size_bytes, &d.dictionary_max_size_bytes().to_string(), mode)),
            ("compression_level", py_option_or_default(&self.compression_level, &d.compression_level().to_string(), mode)),
        ]
    }
}

#[pymethods]
impl PyManifestVirtualChunkLocationCompressionConfig {
    #[new]
    #[pyo3(signature = (min_num_chunks=None, *, dictionary_max_training_samples=None, dictionary_max_size_bytes=None, compression_level=None))]
    fn new(
        min_num_chunks: Option<u16>,
        dictionary_max_training_samples: Option<u16>,
        dictionary_max_size_bytes: Option<u32>,
        compression_level: Option<i32>,
    ) -> Self {
        Self {
            min_num_chunks,
            dictionary_max_training_samples,
            dictionary_max_size_bytes,
            compression_level,
        }
    }

    pub fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl PartialEq for PyManifestVirtualChunkLocationCompressionConfig {
    fn eq(&self, other: &Self) -> bool {
        let x: ManifestVirtualChunkLocationCompressionConfig = self.into();
        let y: ManifestVirtualChunkLocationCompressionConfig = other.into();
        x == y
    }
}

impl From<&PyManifestVirtualChunkLocationCompressionConfig>
    for ManifestVirtualChunkLocationCompressionConfig
{
    fn from(value: &PyManifestVirtualChunkLocationCompressionConfig) -> Self {
        Self {
            min_num_chunks: value.min_num_chunks,
            dictionary_max_training_samples: value.dictionary_max_training_samples,
            dictionary_max_size_bytes: value.dictionary_max_size_bytes,
            compression_level: value.compression_level,
        }
    }
}

impl From<ManifestVirtualChunkLocationCompressionConfig>
    for PyManifestVirtualChunkLocationCompressionConfig
{
    fn from(value: ManifestVirtualChunkLocationCompressionConfig) -> Self {
        Self {
            min_num_chunks: value.min_num_chunks,
            dictionary_max_training_samples: value.dictionary_max_training_samples,
            dictionary_max_size_bytes: value.dictionary_max_size_bytes,
            compression_level: value.compression_level,
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
    #[pyo3(get, set)]
    pub virtual_chunk_location_compression:
        Option<Py<PyManifestVirtualChunkLocationCompressionConfig>>,
}

impl PyRepr for PyManifestConfig {
    const EXECUTABLE: bool = true;
    fn cls_name() -> &'static str {
        "icechunk.config.ManifestConfig"
    }
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("preload", py_option_nested_repr_or_default(&self.preload, mode, || ManifestPreloadConfig::default().into())),
            ("splitting", py_option_nested_repr_or_default(&self.splitting, mode, || ManifestSplittingConfig::default().into())),
            (
                "virtual_chunk_location_compression",
                py_option_nested_repr_or_default(&self.virtual_chunk_location_compression, mode, || ManifestVirtualChunkLocationCompressionConfig::default().into()),
            ),
        ]
    }
}

#[pymethods]
impl PyManifestConfig {
    #[new]
    #[pyo3(signature = (preload=None, splitting=None, virtual_chunk_location_compression=None))]
    fn new(
        preload: Option<Py<PyManifestPreloadConfig>>,
        splitting: Option<Py<PyManifestSplittingConfig>>,
        virtual_chunk_location_compression: Option<
            Py<PyManifestVirtualChunkLocationCompressionConfig>,
        >,
    ) -> Self {
        Self { preload, splitting, virtual_chunk_location_compression }
    }

    pub fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
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
        Python::attach(|py| Self {
            preload: value.preload.as_ref().map(|c| (&*c.borrow(py)).into()),
            splitting: value.splitting.as_ref().map(|c| (&*c.borrow(py)).into()),
            virtual_chunk_location_compression: value
                .virtual_chunk_location_compression
                .as_ref()
                .map(|c| (&*c.borrow(py)).into()),
        })
    }
}

impl From<ManifestConfig> for PyManifestConfig {
    fn from(value: ManifestConfig) -> Self {
        #[expect(clippy::expect_used)]
        Python::attach(|py| {
            Self {
            preload: value.preload.map(|c| {
                Py::new(py, Into::<PyManifestPreloadConfig>::into(c))
                    .expect("Cannot create instance of ManifestPreloadConfig")
            }),
            splitting: value.splitting.map(|c| {
                Py::new(py, Into::<PyManifestSplittingConfig>::into(c))
                    .expect("Cannot create instance of ManifestSplittingConfig")
            }),
            virtual_chunk_location_compression: value
                .virtual_chunk_location_compression
                .map(|c| {
                    Py::new(
                        py,
                        Into::<PyManifestVirtualChunkLocationCompressionConfig>::into(c),
                    )
                    .expect(
                        "Cannot create instance of ManifestVirtualChunkLocationCompressionConfig",
                    )
                }),
        }
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
    pub max_concurrent_requests: Option<u16>,
    #[pyo3(get, set)]
    pub caching: Option<Py<PyCachingConfig>>,
    #[pyo3(get, set)]
    pub storage: Option<Py<PyStorageSettings>>,
    #[pyo3(get)]
    pub virtual_chunk_containers: Option<HashMap<String, PyVirtualChunkContainer>>,
    #[pyo3(get, set)]
    pub manifest: Option<Py<PyManifestConfig>>,
    #[pyo3(get)]
    pub previous_file: Option<String>,
    #[pyo3(get, set)]
    pub repo_update_retries: Option<Py<PyRepoUpdateRetryConfig>>,
    #[pyo3(get, set)]
    pub num_updates_per_repo_info_file: Option<u16>,
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
        Python::attach(|py| {
            Ok(Self {
                inline_chunk_threshold_bytes: value.inline_chunk_threshold_bytes,
                get_partial_values_concurrency: value.get_partial_values_concurrency,
                compression: value.compression.as_ref().map(|c| (&*c.borrow(py)).into()),
                max_concurrent_requests: value.max_concurrent_requests,
                caching: value.caching.as_ref().map(|c| (&*c.borrow(py)).into()),
                storage: value.storage.as_ref().map(|s| (&*s.borrow(py)).into()),
                virtual_chunk_containers: cont,
                manifest: value.manifest.as_ref().map(|c| (&*c.borrow(py)).into()),
                previous_file: value.previous_file.clone(),
                repo_update_retries: value
                    .repo_update_retries
                    .as_ref()
                    .map(|r| (&*r.borrow(py)).into()),
                num_updates_per_repo_info_file: value.num_updates_per_repo_info_file,
            })
        })
    }
}

impl From<RepositoryConfig> for PyRepositoryConfig {
    fn from(value: RepositoryConfig) -> Self {
        #[expect(clippy::expect_used)]
        Python::attach(|py| Self {
            inline_chunk_threshold_bytes: value.inline_chunk_threshold_bytes,
            get_partial_values_concurrency: value.get_partial_values_concurrency,
            compression: value.compression.map(|c| {
                Py::new(py, Into::<PyCompressionConfig>::into(c))
                    .expect("Cannot create instance of CompressionConfig")
            }),
            max_concurrent_requests: value.max_concurrent_requests,
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
            previous_file: value.previous_file,
            repo_update_retries: value.repo_update_retries.map(|r| {
                Py::new(py, Into::<PyRepoUpdateRetryConfig>::into(r))
                    .expect("Cannot create instance of RepoUpdateRetryConfig")
            }),
            num_updates_per_repo_info_file: value.num_updates_per_repo_info_file,
        })
    }
}

impl PyRepr for PyRepositoryConfig {
    const EXECUTABLE: bool = true;

    fn cls_name() -> &'static str {
        "icechunk.config.RepositoryConfig"
    }

    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        // Scalar fields first, then nested objects, so simple values
        // aren't broken up by large nested blocks in str/html output.
        let vccs = match &self.virtual_chunk_containers {
            None => "None".to_string(),
            Some(map) if map.is_empty() => "{}".to_string(),
            Some(map) => {
                use std::fmt::Write as _;
                let mut out = String::new();
                match mode {
                    ReprMode::Html => {
                        let _ = write!(out, "<div class=\"icechunk-repr\">");
                        for (prefix, vcc) in map {
                            let _ = write!(
                                out,
                                "<details><summary class=\"icechunk-field-name\">{prefix}</summary>{}</details>",
                                vcc.render(ReprMode::Html),
                            );
                        }
                        let _ = write!(out, "</div>");
                    }
                    ReprMode::Repr => {
                        // Black/ruff-style dict formatting:
                        // {
                        //     "key": VirtualChunkContainer(
                        //         ...
                        //     ),
                        // }
                        let _ = writeln!(out, "{{");
                        for (prefix, vcc) in map {
                            let rendered = vcc.render(ReprMode::Repr);
                            let mut lines = rendered.lines();
                            if let Some(first) = lines.next() {
                                let _ = write!(out, "    \"{prefix}\": {first}");
                            }
                            for line in lines {
                                let _ = write!(out, "\n    {line}");
                            }
                            let _ = writeln!(out, ",");
                        }
                        let _ = write!(out, "}}");
                    }
                    ReprMode::Str => {
                        // Readable dict:
                        // s3://my-data/:
                        //     <icechunk.VirtualChunkContainer>
                        //     url_prefix: ...
                        for (prefix, vcc) in map {
                            let _ = writeln!(out, "{prefix}:");
                            for line in vcc.render(ReprMode::Str).lines() {
                                let _ = writeln!(out, "    {line}");
                            }
                        }
                    }
                }
                out
            }
        };
        let d = RepositoryConfig::default();
        vec![
            (
                "inline_chunk_threshold_bytes",
                py_option_or_default(&self.inline_chunk_threshold_bytes, &d.inline_chunk_threshold_bytes().to_string(), mode),
            ),
            (
                "get_partial_values_concurrency",
                py_option_or_default(&self.get_partial_values_concurrency, &d.get_partial_values_concurrency().to_string(), mode),
            ),
            ("max_concurrent_requests", py_option_or_default(&self.max_concurrent_requests, &d.max_concurrent_requests().to_string(), mode)),
            (
                "num_updates_per_repo_info_file",
                py_option_or_default(&self.num_updates_per_repo_info_file, &d.num_updates_per_repo_info_file().to_string(), mode),
            ),
            ("compression", py_option_nested_repr_or_default(&self.compression, mode, || CompressionConfig::default().into())),
            ("caching", py_option_nested_repr_or_default(&self.caching, mode, || CachingConfig::default().into())),
            ("storage", py_option_nested_repr(&self.storage, mode)),
            ("manifest", py_option_nested_repr_or_default(&self.manifest, mode, || ManifestConfig::default().into())),
            (
                "repo_update_retries",
                py_option_nested_repr_or_default(&self.repo_update_retries, mode, || RepoUpdateRetryConfig::default().into()),
            ),
            ("virtual_chunk_containers", vccs),
        ]
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
    #[pyo3(signature = (inline_chunk_threshold_bytes = None, get_partial_values_concurrency = None, compression = None, max_concurrent_requests = None, caching = None, storage = None, virtual_chunk_containers = None, manifest = None, repo_update_retries = None, num_updates_per_repo_info_file = None))]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        inline_chunk_threshold_bytes: Option<u16>,
        get_partial_values_concurrency: Option<u16>,
        compression: Option<Py<PyCompressionConfig>>,
        max_concurrent_requests: Option<u16>,
        caching: Option<Py<PyCachingConfig>>,
        storage: Option<Py<PyStorageSettings>>,
        virtual_chunk_containers: Option<HashMap<String, PyVirtualChunkContainer>>,
        manifest: Option<Py<PyManifestConfig>>,
        repo_update_retries: Option<Py<PyRepoUpdateRetryConfig>>,
        num_updates_per_repo_info_file: Option<u16>,
    ) -> Self {
        Self {
            inline_chunk_threshold_bytes,
            get_partial_values_concurrency,
            compression,
            max_concurrent_requests,
            caching,
            storage,
            virtual_chunk_containers,
            manifest,
            previous_file: None,
            repo_update_retries,
            num_updates_per_repo_info_file,
        }
    }

    pub fn set_virtual_chunk_container(
        &mut self,
        cont: PyVirtualChunkContainer,
    ) -> PyResult<()> {
        // TODO: this is a very ugly way to do it but, it avoids duplicating logic
        let this: &PyRepositoryConfig = &*self;
        let mut c: RepositoryConfig = this.try_into().map_err(PyValueError::new_err)?;
        c.set_virtual_chunk_container((&cont).try_into().map_err(PyValueError::new_err)?)
            .map_err(PyValueError::new_err)?;
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

    pub fn merge(&self, other: &PyRepositoryConfig) -> PyResult<PyRepositoryConfig> {
        let this: RepositoryConfig = self.try_into().map_err(PyValueError::new_err)?;
        let other: RepositoryConfig = other.try_into().map_err(PyValueError::new_err)?;
        Ok(this.merge(other).into())
    }

    pub fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    pub fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    pub fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

/// Metadata for an object in storage.
#[pyclass(name = "StorageObjectInfo", frozen, eq)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PyStorageObjectInfo {
    #[pyo3(get)]
    pub key: String,
    #[pyo3(get)]
    pub size_bytes: u64,
    #[pyo3(get)]
    pub created_at: DateTime<Utc>,
}

impl PyRepr for PyStorageObjectInfo {
    const EXECUTABLE: bool = false;
    fn cls_name() -> &'static str {
        "icechunk.storage.StorageObjectInfo"
    }
    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("key", self.key.clone()),
            ("size_bytes", self.size_bytes.to_string()),
            ("created_at", datetime_repr(&self.created_at)),
        ]
    }
}

#[pymethods]
impl PyStorageObjectInfo {
    fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }
    fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }
    fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

#[pyclass(name = "Storage", subclass)]
#[derive(Clone, Debug)]
pub(crate) struct PyStorage(pub Arc<dyn Storage + Send + Sync>);

impl PyRepr for PyStorage {
    const EXECUTABLE: bool = false;

    fn cls_name() -> &'static str {
        "icechunk.Storage"
    }

    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        let info = self.0.storage_info();
        let mut result = vec![("type", info.backend_type.to_string())];
        result.extend(info.fields);
        result
    }
}

/// Storage wrapper that adds artificial read/write latency for testing.
///
/// Wraps any Storage backend and injects configurable delays before write
/// and read operations. Useful for reproducing timing-sensitive bugs
/// or for benchmarking.
///
/// Parameters
/// ----------
/// inner : Storage
///     The storage backend to wrap.
/// `write_latency_ms` : int, default 0
///     Delay in milliseconds before each write operation.
/// `read_latency_ms` : int, default 0
///     Delay in milliseconds before each read operation.
///
/// Examples
/// --------
/// >>> from icechunk.testing import LatencyStorage
/// >>> storage = LatencyStorage(ic.in_memory_storage(), write_latency_ms=15)
/// >>> repo = ic.Repository.create(storage=storage, ...)
/// >>> storage.write_latency_ms = 50  # adjust at runtime
#[pyclass(name = "LatencyStorage", extends = PyStorage)]
#[derive(Clone, Debug)]
pub(crate) struct PyLatencyStorage {
    latency: Arc<storage::latency::LatencyStorage>,
}

#[pymethods]
impl PyLatencyStorage {
    #[new]
    #[pyo3(signature = (inner, *, write_latency_ms = 0, read_latency_ms = 0))]
    fn new(
        inner: PyStorage,
        write_latency_ms: u64,
        read_latency_ms: u64,
    ) -> (Self, PyStorage) {
        let latency = Arc::new(storage::latency::LatencyStorage::new(
            inner.0,
            write_latency_ms,
            read_latency_ms,
        ));
        let base = PyStorage(Arc::clone(&latency) as Arc<dyn Storage + Send + Sync>);
        (Self { latency }, base)
    }

    #[getter]
    fn write_latency_ms(&self) -> u64 {
        self.latency.write_delay_ms()
    }

    #[setter]
    fn set_write_latency_ms(&self, ms: u64) {
        self.latency.set_write_delay_ms(ms);
    }

    #[getter]
    fn read_latency_ms(&self) -> u64 {
        self.latency.read_delay_ms()
    }

    #[setter]
    fn set_read_latency_ms(&self, ms: u64) {
        self.latency.set_read_delay_ms(ms);
    }

    fn __repr__(&self) -> String {
        format!(
            "LatencyStorage(write_latency_ms={}, read_latency_ms={})",
            self.latency.write_delay_ms(),
            self.latency.read_delay_ms(),
        )
    }
}

#[pymethods]
impl PyStorage {
    #[pyo3(signature = ( config, bucket, prefix, credentials=None))]
    #[classmethod]
    pub(crate) fn new_s3(
        _cls: &Bound<'_, PyType>,
        config: &PyS3Options,
        bucket: String,
        prefix: Option<String>,
        credentials: Option<PyS3Credentials>,
    ) -> PyResult<Self> {
        let storage = storage::new_s3_storage(
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
    pub(crate) fn new_s3_object_store(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        config: &PyS3Options,
        bucket: String,
        prefix: Option<String>,
        credentials: Option<PyS3Credentials>,
    ) -> PyResult<Self> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = storage::new_s3_object_store_storage(
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
    pub(crate) fn new_tigris(
        _cls: &Bound<'_, PyType>,
        config: &PyS3Options,
        bucket: String,
        prefix: Option<String>,
        use_weak_consistency: bool,
        credentials: Option<PyS3Credentials>,
    ) -> PyResult<Self> {
        let storage = storage::new_tigris_storage(
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
    pub(crate) fn new_r2(
        _cls: &Bound<'_, PyType>,
        config: &PyS3Options,
        bucket: Option<String>,
        prefix: Option<String>,
        account_id: Option<String>,
        credentials: Option<PyS3Credentials>,
    ) -> PyResult<Self> {
        let storage = storage::new_r2_storage(
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
    pub(crate) fn new_in_memory(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
    ) -> PyResult<Self> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = storage::new_in_memory_storage()
                    .await
                    .map_err(PyIcechunkStoreError::StorageError)?;

                Ok(PyStorage(storage))
            })
        })
    }

    #[classmethod]
    pub(crate) fn new_local_filesystem(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        path: PathBuf,
    ) -> PyResult<Self> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = storage::new_local_filesystem_storage(&path)
                    .await
                    .map_err(PyIcechunkStoreError::StorageError)?;

                Ok(PyStorage(storage))
            })
        })
    }

    #[classmethod]
    #[pyo3(signature = (bucket, prefix, credentials=None, *, config=None))]
    pub(crate) fn new_gcs(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        bucket: String,
        prefix: Option<String>,
        credentials: Option<PyGcsCredentials>,
        config: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        py.detach(move || {
            let storage = storage::new_gcs_storage(
                bucket,
                prefix,
                credentials.map(|cred| cred.into()),
                config,
            )
            .map_err(PyIcechunkStoreError::StorageError)?;

            Ok(PyStorage(storage))
        })
    }

    #[classmethod]
    #[pyo3(signature = (account, container, prefix, credentials=None, *, config=None))]
    pub(crate) fn new_azure_blob(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        account: String,
        container: String,
        prefix: String,
        credentials: Option<PyAzureCredentials>,
        config: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = storage::new_azure_blob_storage(
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

    #[classmethod]
    #[pyo3(signature = (base_url, config=None))]
    pub(crate) fn new_http(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        base_url: &str,
        config: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = storage::new_http_storage(base_url, config)
                    .map_err(PyIcechunkStoreError::StorageError)?;

                Ok(PyStorage(storage))
            })
        })
    }

    #[classmethod]
    pub(crate) fn new_redirect(
        _cls: &Bound<'_, PyType>,
        py: Python<'_>,
        base_url: &str,
    ) -> PyResult<Self> {
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let storage = storage::new_redirect_storage(base_url)
                    .map_err(PyIcechunkStoreError::StorageError)?;

                Ok(PyStorage(storage))
            })
        })
    }

    pub(crate) fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    pub(crate) fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    pub(crate) fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }

    pub(crate) fn default_settings(&self) -> PyResult<PyStorageSettings> {
        let res = pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            self.0
                .default_settings()
                .await
                .map_err(|e| PyValueError::new_err(e.to_string()))
        })?;
        Ok(res.into())
    }

    /// List objects in the storage backend, optionally filtered by a key prefix.
    ///
    /// Returns a list of ``(key, size_in_bytes)`` tuples for each object found.
    /// When ``prefix`` is ``None`` or empty, all objects under the repository root are listed.
    /// Custom ``settings`` can be provided to override the storage's default settings.
    ///
    /// Deprecated: use ``list_objects_metadata`` instead, which also returns
    /// the ``created_at`` timestamp.
    #[pyo3(signature = (settings=None, prefix=None))]
    pub(crate) fn list_objects(
        &self,
        settings: Option<&PyStorageSettings>,
        prefix: Option<String>,
    ) -> PyResult<Vec<(String, u64)>> {
        let prefix = prefix.unwrap_or_default();
        let settings: Option<storage::Settings> = settings.map(|s| s.into());
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let defaults = self
                .0
                .default_settings()
                .await
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            let settings = match settings {
                Some(s) => s.merge(defaults),
                None => defaults,
            };
            let stream = self
                .0
                .list_objects(&settings, &prefix)
                .await
                .map_err(PyIcechunkStoreError::StorageError)?;
            let results: Vec<(String, u64)> = stream
                .map_ok(|info| (info.id, info.size_bytes))
                .try_collect()
                .await
                .map_err(PyIcechunkStoreError::StorageError)?;
            Ok(results)
        })
    }

    /// List objects with full metadata, optionally filtered by a key prefix.
    ///
    /// When ``prefix`` is ``None`` or empty, all objects under the repository root are listed.
    /// Custom ``settings`` can be provided to override the storage's default settings.
    ///
    /// Returns
    /// -------
    /// list[StorageObjectInfo]
    ///     A list of :class:`StorageObjectInfo` objects.
    #[pyo3(signature = (settings=None, prefix=None))]
    pub(crate) fn list_objects_metadata(
        &self,
        settings: Option<&PyStorageSettings>,
        prefix: Option<String>,
    ) -> PyResult<Vec<PyStorageObjectInfo>> {
        let prefix = prefix.unwrap_or_default();
        let settings: Option<storage::Settings> = settings.map(|s| s.into());
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let defaults = self
                .0
                .default_settings()
                .await
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            let settings = match settings {
                Some(s) => s.merge(defaults),
                None => defaults,
            };
            let stream = self
                .0
                .list_objects(&settings, &prefix)
                .await
                .map_err(PyIcechunkStoreError::StorageError)?;
            let results: Vec<PyStorageObjectInfo> = stream
                .map_ok(|info| PyStorageObjectInfo {
                    key: info.id,
                    size_bytes: info.size_bytes,
                    created_at: info.created_at,
                })
                .try_collect()
                .await
                .map_err(PyIcechunkStoreError::StorageError)?;
            Ok(results)
        })
    }
}
