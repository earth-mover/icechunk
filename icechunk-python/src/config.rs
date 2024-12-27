use std::{collections::HashMap, path::PathBuf, sync::Arc};

use icechunk::{
    config::{Credentials, S3Credentials, S3Options, S3StaticCredentials},
    virtual_chunks::VirtualChunkContainer,
    ObjectStoreConfig, RepositoryConfig, Storage,
};
use pyo3::{pyclass, pymethods, PyResult};

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
}

impl From<&PyS3StaticCredentials> for S3StaticCredentials {
    fn from(credentials: &PyS3StaticCredentials) -> Self {
        S3StaticCredentials {
            access_key_id: credentials.access_key_id.clone(),
            secret_access_key: credentials.secret_access_key.clone(),
            session_token: credentials.session_token.clone(),
        }
    }
}

impl From<PyS3StaticCredentials> for S3StaticCredentials {
    fn from(credentials: PyS3StaticCredentials) -> Self {
        S3StaticCredentials {
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.secret_access_key,
            session_token: credentials.session_token,
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
    ))]
    fn new(
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    ) -> Self {
        Self { access_key_id, secret_access_key, session_token }
    }
}

#[pyclass(name = "S3Credentials")]
#[derive(Clone, Debug)]
pub enum PyS3Credentials {
    FromEnv(),
    DontSign(),
    Static(PyS3StaticCredentials),
}

impl From<PyS3Credentials> for S3Credentials {
    fn from(credentials: PyS3Credentials) -> Self {
        match credentials {
            PyS3Credentials::FromEnv() => S3Credentials::FromEnv,
            PyS3Credentials::DontSign() => S3Credentials::DontSign,
            PyS3Credentials::Static(creds) => S3Credentials::Static(creds.into()),
        }
    }
}

#[pyclass(name = "Credentials")]
#[derive(Clone, Debug)]
pub enum PyCredentials {
    S3(PyS3Credentials),
}

impl From<PyCredentials> for Credentials {
    fn from(value: PyCredentials) -> Self {
        match value {
            PyCredentials::S3(cred) => Credentials::S3(cred.into()),
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
    Gcs(),
    Azure(),
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
            PyObjectStoreConfig::Gcs() => ObjectStoreConfig::Gcs {},
            PyObjectStoreConfig::Azure() => ObjectStoreConfig::Azure {},
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
            ObjectStoreConfig::Gcs {} => PyObjectStoreConfig::Gcs(),
            ObjectStoreConfig::Azure {} => PyObjectStoreConfig::Azure(),
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

#[pyclass(name = "RepositoryConfig")]
#[derive(Clone, Debug)]
pub struct PyRepositoryConfig {
    #[pyo3(get, set)]
    pub inline_chunk_threshold_bytes: u16,
    #[pyo3(get, set)]
    pub unsafe_overwrite_refs: bool,
    #[pyo3(get, set)]
    pub get_partial_values_concurrency: u16,
    #[pyo3(get, set)]
    pub virtual_chunk_containers: HashMap<String, PyVirtualChunkContainer>,
}

impl From<PyRepositoryConfig> for RepositoryConfig {
    fn from(value: PyRepositoryConfig) -> Self {
        Self {
            inline_chunk_threshold_bytes: value.inline_chunk_threshold_bytes,
            unsafe_overwrite_refs: value.unsafe_overwrite_refs,
            get_partial_values_concurrency: value.get_partial_values_concurrency,
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
    fn default() -> Self {
        RepositoryConfig::default().into()
    }

    pub fn set_virtual_chunk_container(&mut self, cont: PyVirtualChunkContainer) {
        self.virtual_chunk_containers.insert(cont.name.clone(), cont);
    }

    pub fn virtual_chunk_containers(&self) -> Vec<PyVirtualChunkContainer> {
        self.virtual_chunk_containers.values().cloned().collect()
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
    #[staticmethod]
    pub fn s3(
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

    #[staticmethod]
    pub fn in_memory() -> PyResult<Self> {
        let storage = icechunk::storage::new_in_memory_storage()
            .map_err(PyIcechunkStoreError::StorageError)?;

        Ok(PyStorage(storage))
    }

    #[staticmethod]
    pub fn local_filesystem(path: PathBuf) -> PyResult<Self> {
        let storage = icechunk::storage::new_local_filesystem_storage(&path)
            .map_err(PyIcechunkStoreError::StorageError)?;

        Ok(PyStorage(storage))
    }
}
