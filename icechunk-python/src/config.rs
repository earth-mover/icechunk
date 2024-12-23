use std::{path::PathBuf, sync::Arc};

use icechunk::{
    config::{Credentials, S3CompatibleOptions, StaticCredentials},
    ObjectStoreConfig, RepositoryConfig, Storage,
};
use pyo3::{pyclass, pyfunction, pymethods, PyResult};

use crate::errors::PyIcechunkStoreError;

#[pyclass(name = "StaticCredentials")]
#[derive(Clone, Debug)]
pub struct PyStaticCredentials {
    #[pyo3(get, set)]
    access_key_id: String,
    #[pyo3(get, set)]
    secret_access_key: String,
    #[pyo3(get, set)]
    session_token: Option<String>,
}

impl From<&PyStaticCredentials> for StaticCredentials {
    fn from(credentials: &PyStaticCredentials) -> Self {
        StaticCredentials {
            access_key_id: credentials.access_key_id.clone(),
            secret_access_key: credentials.secret_access_key.clone(),
            session_token: credentials.session_token.clone(),
        }
    }
}

impl From<PyStaticCredentials> for StaticCredentials {
    fn from(credentials: PyStaticCredentials) -> Self {
        StaticCredentials {
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.secret_access_key,
            session_token: credentials.session_token,
        }
    }
}

// impl From<PyStaticCredentials> for S3Credentials {
//     fn from(value: PyS3Credentials) -> Self {
//         S3Credentials::Static(value.into())
//     }
// }

#[pymethods]
impl PyStaticCredentials {
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

#[pyclass(name = "Credentials")]
#[derive(Clone, Debug)]
pub enum PyCredentials {
    FromEnv(),
    DontSign(),
    Static(PyStaticCredentials),
}

impl From<PyCredentials> for Credentials {
    fn from(credentials: PyCredentials) -> Self {
        match credentials {
            PyCredentials::FromEnv() => Credentials::FromEnv,
            PyCredentials::DontSign() => Credentials::DontSign,
            PyCredentials::Static(creds) => Credentials::Static(creds.into()),
        }
    }
}

#[pyclass(name = "S3CompatibleOptions")]
#[derive(Clone, Debug)]
pub struct PyS3CompatibleOptions {
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
impl PyS3CompatibleOptions {
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

impl From<PyS3CompatibleOptions> for S3CompatibleOptions {
    fn from(options: PyS3CompatibleOptions) -> Self {
        S3CompatibleOptions {
            region: options.region.clone(),
            endpoint_url: options.endpoint_url.clone(),
            allow_http: options.allow_http,
            anonymous: options.anonymous,
        }
    }
}

#[pyclass(name = "ObjectStoreConfig")]
#[derive(Clone, Debug)]
pub enum PyObjectStoreConfig {
    InMemory(),
    LocalFileSystem(PathBuf),
    S3Compatible(PyS3CompatibleOptions),
    S3(PyS3CompatibleOptions),
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

#[pyclass(name = "RepositoryConfig")]
#[derive(Clone, Debug)]
pub struct PyRepositoryConfig {
    #[pyo3(get, set)]
    pub inline_chunk_threshold_bytes: u16,
    #[pyo3(get, set)]
    pub unsafe_overwrite_refs: bool,
    //pub virtual_chunk_containers: Vec<VirtualChunkContainer>,
}

impl From<PyRepositoryConfig> for RepositoryConfig {
    fn from(value: PyRepositoryConfig) -> Self {
        Self {
            inline_chunk_threshold_bytes: value.inline_chunk_threshold_bytes,
            unsafe_overwrite_refs: value.unsafe_overwrite_refs,
            // FIXME:
            virtual_chunk_containers: vec![],
        }
    }
}

impl From<RepositoryConfig> for PyRepositoryConfig {
    fn from(value: RepositoryConfig) -> Self {
        Self {
            inline_chunk_threshold_bytes: value.inline_chunk_threshold_bytes,
            unsafe_overwrite_refs: value.unsafe_overwrite_refs,
            // FIXME:
            //virtual_chunk_containers: vec![],
        }
    }
}

#[pymethods]
impl PyRepositoryConfig {
    #[staticmethod]
    fn default() -> Self {
        RepositoryConfig::default().into()
    }
}

#[pyclass(name = "Storage")]
#[derive(Clone, Debug)]
pub struct PyStorage(pub Arc<dyn Storage + Send + Sync>);

#[pyfunction]
#[pyo3(signature = ( config, bucket=None, prefix=None, credentials=None))]
pub(crate) fn make_storage(
    config: PyObjectStoreConfig,
    bucket: Option<String>,
    prefix: Option<String>,
    credentials: Option<PyCredentials>,
) -> PyResult<PyStorage> {
    let storage = pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
        icechunk::storage::make_storage(
            config.into(),
            bucket,
            prefix,
            credentials.map(|cred| cred.into()),
        )
        .await
        .map_err(PyIcechunkStoreError::StorageError)
    })?;

    Ok(PyStorage(storage))
}
