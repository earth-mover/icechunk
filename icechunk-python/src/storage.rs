use std::{path::PathBuf, sync::Arc};

use icechunk::{
    storage::s3::{
        S3ClientOptions, S3Config, S3Credentials, S3Storage, StaticS3Credentials,
    },
    ObjectStorage, Repository, Storage, StorageError,
};
use pyo3::{prelude::*, types::PyType};

#[pyclass(name = "S3Credentials")]
#[derive(Clone, Debug)]
pub struct PyS3Credentials {
    #[pyo3(get, set)]
    access_key_id: String,
    #[pyo3(get, set)]
    secret_access_key: String,
    #[pyo3(get, set)]
    session_token: Option<String>,
}

impl From<&PyS3Credentials> for StaticS3Credentials {
    fn from(credentials: &PyS3Credentials) -> Self {
        StaticS3Credentials {
            access_key_id: credentials.access_key_id.clone(),
            secret_access_key: credentials.secret_access_key.clone(),
            session_token: credentials.session_token.clone(),
        }
    }
}

impl From<PyS3Credentials> for StaticS3Credentials {
    fn from(credentials: PyS3Credentials) -> Self {
        StaticS3Credentials {
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.secret_access_key,
            session_token: credentials.session_token,
        }
    }
}

impl From<PyS3Credentials> for S3Credentials {
    fn from(value: PyS3Credentials) -> Self {
        S3Credentials::Static(value.into())
    }
}

#[pymethods]
impl PyS3Credentials {
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
        PyS3Credentials { access_key_id, secret_access_key, session_token }
    }
}

#[pyclass(name = "S3ClientOptions")]
#[derive(Clone, Debug)]
pub struct PyS3ClientOptions {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub credentials: S3Credentials,
    pub allow_http: bool,
}

impl From<&PyS3ClientOptions> for S3ClientOptions {
    fn from(options: &PyS3ClientOptions) -> Self {
        S3ClientOptions {
            region: options.region.clone(),
            endpoint: options.endpoint.clone(),
            credentials: options.credentials.clone(),
            allow_http: options.allow_http,
        }
    }
}

#[pyclass(name = "S3Config")]
#[derive(Clone, Debug)]
pub struct PyS3Config {
    pub bucket: String,
    pub prefix: String,
    pub config: Option<PyS3ClientOptions>,
}

impl From<&PyS3Config> for S3Config {
    fn from(config: &PyS3Config) -> Self {
        S3Config {
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            options: config.config.as_ref().map(|c| c.into()),
        }
    }
}

#[pyclass(name = "StorageConfig")]
#[derive(Clone, Debug)]
pub enum PyStorageConfig {
    InMemory { prefix: Option<String> },
    LocalFileSystem { root: PathBuf },
    ObjectStore { url: String, options: Vec<(String, String)> },
    S3(PyS3Config),
}

#[pymethods]
impl PyStorageConfig {
    #[classmethod]
    #[pyo3(signature = (prefix = None))]
    fn memory(_cls: &Bound<'_, PyType>, prefix: Option<String>) -> Self {
        Self::InMemory { prefix }
    }

    #[classmethod]
    fn filesystem(_cls: &Bound<'_, PyType>, root: PathBuf) -> Self {
        Self::LocalFileSystem { root }
    }

    #[classmethod]
    fn object_store(
        _cls: &Bound<'_, PyType>,
        url: String,
        options: Vec<(String, String)>,
    ) -> Self {
        Self::ObjectStore { url, options }
    }

    #[classmethod]
    #[pyo3(signature = (
        bucket,
        prefix,
        endpoint_url = None,
        allow_http = false,
        region = None,
    ))]
    fn s3_from_env(
        _cls: &Bound<'_, PyType>,
        bucket: String,
        prefix: String,
        endpoint_url: Option<String>,
        allow_http: bool,
        region: Option<String>,
    ) -> Self {
        let config = PyS3ClientOptions {
            region,
            endpoint: endpoint_url,
            allow_http,
            credentials: mk_credentials(None, false),
        };
        Self::S3(PyS3Config { bucket, prefix, config: Some(config) })
    }

    #[classmethod]
    #[pyo3(signature = (
        bucket,
        prefix,
        credentials,
        endpoint_url = None,
        allow_http = false,
        region = None,
    ))]
    fn s3_from_config(
        _cls: &Bound<'_, PyType>,
        bucket: String,
        prefix: String,
        credentials: PyS3Credentials,
        endpoint_url: Option<String>,
        allow_http: bool,
        region: Option<String>,
    ) -> Self {
        let config = PyS3ClientOptions {
            region,
            endpoint: endpoint_url,
            allow_http,
            credentials: mk_credentials(Some(&credentials), false),
        };
        Self::S3(PyS3Config { bucket, prefix, config: Some(config) })
    }

    #[classmethod]
    #[pyo3(signature = (
        bucket,
        prefix,
        endpoint_url = None,
        allow_http = false,
        region = None,
    ))]
    fn s3_anonymous(
        _cls: &Bound<'_, PyType>,
        bucket: String,
        prefix: String,
        endpoint_url: Option<String>,
        allow_http: bool,
        region: Option<String>,
    ) -> Self {
        let config = PyS3ClientOptions {
            region,
            endpoint: endpoint_url,
            allow_http,
            credentials: mk_credentials(None, true),
        };
        Self::S3(PyS3Config { bucket, prefix, config: Some(config) })
    }
}

impl PyStorageConfig {
    pub async fn create_storage(&self) -> Result<Arc<dyn Storage>, StorageError> {
        let storage: Arc<dyn Storage> = match self {
            PyStorageConfig::InMemory { prefix } => {
                let storage: Arc<dyn Storage> =
                    Arc::new(ObjectStorage::new_in_memory_store(prefix.clone())?);
                Ok::<_, StorageError>(storage)
            }
            PyStorageConfig::LocalFileSystem { root } => {
                let storage: Arc<dyn Storage> =
                    Arc::new(ObjectStorage::new_local_store(root.clone().as_path())?);
                Ok(storage)
            }
            PyStorageConfig::ObjectStore { url, options } => {
                let storage: Arc<dyn Storage> =
                    Arc::new(ObjectStorage::from_url(url, options.clone())?);
                Ok(storage)
            }
            PyStorageConfig::S3(config) => {
                let config = config.into();
                let storage: Arc<dyn Storage> =
                    Arc::new(S3Storage::new_s3_store(&config).await?);
                Ok(storage)
            }
        }?;

        // default memory cached storage for usage in python
        let storage = Repository::add_in_mem_asset_caching(storage);
        Ok(storage)
    }
}

fn mk_credentials(config: Option<&PyS3Credentials>, anon: bool) -> S3Credentials {
    if anon {
        S3Credentials::Anonymous
    } else {
        match config {
            None => S3Credentials::FromEnv,
            Some(credentials) => S3Credentials::Static(credentials.into()),
        }
    }
}

#[pyclass(name = "VirtualRefConfig")]
#[derive(Clone, Debug)]
pub enum PyVirtualRefConfig {
    S3(PyS3ClientOptions),
}

#[pymethods]
impl PyVirtualRefConfig {
    #[classmethod]
    fn s3_from_env(_cls: &Bound<'_, PyType>) -> Self {
        PyVirtualRefConfig::S3(PyS3ClientOptions {
            region: None,
            endpoint: None,
            credentials: S3Credentials::FromEnv,
            allow_http: false,
        })
    }

    #[classmethod]
    #[pyo3(signature = (
        credentials,
        endpoint_url = None,
        allow_http = None,
        region = None,
        anon = None,
    ))]
    fn s3_from_config(
        _cls: &Bound<'_, PyType>,
        credentials: PyS3Credentials,
        endpoint_url: Option<String>,
        allow_http: Option<bool>,
        region: Option<String>,
        anon: Option<bool>,
    ) -> Self {
        PyVirtualRefConfig::S3(PyS3ClientOptions {
            region,
            endpoint: endpoint_url,
            credentials: mk_credentials(Some(&credentials), anon.unwrap_or(false)),
            allow_http: allow_http.unwrap_or(false),
        })
    }

    #[classmethod]
    #[pyo3(signature = (
        endpoint_url = None,
        allow_http = None,
        region = None,
    ))]
    fn s3_anonymous(
        _cls: &Bound<'_, PyType>,
        endpoint_url: Option<String>,
        allow_http: Option<bool>,
        region: Option<String>,
    ) -> Self {
        PyVirtualRefConfig::S3(PyS3ClientOptions {
            region,
            endpoint: endpoint_url,
            credentials: S3Credentials::Anonymous,
            allow_http: allow_http.unwrap_or(false),
        })
    }
}

// impl From<&PyVirtualRefConfig> for ObjectStoreVirtualChunkResolverConfig {
//     fn from(config: &PyVirtualRefConfig) -> Self {
//         match config {
//             PyVirtualRefConfig::S3(options) => {
//                 ObjectStoreVirtualChunkResolverConfig::S3(options.into())
//             }
//         }
//     }
// }
