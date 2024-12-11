use std::path::PathBuf;

use icechunk::{
    storage::s3::{S3ClientOptions, S3Config, S3Credentials, StaticS3Credentials},
    zarr::StorageConfig,
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

#[pyclass(name = "StorageConfig")]
pub struct PyStorageConfig(StorageConfig);

#[pymethods]
impl PyStorageConfig {
    #[classmethod]
    #[pyo3(signature = (prefix = None))]
    fn memory(_cls: &Bound<'_, PyType>, prefix: Option<String>) -> Self {
        Self(StorageConfig::InMemory { prefix })
    }

    #[classmethod]
    fn filesystem(_cls: &Bound<'_, PyType>, root: PathBuf) -> Self {
        Self(StorageConfig::LocalFileSystem { root })
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
        let config: S3ClientOptions = S3ClientOptions {
            region,
            endpoint: endpoint_url,
            allow_http,
            credentials: mk_credentials(None, false),
        };
        Self(StorageConfig::S3ObjectStore { bucket, prefix, config: Some(config) })
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
        let config = S3ClientOptions {
            region,
            endpoint: endpoint_url,
            allow_http,
            credentials: credentials.into(),
        };
        Self(StorageConfig::S3ObjectStore { bucket, prefix, config: Some(config) })
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
        let config = S3ClientOptions {
            region,
            endpoint: endpoint_url,
            allow_http,
            credentials: mk_credentials(None, true),
        };
        Self(StorageConfig::S3ObjectStore { bucket, prefix, config: Some(config) })
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

impl From<PyStorageConfig> for StorageConfig {
    fn from(storage: PyStorageConfig) -> Self {
        storage.0
    }
}

impl From<&PyStorageConfig> for StorageConfig {
    fn from(storage: &PyStorageConfig) -> Self {
        storage.0.clone()
    }
}

impl AsRef<StorageConfig> for PyStorageConfig {
    fn as_ref(&self) -> &StorageConfig {
        &self.0
    }
}

#[pyclass(name = "VirtualRefConfig")]
#[derive(Clone, Debug)]
pub enum PyVirtualRefConfig {
    S3 {
        credentials: Option<PyS3Credentials>,
        endpoint_url: Option<String>,
        allow_http: Option<bool>,
        region: Option<String>,
        anon: bool,
    },
}

#[pymethods]
impl PyVirtualRefConfig {
    #[classmethod]
    fn s3_from_env(_cls: &Bound<'_, PyType>) -> Self {
        PyVirtualRefConfig::S3 {
            credentials: None,
            endpoint_url: None,
            allow_http: None,
            region: None,
            anon: false,
        }
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
        PyVirtualRefConfig::S3 {
            credentials: Some(credentials),
            endpoint_url,
            allow_http,
            region,
            anon: anon.unwrap_or(false),
        }
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
        PyVirtualRefConfig::S3 {
            credentials: None,
            endpoint_url,
            allow_http,
            region,
            anon: true,
        }
    }
}
